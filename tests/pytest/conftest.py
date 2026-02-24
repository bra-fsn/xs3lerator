"""Fixtures for xs3lerator integration tests.

Requires Docker Compose: all services (LocalStack, Elasticsearch, mount-s3,
xs3lerator) must be running.  The test server runs on the host and is
reached by the xs3lerator container via host.docker.internal.

Environment variables:
    PROXY_URL              xs3lerator base URL (required)
    LOCALSTACK_ENDPOINT    S3 endpoint        (default: http://localhost:4566)
    ELASTICSEARCH_URL      ES endpoint        (default: http://localhost:9200)
    TEST_SERVER_BIND_HOST  Bind address        (default: 0.0.0.0)
    TEST_SERVER_HOST       Host xs3lerator uses to reach the test server
                           (default: host.docker.internal)
"""

import hashlib
import os
import socket
import struct
import time
from urllib.parse import quote as url_quote

import boto3
import pytest
import requests

from test_server import TestServer, generate_payload  # noqa: F401 (re-export)

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

PROXY_URL = os.environ.get("PROXY_URL", "http://localhost:8888")
LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
ELASTICSEARCH_URL = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
ES_MANIFEST_INDEX = os.environ.get("XS3_ELASTICSEARCH_MANIFEST_INDEX", "xs3_manifests")
TEST_SERVER_BIND_HOST = os.environ.get("TEST_SERVER_BIND_HOST", "0.0.0.0")
TEST_SERVER_HOST = os.environ.get("TEST_SERVER_HOST", "host.docker.internal")
TEST_BUCKET = "xs3lerator-test"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_url(url: str, timeout: float = 30.0, interval: float = 0.5):
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            requests.get(url, timeout=2)
            return
        except Exception as e:
            last_err = e
            time.sleep(interval)
    raise RuntimeError(f"URL {url} not reachable after {timeout}s: {last_err}")


# ---------------------------------------------------------------------------
# Elasticsearch helpers
# ---------------------------------------------------------------------------


def _es_doc_url(es_url: str, index: str, doc_id: str) -> str:
    encoded_id = url_quote(doc_id, safe="")
    return f"{es_url}/{index}/_doc/{encoded_id}"


def es_get_manifest_b64(es_url: str, index: str, doc_id: str) -> str | None:
    """Fetch manifest_b64 from Elasticsearch, return None if not found."""
    url = _es_doc_url(es_url, index, doc_id)
    resp = requests.get(url, timeout=5)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    data = resp.json()
    if not data.get("found"):
        return None
    return data["_source"]["manifest_b64"]


def es_put_manifest(es_url: str, index: str, doc_id: str, manifest_bytes: bytes):
    """Write a manifest to Elasticsearch."""
    url = _es_doc_url(es_url, index, doc_id)
    b64 = base64.b64encode(manifest_bytes).decode()
    resp = requests.put(
        url,
        json={"manifest_b64": b64},
        headers={"Content-Type": "application/json"},
        timeout=5,
    )
    resp.raise_for_status()
    requests.post(f"{es_url}/{index}/_refresh", timeout=5)


def es_create_index(es_url: str, index: str):
    """Create the ES manifest index if it doesn't exist."""
    url = f"{es_url}/{index}"
    body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "1s",
        },
        "mappings": {
            "dynamic": False,
            "properties": {
                "manifest_b64": {"type": "keyword", "index": False, "doc_values": False}
            },
        },
    }
    resp = requests.put(url, json=body, timeout=5)
    if resp.status_code == 400 and "resource_already_exists_exception" in resp.text:
        return
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def localstack_endpoint():
    """Return the LocalStack endpoint, skipping if not reachable."""
    try:
        resp = requests.get(f"{LOCALSTACK_ENDPOINT}/_localstack/health", timeout=3)
        resp.raise_for_status()
    except Exception:
        pytest.skip(f"LocalStack not reachable at {LOCALSTACK_ENDPOINT}")
    return LOCALSTACK_ENDPOINT


@pytest.fixture(scope="session")
def elasticsearch_url():
    """Return the Elasticsearch URL, skipping if not reachable."""
    try:
        resp = requests.get(ELASTICSEARCH_URL, timeout=3)
        resp.raise_for_status()
    except Exception:
        pytest.skip(f"Elasticsearch not reachable at {ELASTICSEARCH_URL}")
    es_create_index(ELASTICSEARCH_URL, ES_MANIFEST_INDEX)
    return ELASTICSEARCH_URL


@pytest.fixture(scope="session")
def s3_client(localstack_endpoint):
    """Boto3 S3 client connected to LocalStack."""
    return boto3.client(
        "s3",
        region_name="us-east-1",
        endpoint_url=localstack_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture(scope="session")
def test_bucket(s3_client):
    """Create the test bucket in LocalStack."""
    try:
        s3_client.create_bucket(Bucket=TEST_BUCKET)
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass
    except Exception:
        pass  # bucket may already exist
    return TEST_BUCKET


@pytest.fixture(scope="session")
def test_server():
    """Start the test HTTP server.

    Binds to 0.0.0.0 so the xs3lerator container can reach it via
    host.docker.internal.
    """
    server = TestServer(host=TEST_SERVER_BIND_HOST)
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope="session")
def test_server_external_url(test_server):
    """Base URL that xs3lerator should use to reach the test server."""
    return f"http://{TEST_SERVER_HOST}:{test_server.port}"


@pytest.fixture(scope="session")
def proxy(localstack_endpoint, elasticsearch_url, test_bucket, test_server):
    """Wait for xs3lerator to be ready and return its base URL."""
    _wait_for_url(f"{PROXY_URL}/healthz", timeout=60)
    return PROXY_URL


# ---------------------------------------------------------------------------
# Per-test helpers available as fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def proxy_get(proxy, test_server_external_url):
    """Helper to make a GET to xs3lerator with proper contract headers.

    Usage:
        resp = proxy_get("my-cache-key", "/data/1024", cache_skip=True)

    The upstream URL (test_server + upstream_path) is placed in the request
    path.  The cache key is sent in X-Xs3lerator-Cache-Key.
    """
    def _get(
        cache_key: str,
        upstream_path: str,
        *,
        cache_skip: bool = False,
        object_size: int | None = None,
        extra_headers: dict | None = None,
        range_header: str | None = None,
        timeout: int = 30,
    ):
        upstream_url = f"{test_server_external_url}{upstream_path}"
        headers = {
            "X-Xs3lerator-Cache-Key": cache_key,
        }
        if cache_skip:
            headers["X-Xs3lerator-Cache-Skip"] = "true"
        if object_size is not None:
            headers["X-Xs3lerator-Object-Size"] = str(object_size)
        if range_header:
            headers["Range"] = range_header
        if extra_headers:
            headers.update(extra_headers)
        return requests.get(
            f"{proxy}/{upstream_url}",
            headers=headers,
            timeout=timeout,
        )
    return _get


@pytest.fixture
def unique_key():
    """Generate a unique S3 key per test to avoid cross-test interference."""
    import uuid
    return f"test/{uuid.uuid4().hex}"


# ---------------------------------------------------------------------------
# Content-addressed storage helpers
# ---------------------------------------------------------------------------

DATA_PREFIX = "data/"
CHUNK_SIZE = 5 * 1024 * 1024  # 5 MiB -- matches XS3_CHUNK_SIZE in docker-compose


def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _chunk_s3_key(chunk_hash: bytes) -> str:
    """Compute the S3 key for a chunk under data/ with 4-level prefix hashing."""
    h = chunk_hash.hex()
    return f"{DATA_PREFIX}{h[0]}/{h[1]}/{h[2]}/{h[3]}/{h}"


def build_manifest(payload: bytes, chunk_size: int = CHUNK_SIZE) -> tuple[bytes, list[tuple[bytes, bytes]]]:
    """Build a binary manifest and chunk list for a payload.

    Returns (manifest_bytes, [(chunk_hash, chunk_data), ...]).
    """
    total_size = len(payload)
    chunks = []
    offset = 0
    while offset < total_size:
        chunk_data = payload[offset:offset + chunk_size]
        chunk_hash = _sha256(chunk_data)
        chunks.append((chunk_hash, chunk_data))
        offset += chunk_size

    num_chunks = len(chunks)
    buf = bytearray()
    buf.extend(b"XS3M")       # magic
    buf.append(1)              # version
    buf.append(1)              # hash_algo (SHA-256)
    buf.extend(struct.pack("<Q", chunk_size))
    buf.extend(struct.pack("<Q", total_size))
    buf.extend(struct.pack("<I", num_chunks))
    for chunk_hash, _ in chunks:
        buf.extend(chunk_hash)

    return bytes(buf), chunks


def seed_cached_object(
    s3_client,
    bucket: str,
    key: str,
    payload: bytes,
    chunk_size: int = CHUNK_SIZE,
    es_url: str = ELASTICSEARCH_URL,
    es_index: str = ES_MANIFEST_INDEX,
):
    """Upload chunks to S3 and write manifest to ES for cache-hit tests.

    mount-s3 bridges S3 to the xs3lerator filesystem, so writing chunks
    to S3 via boto3 is sufficient — they appear at the expected paths
    inside the xs3lerator container.
    """
    manifest_bytes, chunks = build_manifest(payload, chunk_size)

    for chunk_hash, chunk_data in chunks:
        s3_key = _chunk_s3_key(chunk_hash)
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=chunk_data)

    es_put_manifest(es_url, es_index, key, manifest_bytes)


@pytest.fixture(scope="session")
def seed_object(s3_client, test_bucket, elasticsearch_url):
    """Factory fixture for seeding content-addressed objects."""
    def _seed(key: str, payload: bytes, chunk_size: int = CHUNK_SIZE):
        seed_cached_object(
            s3_client, test_bucket, key, payload, chunk_size,
            es_url=elasticsearch_url, es_index=ES_MANIFEST_INDEX,
        )
    return _seed
