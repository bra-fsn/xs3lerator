"""Fixtures for xs3lerator integration tests.

Requires Docker Compose: all services (LocalStack, FoundationDB,
xs3lerator) must be running.  The test server runs on the host and is
reached by the xs3lerator container via host.docker.internal.

Environment variables:
    PROXY_URL              xs3lerator base URL (required)
    LOCALSTACK_ENDPOINT    S3 endpoint        (default: http://localhost:4566)
    FDB_CLUSTER_FILE       FDB cluster file   (default: None, uses system default)
    TEST_SERVER_BIND_HOST  Bind address        (default: 0.0.0.0)
    TEST_SERVER_HOST       Host xs3lerator uses to reach the test server
                           (default: host.docker.internal)
"""

import os
import socket
import struct
import time
from urllib.parse import quote as url_quote

import boto3
import fdb
import msgpack
import pytest
import requests

from test_server import TestServer, generate_payload  # noqa: F401 (re-export)

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

PROXY_URL = os.environ.get("PROXY_URL", "http://localhost:8888")
LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
FDB_CLUSTER_FILE = os.environ.get("FDB_CLUSTER_FILE", None)
TEST_SERVER_BIND_HOST = os.environ.get("TEST_SERVER_BIND_HOST", "0.0.0.0")
TEST_SERVER_HOST = os.environ.get("TEST_SERVER_HOST", "host.docker.internal")
TEST_BUCKET = "xs3lerator-test"

MANIFEST_PREFIX = b'\x03'
SPLIT_VALUE_CHUNK = 90_000

fdb.api_version(730)


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
# FoundationDB helpers
# ---------------------------------------------------------------------------


def _manifest_key(cache_key: str) -> bytes:
    return MANIFEST_PREFIX + cache_key.encode("utf-8")


def fdb_get_manifest(db, cache_key: str) -> bytes | None:
    """Fetch a (possibly split) manifest from FDB. Returns raw bytes or None."""
    key = _manifest_key(cache_key)
    end_key = key + b'\xff'
    tr = db.create_transaction()
    result = list(tr.get_range(key, end_key))
    if not result:
        return None
    return b''.join(bytes(kv.value) for kv in result)


def fdb_put_manifest(db, cache_key: str, manifest_bytes: bytes):
    """Write a manifest to FDB with value splitting."""
    key = _manifest_key(cache_key)
    end_key = key + b'\xff'

    @fdb.transactional
    def do_put(tr):
        tr.clear_range(key, end_key)
        chunks = [manifest_bytes[i:i + SPLIT_VALUE_CHUNK]
                  for i in range(0, len(manifest_bytes), SPLIT_VALUE_CHUNK)]
        if len(chunks) <= 1:
            tr[key] = manifest_bytes
        else:
            tr[key] = chunks[0]
            for i, chunk in enumerate(chunks[1:]):
                tr[key + bytes([i])] = chunk

    do_put(db)


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
def fdb_database():
    """Return an FDB Database handle, skipping if not reachable."""
    try:
        db = fdb.open(FDB_CLUSTER_FILE)
        tr = db.create_transaction()
        tr.get(b'\x00').wait()
    except Exception:
        pytest.skip("FoundationDB not reachable")
    return db


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
        pass
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
def proxy(localstack_endpoint, fdb_database, test_bucket, test_server):
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
ID_LEN = 16  # UUID bytes


def _chunk_s3_key(chunk_id: bytes) -> str:
    """Compute the S3 key for a chunk under data/ with 4-level prefix hashing."""
    h = chunk_id.hex()
    return f"{DATA_PREFIX}{h[0]}/{h[1]}/{h[2]}/{h[3]}/{h}"


def build_manifest(payload: bytes, chunk_size: int = CHUNK_SIZE) -> tuple[bytes, list[tuple[bytes, bytes]]]:
    """Build a binary manifest and chunk list for a payload.

    Returns (manifest_bytes, [(chunk_id, chunk_data), ...]).
    """
    import uuid as _uuid
    total_size = len(payload)
    chunks = []
    offset = 0
    while offset < total_size:
        chunk_data = payload[offset:offset + chunk_size]
        chunk_id = _uuid.uuid4().bytes
        chunks.append((chunk_id, chunk_data))
        offset += chunk_size

    num_chunks = len(chunks)
    buf = bytearray()
    buf.extend(b"XS3M")       # magic
    buf.append(1)              # version
    buf.append(2)              # id_algo (UUID)
    buf.extend(struct.pack("<Q", chunk_size))
    buf.extend(struct.pack("<Q", total_size))
    buf.extend(struct.pack("<I", num_chunks))
    for chunk_id, _ in chunks:
        buf.extend(chunk_id)

    return bytes(buf), chunks


def seed_cached_object(
    s3_client,
    bucket: str,
    key: str,
    payload: bytes,
    chunk_size: int = CHUNK_SIZE,
    fdb_db=None,
):
    """Upload chunks to S3 and write manifest to FDB for cache-hit tests."""
    manifest_bytes, chunks = build_manifest(payload, chunk_size)

    for chunk_id, chunk_data in chunks:
        s3_key = _chunk_s3_key(chunk_id)
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=chunk_data)

    if fdb_db is not None:
        fdb_put_manifest(fdb_db, key, manifest_bytes)


@pytest.fixture(scope="session")
def seed_object(s3_client, test_bucket, fdb_database):
    """Factory fixture for seeding content-addressed objects."""
    def _seed(key: str, payload: bytes, chunk_size: int = CHUNK_SIZE):
        seed_cached_object(
            s3_client, test_bucket, key, payload, chunk_size,
            fdb_db=fdb_database,
        )
    return _seed
