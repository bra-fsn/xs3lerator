"""Fixtures for xs3lerator integration tests.

Supports two modes:

  Docker Compose mode (CI or `docker compose up`):
    Set PROXY_URL, LOCALSTACK_ENDPOINT, and TEST_SERVER_HOST env vars.
    The test server binds to 0.0.0.0 so the container can reach it via
    host.docker.internal.

  Local mode (no Docker, default):
    Builds the Rust binary, starts LocalStack (must already be running),
    and launches xs3lerator directly.  Everything runs on localhost.

Environment variables:
    PROXY_URL              xs3lerator base URL (skip build/launch if set)
    LOCALSTACK_ENDPOINT    S3 endpoint        (default: http://localhost:4566)
    TEST_SERVER_BIND_HOST  Bind address        (default: 127.0.0.1)
    TEST_SERVER_HOST       Host xs3lerator uses to reach the test server
                           (default: 127.0.0.1, use host.docker.internal
                            when xs3lerator runs in Docker)
"""

import base64
import hashlib
import os
import socket
import struct
import subprocess
import time
from pathlib import Path

import boto3
import pytest
import requests

from test_server import TestServer, generate_payload  # noqa: F401 (re-export)

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

PROXY_URL = os.environ.get("PROXY_URL")  # None → local mode
LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
TEST_SERVER_BIND_HOST = os.environ.get("TEST_SERVER_BIND_HOST", "127.0.0.1")
TEST_SERVER_HOST = os.environ.get("TEST_SERVER_HOST", "127.0.0.1")
TEST_BUCKET = "xs3lerator-test"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_url(url: str, timeout: float = 20.0, interval: float = 0.3):
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            requests.get(url, timeout=1)
            return
        except Exception as e:
            last_err = e
            time.sleep(interval)
    raise RuntimeError(f"URL {url} not reachable after {timeout}s: {last_err}")


def encode_upstream_url(url: str) -> str:
    return base64.b64encode(url.encode()).decode()


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

    Binds to TEST_SERVER_BIND_HOST so Docker containers can reach it
    via host.docker.internal when running in compose mode.
    """
    server = TestServer(host=TEST_SERVER_BIND_HOST)
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope="session")
def test_server_external_url(test_server):
    """Base URL that xs3lerator should use to reach the test server.

    In Docker mode this uses host.docker.internal; locally it's 127.0.0.1.
    """
    return f"http://{TEST_SERVER_HOST}:{test_server.port}"


@pytest.fixture(scope="session")
def proxy_binary():
    """Build the proxy in debug mode and return the binary path.

    Skipped in Docker Compose mode where PROXY_URL is set.
    """
    if PROXY_URL:
        pytest.skip("PROXY_URL set — using external proxy")
    subprocess.run(
        ["cargo", "build", "--quiet"],
        cwd=PROJECT_ROOT,
        check=True,
        timeout=300,
    )
    return PROJECT_ROOT / "target" / "debug" / "xs3lerator"


@pytest.fixture(scope="session")
def proxy(
    proxy_binary,
    localstack_endpoint,
    test_bucket,
    test_server,
    tmp_path_factory,
):
    """Start xs3lerator and return its base URL.

    In Docker Compose mode (PROXY_URL set), returns the external URL directly.
    """
    if PROXY_URL:
        _wait_for_url(f"{PROXY_URL}/healthz", timeout=30)
        return PROXY_URL

    temp_dir = tmp_path_factory.mktemp("xs3-temp")
    port = _free_port()
    env = os.environ.copy()
    env.update({
        "RUST_LOG": "xs3lerator=debug",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_DEFAULT_REGION": "us-east-1",
        "XS3_PORT": str(port),
        "XS3_S3_ENDPOINT_URL": localstack_endpoint,
        "XS3_S3_FORCE_PATH_STYLE": "true",
        "XS3_S3_CONCURRENCY": "4",
        "XS3_HTTP_CONCURRENCY": "4",
        "XS3_MIN_CHUNK_SIZE": "5MiB",
        "XS3_TEMP_DIR": str(temp_dir),
    })
    proc = subprocess.Popen(
        [str(proxy_binary)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_url(f"{base_url}/healthz", timeout=20)
    except RuntimeError:
        proc.terminate()
        stderr = ""
        if proc.stderr:
            stderr = proc.stderr.read().decode(errors="replace")[:4000]
        pytest.fail(f"xs3lerator failed to start:\n{stderr}")

    yield base_url
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


# ---------------------------------------------------------------------------
# Per-test helpers available as fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def proxy_get(proxy, test_bucket, test_server_external_url):
    """Helper to make a GET to xs3lerator with proper contract headers.

    Usage:
        resp = proxy_get("my-key", "/data/1024", cache_skip=True)
    """
    def _get(
        s3_key: str,
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
            "X-Xs3lerator-Upstream-Url": encode_upstream_url(upstream_url),
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
            f"{proxy}/{test_bucket}/{s3_key}",
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
MAP_PREFIX = "_map/"
CHUNK_SIZE = 5 * 1024 * 1024  # 5 MiB -- matches XS3_MIN_CHUNK_SIZE in proxy fixture


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


def seed_cached_object(s3_client, bucket: str, key: str, payload: bytes, chunk_size: int = CHUNK_SIZE):
    """Upload a properly formatted manifest + chunks to S3 for cache hit tests.

    xs3lerator builds cache_key as "{bucket}/{key}", so the manifest S3 key
    is "_map/{bucket}/{key}" stored inside the same bucket.
    """
    manifest_bytes, chunks = build_manifest(payload, chunk_size)

    for chunk_hash, chunk_data in chunks:
        s3_key = _chunk_s3_key(chunk_hash)
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=chunk_data)

    manifest_key = f"{MAP_PREFIX}{bucket}/{key}"
    s3_client.put_object(Bucket=bucket, Key=manifest_key, Body=manifest_bytes)


@pytest.fixture(scope="session")
def seed_object(s3_client, test_bucket):
    """Factory fixture for seeding content-addressed objects."""
    def _seed(key: str, payload: bytes, chunk_size: int = CHUNK_SIZE):
        seed_cached_object(s3_client, test_bucket, key, payload, chunk_size)
    return _seed
