"""Fixtures for xs3lerator integration tests.

Starts a moto S3 backend and the proxy binary for end-to-end testing.
"""

import hashlib
import os
import socket
import subprocess
import time
from pathlib import Path

import boto3
import pytest
import requests


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ── 1 MiB deterministic payload ────────────────────────────────────────────

PAYLOAD_1MB = bytes(range(256)) * 4096  # exactly 1 MiB


# ── Session-scoped fixtures ────────────────────────────────────────────────

@pytest.fixture(scope="session")
def moto_endpoint():
    """Start a moto_server process and return its base URL."""
    port = _free_port()
    proc = subprocess.Popen(
        ["moto_server", "-H", "127.0.0.1", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    endpoint = f"http://127.0.0.1:{port}"
    for _ in range(40):
        try:
            requests.get(endpoint, timeout=0.3)
            break
        except Exception:
            time.sleep(0.25)
    else:
        proc.terminate()
        pytest.fail("moto_server failed to start")
    yield endpoint
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture(scope="session")
def s3(moto_endpoint):
    """Boto3 S3 client connected to the moto endpoint."""
    return boto3.client(
        "s3",
        region_name="us-east-1",
        endpoint_url=moto_endpoint,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture(scope="session")
def bucket(s3):
    """Create and return a test bucket name."""
    name = "xs3lerator-test"
    s3.create_bucket(Bucket=name)
    return name


@pytest.fixture(scope="session")
def fixture_object(s3, bucket):
    """Upload a 1 MiB object with x-amz-meta-sha224 and return (key, payload, sha224)."""
    key = "fixture.bin"
    sha = hashlib.sha224(PAYLOAD_1MB).hexdigest()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=PAYLOAD_1MB,
        Metadata={"sha224": sha},
        ContentType="application/octet-stream",
    )
    return key, PAYLOAD_1MB, sha


@pytest.fixture(scope="session")
def fixture_no_sha(s3, bucket):
    """Upload an object WITHOUT x-amz-meta-sha224 to test fallback."""
    key = "no-sha.bin"
    payload = b"hello xs3lerator\n" * 600
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="text/plain",
    )
    return key, payload


@pytest.fixture(scope="session")
def fixture_large(s3, bucket):
    """Upload a 4 MiB object to exercise multi-chunk downloads."""
    key = "large.bin"
    payload = bytes(range(256)) * (4 * 4096)  # 4 MiB
    sha = hashlib.sha224(payload).hexdigest()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        Metadata={"sha224": sha},
        ContentType="application/octet-stream",
    )
    return key, payload, sha


@pytest.fixture(scope="session")
def proxy_binary():
    """Build the proxy in debug mode and return the binary path."""
    root = Path(__file__).resolve().parents[2]
    subprocess.run(["cargo", "build", "--quiet"], cwd=root, check=True)
    return root / "target" / "debug" / "xs3lerator"


@pytest.fixture(scope="session")
def proxy(
    proxy_binary,
    moto_endpoint,
    bucket,
    fixture_object,
    fixture_no_sha,
    fixture_large,
    tmp_path_factory,
):
    """Start the proxy binary and return its base URL."""
    cache_dir = tmp_path_factory.mktemp("cache")
    port = _free_port()
    env = os.environ.copy()
    env.update(
        {
            "RUST_LOG": "xs3lerator=debug,tower_http=debug",
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "XS3_PORT": str(port),
            "XS3_BUCKET": bucket,
            "XS3_CACHE_DIR": str(cache_dir),
            "XS3_MAX_CACHE_SIZE": "1GiB",
            "XS3_S3_ENDPOINT_URL": moto_endpoint,
            "XS3_S3_FORCE_PATH_STYLE": "true",
            "XS3_GC_INTERVAL_SECONDS": "2",
            "XS3_MAX_CONCURRENCY": "4",
            "XS3_MIN_CHUNK_SIZE": "64KiB",
        }
    )
    proc = subprocess.Popen(
        [str(proxy_binary)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    base = f"http://127.0.0.1:{port}"
    key = fixture_object[0]
    for _ in range(80):
        try:
            requests.head(f"{base}/{key}", timeout=0.3)
            break
        except Exception:
            time.sleep(0.25)
    else:
        proc.terminate()
        stderr = ""
        if proc.stderr:
            stderr = proc.stderr.read().decode(errors="replace")[:2000]
        pytest.fail(f"proxy failed to start: {stderr}")
    yield base
    proc.terminate()
    proc.wait(timeout=10)
