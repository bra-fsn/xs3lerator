"""End-to-end integration tests for xs3lerator.

Requires LocalStack (S3) and the xs3lerator binary.
Fixtures are defined in conftest.py.
"""

import time
from concurrent.futures import ThreadPoolExecutor

import requests

from test_server import generate_payload
from conftest import (
    DATA_PREFIX,
    MAP_PREFIX,
    seed_cached_object,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SMALL = 1_000_000          # 1 MB  (single chunk at 5 MiB min)
MEDIUM = 12_000_000        # 12 MB (2-3 chunks at 5 MiB min)
LARGE = 30_000_000         # 30 MB (6 chunks)
S3_UPLOAD_SETTLE = 5.0     # seconds to wait for async S3 upload


# ── Basic functionality ───────────────────────────────────────────────────


class TestBasic:
    def test_healthz(self, proxy):
        r = requests.get(f"{proxy}/healthz", timeout=5)
        assert r.status_code == 200

    def test_method_not_allowed_post_without_header(self, proxy, test_bucket):
        """POST without X-Xs3lerator-Link-Manifest header returns 500."""
        r = requests.post(f"{proxy}/{test_bucket}/any-key", timeout=5)
        assert r.status_code == 500

    def test_method_not_allowed_put(self, proxy, test_bucket):
        r = requests.put(f"{proxy}/{test_bucket}/any-key", timeout=5)
        assert r.status_code == 405

    def test_method_not_allowed_delete(self, proxy, test_bucket):
        r = requests.delete(f"{proxy}/{test_bucket}/any-key", timeout=5)
        assert r.status_code == 405

    def test_missing_upstream_url(self, proxy, test_bucket):
        r = requests.get(f"{proxy}/{test_bucket}/some-key", timeout=5)
        assert r.status_code >= 400


# ── Cache miss → upstream fetch ───────────────────────────────────────────


class TestCacheMiss:
    def test_cache_miss_returns_upstream_data(self, proxy_get, unique_key):
        r = proxy_get(unique_key, f"/data/{SMALL}", cache_skip=True)
        assert r.status_code == 200
        assert len(r.content) == SMALL
        assert r.content == generate_payload(SMALL)

    def test_cache_miss_headers(self, proxy_get, unique_key):
        r = proxy_get(unique_key, f"/data/{SMALL}", cache_skip=True)
        assert r.headers["x-xs3lerator-cache-hit"] == "false"
        assert int(r.headers["x-xs3lerator-full-size"]) == SMALL

    def test_cache_miss_forwards_content_type(self, proxy_get, unique_key):
        r = proxy_get(unique_key, f"/data/{SMALL}", cache_skip=True)
        assert "application/octet-stream" in r.headers.get("content-type", "")

    def test_cache_miss_forwards_etag(self, proxy_get, unique_key):
        r = proxy_get(unique_key, f"/data/{SMALL}", cache_skip=True)
        assert r.headers.get("etag") == f'"test-{SMALL}"'

    def test_cache_miss_uploads_to_s3(self, proxy_get, unique_key, s3_client, test_bucket):
        r = proxy_get(unique_key, f"/data/{SMALL}", cache_skip=True)
        assert r.status_code == 200
        time.sleep(S3_UPLOAD_SETTLE)

        manifest_key = f"{MAP_PREFIX}{unique_key}"
        obj = s3_client.get_object(Bucket=test_bucket, Key=manifest_key)
        manifest_data = obj["Body"].read()
        assert manifest_data[:4] == b"XS3M", "Manifest should start with XS3M magic"

        paginator = s3_client.get_paginator("list_objects_v2")
        data_keys = []
        for page in paginator.paginate(Bucket=test_bucket, Prefix=DATA_PREFIX):
            for o in page.get("Contents", []):
                data_keys.append(o["Key"])
        assert len(data_keys) > 0, "At least one data chunk should exist"


# ── Cache hit → S3 serve ─────────────────────────────────────────────────


class TestCacheHit:
    @pytest.fixture
    def cached_key(self, unique_key, s3_client, test_bucket):
        """Pre-populate S3 with a properly formatted manifest + chunks."""
        payload = generate_payload(SMALL)
        seed_cached_object(s3_client, test_bucket, unique_key, payload)
        return unique_key, payload

    def test_cache_hit_serves_from_s3(self, proxy_get, cached_key):
        key, payload = cached_key
        r = proxy_get(key, "/data/1", object_size=len(payload))
        assert r.status_code == 200
        assert r.content == payload

    def test_cache_hit_headers(self, proxy_get, cached_key):
        key, payload = cached_key
        r = proxy_get(key, "/data/1", object_size=len(payload))
        assert r.headers["x-xs3lerator-cache-hit"] == "true"
        assert int(r.headers["x-xs3lerator-full-size"]) == len(payload)

    def test_cache_hit_with_object_size_hint(self, proxy_get, cached_key):
        key, payload = cached_key
        r = proxy_get(key, "/data/1", object_size=len(payload))
        assert r.status_code == 200
        assert len(r.content) == len(payload)


# ── S3 fallback (miss on S3 → upstream) ──────────────────────────────────


class TestS3Fallback:
    def test_s3_miss_falls_back_to_upstream(self, proxy_get, unique_key):
        """Default (no cache_skip): S3 miss → upstream fetch."""
        r = proxy_get(unique_key, f"/data/{SMALL}")
        assert r.status_code == 200
        assert r.content == generate_payload(SMALL)
        assert r.headers["x-xs3lerator-cache-hit"] == "false"


# ── Range requests ────────────────────────────────────────────────────────


class TestRangeRequests:
    @pytest.fixture(scope="class")
    def cached_object(self, s3_client, test_bucket):
        key = f"test/range-{SMALL}"
        payload = generate_payload(SMALL)
        seed_cached_object(s3_client, test_bucket, key, payload)
        return key, payload

    def test_range_explicit(self, proxy_get, cached_object):
        key, payload = cached_object
        r = proxy_get(
            key, "/data/1",
            object_size=len(payload),
            range_header="bytes=100-199",
        )
        assert r.status_code == 206
        assert r.content == payload[100:200]
        assert "content-range" in r.headers

    def test_range_open_ended(self, proxy_get, cached_object):
        key, payload = cached_object
        offset = len(payload) - 500
        r = proxy_get(
            key, "/data/1",
            object_size=len(payload),
            range_header=f"bytes={offset}-",
        )
        assert r.status_code == 206
        assert r.content == payload[offset:]

    def test_range_suffix(self, proxy_get, cached_object):
        key, payload = cached_object
        r = proxy_get(
            key, "/data/1",
            object_size=len(payload),
            range_header="bytes=-256",
        )
        assert r.status_code == 206
        assert r.content == payload[-256:]

    def test_range_beyond_size_returns_416(self, proxy_get, cached_object):
        key, payload = cached_object
        start = len(payload) + 100
        r = proxy_get(
            key, "/data/1",
            object_size=len(payload),
            range_header=f"bytes={start}-",
        )
        assert r.status_code == 416


# ── Adaptive parallel / sequential fallback ──────────────────────────────


class TestAdaptiveDownload:
    def test_parallel_with_range_support(self, proxy_get, unique_key):
        """Upstream supports ranges → adaptive probe succeeds → parallel download."""
        r = proxy_get(unique_key, f"/data/{MEDIUM}", cache_skip=True)
        assert r.status_code == 200
        assert len(r.content) == MEDIUM
        assert r.content == generate_payload(MEDIUM)

    def test_sequential_fallback_on_range_reject(self, proxy_get, unique_key):
        """Upstream advertises Accept-Ranges but 403s actual ranges → sequential."""
        r = proxy_get(unique_key, f"/data/{MEDIUM}?ranges_lie", cache_skip=True)
        assert r.status_code == 200
        assert len(r.content) == MEDIUM
        assert r.content == generate_payload(MEDIUM)

    def test_no_ranges_header(self, proxy_get, unique_key):
        """Upstream doesn't advertise Accept-Ranges → sequential."""
        r = proxy_get(unique_key, f"/data/{MEDIUM}?no_ranges", cache_skip=True)
        assert r.status_code == 200
        assert len(r.content) == MEDIUM
        assert r.content == generate_payload(MEDIUM)

    def test_chunked_response(self, proxy_get, unique_key):
        """Upstream sends chunked encoding → sequential."""
        r = proxy_get(unique_key, f"/data/{SMALL}?chunked", cache_skip=True)
        assert r.status_code == 200
        assert len(r.content) == SMALL
        assert r.content == generate_payload(SMALL)


# ── Large file / multi-chunk ─────────────────────────────────────────────


class TestLargeFile:
    def test_large_file_full_download(self, proxy_get, unique_key):
        r = proxy_get(unique_key, f"/data/{LARGE}", cache_skip=True, timeout=60)
        assert r.status_code == 200
        assert len(r.content) == LARGE
        assert r.content == generate_payload(LARGE)

    def test_large_file_uploads_to_s3(self, proxy_get, unique_key, s3_client, test_bucket):
        r = proxy_get(unique_key, f"/data/{LARGE}", cache_skip=True, timeout=60)
        assert r.status_code == 200
        time.sleep(S3_UPLOAD_SETTLE * 2)

        manifest_key = f"{MAP_PREFIX}{unique_key}"
        obj = s3_client.get_object(Bucket=test_bucket, Key=manifest_key)
        manifest_data = obj["Body"].read()
        assert manifest_data[:4] == b"XS3M"

    def test_large_file_cache_hit_after_upload(
        self, proxy_get, s3_client, test_bucket
    ):
        key = "test/large-cache-hit"
        payload = generate_payload(LARGE)
        r1 = proxy_get(key, f"/data/{LARGE}", cache_skip=True, timeout=60)
        assert r1.status_code == 200
        time.sleep(S3_UPLOAD_SETTLE * 2)
        r2 = proxy_get(key, "/data/1", object_size=LARGE, timeout=60)
        assert r2.status_code == 200
        assert r2.headers["x-xs3lerator-cache-hit"] == "true"
        assert len(r2.content) == LARGE
        assert r2.content == payload


# ── Concurrent requests ──────────────────────────────────────────────────


class TestConcurrency:
    def test_concurrent_gets_same_key(self, proxy_get, s3_client, test_bucket):
        key = "test/concurrent-same"
        payload = generate_payload(SMALL)
        seed_cached_object(s3_client, test_bucket, key, payload)

        def fetch():
            return proxy_get(key, "/data/1", object_size=len(payload))

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(fetch) for _ in range(16)]
            results = [f.result() for f in futures]

        assert all(r.status_code == 200 for r in results)
        assert all(r.content == payload for r in results)

    def test_concurrent_range_gets(self, proxy_get, s3_client, test_bucket):
        key = "test/concurrent-ranges"
        payload = generate_payload(SMALL)
        seed_cached_object(s3_client, test_bucket, key, payload)

        def fetch_range(start, end):
            return proxy_get(
                key, "/data/1",
                object_size=len(payload),
                range_header=f"bytes={start}-{end}",
            )

        ranges = [(i * 1000, i * 1000 + 999) for i in range(10)]
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(fetch_range, s, e) for s, e in ranges]
            results = [f.result() for f in futures]

        for r, (s, e) in zip(results, ranges):
            assert r.status_code == 206
            assert r.content == payload[s : e + 1]

    def test_concurrent_cache_misses(self, proxy_get, unique_key):
        """Multiple concurrent requests for the same uncached object."""
        def fetch():
            return proxy_get(unique_key, f"/data/{SMALL}", cache_skip=True)

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(fetch) for _ in range(4)]
            results = [f.result() for f in futures]

        assert all(r.status_code == 200 for r in results)
        assert all(len(r.content) == SMALL for r in results)


# ── Header forwarding ────────────────────────────────────────────────────


class TestHeaders:
    def test_client_headers_forwarded_to_upstream(self, proxy_get, unique_key):
        """Custom client headers should reach the upstream."""
        r = proxy_get(
            unique_key,
            "/echo-headers",
            cache_skip=True,
            extra_headers={"X-Custom-Test": "hello123"},
        )
        assert r.status_code == 200
        echoed = r.json()
        echoed_lower = {k.lower(): v for k, v in echoed.items()}
        assert echoed_lower.get("x-custom-test") == "hello123"

    def test_contract_headers_not_forwarded(self, proxy_get, unique_key):
        """X-Xs3lerator-* headers should be stripped before forwarding upstream."""
        r = proxy_get(
            unique_key,
            "/echo-headers",
            cache_skip=True,
        )
        assert r.status_code == 200
        echoed = r.json()
        for key in echoed:
            assert not key.lower().startswith("x-xs3lerator"), (
                f"contract header {key} leaked to upstream"
            )

    def test_host_header_not_forwarded(self, proxy_get, unique_key):
        """The Host header should not be the proxy's host."""
        r = proxy_get(
            unique_key,
            "/echo-headers",
            cache_skip=True,
        )
        assert r.status_code == 200
        echoed = r.json()
        host = echoed.get("Host", echoed.get("host", ""))
        assert "xs3lerator" not in host.lower()


# ── Manifest alias (POST) ────────────────────────────────────────────────


class TestManifestAlias:
    def test_manifest_alias_creates_copy(
        self, proxy, proxy_get, unique_key, s3_client, test_bucket
    ):
        """POST with X-Xs3lerator-Link-Manifest creates an alias manifest."""
        payload = generate_payload(SMALL)
        r = proxy_get(unique_key, f"/data/{SMALL}", cache_skip=True)
        assert r.status_code == 200
        time.sleep(S3_UPLOAD_SETTLE)

        alias_key = f"{unique_key}-alias"
        resp = requests.post(
            f"{proxy}/{test_bucket}/{alias_key}",
            headers={"X-Xs3lerator-Link-Manifest": unique_key},
            timeout=30,
        )
        assert resp.status_code == 200

        alias_manifest_key = f"{MAP_PREFIX}{alias_key}"
        obj = s3_client.get_object(Bucket=test_bucket, Key=alias_manifest_key)
        assert obj["Body"].read()[:4] == b"XS3M"

    def test_manifest_alias_serves_same_content(
        self, proxy, proxy_get, unique_key, s3_client, test_bucket
    ):
        """Content served via an alias key should match the original."""
        payload = generate_payload(SMALL)
        r1 = proxy_get(unique_key, f"/data/{SMALL}", cache_skip=True)
        assert r1.status_code == 200
        time.sleep(S3_UPLOAD_SETTLE)

        alias_key = f"{unique_key}-alias2"
        resp = requests.post(
            f"{proxy}/{test_bucket}/{alias_key}",
            headers={"X-Xs3lerator-Link-Manifest": unique_key},
            timeout=30,
        )
        assert resp.status_code == 200

        r2 = proxy_get(alias_key, "/data/1", object_size=SMALL)
        assert r2.status_code == 200
        assert r2.content == payload


# ── Upstream error handling ──────────────────────────────────────────────


class TestErrors:
    def test_upstream_404(self, proxy_get, unique_key):
        r = proxy_get(unique_key, "/status/404", cache_skip=True)
        assert r.status_code >= 400

    def test_upstream_500(self, proxy_get, unique_key):
        r = proxy_get(unique_key, "/status/500", cache_skip=True)
        assert r.status_code >= 400
