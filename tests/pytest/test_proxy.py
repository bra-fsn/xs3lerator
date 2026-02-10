"""End-to-end integration tests for xs3lerator.

These tests require a running moto_server and the proxy binary.
Fixtures are defined in conftest.py.
"""

from concurrent.futures import ThreadPoolExecutor

import requests


# ── HEAD ───────────────────────────────────────────────────────────────────


def test_head_returns_metadata(proxy, fixture_object):
    key, payload, _sha = fixture_object
    r = requests.head(f"{proxy}/{key}", timeout=5)
    assert r.status_code == 200
    assert r.headers["server"].startswith("xs3lerator")
    assert int(r.headers["content-length"]) == len(payload)
    assert r.headers["accept-ranges"] == "bytes"
    assert r.headers.get("content-type") == "application/octet-stream"


def test_head_nonexistent_returns_404(proxy):
    r = requests.head(f"{proxy}/does-not-exist", timeout=5)
    assert r.status_code == 404


# ── Full GET ───────────────────────────────────────────────────────────────


def test_get_full_object(proxy, fixture_object):
    key, payload, _sha = fixture_object
    r = requests.get(f"{proxy}/{key}", timeout=10)
    assert r.status_code == 200
    assert r.content == payload


def test_get_cache_hit(proxy, fixture_object):
    key, payload, _sha = fixture_object
    # First request populates cache
    r1 = requests.get(f"{proxy}/{key}", timeout=10)
    assert r1.status_code == 200
    # Second request should be served from cache
    r2 = requests.get(f"{proxy}/{key}", timeout=10)
    assert r2.status_code == 200
    assert r2.content == payload


def test_get_nonexistent_returns_404(proxy):
    r = requests.get(f"{proxy}/nonexistent-key", timeout=5)
    assert r.status_code == 404


# ── Range requests ─────────────────────────────────────────────────────────


def test_range_explicit(proxy, fixture_object):
    key, payload, _sha = fixture_object
    r = requests.get(
        f"{proxy}/{key}",
        headers={"Range": "bytes=100-199"},
        timeout=10,
    )
    assert r.status_code == 206
    assert r.content == payload[100:200]
    assert "content-range" in r.headers


def test_range_open_ended(proxy, fixture_object):
    key, payload, _sha = fixture_object
    offset = len(payload) - 500
    r = requests.get(
        f"{proxy}/{key}",
        headers={"Range": f"bytes={offset}-"},
        timeout=10,
    )
    assert r.status_code == 206
    assert r.content == payload[offset:]


def test_range_suffix(proxy, fixture_object):
    key, payload, _sha = fixture_object
    r = requests.get(
        f"{proxy}/{key}",
        headers={"Range": "bytes=-256"},
        timeout=10,
    )
    assert r.status_code == 206
    assert r.content == payload[-256:]


def test_range_beyond_object_returns_416(proxy, fixture_object):
    key, payload, _sha = fixture_object
    start = len(payload) + 100
    r = requests.get(
        f"{proxy}/{key}",
        headers={"Range": f"bytes={start}-"},
        timeout=10,
    )
    assert r.status_code == 416


# ── Concurrency ────────────────────────────────────────────────────────────


def test_concurrent_gets(proxy, fixture_object):
    key, payload, _sha = fixture_object

    def fetch():
        return requests.get(f"{proxy}/{key}", timeout=15)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(fetch) for _ in range(16)]
        results = [f.result() for f in futures]

    assert all(r.status_code == 200 for r in results)
    assert all(r.content == payload for r in results)


def test_concurrent_range_gets(proxy, fixture_object):
    key, payload, _sha = fixture_object

    def fetch_range(start, end):
        return requests.get(
            f"{proxy}/{key}",
            headers={"Range": f"bytes={start}-{end}"},
            timeout=15,
        )

    ranges = [(i * 1000, i * 1000 + 999) for i in range(10)]
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(fetch_range, s, e) for s, e in ranges]
        results = [f.result() for f in futures]

    for r, (s, e) in zip(results, ranges):
        assert r.status_code == 206
        assert r.content == payload[s : e + 1]


# ── SHA-224 fallback ───────────────────────────────────────────────────────


def test_no_sha224_header_uses_key_hash(proxy, fixture_no_sha):
    key, payload = fixture_no_sha
    r = requests.get(f"{proxy}/{key}", timeout=10)
    assert r.status_code == 200
    assert r.content == payload

    # Second request should hit cache even without sha224 metadata
    r2 = requests.get(f"{proxy}/{key}", timeout=10)
    assert r2.status_code == 200
    assert r2.content == payload


# ── Larger object (multi-chunk) ────────────────────────────────────────────


def test_large_object_full_get(proxy, fixture_large):
    key, payload, _sha = fixture_large
    r = requests.get(f"{proxy}/{key}", timeout=30)
    assert r.status_code == 200
    assert r.content == payload


def test_large_object_range(proxy, fixture_large):
    key, payload, _sha = fixture_large
    mid = len(payload) // 2
    r = requests.get(
        f"{proxy}/{key}",
        headers={"Range": f"bytes={mid}-{mid + 9999}"},
        timeout=30,
    )
    assert r.status_code == 206
    assert r.content == payload[mid : mid + 10000]
