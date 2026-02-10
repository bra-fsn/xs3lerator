# xs3lerator

High-performance Rust HTTP proxy for Amazon S3 with a POSIX filesystem cache,
parallel chunked downloads, content-addressed storage, and background LRU
eviction.

## Features

- **Standard AWS authentication**: environment variables, `~/.aws` profiles,
  IAM instance profiles, ECS/EKS task roles — anything the official AWS SDK
  supports.
- **HTTP/1.1 and HTTP/2** downstream (cleartext, no TLS).
- **GET and HEAD** proxy requests — fully compliant HTTP range-request
  support (single ranges).
- **Content-addressed cache** keyed on `x-amz-meta-sha224` (falls back to
  SHA-224 of the S3 key when the header is absent).
- **Parallel multipart downloads** from S3 using configurable concurrency and
  chunk sizes, optimized based on S3 throughput benchmarks.
- **Stream-while-downloading**: downstream clients receive data as chunks
  arrive — no need to wait for the full object.
- **Download deduplication**: concurrent requests for the same object share a
  single upstream download.
- **Range-aware prioritization**: the first byte ranges a client needs are
  fetched first.
- **Background LRU eviction** based on file mtime — minimal memory overhead,
  no persistent in-memory index.
- **Graceful degradation**: if the cache directory becomes unwritable the
  proxy continues in passthrough mode.

## Build

```bash
cargo build --release
```

## Run

```bash
./target/release/xs3lerator \
  --bucket my-bucket \
  --cache-dir /data/xs3-cache \
  --max-cache-size 500GiB
```

## CLI Reference

Every option is also settable via an environment variable.

```text
OPTION                             ENV VAR                        DEFAULT
--bind-ip <IP>                     XS3_BIND_IP                    0.0.0.0
--port <PORT>                      XS3_PORT                       8080
--bucket <BUCKET>                  XS3_BUCKET                     (required)
--region <REGION>                  XS3_REGION                     (SDK default)
--s3-endpoint-url <URL>            XS3_S3_ENDPOINT_URL            (none)
--s3-force-path-style              XS3_S3_FORCE_PATH_STYLE        false
--cache-dir <PATH>                 XS3_CACHE_DIR                  (required)
--max-cache-size <SIZE>            XS3_MAX_CACHE_SIZE             (required)
--cache-hierarchy-level <N>        XS3_CACHE_HIERARCHY_LEVEL      4
--max-concurrency <N>              XS3_MAX_CONCURRENCY            32
--min-chunk-size <SIZE>            XS3_MIN_CHUNK_SIZE             1MiB
--max-chunk-size <SIZE>            XS3_MAX_CHUNK_SIZE             256MiB
--gc-interval-seconds <SECONDS>    XS3_GC_INTERVAL_SECONDS        15
--gc-watermark-percent <PERCENT>   XS3_GC_WATERMARK_PERCENT       95
--gc-target-percent <PERCENT>      XS3_GC_TARGET_PERCENT          90
```

Run `xs3lerator --help` to see the generated help with defaults.

## Request Behavior

### `HEAD /<key>`

1. Executes `HeadObject` on S3.
2. Returns the object metadata with `Server: xs3lerator/0.1.0` and
   `Accept-Ranges: bytes`.

### `GET /<key>`

1. Executes `HeadObject` to obtain metadata and the `x-amz-meta-sha224`
   content hash.
2. Computes the cache key (SHA-224 from metadata, or SHA-224 of the key
   path if the header is absent).
3. Checks the local cache:
   - **Hit** — verifies that the on-disk file size matches `Content-Length`.
     On match, streams the file directly and updates mtime for LRU. On
     mismatch, logs a warning, deletes the stale entry, and proceeds as a
     miss.
   - **Miss** — starts (or joins an existing) parallel download.
4. The download pre-allocates a temp file, fetches chunks via concurrent
   range-GET requests, writes them with `pwrite`, and streams completed
   chunks to waiting clients as they arrive.
5. On completion, the temp file is fsync'd and atomically renamed to the
   final cache path, ensuring no corrupt data is served after a power
   failure.

If the cache directory is not writable, the proxy falls back to direct S3
streaming without caching.

### Range Requests

Single-range `bytes=START-END`, `bytes=START-`, and `bytes=-SUFFIX` are
fully supported. The proxy responds with `206 Partial Content` and
`Content-Range`.  Multi-range requests are rejected with `416`.

## Cache Layout

For `x-amz-meta-sha224=e8b0356886eb5804f25ad799b3db28ca32ae0205a47639d6e7a0afea`
and hierarchy level 4:

```text
<cache-dir>/e/8/b/0/e8b0356886eb5804f25ad799b3db28ca32ae0205a47639d6e7a0afea
```

All hierarchy directories (16^level leaf dirs) are pre-created on startup.

## Why a Parallelized Proxy?

As of early 2026, a single HTTP stream to S3 inside the same region and
availability zone tops out at roughly **60 MiB/s** for the Standard storage
class and around **150 MiB/s** for the Express One Zone storage class
(directory buckets). These are hard per-connection limits imposed by S3's
internal architecture — they cannot be raised by tuning client buffers,
TCP windows, or instance types.

Any workload that needs to pull multi-gigabyte objects faster than a single
stream allows — ML model checkpoints, large datasets, container images,
build artifacts — must open **multiple parallel byte-range requests** and
reassemble the pieces. xs3lerator does this transparently: a single HTTP GET
from a client fans out into many concurrent range-GETs to S3, and completed
chunks are streamed back to the client as they arrive. With 32 streams the
proxy routinely sustains **~3 GiB/s** from S3 Standard; see the benchmarks
below.

## Download Strategy

The chunk planner adapts concurrency and chunk size to the object:

- Prefers an **8 MiB baseline** chunk size, aligned with S3's inferred
  internal erasure-coding stripe boundary (~8–32 MiB based on throughput
  benchmarks).
- Scales concurrency proportionally to file size — a 1 MiB file uses a
  single stream; a 10 GiB file uses up to `--max-concurrency` streams.
- Respects `--min-chunk-size` and `--max-chunk-size`.

### S3 Throughput Benchmarks (us-west-2)

```
Concurrency  Best chunk   Throughput
    8         8 MiB        ~557 MiB/s
   16         8 MiB        ~1.4 GiB/s
   32        16 MiB        ~2.9 GiB/s
   64         8 MiB        ~5.5 GiB/s
  128         4 MiB        ~5.7 GiB/s
```

Throughput scales linearly up to ~64 connections, then plateaus. Small
chunks (4–8 MiB) consistently outperform large ones at high concurrency.

## LRU Eviction

A background task runs every `--gc-interval-seconds`:

1. Scans the entire cache directory tree.
2. Computes total usage.
3. If usage ≥ `watermark%` of `--max-cache-size`, evicts oldest files (by
   mtime) until usage ≤ `target%`.

The evictor keeps no persistent in-memory index — file count and total size
do not affect the server's baseline memory usage.

## Testing

### Rust unit tests

```bash
cargo test
```

Validates: range parsing, chunk planning, cache path computation, download
state management, SHA-224 fallback.

### Rust integration test (mock upstream)

```bash
cargo test --test mock_integration
```

End-to-end test through the full proxy stack with a mock S3 backend:
full GET, range GET, HEAD, cache hit, concurrent requests.

### Python integration test (moto)

```bash
pip install -r tests/pytest/requirements.txt
cargo build
pytest tests/pytest/ -v
```

Requires `moto[server]` and `boto3`. Starts a real moto S3 endpoint and
the proxy binary, then tests HEAD, GET, range requests, concurrent access,
cache hits, and the SHA-224 fallback path.

## Troubleshooting

If you see `502 upstream error: dispatch failure`:

- Set `--region` explicitly for the bucket.
- Verify credentials: `aws sts get-caller-identity`.
- For LocalStack: use `--s3-endpoint-url` and `--s3-force-path-style`.

## Architecture

```
┌─────────────┐        ┌──────────────────┐        ┌─────────┐
│  HTTP Client │───────▶│  xs3lerator│───────▶│  AWS S3  │
│  (downstream)│◀───────│  (proxy + cache)  │◀───────│(upstream)│
└─────────────┘        └──────────────────┘        └─────────┘
                              │
                              ▼
                       ┌─────────────┐
                       │ POSIX Cache  │
                       │ (hierarchy)  │
                       └─────────────┘
```
