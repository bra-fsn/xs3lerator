# xs3lerator

High-performance Rust HTTP download accelerator and caching proxy with
parallel chunked downloads, content-addressed S3 storage, and per-chunk
temporary file buffering.
Designed as the data-plane companion to [Passsage](https://github.com/bra-fsn/passsage), handling all
GET request fetching and S3 content caching.

## Features

- **Standard AWS authentication**: environment variables, `~/.aws` profiles,
  IAM instance profiles, ECS/EKS task roles — anything the official AWS SDK
  supports.
- **HTTP/1.1 and HTTP/2** downstream (cleartext, no TLS).
- **GET-only proxy** — all other methods return `405 Method Not Allowed`
  (except `POST` for manifest alias creation).
- **Dual-mode operation**:
  - **Caching mode** (default): serves from cache on hit, fetches from upstream
    on miss.  Requires Elasticsearch and a data directory.
  - **Passthrough mode** (`--passthrough`): pure download accelerator.  No
    Elasticsearch, no data directory, no S3 writes.  Just parallel chunked
    downloads from arbitrary HTTP(S) upstreams.
- **Parallel multipart downloads** from both S3 and generic HTTP(S) upstreams
  using configurable concurrency and chunk sizes.
- **Adaptive parallelism**: starts a full GET to upstream (no HEAD first),
  sniffs the response, and converts to parallel range-GETs when possible.
  Falls back to sequential download for chunked/streaming responses.
- **Content-addressed S3 storage**: chunks are stored by SHA-256 hash
  with `If-None-Match: *` deduplication. A manifest file maps each
  cached object to its ordered list of chunk hashes.
- **Manifest alias**: `POST` with `X-Xs3lerator-Link-Manifest-Source` and
  `X-Xs3lerator-Link-Manifest-Target` headers copies a manifest under a
  new key without re-uploading data — used by Passsage for Vary support.
- **Stream-while-downloading**: downstream clients receive data as chunks
  arrive — no need to wait for the full object.
- **In-flight download sharing**: concurrent requests for the same object
  join an existing download while chunks are still held. Once a chunk's fd
  is released (S3 upload done + readers done), subsequent requests re-fetch
  from S3 (if upload completed) or upstream.
- **Range-aware prioritization**: the byte ranges a client needs are fetched
  first, then the rest continues in the background for S3 caching.
- **Per-chunk temp file buffering**: each download chunk gets its own
  temporary file (opened and immediately unlinked). Closing the fd instantly
  reclaims all pages. No persistent cache, no GC, no eviction.
- **ENOSPC graceful degradation**: if temp storage fills up, xs3lerator
  degrades to direct passthrough (no extra memory).

## Architecture

xs3lerator sits between Passsage (the caching policy engine) and both S3 and
upstream HTTP servers. Passsage handles metadata (Elasticsearch indexes) and
caching policies; xs3lerator handles data transfer.

```
                                    ┌──────────────────┐
                                    │    Upstream      │
                                    │  HTTP(S) Servers │
                                    └────────▲─────────┘
                                             │
┌──────────┐     ┌──────────────┐     ┌──────┴───────┐     ┌──────────┐
│  Client  │────▶│   Passsage   │────▶│  xs3lerator  │────▶│  AWS S3  │
│(pip, curl│◀────│ (policy +    │◀────│ (data plane) │◀────│ (cache)  │
│ docker…) │     │  metadata)   │     └──────────────┘     └──────────┘
└──────────┘     └──────────────┘            │
                                             ▼
                                    ┌─────────────────┐
                                    │   Temp Files    │
                                    │ (open + unlink) │
                                    └─────────────────┘
```

### Request Flow

**Passthrough (no `X-Xs3lerator-Cache-Key` header, or `--passthrough` mode)**:
1. xs3lerator fetches from the upstream URL (the request path).
2. Streams the data to the client using parallel chunked downloads.
3. No S3 interaction, no Elasticsearch, no persistence.

**Cache hit** (`X-Xs3lerator-Cache-Key` present, `X-Xs3lerator-Cache-Skip` absent/false):
1. xs3lerator looks up the manifest in Elasticsearch by cache key.
2. Fetches chunks from the data directory (mounted via S3/mount-s3).
3. Streams the requested range to Passsage/client.
4. On cache miss, falls back to upstream fetch.

**Cache miss** (`X-Xs3lerator-Cache-Key` present, `X-Xs3lerator-Cache-Skip: true`):
1. xs3lerator fetches from the upstream URL (the request path).
2. Starts a full GET (no HEAD first — saving a full round-trip to upstream),
   sniffs response headers.
3. If `Content-Length` + `Accept-Ranges: bytes`: converts to parallel download.
4. If chunked/streaming: continues sequential download.
5. Streams the requested range to Passsage/client immediately.
6. Simultaneously uploads chunks to S3 as content-addressed objects
   (SHA-256 keyed, deduplicated via `If-None-Match: *`), then writes a
   manifest mapping the cache key to its chunk hashes.

**S3 fallback**: if a cache-hit S3 fetch fails (404, 5xx), xs3lerator
automatically falls back to fetching from the upstream URL.

## Build

```bash
cargo build --release
```

## Run

### Caching mode (with Passsage)

```bash
./target/release/xs3lerator --data-dir /mnt/s3 --elasticsearch-url http://localhost:9200
```

### Passthrough mode (standalone download accelerator)

```bash
./target/release/xs3lerator --passthrough
```

No data directory or Elasticsearch needed.

### Example requests

```bash
# Caching mode: upstream URL in path, cache key in header
curl -H "X-Xs3lerator-Cache-Key: https/example.com/abc123" \
     -H "X-Xs3lerator-Cache-Skip: true" \
     http://localhost:8080/https://example.com/file.iso

# Passthrough mode: just the upstream URL in path, no cache key needed
curl http://localhost:8080/https://example.com/file.iso
```

## CLI Reference

Every option is also settable via an environment variable.

```text
OPTION                             ENV VAR                            DEFAULT
--bind-ip <IP>                     XS3_BIND_IP                        0.0.0.0
--port <PORT>                      XS3_PORT                           8080
--data-dir <PATH>                  XS3_DATA_DIR                       (required unless --passthrough)
--http-concurrency <N>             XS3_HTTP_CONCURRENCY               8
--chunk-size <SIZE>                XS3_CHUNK_SIZE                     8MiB
--temp-dir <PATH>                  XS3_TEMP_DIR                       (system tmpdir)
--upstream-tls-skip-verify         XS3_UPSTREAM_TLS_SKIP_VERIFY       false
--debug-trace <PATH>               XS3_DEBUG_TRACE                    (none)
--data-prefix <PREFIX>             XS3_DATA_PREFIX                    data/
--elasticsearch-url <URL>          XS3_ELASTICSEARCH_URL              (none)
--elasticsearch-manifest-index     XS3_ELASTICSEARCH_INDEX            passsage_meta
--passthrough                      XS3_PASSTHROUGH                    false
```

Run `xs3lerator --help` to see the generated help with defaults.

## API Contract

### URL Scheme

The upstream URL is passed directly in the request path:

```
GET /<upstream_url>
```

Examples:

```
GET /https://example.com/file.iso
GET /https://pypi.org/packages/numpy-1.0.tar.gz?hash=abc123
```

The cache key (if any) is passed in the `X-Xs3lerator-Cache-Key` header.
When absent, xs3lerator operates as a pure download accelerator (no caching).

### Request Headers (passsage → xs3lerator)

Contract headers (stripped before forwarding upstream):

| Header | Description |
|--------|-------------|
| `X-Xs3lerator-Cache-Key` | Cache key for ES manifest / content-addressed storage. When absent, pure passthrough (download accelerator only). |
| `X-Xs3lerator-Cache-Skip` | `"true"` = skip cache read, go upstream + persist to cache. Requires Cache-Key. |
| `X-Xs3lerator-Object-Size` | Known file size (from metadata), for S3 range-GETs without HEAD |
| `X-Xs3lerator-Tls-Skip-Verify` | `"true"` = skip TLS cert verification for this upstream request |
| `X-Xs3lerator-Follow-Redirects` | `"true"` = follow upstream redirects instead of passing them through |
| `X-Xs3lerator-Link-Manifest-Source` | (POST only) Source cache key for manifest copy |
| `X-Xs3lerator-Link-Manifest-Target` | (POST only) Target cache key for manifest copy |

All other client headers pass through inline. xs3lerator strips contract
headers, `Host`, `Range`, and hop-by-hop headers before forwarding upstream.

### Response Headers (xs3lerator → passsage)

| Header | Description |
|--------|-------------|
| `X-Xs3lerator-Cache-Hit` | `"true"` if served from cache, `"false"` if fetched from upstream. Only present when a cache key was provided. |
| `X-Xs3lerator-Full-Size` | Full object size in bytes (always present, even on range responses) |
| `X-Xs3lerator-Degraded` | Present with value `"enospc"` when temp storage is exhausted |

Passsage strips all `X-Xs3lerator-*` response headers before forwarding to
the client.

### Supported Methods

- `GET` — proxy fetch (cache hit, cache miss, or passthrough). Returns
  `405 Method Not Allowed` with `Allow: GET, POST` for unsupported methods.
- `POST` — manifest alias creation (see Manifest Alias below).
- Health check: `GET /healthz`.

## Download Strategy

### Dual-Ramp Chunk Sizing

Both concurrency and chunk size scale together. For small files, fewer
connections are used; for large files, exactly `max_concurrency` connections
each download 1/Nth:

```
chunk_size = max(min_chunk, ceil(file_size / max_concurrency))
chunk_size = min(chunk_size, 5 GiB)  // S3 multipart part ceiling
```

| File size | Chunk size | Chunks | Connections |
|-----------|-----------|--------|-------------|
| 5 MB      | 8 MB      | 1      | 1           |
| 16 MB     | 8 MB      | 2      | 2           |
| 64 MB     | 8 MB      | 8      | 8           |
| 128 MB    | 16 MB     | 8      | 8           |
| 1 GB      | 128 MB    | 8      | 8           |
| 10 GB     | 1.25 GB   | 8      | 8           |

The same planner is used for both S3 and HTTP upstream downloads — only
the `max_concurrency` differs (32 for S3, 8 for HTTP upstream).

### S3 Throughput Benchmarks (us-west-2)

```
Concurrency  Best chunk   Throughput
    8         8 MiB        ~557 MiB/s
   16         8 MiB        ~1.4 GiB/s
   32        16 MiB        ~2.9 GiB/s
   64         8 MiB        ~5.5 GiB/s
  128         4 MiB        ~5.7 GiB/s
```

### Adaptive Upstream Parallelism

For non-S3 HTTP(S) upstreams (cache miss):

1. Start a full GET (no HEAD first, to minimize round-trips).
2. If `Content-Length` + `Accept-Ranges: bytes`: stop the initial connection
   at the first chunk boundary, open `concurrency - 1` new range connections.
3. If `Transfer-Encoding: chunked`: sequential download, stream into per-chunk
   temp files.
4. If range requests fail (416, unexpected response): fall back to completing
   the full sequential download.

### On-Disk Buffering

Per-chunk temp files are opened in `--temp-dir` and immediately unlinked.
Data stays in the OS page cache. Both the client reader and S3 upload worker
stream concurrently from the same file via progress tracking. When both
consumers finish, the fd is dropped and the kernel instantly reclaims the pages.

For EBS-constrained environments, set `--temp-dir /dev/shm` for pure-RAM
buffering.

### Content-Addressed S3 Upload

On cache miss (when a cache key is provided), xs3lerator uploads the file
to S3 as content-addressed chunks simultaneously with the download:

1. Each chunk is hashed (SHA-256) and uploaded to `{data_prefix}{hash}` using
   `PutObject` with `If-None-Match: *` — if the chunk already exists, the
   upload is skipped (S3 returns `412 Precondition Failed`).
2. Once all chunks are uploaded, a manifest is written to Elasticsearch
   (index configurable via `--elasticsearch-manifest-index`) containing the
   ordered list of chunk hashes, keyed by the cache key.
3. Duplicate data across different objects is stored only once.

### Manifest Alias

`POST` with `X-Xs3lerator-Link-Manifest-Source: {source_key}` and
`X-Xs3lerator-Link-Manifest-Target: {target_key}` copies the manifest for
`source_key` to `target_key` without re-uploading any data. If the source
manifest doesn't exist yet (download still in flight), xs3lerator waits
for it to complete. Returns `204 No Content` on success.

This is used by Passsage for Vary support — when the same upstream object
is cached under a variant key, only a lightweight manifest copy is needed.

### Local Chunk Cache

When `--chunk-cache-dir` is set, xs3lerator maintains a local filesystem
LRU cache of chunks. Cache hits skip the S3 GET entirely. The cache is
bounded by `--chunk-cache-max-size` (default 100 GiB) and evicts
least-recently-used chunks when full.

## Testing

### Unit tests

```bash
cargo test
```

Validates: range parsing, chunk planning, header parsing/filtering,
download state management, temp file creation, byte range calculations.

### Mock integration tests

```bash
cargo test --test mock_integration
```

Tests routing (healthz, 405 for non-GET), error handling.

### Python integration tests

```bash
pytest tests/pytest/ -v
```

End-to-end tests against a real xs3lerator binary + LocalStack S3 +
Elasticsearch. Covers cache miss/hit flows, large file parallel downloads,
range requests, content-addressed uploads, and manifest alias creation.
Requires LocalStack running on `localhost:4566` and Elasticsearch on
`localhost:9200`.

## Troubleshooting

If you see `502 upstream error: dispatch failure`:

- Set `--region` explicitly for the bucket.
- Verify credentials: `aws sts get-caller-identity`.
- For LocalStack: use `--s3-endpoint-url` and `--s3-force-path-style`.

## License

See `LICENSE` and `NOTICE` for copyright and attribution details.

MIT License - see [LICENSE](LICENSE) for details.
