# xs3lerator

High-performance Rust HTTP download accelerator and caching proxy with
parallel chunked downloads, content-addressed S3 chunk storage via
`object_store`, and a local disk cache with LRU garbage collection.
Designed as the data-plane companion to [Passsage](https://github.com/bra-fsn/passsage), handling all
GET request fetching and S3 content caching.

## Features

- **HTTP/1.1 and HTTP/2** downstream (cleartext, no TLS).
- **GET-only proxy** — all other methods return `405 Method Not Allowed`
  (except `POST` for manifest alias creation and `HEAD` for passthrough).
- **Dual-mode operation**:
  - **Caching mode** (default): serves from local disk cache or S3 on hit,
    fetches from upstream on miss.  Requires Elasticsearch, an S3 bucket,
    and a local cache directory.
  - **Passthrough mode** (`--passthrough`): pure download accelerator.  No
    Elasticsearch, no S3, no cache directory.  Just parallel chunked
    downloads from arbitrary HTTP(S) upstreams.
- **Direct S3 operations** via `object_store` — no filesystem-level S3
  mount (mountpoint-s3) required.  Chunks are uploaded/downloaded directly
  to/from S3 using the `object_store` crate.
- **Local disk cache** (`--cache-dir`): EBS- or tmpfs-backed cache for
  chunks with a sharded 256×256 directory layout.  On startup, the directory
  structure is pre-created before the HTTP server begins accepting requests.
- **LRU garbage collection**: a background thread monitors disk usage and
  evicts least-recently-accessed files when the low watermark (default 85%)
  is reached.  If disk usage hits the high watermark (default 95%), random
  eviction kicks in for faster recovery.  Configurable via
  `--cache-low-watermark` and `--cache-high-watermark`.
- **Graceful degradation**: if the cache directory fills up completely,
  xs3lerator enters a degraded mode where it stops caching and serves
  directly from S3 or upstream until space is freed.
- **Parallel multipart downloads** from generic HTTP(S) upstreams using
  configurable concurrency and chunk sizes.
- **Adaptive parallelism**: starts a full GET to upstream (no HEAD first),
  sniffs the response, and converts to parallel range-GETs when possible.
  Falls back to sequential download for chunked/streaming responses.
- **UUID-identified chunk storage**: each chunk gets a random UUIDv4 and is
  stored in S3 under a configurable prefix (`--data-prefix`, default
  `data/`) with a sharded key layout.  A binary manifest in Elasticsearch
  maps each cached object to its ordered list of chunk UUIDs.
- **Manifest alias**: `POST` with `X-Xs3lerator-Link-Manifest-Source` and
  `X-Xs3lerator-Link-Manifest-Target` headers copies a manifest under a
  new key without re-uploading data — used by Passsage for Vary support.
- **Stream-while-downloading**: downstream clients receive data as chunks
  arrive — no need to wait for the full object.
- **Cache-hit streaming from S3**: when a chunk is not in the local disk
  cache, xs3lerator streams it directly from S3 to the downstream client
  while concurrently backfilling the local cache.  The client sees first
  bytes as soon as S3 starts delivering.
- **In-flight download sharing**: concurrent requests for the same object
  join an existing download while chunks are still held.
- **Range-aware prioritization**: the byte ranges a client needs are fetched
  first, then the rest continues in the background for S3 caching.
- **Per-chunk temp file buffering**: each upstream download chunk gets its
  own temporary file (opened and immediately unlinked). Data stays in the
  OS page cache.  When all consumers finish, the fd is dropped and the
  kernel instantly reclaims the pages.
- **Background S3 uploads**: on cache miss, each chunk is first written to
  a temp file, then copied to the local disk cache (`fsync` + `rename`),
  and finally uploaded to S3 via `object_store::put()` as a background
  task.  S3 I/O is fully decoupled from the client response path.
- **Corrupt manifest recovery**: if any S3 chunk is missing during a
  cache-hit serve, xs3lerator logs an error, clears the manifest from
  Elasticsearch, deletes the orphaned S3 chunks, and falls back to a
  full upstream fetch.

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
│ docker…) │     │  metadata)   │     └──────┬───────┘     └──────────┘
└──────────┘     └──────────────┘            │
                                    ┌────────┴────────┐
                                    │  Local Disk     │
                                    │  Cache (EBS /   │
                                    │  tmpfs)         │
                                    └─────────────────┘
```

### Request Flow

**Passthrough (no `X-Xs3lerator-Cache-Key` header, or `--passthrough` mode)**:
1. xs3lerator fetches from the upstream URL (the request path).
2. Streams the data to the client using parallel chunked downloads.
3. No S3 interaction, no Elasticsearch, no persistence.

**Cache hit** (`X-Xs3lerator-Cache-Key` present, `X-Xs3lerator-Cache-Skip` absent/false):
1. xs3lerator looks up the manifest in Elasticsearch by cache key.
2. For each chunk: checks local disk cache first, then falls back to S3.
3. S3-served chunks are streamed to the client while concurrently
   backfilling the local cache.
4. If a chunk is missing from both local cache and S3, xs3lerator clears
   the corrupt manifest, deletes orphaned S3 chunks, and falls back to a
   full upstream fetch.
5. If the local cache read fails (e.g., truncated file), xs3lerator
   transparently falls back to S3 for that chunk.

**Cache miss** (`X-Xs3lerator-Cache-Key` present, `X-Xs3lerator-Cache-Skip: true`):
1. xs3lerator fetches from the upstream URL (the request path).
2. Starts a full GET (no HEAD first — saving a full round-trip to upstream),
   sniffs response headers.
3. If `Content-Length` + `Accept-Ranges: bytes`: converts to parallel download.
4. If chunked/streaming: continues sequential download.
5. Streams the requested range to Passsage/client immediately.
6. Each chunk is written to a temp file, then to the local disk cache
   (`fsync` + `rename`), and finally uploaded to S3 via `object_store::put()`.
   Once all chunks are committed, a finalize task writes the manifest to
   Elasticsearch.

**HEAD passthrough**: HEAD requests are forwarded directly to the upstream and
return headers only (no caching, no ES). This allows callers to reuse
xs3lerator's connection pool for metadata queries.

## Build

```bash
cargo build --release
```

## Run

### Caching mode (with Passsage)

```bash
./target/release/xs3lerator \
    --s3-bucket my-cache-bucket \
    --s3-region us-east-1 \
    --cache-dir /data \
    --elasticsearch-url http://localhost:9200
```

### Passthrough mode (standalone download accelerator)

```bash
./target/release/xs3lerator --passthrough
```

No S3 bucket, cache directory, or Elasticsearch needed.

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
--s3-bucket <BUCKET>               XS3_S3_BUCKET                      (required unless --passthrough)
--s3-region <REGION>               XS3_S3_REGION                      us-east-1
--s3-endpoint <URL>                XS3_S3_ENDPOINT                    (none; for LocalStack/MinIO)
--cache-dir <PATH>                 XS3_CACHE_DIR                      (required unless --passthrough)
--cache-low-watermark <0-100>      XS3_CACHE_LOW_WATERMARK            85
--cache-high-watermark <0-100>     XS3_CACHE_HIGH_WATERMARK           95
--http-concurrency <N>             XS3_HTTP_CONCURRENCY               8
--chunk-size <SIZE>                XS3_CHUNK_SIZE                     32MiB
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
| `X-Xs3lerator-Cache-Key` | Cache key for ES manifest / chunk storage. When absent, pure passthrough (download accelerator only). |
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
  `405 Method Not Allowed` with `Allow: GET, HEAD, POST` for unsupported methods.
- `HEAD` — passthrough to upstream (no caching). Returns upstream headers only.
- `POST` — manifest alias creation (see Manifest Alias below).
- Health check: `GET /healthz`.

## Download Strategy

### Chunk Sizing

The chunk size defaults to 32 MiB (`--chunk-size`). This keeps chunks small
enough that clients can start receiving data quickly, and large enough to
amortize per-request overhead on S3. Chunk size is only increased when the
file would exceed S3's 10,000-part limit or the 5 GiB per-part ceiling.

Actual download concurrency is managed separately by a worker pool with
progressive ramp-up.

### Adaptive Upstream Parallelism

For non-S3 HTTP(S) upstreams (cache miss):

1. Start a full GET (no HEAD first, to minimize round-trips).
2. If `Content-Length` + `Accept-Ranges: bytes`: stop the initial connection
   at the first chunk boundary, open `concurrency - 1` new range connections.
3. If `Transfer-Encoding: chunked`: sequential download, stream into per-chunk
   temp files.
4. If range requests fail (416, unexpected response): fall back to completing
   the full sequential download.

### Local Disk Cache

Chunks are cached in `--cache-dir` under a sharded 256×256 directory layout
(the first two bytes of the chunk UUID determine the subdirectories). On
startup, xs3lerator pre-creates all 65,536 directories before accepting
requests.

On cache hit, chunks are served from local disk first. On local miss, they
are streamed from S3 and concurrently backfilled to the local cache.

The GC thread monitors filesystem usage via `statvfs` and evicts files based
on `atime` (LRU) at the low watermark, or randomly at the high watermark,
without keeping all file metadata in memory.

### On-Disk Buffering (Upstream Downloads)

Per-chunk temp files are opened in `--temp-dir` and immediately unlinked.
Data stays in the OS page cache. The client reader and finalize pipeline
stream concurrently from the same file via progress tracking
(`bytes_written` atomic counters + `Notify`). When all consumers finish,
the fd is dropped and the kernel instantly reclaims the pages.

### S3 Upload via Background Tasks

On cache miss (when a cache key is provided), xs3lerator uploads chunks to
S3 in the background, fully decoupled from the client response:

1. Each chunk gets a random UUIDv4 identifier.
2. The chunk is first written to a temp file, then copied to the local
   disk cache directory (`fsync` + atomic `rename`).
3. A background task reads the finalized chunk from disk and uploads it to
   S3 using `object_store::put()`.
4. Once all chunks are uploaded, a finalize task writes a binary manifest
   to Elasticsearch containing the ordered list of chunk UUIDs.

### Manifest Alias

`POST` with `X-Xs3lerator-Link-Manifest-Source: {source_key}` and
`X-Xs3lerator-Link-Manifest-Target: {target_key}` copies the manifest for
`source_key` to `target_key` without re-uploading any data. If the source
manifest doesn't exist yet (download still in flight), xs3lerator waits
for it to complete. Returns `204 No Content` on success.

This is used by Passsage for Vary support — when the same upstream object
is cached under a variant key, only a lightweight manifest copy is needed.

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

Tests routing (healthz, 405 for non-GET), cache-hit flows, range requests,
redirect handling, error handling.

### Python integration tests (Docker Compose)

```bash
docker compose up -d
pytest tests/pytest/ -v
```

End-to-end tests against a real xs3lerator binary + LocalStack S3 +
Elasticsearch. Covers cache miss/hit flows, large file parallel downloads,
range requests, concurrent access, manifest alias creation, and S3 fallback
on missing chunks. The Docker Compose stack provides all dependencies
(LocalStack on `localhost:4566`, Elasticsearch on `localhost:9200`) with
xs3lerator configured for 5 MiB chunks.

## Troubleshooting

If you see `502 upstream error: dispatch failure`:

- Verify the upstream URL is reachable from the xs3lerator host.
- Check TLS: try `--upstream-tls-skip-verify` or the per-request header.

If S3 uploads fail with IMDS/credential errors:

- Ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment
  variables are set (xs3lerator reads them directly, falling back to the
  default `object_store` credential chain).
- For LocalStack/MinIO, also set `--s3-endpoint`.

## License

See `LICENSE` and `NOTICE` for copyright and attribution details.

MIT License - see [LICENSE](LICENSE) for details.
