# CLAUDE.md

## What This Is

xs3lerator is a high-performance Rust HTTP proxy that sits in front of Amazon S3 and caches objects on a local POSIX filesystem. It serves GET/HEAD requests over HTTP/1.1 and HTTP/2 (cleartext), downloads from S3 using parallel chunked range-GETs, and evicts cached files via background LRU.

## Current State

- **Known design points that were deliberately chosen**:
  - Download workers stream S3 responses to disk via pwrite in ~256 KiB pieces (never buffer whole chunks in memory).
  - Per-chunk progress is tracked with `AtomicU64` so downstream readers can start streaming data within milliseconds — they don't wait for a full chunk.
  - The chunk planner prefers 8 MiB chunks (S3 throughput sweet spot) regardless of file size. This was a deliberate fix over an earlier design that inflated chunks to 256 MiB and caused ~2x object size in RSS.
  - `Notify`-based reader/writer coordination has a known benign race (notify\_waiters doesn't store permits), mitigated by frequent notifications (~every 64 KiB written).

## Architecture

```
src/
  config.rs    — CLI (clap) + env vars + validation → AppConfig
  s3.rs        — Upstream trait + AwsUpstream (aws-sdk-s3)
  planner.rs   — Chunk size / concurrency calculator
  download.rs  — DownloadManager (dedup) + InFlightDownload (progress) + workers
  handler.rs   — Axum GET/HEAD handlers, progressive reader, passthrough fallback
  cache.rs     — CacheStore: hierarchy paths, writability probe, mtime, scan
  evictor.rs   — Background LRU eviction loop
  range.rs     — HTTP Range header parser
  error.rs     — ProxyError enum → HTTP status codes
  server.rs    — Router setup
  main.rs      — Entrypoint: arg parse, S3 client, evictor spawn, serve
```

## Key Dependencies

- `axum` 0.7 / `hyper` 1 — HTTP server
- `aws-sdk-s3` 1 — S3 client (official SDK)
- `tokio` — async runtime
- `clap` 4 — CLI with env fallback
- `sha2` — SHA-224 hashing for cache keys
- `parking_lot` — fast Mutex for download manager
- `async-stream` — try\_stream! macro for progressive body streaming

## Common Commands

```bash
cargo build --release
cargo test                          # unit + mock integration
cargo test --test mock_integration  # just the mock e2e tests
pytest tests/pytest/ -v             # Python/moto integration (needs moto[server], boto3)
```

## Design Constraints

- POSIX filesystem semantics assumed (pwrite/pread, atomic rename, sparse files).
- No HTTPS termination — intended to sit behind a load balancer or be accessed locally.
- Only GET and HEAD downstream; all other methods are rejected.
- Cache key is `x-amz-meta-sha224` from S3 metadata; falls back to SHA-224 of the S3 key path.
- Duplicate dependency versions in Cargo.lock (hyper 0.14/1.x, h2, rustls, etc.) are caused by aws-sdk-s3 internals and cannot be resolved from this project.
