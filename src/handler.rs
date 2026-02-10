use std::sync::Arc;
use std::time::Instant;

use async_stream::try_stream;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH,
    CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE, SERVER,
};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::IntoResponse;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::json;
use sha2::{Digest, Sha224};
use tracing::warn;

use crate::cache::CacheStore;
use crate::config::{AppConfig, OperatingMode};
use crate::download::{spawn_download, DownloadManager, InFlightDownload};
use crate::error::{ProxyError, ProxyResult};
use crate::planner::compute_chunk_plan;
use crate::range::{parse_range_header, ByteRange};
use crate::s3::{ObjectMeta, Upstream};
use crate::trace::{trace_log, TraceWriter};

const SERVER_NAME: &str = "xs3lerator/0.1.0";

/// Shared application state injected into every request handler.
#[derive(Clone)]
pub struct ProxyState {
    pub config: Arc<AppConfig>,
    pub upstream: Arc<dyn Upstream>,
    /// Present in cache mode, None in mount mode.
    pub cache: Option<Arc<CacheStore>>,
    /// Present in cache mode, None in mount mode.
    pub downloads: Option<Arc<DownloadManager>>,
    pub trace: Option<Arc<TraceWriter>>,
}

// ---------------------------------------------------------------------------
// HEAD handler
// ---------------------------------------------------------------------------

pub async fn handle_head(
    State(state): State<ProxyState>,
    Path(key): Path<String>,
) -> ProxyResult<impl IntoResponse> {
    let key = normalize_key(&key);
    let meta = state
        .upstream
        .head_object(&state.config.bucket, &key)
        .await?;
    Ok(build_head_response(&meta))
}

// ---------------------------------------------------------------------------
// GET handler
// ---------------------------------------------------------------------------

pub async fn handle_get(
    State(state): State<ProxyState>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> ProxyResult<Response<Body>> {
    let key = normalize_key(&key);
    let meta = state
        .upstream
        .head_object(&state.config.bucket, &key)
        .await?;

    let range = parse_range_header(
        headers.get(RANGE).and_then(|v| v.to_str().ok()),
        meta.content_length,
    )?;

    if meta.content_length == 0 {
        return Ok(build_get_response(&meta, range, Body::empty()));
    }

    // Mount mode: serve directly from mount-s3 FUSE path
    if let Some(mount_path) = state.config.mount_path() {
        let file_path = mount_path.join(&key);
        return serve_cached_file(file_path, &meta, range).await;
    }

    // Cache mode below
    let cache = state.cache.as_ref().expect("cache required in cache mode");
    let downloads = state.downloads.as_ref().expect("downloads required in cache mode");

    let OperatingMode::Cache {
        max_concurrency,
        min_chunk_size,
        max_chunk_size,
        ..
    } = &state.config.mode
    else {
        unreachable!()
    };

    let cache_key = resolve_cache_key(&meta, &key);
    let cache_path = cache.cache_path(&cache_key);

    match cache.verify_cached(&cache_path, meta.content_length).await {
        Some(true) => {
            trace_log(&state.trace, || json!({
                "event": "cache_hit",
                "key": key,
                "size": meta.content_length,
            }));
            cache.touch_mtime(&cache_path).await;
            return serve_cached_file(cache_path, &meta, range).await;
        }
        Some(false) => {
            warn!(key, "cache size mismatch — removing stale entry");
            let _ = tokio::fs::remove_file(&cache_path).await;
        }
        None => {}
    }

    cache.refresh_writable().await;
    if !cache.is_writable() {
        return stream_passthrough(&state, &key, &meta, range).await;
    }

    let temp_path = cache.temp_path(&cache_key);
    if let Err(e) = cache.ensure_parent_dir(&temp_path).await {
        warn!(key, "cannot create parent dir: {e} — passthrough");
        return stream_passthrough(&state, &key, &meta, range).await;
    }

    let plan = compute_chunk_plan(
        meta.content_length,
        *max_concurrency,
        *min_chunk_size,
        *max_chunk_size,
    );

    let (download, is_new) = downloads.get_or_create(&cache_key, || {
        Arc::new(InFlightDownload::new(
            temp_path.clone(),
            cache_path.clone(),
            meta.content_length,
            plan.chunk_size,
        ))
    });

    let effective = range.unwrap_or(ByteRange {
        start: 0,
        end_inclusive: meta.content_length.saturating_sub(1),
    });
    download.prioritize_range(effective.start, effective.end_inclusive);

    let trace = state.trace.clone();
    if is_new {
        trace_log(&trace, || json!({
            "event": "request",
            "key": key,
            "size": meta.content_length,
            "chunk_size": plan.chunk_size,
            "chunks": download.chunk_count(),
            "concurrency": plan.concurrency,
            "range": range.map(|r| format!("{}-{}", r.start, r.end_inclusive)),
        }));
        spawn_download(
            state.upstream.clone(),
            state.config.bucket.clone(),
            key.clone(),
            cache_key.clone(),
            download.clone(),
            downloads.clone(),
            cache.clone(),
            plan.concurrency,
            trace.clone(),
        );
    }

    stream_in_progress(download, &meta, range, trace).await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn resolve_cache_key(meta: &ObjectMeta, s3_key: &str) -> String {
    meta.metadata
        .get("sha224")
        .cloned()
        .unwrap_or_else(|| sha224_hex(s3_key))
}

fn sha224_hex(input: &str) -> String {
    let mut h = Sha224::new();
    h.update(input.as_bytes());
    format!("{:x}", h.finalize())
}

fn normalize_key(key: &str) -> String {
    key.trim_start_matches('/').to_string()
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

fn build_head_response(meta: &ObjectMeta) -> Response<Body> {
    apply_headers(Response::builder(), meta, None)
        .body(Body::empty())
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

fn build_get_response(
    meta: &ObjectMeta,
    range: Option<ByteRange>,
    body: Body,
) -> Response<Body> {
    apply_headers(Response::builder(), meta, range)
        .body(body)
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

fn apply_headers(
    mut b: http::response::Builder,
    meta: &ObjectMeta,
    range: Option<ByteRange>,
) -> http::response::Builder {
    let status = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let content_length = range.map(|r| r.len()).unwrap_or(meta.content_length);

    b = b
        .status(status)
        .header(SERVER, SERVER_NAME)
        .header(ACCEPT_RANGES, "bytes")
        .header(CONTENT_LENGTH, content_length);

    if let Some(r) = range {
        b = b.header(
            CONTENT_RANGE,
            format!(
                "bytes {}-{}/{}",
                r.start, r.end_inclusive, meta.content_length
            ),
        );
    }
    if let Some(v) = &meta.content_type {
        b = b.header(CONTENT_TYPE, v.as_str());
    }
    if let Some(v) = &meta.etag {
        b = b.header(ETAG, v.as_str());
    }
    if let Some(v) = &meta.last_modified {
        b = b.header(LAST_MODIFIED, v.as_str());
    }
    if let Some(v) = &meta.cache_control {
        b = b.header(CACHE_CONTROL, v.as_str());
    }
    if let Some(v) = &meta.content_encoding {
        b = b.header(CONTENT_ENCODING, v.as_str());
    }
    b
}

// ---------------------------------------------------------------------------
// Serve from a fully cached file
// ---------------------------------------------------------------------------

async fn serve_cached_file(
    path: std::path::PathBuf,
    meta: &ObjectMeta,
    range: Option<ByteRange>,
) -> ProxyResult<Response<Body>> {
    let effective = range.unwrap_or(ByteRange {
        start: 0,
        end_inclusive: meta.content_length.saturating_sub(1),
    });

    let file = std::fs::File::open(&path)
        .map_err(|e| ProxyError::Internal(format!("open cached: {e}")))?;

    // mmap the file — safe because cached files are immutable after rename.
    // The evictor may unlink the path, but Linux keeps the inode alive
    // as long as this mapping exists.
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| ProxyError::Internal(format!("mmap: {e}")))?;
    mmap.advise(memmap2::Advice::Sequential)
        .map_err(|e| ProxyError::Internal(format!("madvise: {e}")))?;

    let full = Bytes::from_owner(mmap);
    let start = effective.start as usize;
    let end = (effective.end_inclusive + 1) as usize;
    const MMAP_PIECE: usize = 2 * 1024 * 1024;

    let stream = futures::stream::iter(
        (start..end)
            .step_by(MMAP_PIECE)
            .map(move |off| {
                let piece_end = std::cmp::min(off + MMAP_PIECE, end);
                Ok::<Bytes, std::io::Error>(full.slice(off..piece_end))
            }),
    );

    Ok(build_get_response(meta, range, Body::from_stream(stream)))
}

// ---------------------------------------------------------------------------
// Direct S3 passthrough (when cache is not writable)
// ---------------------------------------------------------------------------

async fn stream_passthrough(
    state: &ProxyState,
    key: &str,
    meta: &ObjectMeta,
    range: Option<ByteRange>,
) -> ProxyResult<Response<Body>> {
    let effective = range.unwrap_or(ByteRange {
        start: 0,
        end_inclusive: meta.content_length.saturating_sub(1),
    });
    let byte_stream = state
        .upstream
        .get_range_stream(
            &state.config.bucket,
            key,
            effective.start,
            effective.end_inclusive,
        )
        .await?;
    let body = Body::from_stream(
        byte_stream.map(|r| r.map_err(|e| std::io::Error::other(e.to_string()))),
    );
    Ok(build_get_response(meta, range, body))
}

// ---------------------------------------------------------------------------
// Stream from an in-progress download (the hot path)
// ---------------------------------------------------------------------------

/// Maximum bytes per downstream write.  Small enough to keep per-reader
/// memory low, large enough for efficient pread + TCP send.
const READ_PIECE_SIZE: u64 = 256 * 1024;

/// Stream data from the temp/final file to the client as it is written
/// by the download workers.
///
/// Unlike the previous implementation that waited for whole chunks, this
/// reads in 256 KiB pieces and starts streaming as soon as the first bytes
/// of the first relevant chunk hit disk.  Uses pread (positional read)
/// so multiple readers can share a single file descriptor safely.
async fn stream_in_progress(
    download: Arc<InFlightDownload>,
    meta: &ObjectMeta,
    range: Option<ByteRange>,
    trace: Option<Arc<TraceWriter>>,
) -> ProxyResult<Response<Body>> {
    let effective = range.unwrap_or(ByteRange {
        start: 0,
        end_inclusive: meta.content_length.saturating_sub(1),
    });

    let chunk_size = download.chunk_size;
    let obj_size = download.object_size;
    let start_chunk = (effective.start / chunk_size) as usize;
    let end_chunk = (effective.end_inclusive / chunk_size) as usize;
    let temp_path = download.temp_path.clone();
    let final_path = download.final_path.clone();

    let body_stream = try_stream! {
        let mut file_handle: Option<Arc<std::fs::File>> = None;
        let reader_t0 = Instant::now();
        let mut total_sent = 0u64;

        for idx in start_chunk..=end_chunk {
            let chunk_t0 = Instant::now();

            // What slice of this chunk does the client need?
            let chunk_start = idx as u64 * chunk_size;
            let chunk_end = std::cmp::min(chunk_start + chunk_size, obj_size);
            let read_start = std::cmp::max(chunk_start, effective.start);
            let read_end = std::cmp::min(chunk_end - 1, effective.end_inclusive);
            let total_needed = read_end - read_start + 1;
            let within_chunk_offset = read_start - chunk_start;

            trace_log(&trace, || json!({
                "event": "rd_chunk_enter",
                "chunk": idx,
                "read_from": read_start,
                "read_to": read_end,
                "need_bytes": total_needed,
            }));

            let mut sent = 0u64;

            while sent < total_needed {
                // Wait until at least 1 byte past our current position is
                // written to disk.
                let need_in_chunk = within_chunk_offset + sent + 1;

                // Log if we're about to actually block (data not yet available)
                let available_before = download.bytes_written(idx);
                if available_before < need_in_chunk {
                    trace_log(&trace, || json!({
                        "event": "rd_wait",
                        "chunk": idx,
                        "need": need_in_chunk,
                        "have": available_before,
                        "total_sent": total_sent,
                    }));
                }

                let available_in_chunk =
                    download.wait_for_bytes(idx, need_in_chunk).await?;

                // Open file lazily (the download worker has created it by now).
                if file_handle.is_none() {
                    let f = std::fs::File::open(&temp_path)
                        .or_else(|_| std::fs::File::open(&final_path))
                        .map_err(|e| ProxyError::Internal(
                            format!("open cache file: {e}")
                        ))?;
                    file_handle = Some(Arc::new(f));
                }

                // Read up to READ_PIECE_SIZE of whatever is available now.
                let readable =
                    available_in_chunk.saturating_sub(within_chunk_offset + sent);
                let to_read = std::cmp::min(
                    std::cmp::min(readable, total_needed - sent),
                    READ_PIECE_SIZE,
                ) as usize;

                if to_read == 0 {
                    continue;
                }

                let file = file_handle.as_ref().unwrap().clone();
                let file_offset = read_start + sent;
                let data = tokio::task::spawn_blocking(move || -> Result<Bytes, ProxyError> {
                    use std::os::unix::fs::FileExt;
                    let mut buf = vec![0u8; to_read];
                    file.read_exact_at(&mut buf, file_offset)
                        .map_err(|e| ProxyError::Internal(format!("pread: {e}")))?;
                    Ok(Bytes::from(buf))
                })
                .await
                .map_err(|e| ProxyError::Internal(format!("join: {e}")))?
                ?;

                sent += data.len() as u64;
                total_sent += data.len() as u64;
                yield data;
            }

            let chunk_elapsed = chunk_t0.elapsed();
            trace_log(&trace, || json!({
                "event": "rd_chunk_done",
                "chunk": idx,
                "bytes": sent,
                "elapsed_ms": chunk_elapsed.as_secs_f64() * 1000.0,
                "total_sent": total_sent,
            }));
        }

        let total_elapsed = reader_t0.elapsed();
        trace_log(&trace, || json!({
            "event": "rd_done",
            "total_bytes": total_sent,
            "elapsed_ms": total_elapsed.as_secs_f64() * 1000.0,
            "mbps": if total_elapsed.as_nanos() > 0 {
                (total_sent as f64) / total_elapsed.as_secs_f64() / 1_000_000.0
            } else { 0.0 },
        }));
    };

    let body = Body::from_stream(
        body_stream.map(|r: Result<Bytes, ProxyError>| {
            r.map_err(|e| std::io::Error::other(e.to_string()))
        }),
    );
    Ok(build_get_response(meta, range, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha224_fallback_is_stable() {
        let hash = sha224_hex("test");
        assert_eq!(
            hash,
            "90a3ed9e32b2aaf4c61c410eb925426119e1a9dc53d4286ade99a809"
        );
    }

    #[test]
    fn key_normalization() {
        assert_eq!(normalize_key("/a/b/c"), "a/b/c");
        assert_eq!(normalize_key("a/b"), "a/b");
        assert_eq!(normalize_key("///x"), "x");
    }

    #[test]
    fn cache_key_uses_sha224_header_when_present() {
        let meta = ObjectMeta {
            content_length: 100,
            content_type: None,
            etag: None,
            last_modified: None,
            cache_control: None,
            content_encoding: None,
            metadata: [("sha224".into(), "deadbeef".into())]
                .into_iter()
                .collect(),
        };
        assert_eq!(resolve_cache_key(&meta, "some/key"), "deadbeef");
    }

    #[test]
    fn cache_key_falls_back_to_key_hash() {
        let meta = ObjectMeta {
            content_length: 100,
            content_type: None,
            etag: None,
            last_modified: None,
            cache_control: None,
            content_encoding: None,
            metadata: Default::default(),
        };
        let key = resolve_cache_key(&meta, "test");
        assert_eq!(
            key,
            "90a3ed9e32b2aaf4c61c410eb925426119e1a9dc53d4286ade99a809"
        );
    }
}
