use std::cmp::min;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_stream::try_stream;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::json;
use tracing::{error, info};

use crate::config::AppConfig;
use crate::download::{create_temp_chunk_file, pwrite_all, DownloadManager, InFlightDownload};
use crate::error::{ProxyError, ProxyResult};
use crate::headers::{
    self, parse_bucket_key, parse_contract_headers, RESP_CACHE_HIT, RESP_DEGRADED, RESP_FULL_SIZE,
};
use crate::planner::compute_chunk_plan;
use crate::range::{parse_range_header, ByteRange};
use crate::s3::{AwsUpstream, S3Uploader, Upstream};
use crate::trace::{trace_log, TraceWriter};
use crate::upstream_fetcher;

/// RAII guard that cancels an in-flight download when dropped.
/// Used for S3 cache-hit reads so that workers stop when the client disconnects.
struct CancelGuard(Arc<InFlightDownload>);

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub s3_upstream: Arc<AwsUpstream>,
    pub s3_uploader: Arc<S3Uploader>,
    pub downloads: Arc<DownloadManager>,
    pub trace: Option<Arc<TraceWriter>>,
}

/// Health check endpoint: `GET /healthz`
pub async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Method-not-allowed handler for non-GET methods.
pub async fn method_not_allowed() -> impl IntoResponse {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [(axum::http::header::ALLOW, "GET")],
        "only GET is supported",
    )
}

/// Main GET handler: `GET /<bucket>/<s3_key...>`
pub async fn handle_get(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Result<Response, ProxyError> {
    let path = req.uri().path().to_string();
    let headers = req.headers().clone();

    let (bucket, key) = parse_bucket_key(&path)
        .ok_or_else(|| ProxyError::Internal("invalid path: expected /<bucket>/<key>".into()))?;

    let contract = parse_contract_headers(&headers);
    let cache_key = [bucket.as_str(), "/", key.as_str()].concat();
    let client_range_header = headers
        .get("range")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    trace_log(&state.trace, || json!({
        "event": "request",
        "bucket": bucket,
        "key": key,
        "cache_skip": contract.cache_skip,
        "object_size": contract.object_size,
    }));

    if contract.cache_skip {
        return handle_upstream_path(
            &state,
            &contract,
            &headers,
            &bucket,
            &key,
            &cache_key,
            client_range_header.as_deref(),
        )
        .await;
    }

    // S3-first path: try to serve from cache
    match handle_s3_path(
        &state,
        &contract,
        &bucket,
        &key,
        &cache_key,
        client_range_header.as_deref(),
    )
    .await
    {
        Ok(resp) => Ok(resp),
        Err(e) => {
            info!(
                key = key.as_str(),
                error = %e,
                "S3 fetch failed, falling back to upstream"
            );
            handle_upstream_path(
                &state,
                &contract,
                &headers,
                &bucket,
                &key,
                &cache_key,
                client_range_header.as_deref(),
            )
            .await
        }
    }
}

/// Serve from S3 cache using parallel range-GETs.
async fn handle_s3_path(
    state: &AppState,
    contract: &headers::ContractHeaders,
    bucket: &str,
    key: &str,
    cache_key: &str,
    client_range: Option<&str>,
) -> ProxyResult<Response> {
    let object_size = if let Some(size) = contract.object_size {
        size
    } else {
        let meta = state.s3_upstream.head_object(bucket, key).await?;
        meta.content_length
    };

    let client_byte_range = parse_range_header(client_range, object_size)?;

    let (serve_start, serve_end) = match client_byte_range {
        Some(ref r) => (r.start, r.end_inclusive),
        None => (0, object_size.saturating_sub(1)),
    };

    // Only download the requested range from S3 (not the full file)
    let plan = compute_chunk_plan(
        serve_end - serve_start + 1,
        state.config.s3_concurrency,
        state.config.min_chunk_size,
    );

    let download = Arc::new(InFlightDownload::new(
        serve_end - serve_start + 1,
        plan.chunk_size,
    ));

    let (download, is_new) = state.downloads.get_or_create(
        &format!("{cache_key}:{serve_start}-{serve_end}"),
        || download,
    );

    if is_new {
        // Spawn S3 download workers
        let dl = download.clone();
        let s3 = state.s3_upstream.clone();
        let b = bucket.to_string();
        let k = key.to_string();
        let temp_dir = state.config.temp_dir.clone();
        let trace = state.trace.clone();
        let ck = format!("{cache_key}:{serve_start}-{serve_end}");
        let dm = state.downloads.clone();
        let s3_concurrency = state.config.s3_concurrency;

        tokio::spawn(async move {
            let result = run_s3_download(
                &s3, &b, &k, serve_start, &dl, &temp_dir, &trace, s3_concurrency,
            )
            .await;

            match result {
                Ok(()) => {
                    info!(key = k, "S3 download complete");
                }
                Err(e) => {
                    error!(key = k, "S3 download failed: {e}");
                    dl.mark_failed();
                }
            }
            dm.remove(&ck);
        });
    }

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("true"));
    resp_headers.insert(
        RESP_FULL_SIZE,
        HeaderValue::from_str(&object_size.to_string()).unwrap(),
    );

    build_streaming_response(
        download,
        object_size,
        client_byte_range,
        resp_headers,
        true, // cancel S3 workers when client disconnects
    )
}

/// Persistent S3 download workers: each loops pulling the *next* chunk
/// in priority order from a shared atomic cursor, streaming each range-GET
/// incrementally to its temp file.  This guarantees near-sequential chunk
/// completion and allows the client to stream smoothly without stalls.
async fn run_s3_download(
    s3: &AwsUpstream,
    bucket: &str,
    key: &str,
    global_offset: u64,
    download: &InFlightDownload,
    temp_dir: &std::path::Path,
    trace: &Option<Arc<TraceWriter>>,
    max_concurrency: usize,
) -> ProxyResult<()> {
    let chunk_indices: Vec<usize> = std::iter::from_fn(|| download.pop_chunk()).collect();

    // Create temp files for all chunks up front.
    for &idx in &chunk_indices {
        let chunk_file = create_temp_chunk_file(temp_dir)
            .map_err(|e| ProxyError::Internal(format!("create temp file: {e}")))?;
        download.chunk(idx).set_file(Arc::new(chunk_file));
    }

    let num_workers = max_concurrency.min(chunk_indices.len());
    let cursor = Arc::new(AtomicUsize::new(0));
    let chunks = Arc::new(chunk_indices);
    let mut handles = Vec::with_capacity(num_workers);

    for _ in 0..num_workers {
        let cursor = cursor.clone();
        let chunks = chunks.clone();
        let s3 = s3.clone();
        let b = bucket.to_string();
        let k = key.to_string();
        let dl_ptr = download as *const InFlightDownload as usize;
        let trace_c = trace.clone();

        handles.push(tokio::spawn(async move {
            let download = unsafe { &*(dl_ptr as *const InFlightDownload) };
            loop {
                if download.is_cancelled() { break; }

                let pos = cursor.fetch_add(1, Ordering::Relaxed);
                if pos >= chunks.len() { break; }
                let idx = chunks[pos];

                let chunk_file = download.chunk(idx).get_file()
                    .ok_or_else(|| ProxyError::Internal(
                        format!("s3 chunk {idx} file gone")
                    ))?;
                let (local_start, local_end) = download.chunk_byte_range(idx);
                let s3_start = global_offset + local_start;
                let s3_end = global_offset + local_end;
                let expected = download.expected_chunk_len(idx);

                let mut body = s3.get_range_stream(&b, &k, s3_start, s3_end).await?;
                let mut offset = 0u64;
                while let Some(piece) = body.next().await {
                    if download.is_cancelled() { break; }
                    let data = piece.map_err(|e| {
                        ProxyError::Upstream(format!("s3 stream chunk {idx}: {e}"))
                    })?;
                    let to_write = min(data.len() as u64, expected - offset) as usize;
                    if to_write == 0 { break; }
                    pwrite_all(&chunk_file, offset, &data[..to_write])
                        .map_err(|e| ProxyError::Internal(
                            format!("pwrite s3 chunk {idx}: {e}")
                        ))?;
                    offset += to_write as u64;
                    download.record_written(idx, to_write as u64);
                }
                if !download.is_cancelled() {
                    download.mark_chunk_done(idx);
                }

                trace_log(&trace_c, || json!({
                    "event": "s3_chunk_done",
                    "chunk": idx,
                    "bytes": offset,
                    "cancelled": download.is_cancelled(),
                }));
            }
            Ok::<(), ProxyError>(())
        }));
    }

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(ProxyError::Internal(format!("s3 worker panicked: {e}"))),
        }
    }

    Ok(())
}

/// Serve from upstream (cache miss or S3 fallback).
async fn handle_upstream_path(
    state: &AppState,
    contract: &headers::ContractHeaders,
    client_headers: &HeaderMap,
    bucket: &str,
    key: &str,
    cache_key: &str,
    client_range: Option<&str>,
) -> ProxyResult<Response> {
    let result = upstream_fetcher::fetch_upstream(
        &state.config,
        contract,
        client_headers,
        bucket,
        key,
        cache_key,
        client_range,
        &state.downloads,
        Some(state.s3_uploader.clone()),
        &state.trace,
    )
    .await?;

    // ENOSPC degradation: stream upstream response directly without buffering.
    // No S3 upload occurred; passsage should not record this as cached.
    if let Some(direct_response) = result.degraded_body {
        return build_passthrough_response(
            direct_response,
            result.full_size,
            result.content_type,
            result.etag,
            result.last_modified,
            result.cache_control,
        );
    }

    let full_size = result.full_size.unwrap_or(result.download.object_size);
    let client_byte_range = if full_size > 0 {
        parse_range_header(client_range, full_size)?
    } else {
        None
    };

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("false"));
    if full_size > 0 {
        resp_headers.insert(
            RESP_FULL_SIZE,
            HeaderValue::from_str(&full_size.to_string()).unwrap(),
        );
    }
    if let Some(ct) = &result.content_type {
        if let Ok(v) = HeaderValue::from_str(ct) {
            resp_headers.insert("content-type", v);
        }
    }
    if let Some(et) = &result.etag {
        if let Ok(v) = HeaderValue::from_str(et) {
            resp_headers.insert("etag", v);
        }
    }
    if let Some(lm) = &result.last_modified {
        if let Ok(v) = HeaderValue::from_str(lm) {
            resp_headers.insert("last-modified", v);
        }
    }
    if let Some(cc) = &result.cache_control {
        if let Ok(v) = HeaderValue::from_str(cc) {
            resp_headers.insert("cache-control", v);
        }
    }

    build_streaming_response(
        result.download,
        full_size,
        client_byte_range,
        resp_headers,
        false, // keep downloading for S3 upload even if client disconnects
    )
}

/// Build the HTTP response that streams data from in-flight chunk files.
///
/// When `cancel_on_drop` is true, the download is cancelled when the response
/// stream is dropped (e.g. client disconnects).  Use this for S3 cache-hit
/// reads where continuing is wasteful.  For upstream cache-miss downloads,
/// pass false so the S3 upload can complete.
fn build_streaming_response(
    download: Arc<InFlightDownload>,
    full_size: u64,
    client_range: Option<ByteRange>,
    mut extra_headers: HeaderMap,
    cancel_on_drop: bool,
) -> ProxyResult<Response> {
    let (serve_start, serve_end, status) = match client_range {
        Some(ref r) => (r.start, r.end_inclusive, StatusCode::PARTIAL_CONTENT),
        None => (0, full_size.saturating_sub(1), StatusCode::OK),
    };
    let serve_len = if full_size == 0 {
        0
    } else {
        serve_end - serve_start + 1
    };

    if status == StatusCode::PARTIAL_CONTENT {
        extra_headers.insert(
            "content-range",
            HeaderValue::from_str(&format!("bytes {serve_start}-{serve_end}/{full_size}"))
                .unwrap(),
        );
    }
    extra_headers.insert(
        "content-length",
        HeaderValue::from_str(&serve_len.to_string()).unwrap(),
    );
    extra_headers.insert("accept-ranges", HeaderValue::from_static("bytes"));

    let stream = make_download_stream(download, serve_start, serve_end, serve_len, cancel_on_drop);
    let body = Body::from_stream(stream);
    let mut response = Response::builder()
        .status(status)
        .body(body)
        .map_err(|e| ProxyError::Internal(format!("build response: {e}")))?;

    *response.headers_mut() = extra_headers;

    Ok(response)
}

/// Build a direct passthrough response for ENOSPC degradation.
/// Streams the upstream response body directly to the client without temp-file
/// buffering. Returns 200 OK with the full body (no range support in this mode).
fn build_passthrough_response(
    response: reqwest::Response,
    full_size: Option<u64>,
    content_type: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    cache_control: Option<String>,
) -> ProxyResult<Response> {
    let mut headers = HeaderMap::new();
    headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("false"));
    headers.insert(RESP_DEGRADED, HeaderValue::from_static("enospc"));
    if let Some(size) = full_size {
        headers.insert(
            RESP_FULL_SIZE,
            HeaderValue::from_str(&size.to_string()).unwrap(),
        );
        headers.insert(
            "content-length",
            HeaderValue::from_str(&size.to_string()).unwrap(),
        );
    }
    if let Some(ref ct) = content_type {
        if let Ok(v) = HeaderValue::from_str(ct) {
            headers.insert("content-type", v);
        }
    }
    if let Some(ref et) = etag {
        if let Ok(v) = HeaderValue::from_str(et) {
            headers.insert("etag", v);
        }
    }
    if let Some(ref lm) = last_modified {
        if let Ok(v) = HeaderValue::from_str(lm) {
            headers.insert("last-modified", v);
        }
    }
    if let Some(ref cc) = cache_control {
        if let Ok(v) = HeaderValue::from_str(cc) {
            headers.insert("cache-control", v);
        }
    }
    headers.insert("accept-ranges", HeaderValue::from_static("bytes"));

    let stream = response
        .bytes_stream()
        .map(|r| r.map_err(|e| ProxyError::Upstream(format!("passthrough: {e}"))));
    let body = Body::from_stream(stream);
    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .body(body)
        .map_err(|e| ProxyError::Internal(format!("build response: {e}")))?;
    *resp.headers_mut() = headers;
    Ok(resp)
}

fn make_download_stream(
    download: Arc<InFlightDownload>,
    serve_start: u64,
    serve_end: u64,
    serve_len: u64,
    cancel_on_drop: bool,
) -> impl futures::Stream<Item = Result<Bytes, ProxyError>> {
    try_stream! {
        // When this guard is dropped (stream finished or client disconnected),
        // the download is cancelled so workers stop fetching from S3.
        let _cancel_guard = if cancel_on_drop {
            Some(CancelGuard(download.clone()))
        } else {
            None
        };

        if serve_len == 0 {
            return;
        }

        let chunk_size = download.chunk_size;
        let mut pos = serve_start;

        while pos <= serve_end {
            let chunk_idx = (pos / chunk_size) as usize;
            if chunk_idx >= download.chunk_count() {
                break;
            }

            let chunk_offset = pos % chunk_size;
            let chunk_remaining = download.expected_chunk_len(chunk_idx) - chunk_offset;
            let serve_remaining = serve_end - pos + 1;
            let to_read = min(chunk_remaining, serve_remaining).min(256 * 1024);

            download.wait_for_bytes(chunk_idx, chunk_offset + to_read).await?;

            let file = download.chunk(chunk_idx).get_file()
                .ok_or_else(|| ProxyError::Internal("chunk released before read".into()))?;

            let read_len = to_read as usize;
            let data = tokio::task::spawn_blocking(move || -> Result<Bytes, std::io::Error> {
                use std::os::unix::fs::FileExt;
                let mut buf = vec![0u8; read_len];
                file.read_exact_at(&mut buf, chunk_offset)?;
                Ok(Bytes::from(buf))
            })
            .await
            .map_err(|e| ProxyError::Internal(format!("read task: {e}")))?
            .map_err(|e| ProxyError::Internal(format!("pread: {e}")))?;

            pos += data.len() as u64;
            yield data;
        }

        // Signal that all downstream readers are done for chunks we've consumed
        let first_chunk = (serve_start / chunk_size) as usize;
        let last_chunk = (serve_end / chunk_size) as usize;
        for idx in first_chunk..=last_chunk.min(download.chunk_count() - 1) {
            download.chunk(idx).readers_done.store(true, Ordering::Release);
            download.chunk(idx).try_release();
        }
    }
}
