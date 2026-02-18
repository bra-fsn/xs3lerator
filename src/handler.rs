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

use crate::chunk_cache::ChunkCache;
use crate::chunk_upload;
use crate::config::AppConfig;
use crate::download::{create_temp_chunk_file, pwrite_all, DownloadManager, InFlightDownload};
use crate::error::{ProxyError, ProxyResult};
use crate::headers::{
    self, parse_bucket_key, parse_contract_headers, RESP_CACHE_HIT, RESP_DEGRADED, RESP_FULL_SIZE,
};
use crate::manifest::{Manifest, ManifestCache};
use crate::range::{parse_range_header, ByteRange};
use crate::s3::{AwsUpstream, S3Uploader};
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
    pub manifest_cache: Arc<ManifestCache>,
    pub chunk_cache: Option<Arc<ChunkCache>>,
}

/// Health check endpoint: `GET /healthz`
pub async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Method-not-allowed handler for non-GET/POST methods.
pub async fn method_not_allowed() -> impl IntoResponse {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [(axum::http::header::ALLOW, "GET, POST")],
        "only GET and POST are supported",
    )
}

/// POST handler for manifest alias: `POST /{bucket}/{target_key}`
/// with `X-Xs3lerator-Link-Manifest: {source_key}`.
/// Race-free Vary support: waits for in-flight download then copies manifest.
pub async fn handle_post(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Result<Response, ProxyError> {
    let path = req.uri().path().to_string();
    let headers = req.headers().clone();

    let (bucket, target_key) = parse_bucket_key(&path)
        .ok_or_else(|| ProxyError::Internal("invalid path: expected /<bucket>/<key>".into()))?;

    let source_key = headers
        .get("x-xs3lerator-link-manifest")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ProxyError::Internal("missing X-Xs3lerator-Link-Manifest header".into())
        })?;

    let source_cache_key = format!("{}/{}", bucket, source_key);
    let target_cache_key = format!("{}/{}", bucket, target_key);

    info!(
        source = source_cache_key,
        target = target_cache_key,
        "manifest alias request"
    );

    // Wait for any in-flight download of the source key to complete its S3 upload
    {
        let active = state.downloads.get_inflight(&source_cache_key);
        if let Some(inflight) = active {
            info!(
                source = source_cache_key,
                "waiting for in-flight download to complete manifest write"
            );
            inflight.wait_for_s3_complete().await;
        }
    }

    // Read source manifest (LRU or S3)
    let manifest = fetch_manifest(&state, &bucket, &source_cache_key).await?;

    // Write manifest under target key
    let manifest_bytes = manifest.serialize();
    chunk_upload::put_manifest(
        &state.s3_uploader,
        &bucket,
        &state.config.map_prefix,
        &target_cache_key,
        manifest_bytes,
    )
    .await?;

    // Cache under target key in LRU
    state
        .manifest_cache
        .insert(target_cache_key.clone(), manifest);

    info!(target = target_cache_key, "manifest alias created");

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
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
        state.manifest_cache.evict(&cache_key);
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

/// Serve from S3 cache using manifest-based chunk serving.
/// 1. Fetch manifest from LRU cache or S3 at `_map/{key}`
/// 2. Compute affected chunks for client range
/// 3. Fetch chunks concurrently from local cache then `data/` prefix
async fn handle_s3_path(
    state: &AppState,
    _contract: &headers::ContractHeaders,
    bucket: &str,
    key: &str,
    cache_key: &str,
    client_range: Option<&str>,
) -> ProxyResult<Response> {
    let manifest = fetch_manifest(state, bucket, cache_key).await?;
    let object_size = manifest.total_size;

    let client_byte_range = parse_range_header(client_range, object_size)?;
    let (serve_start, serve_end) = match client_byte_range {
        Some(ref r) => (r.start, r.end_inclusive),
        None => (0, object_size.saturating_sub(1)),
    };

    let chunk_range = manifest.chunks_for_range(serve_start, serve_end);

    let first_chunk_start = chunk_range.start as u64 * manifest.chunk_size;
    let last_chunk_end = if chunk_range.end >= manifest.num_chunks() {
        manifest.total_size
    } else {
        chunk_range.end as u64 * manifest.chunk_size
    };
    let chunk_data_size = last_chunk_end - first_chunk_start;
    let offset_in_chunks = serve_start - first_chunk_start;

    let download = Arc::new(InFlightDownload::new(chunk_data_size, manifest.chunk_size));
    let ck = format!("{cache_key}:{serve_start}-{serve_end}");

    let (download, is_new) = state.downloads.get_or_create(&ck, || download);
    download.mark_no_upload_needed();

    if is_new {
        let dl = download.clone();
        let s3 = state.s3_upstream.clone();
        let b = bucket.to_string();
        let k = key.to_string();
        let temp_dir = state.config.temp_dir.clone();
        let trace = state.trace.clone();
        let dm = state.downloads.clone();
        let s3_concurrency = state.config.s3_concurrency;
        let manifest = manifest.clone();
        let data_prefix = state.config.data_prefix.clone();
        let chunk_cache = state.chunk_cache.clone();
        let ck_owned = ck.clone();

        tokio::spawn(async move {
            let result = run_manifest_download(
                &s3,
                &b,
                &manifest,
                &data_prefix,
                chunk_range,
                serve_start,
                &dl,
                &temp_dir,
                &trace,
                s3_concurrency,
                chunk_cache.as_deref(),
            )
            .await;

            match result {
                Ok(()) => {
                    info!(key = k, "manifest-based download complete");
                }
                Err(e) => {
                    error!(key = k, "manifest-based download failed: {e}");
                    dl.mark_failed();
                }
            }
            dm.remove(&ck_owned);
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
        true,
        offset_in_chunks,
    )
}

/// Fetch manifest from LRU cache or S3. Caches in LRU on success.
async fn fetch_manifest(
    state: &AppState,
    bucket: &str,
    cache_key: &str,
) -> ProxyResult<Arc<Manifest>> {
    if let Some(cached) = state.manifest_cache.get(cache_key) {
        return Ok(cached);
    }

    let manifest = chunk_upload::get_manifest(
        &state.s3_uploader,
        bucket,
        &state.config.map_prefix,
        cache_key,
    )
    .await?
    .ok_or_else(|| ProxyError::NotFound(format!("no manifest for {cache_key}")))?;

    let arc = Arc::new(manifest);
    state
        .manifest_cache
        .insert(cache_key.to_string(), arc.clone());
    Ok(arc)
}

/// Download chunks identified by a manifest, fetching from local cache then S3 `data/` prefix.
async fn run_manifest_download(
    s3: &AwsUpstream,
    bucket: &str,
    manifest: &Manifest,
    data_prefix: &str,
    chunk_range: std::ops::Range<usize>,
    _serve_start: u64,
    download: &InFlightDownload,
    temp_dir: &std::path::Path,
    trace: &Option<Arc<TraceWriter>>,
    max_concurrency: usize,
    chunk_cache: Option<&ChunkCache>,
) -> ProxyResult<()> {
    let chunk_indices: Vec<usize> = (0..download.chunk_count()).collect();
    let manifest_chunk_indices: Vec<usize> = chunk_range.collect();

    let num_workers = max_concurrency.min(chunk_indices.len().max(1));
    let cursor = Arc::new(AtomicUsize::new(0));
    let chunks = Arc::new(chunk_indices);
    let manifest_chunks = Arc::new(manifest_chunk_indices);
    let mut handles = Vec::with_capacity(num_workers);

    for _ in 0..num_workers {
        let cursor = cursor.clone();
        let chunks = chunks.clone();
        let manifest_chunks = manifest_chunks.clone();
        let s3 = s3.clone();
        let b = bucket.to_string();
        let temp_dir = temp_dir.to_path_buf();
        let dl_ptr = download as *const InFlightDownload as usize;
        let trace_c = trace.clone();
        let data_prefix = data_prefix.to_string();
        let _chunk_size = manifest.chunk_size;

        // Build hash list for the chunks we need
        let hashes: Vec<[u8; 32]> = manifest_chunks
            .iter()
            .map(|&mi| manifest.hashes[mi])
            .collect();

        let chunk_cache_ptr = chunk_cache.map(|c| c as *const ChunkCache as usize);

        handles.push(tokio::spawn(async move {
            let download = unsafe { &*(dl_ptr as *const InFlightDownload) };
            let chunk_cache: Option<&ChunkCache> =
                chunk_cache_ptr.map(|p| unsafe { &*(p as *const ChunkCache) });

            loop {
                if download.is_cancelled() {
                    break;
                }

                let pos = cursor.fetch_add(1, Ordering::Relaxed);
                if pos >= chunks.len() {
                    break;
                }
                let local_idx = chunks[pos];
                let manifest_idx = manifest_chunks[pos];
                let hash = &hashes[pos];

                download
                    .wait_for_consumer_window(local_idx, S3_PREFETCH_WINDOW)
                    .await;
                if download.is_cancelled() {
                    break;
                }

                // Try local chunk cache first
                if let Some(cc) = chunk_cache {
                    if let Some(file) = cc.get(hash) {
                        let file = Arc::new(file);
                        download.chunk(local_idx).set_file(file);
                        download.mark_chunk_done(local_idx);
                        continue;
                    }
                }

                // Fetch from S3 data/ prefix
                let s3_key = crate::manifest::hash_to_s3_key(hash, &data_prefix);

                let chunk_file = create_temp_chunk_file(&temp_dir).map_err(|e| {
                    ProxyError::Internal(format!("create temp file: {e}"))
                })?;
                let chunk_file = Arc::new(chunk_file);
                download.chunk(local_idx).set_file(chunk_file.clone());

                let expected = download.expected_chunk_len(local_idx);
                let mut body = s3.get_range_stream(&b, &s3_key, 0, expected - 1).await?;
                let mut offset = 0u64;
                while let Some(piece) = body.next().await {
                    if download.is_cancelled() {
                        break;
                    }
                    let data = piece.map_err(|e| {
                        ProxyError::Upstream(format!("s3 stream chunk {local_idx}: {e}"))
                    })?;
                    let to_write = min(data.len() as u64, expected - offset) as usize;
                    if to_write == 0 {
                        break;
                    }
                    pwrite_all(&chunk_file, offset, &data[..to_write]).map_err(|e| {
                        ProxyError::Internal(format!("pwrite chunk {local_idx}: {e}"))
                    })?;
                    offset += to_write as u64;
                    download.record_written(local_idx, to_write as u64);
                }
                if !download.is_cancelled() {
                    download.mark_chunk_done(local_idx);
                }

                trace_log(&trace_c, || {
                    json!({
                        "event": "manifest_chunk_done",
                        "local_idx": local_idx,
                        "manifest_idx": manifest_idx,
                        "bytes": offset,
                    })
                });
            }
            Ok::<(), ProxyError>(())
        }));
    }

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(ProxyError::Internal(format!("worker panicked: {e}"))),
        }
    }

    Ok(())
}

/// Prefetch window: workers stay at most this many chunks ahead of the
/// consumer.  Keeps page-cache pressure bounded so the kernel doesn't
/// throttle on dirty-page writeback (critical on EBS-backed instances).
const S3_PREFETCH_WINDOW: usize = 16;

/// Headers that should not be forwarded from upstream to the client.
const UPSTREAM_STRIP_HEADERS: &[&str] = &[
    "connection",
    "transfer-encoding",
    "content-length",
    "content-range",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "upgrade",
];

/// Merge upstream response headers into an axum HeaderMap, stripping
/// hop-by-hop headers, xs3lerator contract headers, and headers that
/// xs3lerator manages itself (Content-Length, Content-Range, etc).
fn merge_upstream_headers(
    upstream: &reqwest::header::HeaderMap,
    out: &mut HeaderMap,
) {
    for (name, value) in upstream.iter() {
        let key = name.as_str().to_lowercase();
        if key.starts_with(headers::CONTRACT_PREFIX) {
            continue;
        }
        if UPSTREAM_STRIP_HEADERS.contains(&key.as_str()) {
            continue;
        }
        if let Ok(hname) = axum::http::header::HeaderName::from_bytes(name.as_str().as_bytes()) {
            if let Ok(hval) = axum::http::HeaderValue::from_bytes(value.as_bytes()) {
                out.append(hname, hval);
            }
        }
    }
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
        Some(state.manifest_cache.clone()),
        state.chunk_cache.clone(),
    )
    .await?;

    // Redirect passthrough: upstream returned a 3xx and follow_redirects was
    // false.  Return the redirect response verbatim — no S3 caching, no body.
    if let Some(redirect_code) = result.redirect_status {
        let status = StatusCode::from_u16(redirect_code)
            .unwrap_or(StatusCode::FOUND);
        let mut resp_headers = HeaderMap::new();
        if let Some(ref rh) = result.redirect_headers {
            merge_upstream_headers(rh, &mut resp_headers);
        }
        resp_headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("false"));
        let resp = Response::builder()
            .status(status)
            .body(Body::empty())
            .map_err(|e| ProxyError::Internal(format!("build redirect response: {e}")))?;
        let mut resp = resp;
        *resp.headers_mut() = resp_headers;
        return Ok(resp);
    }

    // Error passthrough: upstream returned a non-2xx status. Stream the error
    // response body to the client with the original status code intact.
    if let Some((status_code, error_response)) = result.error_passthrough {
        let status = StatusCode::from_u16(status_code)
            .unwrap_or(StatusCode::BAD_GATEWAY);
        let mut resp_headers = HeaderMap::new();
        if let Some(ref uh) = result.upstream_headers {
            merge_upstream_headers(uh, &mut resp_headers);
        }
        resp_headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("false"));

        let stream = error_response
            .bytes_stream()
            .map(|r| r.map_err(|e| ProxyError::Upstream(format!("error passthrough: {e}"))));
        let body = Body::from_stream(stream);
        let mut resp = Response::builder()
            .status(status)
            .body(body)
            .map_err(|e| ProxyError::Internal(format!("build error response: {e}")))?;
        *resp.headers_mut() = resp_headers;
        return Ok(resp);
    }

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

    // Unknown-size (chunked upstream): stream from the download without
    // Content-Length.  The downstream transport uses chunked encoding.
    // No range support in this mode.
    if result.full_size.is_none() {
        let mut resp_headers = HeaderMap::new();
        if let Some(ref uh) = result.upstream_headers {
            merge_upstream_headers(uh, &mut resp_headers);
        }
        resp_headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("false"));

        let stream = make_unknown_size_stream(result.download);
        let body = Body::from_stream(stream);
        let mut resp = Response::builder()
            .status(StatusCode::OK)
            .body(body)
            .map_err(|e| ProxyError::Internal(format!("build chunked response: {e}")))?;
        *resp.headers_mut() = resp_headers;
        return Ok(resp);
    }

    let full_size = result.full_size.unwrap_or(result.download.object_size);
    let client_byte_range = if full_size > 0 {
        parse_range_header(client_range, full_size)?
    } else {
        None
    };

    let mut resp_headers = HeaderMap::new();
    if let Some(ref uh) = result.upstream_headers {
        merge_upstream_headers(uh, &mut resp_headers);
    }
    resp_headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("false"));
    if full_size > 0 {
        resp_headers.insert(
            RESP_FULL_SIZE,
            HeaderValue::from_str(&full_size.to_string()).unwrap(),
        );
    }

    let download_start = client_byte_range.as_ref().map_or(0, |r| r.start);
    build_streaming_response(
        result.download,
        full_size,
        client_byte_range,
        resp_headers,
        false, // keep downloading for S3 upload even if client disconnects
        download_start, // download contains the full file
    )
}

/// Build the HTTP response that streams data from in-flight chunk files.
///
/// When `cancel_on_drop` is true, the download is cancelled when the response
/// stream is dropped (e.g. client disconnects).  Use this for S3 cache-hit
/// reads where continuing is wasteful.  For upstream cache-miss downloads,
/// pass false so the S3 upload can complete.
///
/// `download_start` is the byte offset within the download's byte space where
/// reading begins. For upstream downloads (which contain the full file), this
/// equals `serve_start`. For S3 cache-hit range requests (which only contain
/// the requested range), this is 0.
fn build_streaming_response(
    download: Arc<InFlightDownload>,
    full_size: u64,
    client_range: Option<ByteRange>,
    mut extra_headers: HeaderMap,
    cancel_on_drop: bool,
    download_start: u64,
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

    let stream = make_download_stream(download, download_start, serve_len, cancel_on_drop);
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
    download_start: u64,
    read_len: u64,
    cancel_on_drop: bool,
) -> impl futures::Stream<Item = Result<Bytes, ProxyError>> {
    try_stream! {
        let _cancel_guard = if cancel_on_drop {
            Some(CancelGuard(download.clone()))
        } else {
            None
        };

        if read_len == 0 {
            return;
        }

        let chunk_size = download.chunk_size;
        let read_end = download_start + read_len - 1;
        let mut pos = download_start;
        let mut prev_chunk: Option<usize> = None;

        while pos <= read_end {
            let chunk_idx = (pos / chunk_size) as usize;
            if chunk_idx >= download.chunk_count() {
                break;
            }

            if let Some(prev) = prev_chunk {
                if chunk_idx != prev {
                    download.notify_consumed(prev);
                    prev_chunk = Some(chunk_idx);
                }
            } else {
                prev_chunk = Some(chunk_idx);
            }

            let chunk_offset = pos % chunk_size;
            let chunk_remaining = download.expected_chunk_len(chunk_idx) - chunk_offset;
            let remaining = read_end - pos + 1;
            let to_read = min(chunk_remaining, remaining).min(256 * 1024);

            download.wait_for_bytes(chunk_idx, chunk_offset + to_read).await?;

            let file = download.chunk(chunk_idx).get_file()
                .ok_or_else(|| ProxyError::Internal("chunk released before read".into()))?;

            let buf_len = to_read as usize;
            let data = tokio::task::spawn_blocking(move || -> Result<Bytes, std::io::Error> {
                use std::os::unix::fs::FileExt;
                let mut buf = vec![0u8; buf_len];
                file.read_exact_at(&mut buf, chunk_offset)?;
                Ok(Bytes::from(buf))
            })
            .await
            .map_err(|e| ProxyError::Internal(format!("read task: {e}")))?
            .map_err(|e| ProxyError::Internal(format!("pread: {e}")))?;

            pos += data.len() as u64;
            yield data;
        }

        if let Some(last) = prev_chunk {
            download.notify_consumed(last);
        }
    }
}

/// Stream data from an unknown-size download (chunked upstream).
/// Reads until `stream_complete` is set and all written bytes are consumed.
/// Does NOT use a fixed read length — adapts to whatever `wait_for_bytes`
/// reports as available.
fn make_unknown_size_stream(
    download: Arc<InFlightDownload>,
) -> impl futures::Stream<Item = Result<Bytes, ProxyError>> {
    try_stream! {
        let chunk_size = download.chunk_size;
        let mut pos = 0u64;
        let mut prev_chunk: Option<usize> = None;

        loop {
            let chunk_idx = (pos / chunk_size) as usize;
            if chunk_idx >= download.chunk_count() {
                break;
            }

            if let Some(prev) = prev_chunk {
                if chunk_idx != prev {
                    download.notify_consumed(prev);
                    prev_chunk = Some(chunk_idx);
                }
            } else {
                prev_chunk = Some(chunk_idx);
            }

            let chunk_offset = pos % chunk_size;
            let want = (256 * 1024) as u64;

            let available = download
                .wait_for_bytes(chunk_idx, chunk_offset + want)
                .await?;
            let readable = available.saturating_sub(chunk_offset);
            if readable == 0 {
                if download.is_stream_complete() {
                    break;
                }
                pos = (chunk_idx as u64 + 1) * chunk_size;
                continue;
            }
            let to_read = min(readable, want) as usize;

            let file = download.chunk(chunk_idx).get_file()
                .ok_or_else(|| ProxyError::Internal("chunk released before read".into()))?;

            let offset = chunk_offset;
            let data = tokio::task::spawn_blocking(move || -> Result<Bytes, std::io::Error> {
                use std::os::unix::fs::FileExt;
                let mut buf = vec![0u8; to_read];
                file.read_exact_at(&mut buf, offset)?;
                Ok(Bytes::from(buf))
            })
            .await
            .map_err(|e| ProxyError::Internal(format!("read task: {e}")))?
            .map_err(|e| ProxyError::Internal(format!("pread: {e}")))?;

            pos += data.len() as u64;
            yield data;
        }

        if let Some(last) = prev_chunk {
            download.notify_consumed(last);
        }
    }
}
