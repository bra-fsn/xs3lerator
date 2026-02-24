use std::cmp::min;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;

use async_stream::try_stream;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::StreamExt;
use nix::fcntl::{posix_fadvise, PosixFadviseAdvice};
use serde_json::json;
use axum::http::header::CONTENT_LENGTH;
use tracing::{debug, info};

use crate::config::AppConfig;
use crate::download::{InFlightDownload, DownloadManager};
use crate::error::{ProxyError, ProxyResult};
use crate::es_client::EsClient;
use crate::headers::{
    self, parse_key, parse_contract_headers, RESP_CACHE_HIT, RESP_DEGRADED, RESP_FULL_SIZE,
};
use crate::manifest::{Manifest, hash_to_chunk_path};
use crate::range::{parse_range_header, ByteRange};
use crate::trace::{trace_log, TraceWriter};
use crate::upstream_fetcher;

/// Log a single access-log style line when a response is sent.
fn log_access(method: &str, key: &str, response: &Response) {
    let status = response.status().as_u16();
    let size = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");
    info!(method, key, status, size, "access");
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub data_dir: PathBuf,
    pub downloads: Arc<DownloadManager>,
    pub trace: Option<Arc<TraceWriter>>,
    pub es_client: Option<Arc<EsClient>>,
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

/// POST handler for manifest alias: `POST /{target_key}`
/// with `X-Xs3lerator-Link-Manifest: {source_key}`.
/// Race-free Vary support: waits for in-flight download then copies manifest.
pub async fn handle_post(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Result<Response, ProxyError> {
    let path = req.uri().path().to_string();
    let headers = req.headers().clone();

    let target_key = parse_key(&path)
        .ok_or_else(|| ProxyError::Internal("invalid path: expected /<key>".into()))?;

    let source_key = headers
        .get("x-xs3lerator-link-manifest")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ProxyError::Internal("missing X-Xs3lerator-Link-Manifest header".into())
        })?
        .to_string();

    debug!(
        source = source_key,
        target = target_key,
        "manifest alias request"
    );

    // Wait for any in-flight download of the source key to complete its persist
    {
        let active = state.downloads.get_inflight(&source_key);
        if let Some(inflight) = active {
            debug!(
                source = source_key,
                "waiting for in-flight download to complete manifest write"
            );
            inflight.wait_for_s3_complete().await;
        }
    }

    // Read source manifest from ES
    let manifest = fetch_manifest(&state, &source_key).await?;

    // Write manifest under target key to ES
    let manifest_bytes = manifest.serialize();
    if let Some(ref es) = state.es_client {
        es.put_manifest(&target_key, manifest_bytes).await?;
    }

    debug!(target = target_key, "manifest alias created");

    let resp = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap();
    log_access("POST", &target_key, &resp);
    Ok(resp)
}

/// Main GET handler: `GET /<key...>`
pub async fn handle_get(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Result<Response, ProxyError> {
    let path = req.uri().path().to_string();
    let headers = req.headers().clone();

    let key = parse_key(&path)
        .ok_or_else(|| ProxyError::Internal("invalid path: expected /<key>".into()))?;

    let contract = parse_contract_headers(&headers);
    let client_range_header = headers
        .get("range")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    trace_log(&state.trace, || json!({
        "event": "request",
        "key": key,
        "cache_skip": contract.cache_skip,
        "object_size": contract.object_size,
    }));

    if contract.cache_skip {
        return handle_upstream_path(
            &state,
            &contract,
            &headers,
            &key,
            client_range_header.as_deref(),
        )
        .await;
    }

    // Try to serve from cache first
    match handle_cache_hit(
        &state,
        &contract,
        &key,
        client_range_header.as_deref(),
    )
    .await
    {
        Ok(resp) => Ok(resp),
        Err(e) => {
            debug!(
                key = key.as_str(),
                error = %e,
                "cache fetch failed, falling back to upstream"
            );
            handle_upstream_path(
                &state,
                &contract,
                &headers,
                &key,
                client_range_header.as_deref(),
            )
            .await
        }
    }
}

/// Serve from cache: open chunk files directly from data_dir with
/// posix_fadvise prefetching.
async fn handle_cache_hit(
    state: &AppState,
    _contract: &headers::ContractHeaders,
    key: &str,
    client_range: Option<&str>,
) -> ProxyResult<Response> {
    let manifest = fetch_manifest(state, key).await?;
    let object_size = manifest.total_size;

    let client_byte_range = parse_range_header(client_range, object_size)?;
    let (serve_start, serve_end) = match client_byte_range {
        Some(ref r) => (r.start, r.end_inclusive),
        None => (0, object_size.saturating_sub(1)),
    };

    let chunk_range = manifest.chunks_for_range(serve_start, serve_end);
    let data_prefix = &state.config.data_prefix;

    // Open all needed chunk files and issue posix_fadvise WILLNEED
    let mut chunk_files: Vec<(usize, Arc<std::fs::File>, u64)> = Vec::new();
    for idx in chunk_range.clone() {
        let hash = &manifest.hashes[idx];
        let rel_path = hash_to_chunk_path(hash, data_prefix);
        let full_path = state.data_dir.join(&rel_path);
        let file = std::fs::File::open(&full_path).map_err(|e| {
            ProxyError::NotFound(format!("chunk file {}: {e}", full_path.display()))
        })?;
        let chunk_len = if idx == manifest.num_chunks() - 1 {
            manifest.total_size - (idx as u64 * manifest.chunk_size)
        } else {
            manifest.chunk_size
        };
        chunk_files.push((idx, Arc::new(file), chunk_len));
    }

    // Issue WILLNEED advice on all chunk files to trigger mount-s3 prefetch
    for (_, file, chunk_len) in &chunk_files {
        let _ = posix_fadvise(
            file.as_raw_fd(),
            0,
            *chunk_len as i64,
            PosixFadviseAdvice::POSIX_FADV_WILLNEED,
        );
    }

    let first_chunk_start = chunk_range.start as u64 * manifest.chunk_size;
    let offset_in_first_chunk = serve_start - first_chunk_start;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("true"));
    resp_headers.insert(
        RESP_FULL_SIZE,
        HeaderValue::from_str(&object_size.to_string()).unwrap(),
    );

    let serve_len = if object_size == 0 {
        0
    } else {
        serve_end - serve_start + 1
    };

    let status = if client_byte_range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    if status == StatusCode::PARTIAL_CONTENT {
        resp_headers.insert(
            "content-range",
            HeaderValue::from_str(&format!("bytes {serve_start}-{serve_end}/{object_size}"))
                .unwrap(),
        );
    }
    resp_headers.insert(
        "content-length",
        HeaderValue::from_str(&serve_len.to_string()).unwrap(),
    );
    resp_headers.insert("accept-ranges", HeaderValue::from_static("bytes"));

    let stream = make_file_stream(chunk_files, offset_in_first_chunk, serve_len);
    let body = Body::from_stream(stream);
    let mut response = Response::builder()
        .status(status)
        .body(body)
        .map_err(|e| ProxyError::Internal(format!("build response: {e}")))?;

    *response.headers_mut() = resp_headers;
    log_access("GET", key, &response);
    Ok(response)
}

/// Stream data directly from opened chunk files.
fn make_file_stream(
    chunk_files: Vec<(usize, Arc<std::fs::File>, u64)>,
    offset_in_first_chunk: u64,
    read_len: u64,
) -> impl futures::Stream<Item = Result<Bytes, ProxyError>> {
    try_stream! {
        if read_len == 0 {
            return;
        }

        let mut remaining = read_len;
        let mut first = true;

        for (_idx, file, chunk_len) in &chunk_files {
            let start_offset = if first {
                first = false;
                offset_in_first_chunk
            } else {
                0
            };

            let readable_in_chunk = chunk_len - start_offset;
            let to_read_from_chunk = min(readable_in_chunk, remaining);
            let mut pos = start_offset;
            let mut left = to_read_from_chunk;

            while left > 0 {
                let buf_size = min(left, 256 * 1024) as usize;
                let file_clone = file.clone();
                let offset = pos;

                let data = tokio::task::spawn_blocking(move || -> Result<Bytes, std::io::Error> {
                    use std::os::unix::fs::FileExt;
                    let mut buf = vec![0u8; buf_size];
                    file_clone.read_exact_at(&mut buf, offset)?;
                    Ok(Bytes::from(buf))
                })
                .await
                .map_err(|e| ProxyError::Internal(format!("read task: {e}")))?
                .map_err(|e| ProxyError::Internal(format!("pread: {e}")))?;

                let n = data.len() as u64;
                pos += n;
                left -= n;
                remaining -= n;
                yield data;
            }

            if remaining == 0 {
                break;
            }
        }
    }
}

/// Fetch manifest from Elasticsearch.
async fn fetch_manifest(
    state: &AppState,
    cache_key: &str,
) -> ProxyResult<Arc<Manifest>> {
    let manifest = if let Some(ref es) = state.es_client {
        es.get_manifest(cache_key).await?
    } else {
        None
    };

    let manifest: Manifest = manifest
        .ok_or_else(|| ProxyError::NotFound(format!("no manifest for {cache_key}")))?;

    Ok(Arc::new(manifest))
}

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

/// Merge upstream response headers into an axum HeaderMap.
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

/// Serve from upstream (cache miss or fallback).
async fn handle_upstream_path(
    state: &AppState,
    contract: &headers::ContractHeaders,
    client_headers: &HeaderMap,
    key: &str,
    client_range: Option<&str>,
) -> ProxyResult<Response> {
    let result = upstream_fetcher::fetch_upstream(
        &state.config,
        contract,
        client_headers,
        key,
        client_range,
        &state.downloads,
        &state.data_dir,
        &state.trace,
        state.es_client.clone(),
    )
    .await?;

    // Redirect passthrough
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
        log_access("GET", key, &resp);
        return Ok(resp);
    }

    // Error passthrough
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
        log_access("GET", key, &resp);
        return Ok(resp);
    }

    // ENOSPC degradation
    if let Some(direct_response) = result.degraded_body {
        let resp = build_passthrough_response(
            direct_response,
            result.full_size,
            result.content_type,
            result.etag,
            result.last_modified,
            result.cache_control,
        )?;
        log_access("GET", key, &resp);
        return Ok(resp);
    }

    // Unknown-size (chunked upstream)
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
        log_access("GET", key, &resp);
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
    let resp = build_streaming_response(
        result.download,
        full_size,
        client_byte_range,
        resp_headers,
        false,
        download_start,
    )?;
    log_access("GET", key, &resp);
    Ok(resp)
}

/// RAII guard that cancels an in-flight download when dropped.
struct CancelGuard(Arc<InFlightDownload>);

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Build the HTTP response that streams data from in-flight chunk files.
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
