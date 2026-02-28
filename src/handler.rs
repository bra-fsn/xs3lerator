use std::cmp::min;
use std::sync::Arc;

use async_stream::try_stream;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::json;
use axum::http::header::CONTENT_LENGTH;
use tracing::{debug, error, info, warn};

use crate::config::AppConfig;
use crate::disk_cache::DiskCache;
use crate::download::{InFlightDownload, DownloadManager};
use crate::error::{ProxyError, ProxyResult};
use crate::es_client::EsClient;
use crate::headers::{
    self, parse_upstream_url, parse_contract_headers, RESP_CACHE_HIT, RESP_DEGRADED, RESP_FULL_SIZE,
};
use crate::http_pool::HttpClientPool;
use crate::manifest::Manifest;
use crate::range::{parse_range_header, ByteRange};
use crate::s3_client::{self, S3Client};
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
    pub downloads: Arc<DownloadManager>,
    pub trace: Option<Arc<TraceWriter>>,
    pub es_client: Option<Arc<EsClient>>,
    pub http_pool: Arc<HttpClientPool>,
    pub s3: Option<Arc<S3Client>>,
    pub disk_cache: Option<Arc<DiskCache>>,
}

/// Health check endpoint: `GET /healthz`
pub async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Method-not-allowed handler for non-GET/HEAD/POST methods.
pub async fn method_not_allowed() -> impl IntoResponse {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [(axum::http::header::ALLOW, "GET, HEAD, POST")],
        "only GET, HEAD, and POST are supported",
    )
}

/// HEAD handler: pure passthrough to upstream, returns headers only.
///
/// No caching, no ES updates, no body — just forwards the HEAD to the
/// upstream URL and returns the response headers.  This lets callers
/// (e.g. passsage) benefit from xs3lerator's HTTP connection pool.
pub async fn handle_head(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Result<Response, ProxyError> {
    let uri = req.uri().clone();
    let headers = req.headers().clone();

    let upstream_url = parse_upstream_url(&uri)
        .ok_or_else(|| ProxyError::Internal("invalid path: expected /<upstream_url>".into()))?;

    let contract = parse_contract_headers(&headers);
    let skip_tls = state.config.upstream_tls_skip_verify || contract.tls_skip_verify;
    let http_client = state.http_pool
        .get(skip_tls, contract.follow_redirects)
        .map_err(|e| ProxyError::Internal(format!("get http client: {e}")))?;

    let upstream_headers = headers::filter_upstream_headers(&headers);
    let mut req_builder = http_client.head(&upstream_url);
    for (name, value) in upstream_headers.iter() {
        if let Ok(v) = value.to_str() {
            req_builder = req_builder.header(name.as_str(), v);
        }
    }

    let response = req_builder.send().await.map_err(|e| {
        ProxyError::Upstream(format!("upstream HEAD failed: {e}"))
    })?;

    let status = StatusCode::from_u16(response.status().as_u16())
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let mut resp_headers = HeaderMap::new();
    merge_upstream_headers(response.headers(), &mut resp_headers);

    let resp = Response::builder()
        .status(status)
        .body(Body::empty())
        .map_err(|e| ProxyError::Internal(format!("build HEAD response: {e}")))?;
    let mut resp = resp;
    *resp.headers_mut() = resp_headers;
    log_access("HEAD", &upstream_url, &resp);
    Ok(resp)
}

/// POST handler for manifest alias.
///
/// Headers:
///   - `X-Xs3lerator-Link-Manifest-Source`: source cache key
///   - `X-Xs3lerator-Link-Manifest-Target`: target cache key
///
/// Race-free Vary support: waits for in-flight download then copies manifest.
pub async fn handle_post(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Result<Response, ProxyError> {
    let headers = req.headers().clone();

    let source_key = headers
        .get("x-xs3lerator-link-manifest-source")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ProxyError::Internal("missing X-Xs3lerator-Link-Manifest-Source header".into())
        })?
        .to_string();

    let target_key = headers
        .get("x-xs3lerator-link-manifest-target")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ProxyError::Internal("missing X-Xs3lerator-Link-Manifest-Target header".into())
        })?
        .to_string();

    debug!(
        source = source_key,
        target = target_key,
        "manifest alias request"
    );

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

    let manifest = fetch_manifest(&state, &source_key).await?;

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

/// Main GET handler: `GET /<upstream_url>`
///
/// The upstream URL is extracted from the request path+query.
/// The optional cache key comes from the `X-Xs3lerator-Cache-Key` header.
pub async fn handle_get(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Result<Response, ProxyError> {
    let uri = req.uri().clone();
    let headers = req.headers().clone();

    let upstream_url = parse_upstream_url(&uri)
        .ok_or_else(|| ProxyError::Internal("invalid path: expected /<upstream_url>".into()))?;

    let contract = parse_contract_headers(&headers);
    let client_range_header = headers
        .get("range")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let cache_key = if state.config.passthrough {
        None
    } else {
        contract.cache_key.clone()
    };

    let log_key = cache_key.as_deref().unwrap_or(&upstream_url);

    trace_log(&state.trace, || json!({
        "event": "request",
        "upstream_url": upstream_url,
        "cache_key": cache_key,
        "cache_skip": contract.cache_skip,
        "object_size": contract.object_size,
    }));

    // No cache key → pure passthrough (download accelerator only)
    if cache_key.is_none() || contract.cache_skip {
        return handle_upstream_path(
            &state,
            &contract,
            &headers,
            &upstream_url,
            cache_key.as_deref(),
            client_range_header.as_deref(),
        )
        .await
        .map(|resp| { log_access("GET", log_key, &resp); resp });
    }

    let cache_key_str = cache_key.as_deref().unwrap();

    // If passsage supplied the manifest in the header, try to use it directly
    // to skip the ES lookup entirely.
    let header_manifest = contract.manifest_b64.as_deref().and_then(|b64| {
        match crate::es_client::decode_manifest_b64(b64) {
            Ok(m) => Some(Arc::new(m)),
            Err(e) => {
                debug!(cache_key = cache_key_str, error = %e, "header manifest decode failed, will fetch from ES");
                None
            }
        }
    });

    // Try to serve from cache first
    match handle_cache_hit(
        &state,
        cache_key_str,
        client_range_header.as_deref(),
        header_manifest,
    )
    .await
    {
        Ok(resp) => {
            log_access("GET", log_key, &resp);
            Ok(resp)
        }
        Err(e) => {
            debug!(
                cache_key = cache_key_str,
                error = %e,
                "cache fetch failed, falling back to upstream"
            );
            handle_upstream_path(
                &state,
                &contract,
                &headers,
                &upstream_url,
                cache_key.as_deref(),
                client_range_header.as_deref(),
            )
            .await
            .map(|resp| { log_access("GET", log_key, &resp); resp })
        }
    }
}

/// Serve from cache: try local disk first, fall back to streaming from S3
/// with concurrent disk backfill.
async fn handle_cache_hit(
    state: &AppState,
    cache_key: &str,
    client_range: Option<&str>,
    pre_manifest: Option<Arc<Manifest>>,
) -> ProxyResult<Response> {
    let manifest = match pre_manifest {
        Some(m) => m,
        None => fetch_manifest(state, cache_key).await?,
    };
    let object_size = manifest.total_size;

    let client_byte_range = parse_range_header(client_range, object_size)?;
    let (serve_start, serve_end) = match client_byte_range {
        Some(ref r) => (r.start, r.end_inclusive),
        None => (0, object_size.saturating_sub(1)),
    };

    let chunk_range = manifest.chunks_for_range(serve_start, serve_end);
    let data_prefix = state.config.data_prefix.clone();

    let s3 = state.s3.clone();
    let disk_cache = state.disk_cache.clone();
    let es_client = state.es_client.clone();

    let chunks_meta: Vec<(usize, [u8; 16], u64)> = chunk_range.clone().map(|idx| {
        let id = manifest.chunk_ids[idx];
        let chunk_len = if idx == manifest.num_chunks() - 1 {
            manifest.total_size - (idx as u64 * manifest.chunk_size)
        } else {
            manifest.chunk_size
        };
        (idx, id, chunk_len)
    }).collect();

    let num_chunks = chunks_meta.len();
    debug!(cache_key, num_chunks, object_size, "cache_hit: serving");

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

    let all_chunk_ids: Vec<[u8; 16]> = manifest.chunk_ids.clone();
    let cache_key_owned = cache_key.to_string();

    let stream = make_cache_hit_stream(
        chunks_meta,
        offset_in_first_chunk,
        serve_len,
        s3,
        disk_cache,
        es_client,
        data_prefix,
        cache_key_owned,
        all_chunk_ids,
    );
    let body = Body::from_stream(stream);
    let mut response = Response::builder()
        .status(status)
        .body(body)
        .map_err(|e| ProxyError::Internal(format!("build response: {e}")))?;

    *response.headers_mut() = resp_headers;
    Ok(response)
}

/// Stream cache-hit data: local disk when available, S3 streaming backfill otherwise.
/// On S3 miss, triggers cleanup and returns an error (caller falls through to upstream).
fn make_cache_hit_stream(
    chunks_meta: Vec<(usize, [u8; 16], u64)>,
    offset_in_first_chunk: u64,
    read_len: u64,
    s3: Option<Arc<S3Client>>,
    disk_cache: Option<Arc<DiskCache>>,
    es_client: Option<Arc<EsClient>>,
    data_prefix: String,
    cache_key: String,
    all_chunk_ids: Vec<[u8; 16]>,
) -> impl futures::Stream<Item = Result<Bytes, ProxyError>> {
    try_stream! {
        if read_len == 0 {
            return;
        }

        let pipeline_start = std::time::Instant::now();
        let mut remaining = read_len;
        let mut first = true;

        for (_idx, id, chunk_len) in &chunks_meta {
            let start_offset = if first {
                first = false;
                offset_in_first_chunk as usize
            } else {
                0
            };

            let chunk_len = *chunk_len as usize;

            // Try local disk first
            let local_path = disk_cache.as_ref().and_then(|dc| dc.lookup(id));

            if let Some(path) = local_path {
                // Local disk hit: read in 256 KiB pieces via spawn_blocking
                let mut pos = start_offset;
                let end = chunk_len.min(start_offset + remaining as usize);
                while pos < end {
                    let piece = min(end - pos, 256 * 1024);
                    let p = path.clone();
                    let offset = pos as u64;
                    let len = piece;
                    let data = tokio::task::spawn_blocking(move || -> Result<Bytes, std::io::Error> {
                        use std::os::unix::fs::FileExt;
                        let f = std::fs::File::open(&p)?;
                        let mut buf = vec![0u8; len];
                        f.read_exact_at(&mut buf, offset)?;
                        Ok(Bytes::from(buf))
                    })
                    .await
                    .map_err(|e| ProxyError::Internal(format!("read task: {e}")))?
                    .map_err(|e| ProxyError::Internal(format!("local read: {e}")))?;

                    remaining -= data.len() as u64;
                    pos += data.len();
                    yield data;
                }
            } else {
                // Local miss: stream from S3 with tee to disk cache
                let s3_ref = match s3.as_ref() {
                    Some(s) => s,
                    None => {
                        Err(ProxyError::NotFound(format!(
                            "chunk not in local cache and no S3 client configured"
                        )))?;
                        return;
                    }
                };
                let s3_stream = match s3_ref.get_chunk_stream(id, &data_prefix).await {
                    Ok(stream) => stream,
                    Err(ProxyError::NotFound(msg)) => {
                        error!(
                            cache_key = cache_key.as_str(),
                            chunk_id = crate::manifest::hex_encode_id(id),
                            "S3 chunk missing during cache hit: {msg}"
                        );
                        // Cleanup in background
                        let es = es_client.clone();
                        let s3c = s3.clone();
                        let ck = cache_key.clone();
                        let ids = all_chunk_ids.clone();
                        let prefix = data_prefix.clone();
                        tokio::spawn(async move {
                            if let (Some(es), Some(s3c)) = (es, s3c) {
                                s3_client::cleanup_corrupt_manifest(
                                    &es, &s3c, &ck, &ids, &prefix,
                                ).await;
                            }
                        });
                        Err(ProxyError::NotFound(msg))?;
                        return;
                    }
                    Err(e) => {
                        Err(e)?;
                        return;
                    }
                };

                // Set up disk backfill writer
                let backfill = disk_cache.as_ref().and_then(|dc| {
                    if dc.is_degraded() {
                        return None;
                    }
                    match dc.temp_file() {
                        Ok((file, path)) => Some((file, path, dc.clone(), *id)),
                        Err(e) => {
                            warn!("cache backfill temp file failed: {e}");
                            None
                        }
                    }
                });

                let (backfill_tx, backfill_handle) = if let Some((file, temp_path, dc, chunk_id)) = backfill {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(16);
                    let handle = tokio::task::spawn_blocking(move || {
                        use std::io::Write;
                        let mut writer = std::io::BufWriter::new(file);
                        while let Some(chunk) = rx.blocking_recv() {
                            if let Err(e) = writer.write_all(&chunk) {
                                warn!("backfill write failed: {e}");
                                return;
                            }
                        }
                        drop(writer);
                        if let Err(e) = dc.finalize(&temp_path, &chunk_id) {
                            warn!("backfill finalize failed: {e}");
                        }
                    });
                    (Some(tx), Some(handle))
                } else {
                    (None, None)
                };

                // Tee: stream S3 bytes to client + backfill writer
                let mut s3_stream = s3_stream;
                let mut s3_offset = 0usize;
                while let Some(piece_result) = s3_stream.next().await {
                    let piece = piece_result?;
                    let piece_len = piece.len();

                    // Send to backfill writer (non-blocking, drop if full)
                    if let Some(ref tx) = backfill_tx {
                        let _ = tx.try_send(piece.clone());
                    }

                    // Yield the client's slice
                    let piece_start = if s3_offset < start_offset {
                        let skip = start_offset - s3_offset;
                        if skip >= piece_len {
                            s3_offset += piece_len;
                            continue;
                        }
                        skip
                    } else {
                        0
                    };

                    let available = piece_len - piece_start;
                    let to_yield = min(available as u64, remaining) as usize;
                    if to_yield > 0 {
                        yield piece.slice(piece_start..piece_start + to_yield);
                        remaining -= to_yield as u64;
                    }
                    s3_offset += piece_len;
                }

                // Signal backfill writer to finish
                drop(backfill_tx);
                if let Some(handle) = backfill_handle {
                    let _ = handle.await;
                }
            }

            if remaining == 0 {
                break;
            }
        }

        let total_ms = pipeline_start.elapsed().as_millis() as u64;
        let served_bytes = read_len - remaining;
        debug!(
            cache_key = cache_key.as_str(),
            num_chunks = chunks_meta.len(),
            total_ms,
            served_bytes,
            "cache_hit: transfer complete"
        );
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

/// Serve from upstream (cache miss, passthrough, or fallback).
async fn handle_upstream_path(
    state: &AppState,
    contract: &headers::ContractHeaders,
    client_headers: &HeaderMap,
    upstream_url: &str,
    cache_key: Option<&str>,
    client_range: Option<&str>,
) -> ProxyResult<Response> {
    let t0 = std::time::Instant::now();
    let result = upstream_fetcher::fetch_upstream(
        &state.config,
        contract,
        client_headers,
        upstream_url,
        cache_key,
        client_range,
        &state.downloads,
        &state.trace,
        state.es_client.clone(),
        &state.http_pool,
        state.s3.clone(),
        state.disk_cache.clone(),
    )
    .await?;
    let fetch_ms = t0.elapsed().as_secs_f64() * 1000.0;
    debug!(
        upstream_url,
        fetch_ms = fetch_ms as u64,
        unknown_size = result.full_size.is_none(),
        "upstream fetch returned"
    );

    // Redirect passthrough
    if let Some(redirect_code) = result.redirect_status {
        let status = StatusCode::from_u16(redirect_code)
            .unwrap_or(StatusCode::FOUND);
        let mut resp_headers = HeaderMap::new();
        if let Some(ref rh) = result.redirect_headers {
            merge_upstream_headers(rh, &mut resp_headers);
        }
        if cache_key.is_some() {
            resp_headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("false"));
        }
        let resp = Response::builder()
            .status(status)
            .body(Body::empty())
            .map_err(|e| ProxyError::Internal(format!("build redirect response: {e}")))?;
        let mut resp = resp;
        *resp.headers_mut() = resp_headers;
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
        if cache_key.is_some() {
            resp_headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("false"));
        }

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

    // ENOSPC degradation
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

    // Unknown-size (chunked upstream)
    if result.full_size.is_none() {
        let mut resp_headers = HeaderMap::new();
        if let Some(ref uh) = result.upstream_headers {
            merge_upstream_headers(uh, &mut resp_headers);
        }
        if cache_key.is_some() {
            resp_headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("false"));
        }

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
    if cache_key.is_some() {
        resp_headers.insert(RESP_CACHE_HIT, HeaderValue::from_static("false"));
    }
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
        false,
        download_start,
    )
}

/// RAII guard that cancels an in-flight download when dropped.
struct CancelGuard(Arc<InFlightDownload>);

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// RAII guard that decrements reader_count for all not-yet-consumed chunks
/// when the client stream is dropped (normal completion or early disconnect).
struct ReaderGuard {
    download: Arc<InFlightDownload>,
    live_chunks: Vec<usize>,
}

impl ReaderGuard {
    fn new(download: Arc<InFlightDownload>, chunks: Vec<usize>) -> Self {
        for &idx in &chunks {
            download.chunk(idx).increment_readers();
        }
        Self { download, live_chunks: chunks }
    }

    fn consumed(&mut self, chunk_idx: usize) {
        if let Some(pos) = self.live_chunks.iter().position(|&c| c == chunk_idx) {
            self.live_chunks.swap_remove(pos);
            self.download.chunk(chunk_idx).decrement_reader();
        }
    }
}

impl Drop for ReaderGuard {
    fn drop(&mut self) {
        for &idx in &self.live_chunks {
            self.download.chunk(idx).decrement_reader();
        }
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

        let start_chunk = (download_start / chunk_size) as usize;
        let end_chunk = (read_end / chunk_size) as usize;
        let max_chunk = download.chunk_count().saturating_sub(1);
        let chunk_range: Vec<usize> = (start_chunk..=end_chunk.min(max_chunk)).collect();
        let mut reader_guard = ReaderGuard::new(download.clone(), chunk_range);

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
                    reader_guard.consumed(prev);
                    prev_chunk = Some(chunk_idx);
                }
            } else {
                prev_chunk = Some(chunk_idx);
            }

            let chunk_offset = pos % chunk_size;
            let chunk_remaining = download.expected_chunk_len(chunk_idx) - chunk_offset;
            let remaining = read_end - pos + 1;
            let to_read = min(chunk_remaining, remaining).min(256 * 1024);

            // Wait for bytes, then read from temp file
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
            reader_guard.consumed(last);
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
        let mut reader_guard = ReaderGuard::new(download.clone(), vec![]);

        loop {
            let chunk_idx = (pos / chunk_size) as usize;
            if chunk_idx >= download.chunk_count() {
                break;
            }

            if let Some(prev) = prev_chunk {
                if chunk_idx != prev {
                    download.notify_consumed(prev);
                    reader_guard.consumed(prev);
                    download.chunk(chunk_idx).increment_readers();
                    reader_guard.live_chunks.push(chunk_idx);
                    prev_chunk = Some(chunk_idx);
                }
            } else {
                download.chunk(chunk_idx).increment_readers();
                reader_guard.live_chunks.push(chunk_idx);
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
            reader_guard.consumed(last);
        }
    }
}
