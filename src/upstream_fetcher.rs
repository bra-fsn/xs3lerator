use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use reqwest::header::{ACCEPT_RANGES, CONTENT_LENGTH, TRANSFER_ENCODING};
use serde_json::json;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::download::{
    create_temp_chunk_file, is_enospc, pwrite_all, DownloadManager, InFlightDownload,
};
use crate::error::ProxyError;
use crate::headers::{filter_upstream_headers, ContractHeaders};
use crate::planner::compute_chunk_plan;
use crate::range::parse_range_header;
use crate::s3::S3Uploader;
use crate::s3_upload::spawn_s3_upload;
use crate::trace::{trace_log, TraceWriter};

/// Fetch a file from an upstream HTTP(S) URL, buffer through per-chunk temp
/// files, stream the result to the client, and simultaneously upload to S3.
///
/// Returns the [`InFlightDownload`] that the handler can stream from, along
/// with metadata about the upstream response.
pub struct UpstreamResult {
    pub download: Arc<InFlightDownload>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub cache_control: Option<String>,
    pub full_size: Option<u64>,
    /// When set, temp storage was full (ENOSPC). The handler should stream this
    /// response body directly to the client without buffering, and set the
    /// `X-Xs3lerator-Degraded: enospc` header. No S3 upload occurs.
    pub degraded_body: Option<reqwest::Response>,
}

/// Start an upstream download, creating an `InFlightDownload` that the handler
/// can stream from.
pub async fn fetch_upstream(
    config: &AppConfig,
    contract: &ContractHeaders,
    client_headers: &axum::http::HeaderMap,
    s3_bucket: &str,
    s3_key: &str,
    cache_key: &str,
    client_range: Option<&str>,
    downloads: &DownloadManager,
    s3_uploader: Option<Arc<S3Uploader>>,
    trace: &Option<Arc<TraceWriter>>,
) -> Result<UpstreamResult, ProxyError> {
    let upstream_url = contract
        .upstream_url
        .as_deref()
        .ok_or_else(|| ProxyError::Internal("missing X-Xs3lerator-Upstream-Url".into()))?;

    // Check for an existing in-flight download for the same cache key.
    // This handles the deduplication case within a single instance.
    {
        let (existing, is_new) = downloads.get_or_create(cache_key, || {
            // Placeholder — will be replaced below if we're the creator.
            Arc::new(InFlightDownload::new(0, config.min_chunk_size))
        });
        if !is_new {
            let full_size = existing.object_size;
            return Ok(UpstreamResult {
                download: existing,
                content_type: None,
                etag: None,
                last_modified: None,
                cache_control: None,
                full_size: Some(full_size),
                degraded_body: None,
            });
        }
        // We created a placeholder — remove it so we can insert the real one.
        downloads.remove(cache_key);
    }

    // Build the reqwest client
    let skip_tls = config.upstream_tls_skip_verify || contract.tls_skip_verify;
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(skip_tls)
        .build()
        .map_err(|e| ProxyError::Internal(format!("build http client: {e}")))?;

    // Build filtered headers for the upstream request
    let upstream_headers = filter_upstream_headers(client_headers);
    let mut req_builder = http_client.get(upstream_url);
    for (name, value) in upstream_headers.iter() {
        if let Ok(v) = value.to_str() {
            req_builder = req_builder.header(name.as_str(), v);
        }
    }

    trace_log(trace, || json!({
        "event": "upstream_fetch_start",
        "url": upstream_url,
        "tls_skip_verify": skip_tls,
    }));

    let t0 = Instant::now();
    let response = req_builder.send().await.map_err(|e| {
        ProxyError::Upstream(format!("upstream request failed: {e}"))
    })?;

    let status = response.status();
    if !status.is_success() {
        return Err(ProxyError::Upstream(format!(
            "upstream returned {status}"
        )));
    }

    // Extract response metadata
    let resp_headers = response.headers().clone();
    let content_type = resp_headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let etag = resp_headers
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let last_modified = resp_headers
        .get("last-modified")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let cache_control = resp_headers
        .get("cache-control")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let content_length: Option<u64> = resp_headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    let accept_ranges = resp_headers
        .get(ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("bytes"))
        .unwrap_or(false);
    let is_chunked = resp_headers
        .get(TRANSFER_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("chunked"))
        .unwrap_or(false);

    let can_parallel = content_length.is_some() && accept_ranges && !is_chunked;

    trace_log(trace, || json!({
        "event": "upstream_response",
        "status": status.as_u16(),
        "content_length": content_length,
        "accept_ranges": accept_ranges,
        "chunked": is_chunked,
        "can_parallel": can_parallel,
        "latency_ms": t0.elapsed().as_secs_f64() * 1000.0,
    }));

    if let Some(file_size) = content_length {
        if can_parallel && file_size > config.min_chunk_size {
            return start_parallel_upstream_download(
                config,
                upstream_url,
                &http_client,
                &upstream_headers,
                response,
                file_size,
                cache_key,
                s3_bucket,
                s3_key,
                downloads,
                s3_uploader,
                trace,
                client_range,
                content_type,
                etag,
                last_modified,
                cache_control,
            )
            .await;
        }
        // Known size but can't parallelize — sequential with known size
        return start_sequential_download(
            config,
            response,
            Some(file_size),
            cache_key,
            s3_bucket,
            s3_key,
            downloads,
            s3_uploader,
            trace,
            content_type,
            etag,
            last_modified,
            cache_control,
        )
        .await;
    }

    // Chunked / unknown size — sequential
    start_sequential_download(
        config,
        response,
        None,
        cache_key,
        s3_bucket,
        s3_key,
        downloads,
        s3_uploader,
        trace,
        content_type,
        etag,
        last_modified,
        cache_control,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_parallel_upstream_download(
    config: &AppConfig,
    upstream_url: &str,
    http_client: &reqwest::Client,
    upstream_headers: &axum::http::HeaderMap,
    initial_response: reqwest::Response,
    file_size: u64,
    cache_key: &str,
    s3_bucket: &str,
    s3_key: &str,
    downloads: &DownloadManager,
    s3_uploader: Option<Arc<S3Uploader>>,
    trace: &Option<Arc<TraceWriter>>,
    client_range: Option<&str>,
    content_type: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    cache_control: Option<String>,
) -> Result<UpstreamResult, ProxyError> {
    let plan = compute_chunk_plan(file_size, config.http_concurrency, config.min_chunk_size);

    let download = Arc::new(InFlightDownload::new(file_size, plan.chunk_size));
    let (download, is_new) = downloads.get_or_create(cache_key, || download);
    if !is_new {
        return Ok(UpstreamResult {
            download,
            content_type,
            etag,
            last_modified,
            cache_control,
            full_size: Some(file_size),
            degraded_body: None,
        });
    }

    // Probe temp dir for available space; if ENOSPC, degrade to direct passthrough
    if let Err(e) = create_temp_chunk_file(&config.temp_dir) {
        downloads.remove(cache_key);
        if is_enospc(&e) {
            warn!("ENOSPC: degrading to direct passthrough (parallel)");
            return Ok(UpstreamResult {
                download: Arc::new(InFlightDownload::new(0, config.min_chunk_size)),
                content_type,
                etag,
                last_modified,
                cache_control,
                full_size: Some(file_size),
                degraded_body: Some(initial_response),
            });
        }
        return Err(ProxyError::Internal(format!("temp dir unavailable: {e}")));
    }

    // Prioritize client's requested range so those chunks are fetched first
    if let Some(range_str) = client_range {
        if let Ok(Some(byte_range)) = parse_range_header(Some(range_str), file_size) {
            download.prioritize_range(byte_range.start, byte_range.end_inclusive);
        }
    }

    // Spawn S3 upload if requested
    if let Some(uploader) = s3_uploader {
        spawn_s3_upload(
            uploader,
            s3_bucket.to_string(),
            s3_key.to_string(),
            download.clone(),
        );
    }

    let temp_dir = config.temp_dir.clone();
    let dl = download.clone();
    let trace_clone = trace.clone();
    let url = upstream_url.to_string();
    let client = http_client.clone();
    let hdrs = upstream_headers.clone();
    let ck = cache_key.to_string();
    let dm_ptr = downloads as *const DownloadManager as usize;

    // Spawn the adaptive download workers
    tokio::spawn(async move {
        let downloads = unsafe { &*(dm_ptr as *const DownloadManager) };
        let result = run_adaptive_upstream(
            &temp_dir, &url, &client, &hdrs, initial_response,
            &dl, &trace_clone,
        )
        .await;

        match result {
            Ok(()) => {
                info!(key = ck, "upstream download complete");
            }
            Err(e) => {
                tracing::error!(key = ck, "upstream download failed: {e}");
                dl.mark_failed();
            }
        }
        downloads.remove(&ck);
    });

    Ok(UpstreamResult {
        download,
        content_type,
        etag,
        last_modified,
        cache_control,
        full_size: Some(file_size),
        degraded_body: None,
    })
}

/// Adaptive upstream download strategy:
///
/// 1. Pre-create temp files for all chunks.
/// 2. Start streaming the initial full-GET response into chunks sequentially
///    (chunk 0, then 1, 2, …).
/// 3. Simultaneously probe one range-GET for the first priority chunk.
/// 4. If the probe succeeds (2xx), convert to parallel: tell the sequential
///    stream to stop after its current chunk and spawn range-GET workers for
///    the rest.
/// 5. If the probe fails (403, connection error, etc.), let the sequential
///    stream continue through all chunks — the server lied about Accept-Ranges.
async fn run_adaptive_upstream(
    temp_dir: &std::path::Path,
    upstream_url: &str,
    http_client: &reqwest::Client,
    upstream_headers: &axum::http::HeaderMap,
    initial_response: reqwest::Response,
    download: &InFlightDownload,
    trace: &Option<Arc<TraceWriter>>,
) -> Result<(), ProxyError> {
    // Pre-create temp files for all chunks so the sequential stream can use them.
    for idx in 0..download.chunk_count() {
        let file = create_temp_chunk_file(temp_dir)
            .map_err(|e| ProxyError::Internal(format!("create temp file: {e}")))?;
        download.chunk(idx).set_file(Arc::new(file));
    }

    // stop_after controls how far the sequential stream progresses.
    // usize::MAX means "fill all chunks"; a specific index means "stop after
    // finishing that chunk" so parallel workers can take over.
    let stop_after = Arc::new(AtomicUsize::new(usize::MAX));

    // Spawn the sequential stream from the initial full-GET response.
    let stop_clone = stop_after.clone();
    let dl_ptr = download as *const InFlightDownload as usize;
    let trace_seq = trace.clone();
    let seq_handle = tokio::spawn(async move {
        let download = unsafe { &*(dl_ptr as *const InFlightDownload) };
        stream_response_into_chunks(initial_response, download, stop_clone, &trace_seq).await
    });

    // Drain remaining chunks in priority order (chunk 0 handled by sequential).
    let remaining: Vec<usize> = std::iter::from_fn(|| download.pop_chunk())
        .filter(|&idx| idx != 0)
        .collect();

    let mut parallel_handles: Vec<tokio::task::JoinHandle<Result<(), ProxyError>>> = Vec::new();

    if !remaining.is_empty() {
        // Probe: send one range-GET for the first priority chunk.
        let probe_idx = remaining[0];
        let (probe_start, probe_end) = download.chunk_byte_range(probe_idx);

        let probe_resp = {
            let mut req = http_client.get(upstream_url);
            for (name, value) in upstream_headers.iter() {
                if let Ok(v) = value.to_str() {
                    req = req.header(name.as_str(), v);
                }
            }
            req = req.header("Range", format!("bytes={probe_start}-{probe_end}"));
            req.send().await
        };

        match probe_resp {
            Ok(resp) if resp.status().is_success() || resp.status().as_u16() == 206 => {
                trace_log(trace, || json!({
                    "event": "range_probe_ok",
                    "probe_chunk": probe_idx,
                    "status": resp.status().as_u16(),
                }));

                // Tell sequential to stop after its current chunk.
                let seq_chunk = active_sequential_chunk(download);
                stop_after.store(seq_chunk, Ordering::Release);
                info!(
                    seq_chunk,
                    probe_chunk = probe_idx,
                    "range probe succeeded, converting to parallel"
                );

                // Download probe chunk from the already-open response.
                let probe_file = download.chunk(probe_idx).get_file()
                    .ok_or_else(|| ProxyError::Internal("probe chunk file gone".into()))?;
                let dl_ptr2 = download as *const InFlightDownload as usize;
                let expected = download.expected_chunk_len(probe_idx);
                let trace_c = trace.clone();
                parallel_handles.push(tokio::spawn(async move {
                    let download = unsafe { &*(dl_ptr2 as *const InFlightDownload) };
                    download_chunk_from_stream(
                        resp, &probe_file, download, probe_idx, expected, &trace_c,
                    )
                    .await
                }));

                // Start range-GETs for remaining chunks not covered by sequential.
                for &idx in &remaining[1..] {
                    if idx <= seq_chunk || download.is_chunk_done(idx) {
                        continue;
                    }
                    let (start, end) = download.chunk_byte_range(idx);
                    let chunk_file = download.chunk(idx).get_file()
                        .ok_or_else(|| ProxyError::Internal(format!("chunk {idx} file gone")))?;
                    let client = http_client.clone();
                    let url = upstream_url.to_string();
                    let hdrs = upstream_headers.clone();
                    let dl_ptr3 = download as *const InFlightDownload as usize;
                    let expected = download.expected_chunk_len(idx);
                    let trace_c = trace.clone();

                    parallel_handles.push(tokio::spawn(async move {
                        let download = unsafe { &*(dl_ptr3 as *const InFlightDownload) };
                        let mut req = client.get(&url);
                        for (name, value) in hdrs.iter() {
                            if let Ok(v) = value.to_str() {
                                req = req.header(name.as_str(), v);
                            }
                        }
                        req = req.header("Range", format!("bytes={start}-{end}"));
                        let response = req.send().await.map_err(|e| {
                            ProxyError::Upstream(format!("range request chunk {idx}: {e}"))
                        })?;
                        if !response.status().is_success() && response.status().as_u16() != 206 {
                            return Err(ProxyError::Upstream(format!(
                                "range request chunk {idx} returned {}",
                                response.status()
                            )));
                        }
                        download_chunk_from_stream(
                            response, &chunk_file, download, idx, expected, &trace_c,
                        )
                        .await
                    }));
                }
            }
            Ok(resp) => {
                trace_log(trace, || json!({
                    "event": "range_probe_rejected",
                    "probe_chunk": probe_idx,
                    "status": resp.status().as_u16(),
                }));
                info!(
                    status = resp.status().as_u16(),
                    "range probe rejected, continuing sequential"
                );
            }
            Err(e) => {
                warn!("range probe connection failed: {e}, continuing sequential");
            }
        }
    }

    // Wait for the sequential stream to finish.
    match seq_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            if parallel_handles.is_empty() {
                download.mark_failed();
                return Err(e);
            }
            warn!("sequential stream errored (parallel active): {e}");
        }
        Err(e) => {
            download.mark_failed();
            return Err(ProxyError::Internal(format!("sequential task panicked: {e}")));
        }
    }

    // Wait for parallel workers.
    for handle in parallel_handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                download.mark_failed();
                return Err(e);
            }
            Err(e) => {
                download.mark_failed();
                return Err(ProxyError::Internal(format!("worker panicked: {e}")));
            }
        }
    }

    // Mark any unfinished chunks as done (handles short responses gracefully).
    for idx in 0..download.chunk_count() {
        if !download.is_chunk_done(idx) && !download.has_failed() {
            download.mark_chunk_done(idx);
        }
    }

    Ok(())
}

/// Find which chunk the sequential stream is currently writing to.
fn active_sequential_chunk(download: &InFlightDownload) -> usize {
    for idx in 0..download.chunk_count() {
        if download.chunk(idx).bytes_written() < download.expected_chunk_len(idx) {
            return idx;
        }
    }
    download.chunk_count().saturating_sub(1)
}

/// Stream a full-GET response body into sequential chunks, stopping after the
/// chunk indicated by `stop_after`.  This is the "initial connection" stream
/// that keeps running unless parallel workers take over.
async fn stream_response_into_chunks(
    response: reqwest::Response,
    download: &InFlightDownload,
    stop_after: Arc<AtomicUsize>,
    trace: &Option<Arc<TraceWriter>>,
) -> Result<(), ProxyError> {
    let mut stream = response.bytes_stream();
    let mut global_offset = 0u64;

    while let Some(piece) = stream.next().await {
        let data = piece.map_err(|e| ProxyError::Upstream(format!("sequential stream: {e}")))?;
        let chunk_idx = (global_offset / download.chunk_size) as usize;

        if chunk_idx > stop_after.load(Ordering::Acquire) {
            trace_log(trace, || json!({
                "event": "sequential_stopped",
                "at_chunk": chunk_idx,
            }));
            break;
        }
        if chunk_idx >= download.chunk_count() {
            break;
        }

        let chunk_offset = global_offset - chunk_idx as u64 * download.chunk_size;
        let to_write =
            std::cmp::min(data.len() as u64, download.expected_chunk_len(chunk_idx) - chunk_offset)
                as usize;
        if to_write == 0 {
            continue;
        }

        let file = download
            .chunk(chunk_idx)
            .get_file()
            .ok_or_else(|| ProxyError::Internal("chunk file released during sequential".into()))?;

        pwrite_all(&file, chunk_offset, &data[..to_write])
            .map_err(|e| ProxyError::Internal(format!("pwrite chunk {chunk_idx}: {e}")))?;

        global_offset += to_write as u64;
        download.record_written(chunk_idx, to_write as u64);

        if download.is_chunk_done(chunk_idx) {
            download.mark_chunk_done(chunk_idx);
            trace_log(trace, || json!({
                "event": "sequential_chunk_done",
                "chunk": chunk_idx,
            }));
        }
    }

    Ok(())
}

async fn download_chunk_from_stream(
    response: reqwest::Response,
    file: &std::fs::File,
    download: &InFlightDownload,
    idx: usize,
    expected: u64,
    trace: &Option<Arc<TraceWriter>>,
) -> Result<(), ProxyError> {
    let t0 = Instant::now();
    let mut stream = response.bytes_stream();
    let mut offset = 0u64;

    while let Some(piece) = stream.next().await {
        let piece = piece.map_err(|e| ProxyError::Upstream(format!("stream chunk {idx}: {e}")))?;
        let to_write = std::cmp::min(piece.len() as u64, expected - offset) as usize;
        if to_write == 0 {
            break;
        }
        pwrite_all(file, offset, &piece[..to_write])
            .map_err(|e| ProxyError::Internal(format!("pwrite chunk {idx}: {e}")))?;
        offset += to_write as u64;
        download.record_written(idx, to_write as u64);
    }

    download.mark_chunk_done(idx);

    trace_log(trace, || json!({
        "event": "upstream_chunk_done",
        "chunk": idx,
        "bytes": offset,
        "elapsed_ms": t0.elapsed().as_secs_f64() * 1000.0,
    }));

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn start_sequential_download(
    config: &AppConfig,
    response: reqwest::Response,
    file_size: Option<u64>,
    cache_key: &str,
    s3_bucket: &str,
    s3_key: &str,
    downloads: &DownloadManager,
    s3_uploader: Option<Arc<S3Uploader>>,
    _trace: &Option<Arc<TraceWriter>>,
    content_type: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    cache_control: Option<String>,
) -> Result<UpstreamResult, ProxyError> {
    let effective_size = file_size.unwrap_or(config.min_chunk_size);
    let chunk_size = config.min_chunk_size;

    let download = Arc::new(InFlightDownload::new(effective_size, chunk_size));
    let (download, is_new) = downloads.get_or_create(cache_key, || download);
    if !is_new {
        return Ok(UpstreamResult {
            download,
            content_type,
            etag,
            last_modified,
            cache_control,
            full_size: file_size,
            degraded_body: None,
        });
    }

    // Probe temp dir for available space; if ENOSPC, degrade to direct passthrough
    if let Err(e) = create_temp_chunk_file(&config.temp_dir) {
        downloads.remove(cache_key);
        if is_enospc(&e) {
            warn!("ENOSPC: degrading to direct passthrough (sequential)");
            return Ok(UpstreamResult {
                download: Arc::new(InFlightDownload::new(0, config.min_chunk_size)),
                content_type,
                etag,
                last_modified,
                cache_control,
                full_size: file_size,
                degraded_body: Some(response),
            });
        }
        return Err(ProxyError::Internal(format!("temp dir unavailable: {e}")));
    }

    if let Some(uploader) = s3_uploader {
        if file_size.is_some() {
            spawn_s3_upload(
                uploader,
                s3_bucket.to_string(),
                s3_key.to_string(),
                download.clone(),
            );
        }
    }

    // Create temp files for all chunks
    for idx in 0..download.chunk_count() {
        let chunk_file = create_temp_chunk_file(&config.temp_dir)
            .map_err(|e| ProxyError::Internal(format!("create temp file: {e}")))?;
        download.chunk(idx).set_file(Arc::new(chunk_file));
    }

    let dl = download.clone();
    let ck = cache_key.to_string();
    let dm_ptr = downloads as *const DownloadManager as usize;

    tokio::spawn(async move {
        let downloads = unsafe { &*(dm_ptr as *const DownloadManager) };
        let mut stream = response.bytes_stream();
        let mut global_offset = 0u64;

        while let Some(piece) = stream.next().await {
            match piece {
                Ok(data) => {
                    let chunk_idx = (global_offset / dl.chunk_size) as usize;
                    if chunk_idx >= dl.chunk_count() {
                        break;
                    }
                    let chunk_offset = global_offset - chunk_idx as u64 * dl.chunk_size;

                    if let Some(file) = dl.chunk(chunk_idx).get_file() {
                        let to_write = std::cmp::min(
                            data.len() as u64,
                            dl.expected_chunk_len(chunk_idx) - chunk_offset,
                        ) as usize;
                        if to_write > 0 {
                            if let Err(e) = pwrite_all(&file, chunk_offset, &data[..to_write]) {
                                warn!(key = ck, "sequential write error: {e}");
                                dl.mark_failed();
                                break;
                            }
                            global_offset += to_write as u64;
                            dl.record_written(chunk_idx, to_write as u64);

                            if dl.is_chunk_done(chunk_idx) {
                                dl.mark_chunk_done(chunk_idx);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(key = ck, "sequential stream error: {e}");
                    dl.mark_failed();
                    break;
                }
            }
        }

        // Mark any incomplete chunks as done (for short responses)
        for idx in 0..dl.chunk_count() {
            if !dl.is_chunk_done(idx) && !dl.has_failed() {
                dl.mark_chunk_done(idx);
            }
        }

        downloads.remove(&ck);
    });

    Ok(UpstreamResult {
        download,
        content_type,
        etag,
        last_modified,
        cache_control,
        full_size: file_size,
        degraded_body: None,
    })
}
