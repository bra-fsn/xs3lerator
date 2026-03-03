use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use reqwest::header::{ACCEPT_RANGES, CONTENT_LENGTH, TRANSFER_ENCODING};
use serde_json::json;
use tracing::{debug, warn};

use crate::config::AppConfig;
use crate::disk_cache::DiskCache;
use crate::download::{
    create_temp_chunk_file, generate_chunk_ids, is_enospc, pwrite_all,
    DownloadManager, InFlightDownload,
};
use crate::error::ProxyError;
use crate::fdb_client::FdbClient;
use crate::finalize;
use crate::headers::{filter_upstream_headers, ContractHeaders};
use crate::http_pool::HttpClientPool;
use crate::planner::{compute_chunk_plan, ConcurrencyRamp};
use crate::range::parse_range_header;
use crate::s3_client::S3Client;
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
    /// When set, the upstream returned a 3xx redirect and follow_redirects was
    /// false.  The handler should return this status + headers verbatim to the
    /// caller (no S3 caching, no body).
    pub redirect_status: Option<u16>,
    pub redirect_headers: Option<reqwest::header::HeaderMap>,
    /// Full upstream response headers for forwarding to the client.
    pub upstream_headers: Option<reqwest::header::HeaderMap>,
    /// When set, upstream returned a non-2xx/non-3xx error. The handler should
    /// stream this response body with the original status code (no S3 caching).
    pub error_passthrough: Option<(u16, reqwest::Response)>,
    /// When true, upstream returned 304 Not Modified in response to a
    /// conditional GET (If-None-Match / If-Modified-Since). No body was
    /// downloaded — the handler should serve from the existing S3 cache.
    pub revalidated: bool,
}

/// Start an upstream download, creating an `InFlightDownload` that the handler
/// can stream from.
pub async fn fetch_upstream(
    config: &AppConfig,
    contract: &ContractHeaders,
    client_headers: &axum::http::HeaderMap,
    upstream_url: &str,
    cache_key: Option<&str>,
    client_range: Option<&str>,
    downloads: &Arc<DownloadManager>,
    trace: &Option<Arc<TraceWriter>>,
    fdb_client: Option<Arc<FdbClient>>,
    http_pool: &HttpClientPool,
    s3_client: Option<Arc<S3Client>>,
    disk_cache: Option<Arc<DiskCache>>,
) -> Result<UpstreamResult, ProxyError> {
    // Check for an existing in-flight download for the same cache key.
    // This handles the deduplication case within a single instance.
    // When cache_skip is set the caller explicitly wants a fresh upstream
    // fetch (e.g. AlwaysUpstream / no-store), so bypass the dedup check.
    // When cache_key is None (passthrough), no dedup is possible.
    if let Some(key) = cache_key {
        if !contract.cache_skip {
            let (existing, is_new) = downloads.get_or_create(key, || {
                Arc::new(InFlightDownload::new_placeholder(config.chunk_size))
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
                    redirect_status: None,
                    redirect_headers: None,
                    upstream_headers: None,
                    error_passthrough: None,
                    revalidated: false,
                });
            }
            // We created a placeholder — remove it so we can insert the real one.
            downloads.remove(key);
        }
    }

    // Get a pooled reqwest client (connections are reused across requests)
    let skip_tls = config.upstream_tls_skip_verify || contract.tls_skip_verify;
    let connect_timeout = contract.connect_timeout.unwrap_or(config.upstream_connect_timeout);
    let read_timeout = match contract.read_timeout {
        Some(d) if d.is_zero() => None,
        Some(d) => Some(d),
        None => config.upstream_read_timeout,
    };
    let http_client = http_pool
        .get(skip_tls, contract.follow_redirects, connect_timeout, read_timeout)
        .map_err(|e| ProxyError::Internal(format!("get http client: {e}")))?;

    // Build filtered headers for the upstream request
    let upstream_headers = filter_upstream_headers(client_headers);
    let mut req_builder = http_client.get(upstream_url);
    for (name, value) in upstream_headers.iter() {
        if let Ok(v) = value.to_str() {
            req_builder = req_builder.header(name.as_str(), v);
        }
    }

    // Add conditional revalidation headers from the contract
    if let Some(ref etag) = contract.if_none_match {
        req_builder = req_builder.header("if-none-match", etag.as_str());
    }
    if let Some(ref last_mod) = contract.if_modified_since {
        req_builder = req_builder.header("if-modified-since", last_mod.as_str());
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
    let upstream_ttfb_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let status = response.status();
    debug!(
        upstream_url,
        status = status.as_u16(),
        upstream_ttfb_ms = upstream_ttfb_ms as u64,
        "upstream response received"
    );

    // 304 Not Modified: upstream confirmed cached content is still valid.
    // Return immediately — no body to download.
    if status.as_u16() == 304 {
        debug!(upstream_url, "upstream returned 304 Not Modified");
        return Ok(UpstreamResult {
            download: Arc::new(InFlightDownload::new_placeholder(config.chunk_size)),
            content_type: None,
            etag: None,
            last_modified: None,
            cache_control: None,
            full_size: None,
            degraded_body: None,
            redirect_status: None,
            redirect_headers: None,
            upstream_headers: Some(response.headers().clone()),
            error_passthrough: None,
            revalidated: true,
        });
    }

    if status.is_redirection() && !contract.follow_redirects {
        let redirect_headers = response.headers().clone();
        debug!(
            status = status.as_u16(),
            location = redirect_headers.get("location").and_then(|v| v.to_str().ok()).unwrap_or(""),
            "upstream returned redirect, passing through to client"
        );
        return Ok(UpstreamResult {
            download: Arc::new(InFlightDownload::new_placeholder(config.chunk_size)),
            content_type: None,
            etag: None,
            last_modified: None,
            cache_control: None,
            full_size: None,
            degraded_body: None,
            redirect_status: Some(status.as_u16()),
            redirect_headers: Some(redirect_headers),
            upstream_headers: None,
            error_passthrough: None,
            revalidated: false,
        });
    }
    if !status.is_success() {
        let resp_headers = response.headers().clone();
        let status_code = status.as_u16();
        warn!(status = status_code, "upstream returned error, passing through");
        return Ok(UpstreamResult {
            download: Arc::new(InFlightDownload::new_placeholder(config.chunk_size)),
            content_type: None,
            etag: None,
            last_modified: None,
            cache_control: None,
            full_size: None,
            degraded_body: None,
            redirect_status: None,
            redirect_headers: None,
            upstream_headers: Some(resp_headers),
            error_passthrough: Some((status_code, response)),
            revalidated: false,
        });
    }

    // Extract response metadata
    let resp_headers = response.headers().clone();
    let all_upstream_headers = resp_headers.clone();
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
        if can_parallel && file_size > config.chunk_size {
            return start_parallel_upstream_download(
                config,
                upstream_url,
                &http_client,
                &upstream_headers,
                response,
                file_size,
                cache_key,
                downloads,
                trace,
                client_range,
                content_type,
                etag,
                last_modified,
                cache_control,
                Some(all_upstream_headers),
                fdb_client.clone(),
                s3_client.clone(),
                disk_cache.clone(),
            )
            .await;
        }
        // Known size but can't parallelize — sequential with known size
        return start_sequential_download(
            config,
            response,
            Some(file_size),
            cache_key,
            downloads,
            trace,
            content_type,
            etag,
            last_modified,
            cache_control,
            Some(all_upstream_headers),
            fdb_client,
            s3_client,
            disk_cache,
        )
        .await;
    }

    // Chunked / unknown size — buffer through sequential download
    debug!("upstream response is chunked/unknown size, using sequential download");
    start_sequential_download(
        config,
        response,
        None,
        cache_key,
        downloads,
        trace,
        content_type,
        etag,
        last_modified,
        cache_control,
        Some(all_upstream_headers),
        fdb_client,
        s3_client,
        disk_cache,
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
    cache_key: Option<&str>,
    downloads: &Arc<DownloadManager>,
    trace: &Option<Arc<TraceWriter>>,
    client_range: Option<&str>,
    content_type: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    cache_control: Option<String>,
    all_upstream_headers: Option<reqwest::header::HeaderMap>,
    fdb_client: Option<Arc<FdbClient>>,
    s3_client: Option<Arc<S3Client>>,
    disk_cache: Option<Arc<DiskCache>>,
) -> Result<UpstreamResult, ProxyError> {
    let plan = compute_chunk_plan(file_size, config.http_concurrency, config.chunk_size);

    let num_chunks = file_size.div_ceil(plan.chunk_size) as usize;
    let chunk_ids = generate_chunk_ids(num_chunks);
    let download = Arc::new(InFlightDownload::new(file_size, plan.chunk_size, chunk_ids));

    // When we have a cache key, register in the download manager for dedup.
    // Otherwise just use the download directly with no dedup.
    let download = if let Some(key) = cache_key {
        let (download, is_new) = downloads.get_or_create(key, || download);
        if !is_new {
            return Ok(UpstreamResult {
                download,
                content_type,
                etag,
                last_modified,
                cache_control,
                full_size: Some(file_size),
                degraded_body: None,
                redirect_status: None,
                redirect_headers: None,
                upstream_headers: all_upstream_headers,
                error_passthrough: None,
                revalidated: false,
            });
        }
        download
    } else {
        download
    };

    // Probe temp dir for available space; if ENOSPC, degrade to direct passthrough
    if let Err(e) = create_temp_chunk_file(&config.temp_dir) {
        if let Some(key) = cache_key {
            downloads.remove(key);
        }
        if is_enospc(&e) {
            warn!("ENOSPC: degrading to direct passthrough (parallel)");
            return Ok(UpstreamResult {
                download: Arc::new(InFlightDownload::new_placeholder(config.chunk_size)),
                content_type,
                etag,
                last_modified,
                cache_control,
                full_size: Some(file_size),
                degraded_body: Some(initial_response),
                redirect_status: None,
                redirect_headers: None,
                upstream_headers: all_upstream_headers,
                error_passthrough: None,
                revalidated: false,
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

    if let Some(key) = cache_key {
        finalize::spawn_finalize(
            key.to_string(),
            download.clone(),
            fdb_client,
        );
    }

    let temp_dir = config.temp_dir.clone();
    let dl = download.clone();
    let trace_clone = trace.clone();
    let url = upstream_url.to_string();
    let client = http_client.clone();
    let hdrs = upstream_headers.clone();
    let ck = cache_key.map(str::to_owned);
    let dm = downloads.clone();
    let http_concurrency = config.http_concurrency;
    let data_prefix_clone = config.data_prefix.clone();
    let caching = cache_key.is_some();
    let s3c = s3_client.clone();
    let dc = disk_cache.clone();

    // Spawn the adaptive download workers
    tokio::spawn(async move {
        let result = run_adaptive_upstream(
            &temp_dir, &url, &client, &hdrs, initial_response,
            &dl, &trace_clone, http_concurrency,
            &data_prefix_clone, caching, s3c.as_ref(), dc.as_ref(),
        )
        .await;

        match result {
            Ok(()) => {
                debug!(cache_key = ?ck, "upstream download complete");
            }
            Err(e) => {
                tracing::error!(cache_key = ?ck, "upstream download failed: {e}");
                dl.mark_failed();
            }
        }
        if let Some(ref key) = ck {
            dl.wait_for_s3_complete().await;
            dm.remove(key);
        }
    });

    Ok(UpstreamResult {
        download,
        content_type,
        etag,
        last_modified,
        cache_control,
        full_size: Some(file_size),
        degraded_body: None,
        redirect_status: None,
        redirect_headers: None,
        upstream_headers: all_upstream_headers,
        error_passthrough: None,
        revalidated: false,
    })
}

/// Adaptive upstream download strategy:
///
/// 1. Pre-create temp files for all chunks.
/// 2. Start streaming the initial full-GET response into chunks sequentially
///    (chunk 0, then 1, 2, …).
/// 3. Compute a sequential *runway*: the sequential stream handles the next
///    `max_concurrency` chunks at full single-connection speed, giving the
///    client smooth, uninterrupted data delivery.
/// 4. Send a range-GET probe for the first chunk *beyond* the runway.
/// 5. If the probe succeeds (2xx), set `stop_after` to the runway end and
///    spawn persistent worker tasks that pull chunks **in order** from a
///    shared cursor (aria2-style).  Workers reuse HTTP connections via
///    reqwest's pool.
/// 6. If the probe fails (403, connection error, etc.), let the sequential
///    stream continue through all chunks — the server lied about Accept-Ranges.
async fn run_adaptive_upstream(
    temp_dir: &std::path::Path,
    upstream_url: &str,
    http_client: &reqwest::Client,
    upstream_headers: &axum::http::HeaderMap,
    initial_response: reqwest::Response,
    download: &Arc<InFlightDownload>,
    trace: &Option<Arc<TraceWriter>>,
    max_concurrency: usize,
    data_prefix: &str,
    caching: bool,
    s3_client: Option<&Arc<S3Client>>,
    disk_cache: Option<&Arc<DiskCache>>,
) -> Result<(), ProxyError> {
    // Pre-create temp files for all chunks.
    // When caching is enabled and we have a disk cache, also set up background
    // S3 upload + local cache finalization (replacing the old shadow writer).
    for idx in 0..download.chunk_count() {
        let file = create_temp_chunk_file(temp_dir)
            .map_err(|e| ProxyError::Internal(format!("create temp file: {e}")))?;
        download.chunk(idx).set_file(Arc::new(file));

        if caching {
            download.chunk(idx).increment_readers();
            let dl = download.clone();
            let chunk_idx = idx;
            let s3c = s3_client.cloned();
            let dc = disk_cache.cloned();
            let prefix = data_prefix.to_string();
            spawn_post_chunk_upload(dl, chunk_idx, s3c, dc, prefix);
        }
    }

    // stop_after controls how far the sequential stream progresses.
    // usize::MAX means "fill all chunks"; a specific index means "stop after
    // finishing that chunk" so parallel workers can take over.
    let stop_after = Arc::new(AtomicUsize::new(usize::MAX));

    // Spawn the sequential stream from the initial full-GET response.
    let stop_clone = stop_after.clone();
    let dl_seq = download.clone();
    let trace_seq = trace.clone();
    let seq_handle = tokio::spawn(async move {
        stream_response_into_chunks(initial_response, &dl_seq, stop_clone, &trace_seq).await
    });

    // Drain remaining chunks in priority order (chunk 0 handled by sequential).
    let remaining: Vec<usize> = std::iter::from_fn(|| download.pop_chunk())
        .filter(|&idx| idx != 0)
        .collect();

    let mut parallel_handles: Vec<tokio::task::JoinHandle<Result<(), ProxyError>>> = Vec::new();

    if !remaining.is_empty() {
        // Minimal runway: sequential only keeps the chunk it is currently
        // streaming.  Parallel workers take over from the next chunk onward.
        // For slow upstreams the time-based ramp in ConcurrencyRamp will
        // detect sluggish progress and scale up workers within seconds.
        let seq_chunk_now = active_sequential_chunk(download);
        let runway = 1usize;
        let estimated_stop = (seq_chunk_now + runway)
            .min(download.chunk_count().saturating_sub(1));

        // Find a probe target *beyond* the runway — no point probing a
        // chunk the sequential stream will fill anyway.
        let probe_target = remaining.iter()
            .find(|&&i| i > estimated_stop && !download.is_chunk_done(i))
            .copied();

        if let Some(probe_idx) = probe_target {
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
                    let seq_chunk = active_sequential_chunk(download);
                    let effective_stop = (seq_chunk + runway)
                        .min(download.chunk_count().saturating_sub(1));
                    stop_after.store(effective_stop, Ordering::Release);

                    trace_log(trace, || json!({
                        "event": "range_probe_ok",
                        "probe_chunk": probe_idx,
                        "status": resp.status().as_u16(),
                        "seq_chunk": seq_chunk,
                        "effective_stop": effective_stop,
                    }));
                    debug!(
                        seq_chunk,
                        effective_stop,
                        probe_chunk = probe_idx,
                        "range probe succeeded, sequential runway {seq_chunk}..{effective_stop}"
                    );

                    // Ordered list of chunks for parallel workers
                    // (probe_idx handled separately by the probe worker).
                    let parallel_chunks: Vec<usize> = remaining.iter()
                        .filter(|&&i| i > effective_stop && i != probe_idx && !download.is_chunk_done(i))
                        .copied()
                        .collect();

                    // Probe worker: download the probed chunk from the already-open response.
                    {
                        let probe_file = download.chunk(probe_idx).get_file()
                            .ok_or_else(|| ProxyError::Internal("probe chunk file gone".into()))?;
                        let dl_clone = download.clone();
                        let expected = download.expected_chunk_len(probe_idx);
                        let trace_c = trace.clone();
                        parallel_handles.push(tokio::spawn(async move {
                            download_chunk_from_stream(
                                resp, &probe_file, &dl_clone, probe_idx, expected, &trace_c,
                            ).await
                        }));
                    }

                    // Persistent workers: each loops pulling the *next* chunk in order
                    // from a shared atomic cursor.  Workers check the ConcurrencyRamp
                    // before starting each chunk so the pool grows gradually.
                    if !parallel_chunks.is_empty() {
                        let num_workers = max_concurrency.min(parallel_chunks.len());
                        let cursor = Arc::new(AtomicUsize::new(0));
                        let chunks = Arc::new(parallel_chunks);
                        let ramp = Arc::new(ConcurrencyRamp::new(max_concurrency));

                        for worker_id in 0..num_workers {
                            let cursor = cursor.clone();
                            let chunks = chunks.clone();
                            let client = http_client.clone();
                            let url = upstream_url.to_string();
                            let hdrs = upstream_headers.clone();
                            let dl_clone = download.clone();
                            let trace_c = trace.clone();
                            let ramp = ramp.clone();

                            parallel_handles.push(tokio::spawn(async move {
                                loop {
                                    // Ramp gate: workers beyond the current active
                                    // level yield until the ramp admits them.
                                    if worker_id >= ramp.active_workers() {
                                        tokio::task::yield_now().await;
                                        if worker_id >= ramp.active_workers() {
                                            tokio::time::sleep(
                                                std::time::Duration::from_millis(50),
                                            ).await;
                                            ramp.check_time_trigger();
                                            if worker_id >= ramp.active_workers() {
                                                if ramp.is_frozen() {
                                                    break;
                                                }
                                                continue;
                                            }
                                        }
                                    }

                                    let pos = cursor.fetch_add(1, Ordering::Relaxed);
                                    if pos >= chunks.len() { break; }
                                    let idx = chunks[pos];
                                    if dl_clone.is_chunk_done(idx) { continue; }

                                    let chunk_file = dl_clone.chunk(idx).get_file()
                                        .ok_or_else(|| ProxyError::Internal(
                                            format!("chunk {idx} file gone")
                                        ))?;
                                    let (start, end) = dl_clone.chunk_byte_range(idx);
                                    let expected = dl_clone.expected_chunk_len(idx);

                                    let mut req = client.get(&url);
                                    for (name, value) in hdrs.iter() {
                                        if let Ok(v) = value.to_str() {
                                            req = req.header(name.as_str(), v);
                                        }
                                    }
                                    req = req.header("Range", format!("bytes={start}-{end}"));
                                    let response = req.send().await;
                                    match response {
                                        Ok(resp) if resp.status().is_success()
                                            || resp.status().as_u16() == 206 =>
                                        {
                                            match download_chunk_from_stream(
                                                resp, &chunk_file, &dl_clone,
                                                idx, expected, &trace_c,
                                            ).await {
                                                Ok(()) => ramp.record_chunk(expected),
                                                Err(e) => {
                                                    ramp.record_error();
                                                    return Err(e);
                                                }
                                            }
                                        }
                                        Ok(resp) => {
                                            ramp.record_error();
                                            return Err(ProxyError::Upstream(format!(
                                                "range chunk {idx} returned {}",
                                                resp.status()
                                            )));
                                        }
                                        Err(e) => {
                                            ramp.record_error();
                                            return Err(ProxyError::Upstream(
                                                format!("range chunk {idx}: {e}")
                                            ));
                                        }
                                    }
                                }
                                Ok(())
                            }));
                        }
                    }
                }
                Ok(resp) => {
                    trace_log(trace, || json!({
                        "event": "range_probe_rejected",
                        "probe_chunk": probe_target,
                        "status": resp.status().as_u16(),
                    }));
                    debug!(
                        status = resp.status().as_u16(),
                        "range probe rejected, continuing sequential"
                    );
                }
                Err(e) => {
                    warn!("range probe connection failed: {e}, continuing sequential");
                }
            }
        }
        // else: no chunks beyond the runway — sequential handles the entire file.
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

/// Background task that waits for a chunk to be fully written, then:
/// 1. Copies the temp file to the local disk cache (fsync + rename).
/// 2. Uploads the chunk to S3 via object_store::put().
fn spawn_post_chunk_upload(
    download: Arc<InFlightDownload>,
    idx: usize,
    s3_client: Option<Arc<S3Client>>,
    disk_cache: Option<Arc<DiskCache>>,
    data_prefix: String,
) {
    let expected = download.expected_chunk_len(idx);
    let chunk_id = *download.chunk_id(idx);

    tokio::spawn(async move {
        // Wait for the chunk to be fully written
        if let Err(_) = download.wait_for_bytes(idx, expected).await {
            download.wake_waiters();
            return;
        }

        let temp_file = match download.chunk(idx).get_file() {
            Some(f) => f,
            None => {
                download.chunk(idx).s3_committed.store(true, Ordering::Release);
                download.wake_waiters();
                return;
            }
        };

        // 1. Finalize to local disk cache
        if let Some(ref dc) = disk_cache {
            if !dc.is_degraded() {
                let dc2 = dc.clone();
                let tf = temp_file.clone();
                let cid = chunk_id;
                let finalize_result = tokio::task::spawn_blocking(move || {
                    use std::io::Write;
                    use std::os::unix::fs::FileExt;

                    let (mut cache_file, temp_path) = match dc2.temp_file() {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(chunk = idx, "cache finalize temp_file failed: {e}");
                            return;
                        }
                    };

                    let mut offset = 0u64;
                    let mut buf = vec![0u8; 256 * 1024];
                    let len = expected;
                    while offset < len {
                        let to_read = std::cmp::min((len - offset) as usize, buf.len());
                        if let Err(e) = tf.read_exact_at(&mut buf[..to_read], offset) {
                            warn!(chunk = idx, "cache finalize read failed: {e}");
                            return;
                        }
                        if let Err(e) = cache_file.write_all(&buf[..to_read]) {
                            warn!(chunk = idx, "cache finalize write failed: {e}");
                            return;
                        }
                        offset += to_read as u64;
                    }
                    drop(cache_file);

                    if let Err(e) = dc2.finalize(&temp_path, &cid) {
                        warn!(chunk = idx, "cache finalize rename failed: {e}");
                    }
                }).await;

                if let Err(e) = finalize_result {
                    warn!(chunk = idx, "cache finalize task panicked: {e}");
                }
            }
        }

        // 2. Upload to S3
        if let Some(ref s3) = s3_client {
            let tf = temp_file.clone();
            let len = expected as usize;
            let read_result = tokio::task::spawn_blocking(move || {
                use std::os::unix::fs::FileExt;
                let mut buf = vec![0u8; len];
                tf.read_exact_at(&mut buf, 0)?;
                Ok::<Bytes, std::io::Error>(Bytes::from(buf))
            }).await;

            match read_result {
                Ok(Ok(data)) => {
                    if let Err(e) = s3.put_chunk(&chunk_id, &data_prefix, data).await {
                        warn!(chunk = idx, "S3 put_chunk failed: {e}");
                    }
                }
                Ok(Err(e)) => warn!(chunk = idx, "S3 upload read failed: {e}"),
                Err(e) => warn!(chunk = idx, "S3 upload task panicked: {e}"),
            }
        }

        download.chunk(idx).s3_committed.store(true, Ordering::Release);
        download.wake_waiters();
        debug!(chunk = idx, "post-chunk upload done");
    });
}

/// Stream a full-GET response body into sequential chunks, stopping after the
/// chunk indicated by `stop_after`.  This is the "initial connection" stream
/// that keeps running unless parallel workers take over.
async fn stream_response_into_chunks(
    response: reqwest::Response,
    download: &Arc<InFlightDownload>,
    stop_after: Arc<AtomicUsize>,
    trace: &Option<Arc<TraceWriter>>,
) -> Result<(), ProxyError> {
    let mut stream = response.bytes_stream();
    let mut global_offset = 0u64;

    'outer: while let Some(piece) = stream.next().await {
        let data = piece.map_err(|e| ProxyError::Upstream(format!("sequential stream: {e}")))?;
        let mut piece_offset = 0usize;

        while piece_offset < data.len() {
            let chunk_idx = (global_offset / download.chunk_size) as usize;

            if chunk_idx > stop_after.load(Ordering::Acquire) {
                trace_log(trace, || json!({
                    "event": "sequential_stopped",
                    "at_chunk": chunk_idx,
                }));
                break 'outer;
            }
            if chunk_idx >= download.chunk_count() {
                break 'outer;
            }

            let chunk_offset = global_offset - chunk_idx as u64 * download.chunk_size;
            let to_write = std::cmp::min(
                (data.len() - piece_offset) as u64,
                download.expected_chunk_len(chunk_idx) - chunk_offset,
            ) as usize;
            if to_write == 0 {
                break;
            }

            let file = download.chunk(chunk_idx).get_file().ok_or_else(|| {
                ProxyError::Internal("chunk file released during sequential".into())
            })?;

            let slice = &data[piece_offset..piece_offset + to_write];
            pwrite_all(&file, chunk_offset, slice)
                .map_err(|e| ProxyError::Internal(format!("pwrite chunk {chunk_idx}: {e}")))?;

            piece_offset += to_write;
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
    }

    Ok(())
}

async fn download_chunk_from_stream(
    response: reqwest::Response,
    file: &std::fs::File,
    download: &Arc<InFlightDownload>,
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
        let slice = &piece[..to_write];
        pwrite_all(file, offset, slice)
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

/// Upper bound on the number of chunks allocated for unknown-size (chunked)
/// responses.  With 32 MiB chunks this supports responses up to 64 GiB.
const MAX_UNKNOWN_SIZE_CHUNKS: u64 = 2048;

#[allow(clippy::too_many_arguments)]
async fn start_sequential_download(
    config: &AppConfig,
    response: reqwest::Response,
    file_size: Option<u64>,
    cache_key: Option<&str>,
    downloads: &Arc<DownloadManager>,
    _trace: &Option<Arc<TraceWriter>>,
    content_type: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    cache_control: Option<String>,
    all_upstream_headers: Option<reqwest::header::HeaderMap>,
    fdb_client: Option<Arc<FdbClient>>,
    s3_client: Option<Arc<S3Client>>,
    disk_cache: Option<Arc<DiskCache>>,
) -> Result<UpstreamResult, ProxyError> {
    let is_unknown_size = file_size.is_none();
    let effective_size = file_size
        .unwrap_or(config.chunk_size * MAX_UNKNOWN_SIZE_CHUNKS);
    let chunk_size = config.chunk_size;

    let num_chunks = if effective_size == 0 {
        0
    } else {
        effective_size.div_ceil(chunk_size) as usize
    };
    let chunk_ids = generate_chunk_ids(num_chunks);

    let download = Arc::new(if is_unknown_size {
        InFlightDownload::new_unknown_size(effective_size, chunk_size, chunk_ids)
    } else {
        InFlightDownload::new(effective_size, chunk_size, chunk_ids)
    });

    let download = if let Some(key) = cache_key {
        let (download, is_new) = downloads.get_or_create(key, || download);
        if !is_new {
            return Ok(UpstreamResult {
                download,
                content_type,
                etag,
                last_modified,
                cache_control,
                full_size: file_size,
                degraded_body: None,
                redirect_status: None,
                redirect_headers: None,
                upstream_headers: all_upstream_headers,
                error_passthrough: None,
                revalidated: false,
            });
        }
        download
    } else {
        download
    };

    // Probe temp dir for available space; if ENOSPC, degrade to direct passthrough
    if let Err(e) = create_temp_chunk_file(&config.temp_dir) {
        if let Some(key) = cache_key {
            downloads.remove(key);
        }
        if is_enospc(&e) {
            warn!("ENOSPC: degrading to direct passthrough (sequential)");
            return Ok(UpstreamResult {
                download: Arc::new(InFlightDownload::new_placeholder(config.chunk_size)),
                content_type,
                etag,
                last_modified,
                cache_control,
                full_size: file_size,
                degraded_body: Some(response),
                redirect_status: None,
                redirect_headers: None,
                upstream_headers: all_upstream_headers,
                error_passthrough: None,
                revalidated: false,
            });
        }
        return Err(ProxyError::Internal(format!("temp dir unavailable: {e}")));
    }

    // Spawn finalize only when caching
    if let Some(key) = cache_key {
        finalize::spawn_finalize(
            key.to_string(),
            download.clone(),
            fdb_client.clone(),
        );
    }

    if !is_unknown_size {
        let caching_setup = cache_key.is_some();
        let setup_t0 = std::time::Instant::now();
        for idx in 0..download.chunk_count() {
            let chunk_file = create_temp_chunk_file(&config.temp_dir)
                .map_err(|e| ProxyError::Internal(format!("create temp file: {e}")))?;
            download.chunk(idx).set_file(Arc::new(chunk_file));

            if caching_setup {
                download.chunk(idx).increment_readers();
                let dl = download.clone();
                let s3c = s3_client.clone();
                let dc = disk_cache.clone();
                let prefix = config.data_prefix.clone();
                spawn_post_chunk_upload(dl, idx, s3c, dc, prefix);
            }
        }
        if caching_setup {
            let setup_ms = setup_t0.elapsed().as_secs_f64() * 1000.0;
            debug!(
                chunks = download.chunk_count(),
                setup_ms = setup_ms as u64,
                "chunk setup complete"
            );
        }
    }

    let dl = download.clone();
    let ck = cache_key.map(str::to_owned);
    let dm = downloads.clone();
    let temp_dir = config.temp_dir.clone();
    let data_prefix_clone = config.data_prefix.clone();
    let caching = cache_key.is_some();
    let s3c = s3_client.clone();
    let dc = disk_cache.clone();

    tokio::spawn(async move {
        let mut stream = response.bytes_stream();
        let mut global_offset = 0u64;

        'outer: while let Some(piece) = stream.next().await {
            match piece {
                Ok(data) => {
                    let mut piece_offset = 0usize;
                    while piece_offset < data.len() {
                        let chunk_idx = (global_offset / dl.chunk_size) as usize;
                        if chunk_idx >= dl.chunk_count() {
                            break 'outer;
                        }

                        // Lazy temp file creation for unknown-size responses
                        if is_unknown_size && dl.chunk(chunk_idx).get_file().is_none() {
                            match create_temp_chunk_file(&temp_dir) {
                                Ok(f) => dl.chunk(chunk_idx).set_file(Arc::new(f)),
                                Err(e) => {
                                    warn!(cache_key = ?ck, "temp file creation failed: {e}");
                                    dl.mark_failed();
                                    break 'outer;
                                }
                            }
                            if caching {
                                let dl2 = dl.clone();
                                dl.chunk(chunk_idx).increment_readers();
                                let s3c2 = s3c.clone();
                                let dc2 = dc.clone();
                                let prefix2 = data_prefix_clone.clone();
                                spawn_post_chunk_upload(dl2, chunk_idx, s3c2, dc2, prefix2);
                            }
                        }

                        let chunk_offset = global_offset - chunk_idx as u64 * dl.chunk_size;

                        if let Some(file) = dl.chunk(chunk_idx).get_file() {
                            let to_write = std::cmp::min(
                                (data.len() - piece_offset) as u64,
                                dl.expected_chunk_len(chunk_idx) - chunk_offset,
                            ) as usize;
                            if to_write == 0 {
                                break;
                            }
                            let slice = &data[piece_offset..piece_offset + to_write];
                            if let Err(e) = pwrite_all(&file, chunk_offset, slice) {
                                warn!(cache_key = ?ck, "sequential write error: {e}");
                                dl.mark_failed();
                                break 'outer;
                            }
                            piece_offset += to_write;
                            global_offset += to_write as u64;
                            dl.record_written(chunk_idx, to_write as u64);

                            if dl.is_chunk_done(chunk_idx) {
                                dl.mark_chunk_done(chunk_idx);
                            }
                        } else {
                            break;
                        }
                    }
                }
                Err(e) => {
                    warn!(cache_key = ?ck, "sequential stream error: {e}");
                    dl.mark_failed();
                    break;
                }
            }
        }

        if is_unknown_size {
            dl.mark_stream_complete(global_offset);
        } else {
            for idx in 0..dl.chunk_count() {
                if !dl.is_chunk_done(idx) && !dl.has_failed() {
                    dl.mark_chunk_done(idx);
                }
            }
        }

        if let Some(ref key) = ck {
            dl.wait_for_s3_complete().await;
            dm.remove(key);
        }
    });

    Ok(UpstreamResult {
        download,
        content_type,
        etag,
        last_modified,
        cache_control,
        full_size: file_size,
        degraded_body: None,
        redirect_status: None,
        redirect_headers: None,
        upstream_headers: all_upstream_headers,
        error_passthrough: None,
        revalidated: false,
    })
}
