use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use parking_lot::Mutex;
use serde_json::json;
use tokio::sync::Notify;
use tracing::{error, info};

use crate::cache::CacheStore;
use crate::error::ProxyError;
use crate::s3::Upstream;
use crate::trace::{trace_log, TraceWriter};

/// Minimum bytes between reader notifications.  Smaller values give lower
/// time-to-first-byte but more wakeup overhead.  64 KiB is a good balance —
/// roughly one HTTP/2 DATA frame from S3.
const NOTIFY_INTERVAL: u64 = 64 * 1024;

// ---------------------------------------------------------------------------
// Download manager: deduplication of concurrent requests for the same object
// ---------------------------------------------------------------------------

/// Tracks all in-flight downloads, keyed by content hash.
/// When multiple downstream clients request the same object simultaneously,
/// only one download is started — others attach to the existing one.
#[derive(Default)]
pub struct DownloadManager {
    active: Mutex<HashMap<String, Arc<InFlightDownload>>>,
}

impl DownloadManager {
    /// Return the existing download for `cache_key`, or create a new one.
    /// The boolean indicates whether the caller is the creator (true = new).
    pub fn get_or_create(
        &self,
        cache_key: &str,
        create_fn: impl FnOnce() -> Arc<InFlightDownload>,
    ) -> (Arc<InFlightDownload>, bool) {
        let mut map = self.active.lock();
        if let Some(existing) = map.get(cache_key) {
            return (existing.clone(), false);
        }
        let dl = create_fn();
        map.insert(cache_key.to_string(), dl.clone());
        (dl, true)
    }

    pub fn remove(&self, cache_key: &str) {
        self.active.lock().remove(cache_key);
    }
}

// ---------------------------------------------------------------------------
// Per-object download state
// ---------------------------------------------------------------------------

/// Shared state for a single object being downloaded from S3.
///
/// Download workers stream data to disk and record progress in
/// `chunk_written`.  Downstream readers use [`wait_for_bytes`] to
/// block until enough data is on disk, then read via pread in small
/// pieces — they never have to wait for a full chunk to complete.
pub struct InFlightDownload {
    pub temp_path: PathBuf,
    pub final_path: PathBuf,
    pub object_size: u64,
    pub chunk_size: u64,
    /// Bytes written to disk for each chunk (monotonically increasing).
    chunk_written: Vec<AtomicU64>,
    chunk_queue: Mutex<VecDeque<usize>>,
    notify: Notify,
    failed: AtomicU8,
}

impl InFlightDownload {
    pub fn new(
        temp_path: PathBuf,
        final_path: PathBuf,
        object_size: u64,
        chunk_size: u64,
    ) -> Self {
        let chunk_count = if object_size == 0 {
            0
        } else {
            object_size.div_ceil(chunk_size) as usize
        };
        let queue: VecDeque<usize> = (0..chunk_count).collect();
        Self {
            temp_path,
            final_path,
            object_size,
            chunk_size,
            chunk_written: (0..chunk_count)
                .map(|_| AtomicU64::new(0))
                .collect(),
            chunk_queue: Mutex::new(queue),
            notify: Notify::new(),
            failed: AtomicU8::new(0),
        }
    }

    pub fn chunk_count(&self) -> usize {
        self.chunk_written.len()
    }

    /// Expected byte count for a given chunk (last chunk may be shorter).
    pub fn expected_chunk_len(&self, idx: usize) -> u64 {
        let start = idx as u64 * self.chunk_size;
        let end = std::cmp::min(start + self.chunk_size, self.object_size);
        end - start
    }

    /// Bytes written to disk so far for the given chunk.
    pub fn bytes_written(&self, idx: usize) -> u64 {
        self.chunk_written[idx].load(Ordering::Acquire)
    }

    /// Record additional bytes written.  Notifies waiting readers every
    /// [`NOTIFY_INTERVAL`] bytes so they can start streaming immediately.
    fn record_written(&self, idx: usize, bytes: u64) {
        let prev = self.chunk_written[idx].fetch_add(bytes, Ordering::AcqRel);
        let new = prev + bytes;
        if new / NOTIFY_INTERVAL > prev / NOTIFY_INTERVAL {
            self.notify.notify_waiters();
        }
    }

    /// Move chunks overlapping `[byte_start, byte_end_inclusive]` to the
    /// front of the download queue so they are fetched first.
    pub fn prioritize_range(&self, byte_start: u64, byte_end_inclusive: u64) {
        if self.chunk_count() == 0 {
            return;
        }
        let start_idx = (byte_start / self.chunk_size) as usize;
        let end_idx =
            (byte_end_inclusive / self.chunk_size).min(self.chunk_count() as u64 - 1) as usize;
        let priority_set: HashSet<usize> = (start_idx..=end_idx).collect();

        let mut q = self.chunk_queue.lock();
        let mut priority = Vec::new();
        let mut rest = VecDeque::new();
        for idx in q.drain(..) {
            if priority_set.contains(&idx) {
                priority.push(idx);
            } else {
                rest.push_back(idx);
            }
        }
        // Keep priority chunks in ascending order for sequential reads
        priority.sort_unstable();
        for idx in priority {
            q.push_back(idx);
        }
        q.append(&mut rest);
    }

    fn pop_chunk(&self) -> Option<usize> {
        self.chunk_queue.lock().pop_front()
    }

    fn mark_chunk_done(&self, idx: usize) {
        // Ensure bytes_written reflects the full expected size (defensive).
        self.chunk_written[idx].store(self.expected_chunk_len(idx), Ordering::Release);
        self.notify.notify_waiters();
    }

    fn mark_failed(&self) {
        self.failed.store(1, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub fn is_chunk_done(&self, idx: usize) -> bool {
        self.bytes_written(idx) >= self.expected_chunk_len(idx)
    }

    pub fn has_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire) != 0
    }

    /// Wait until at least `min_bytes` of chunk `idx` are written to disk.
    /// Returns the current total bytes written for that chunk.
    pub async fn wait_for_bytes(
        &self,
        idx: usize,
        min_bytes: u64,
    ) -> Result<u64, ProxyError> {
        loop {
            let written = self.bytes_written(idx);
            if written >= min_bytes {
                return Ok(written);
            }
            if self.has_failed() {
                return Err(ProxyError::Upstream("upstream download failed".into()));
            }
            self.notify.notified().await;
        }
    }

    /// Wait until the specified chunk is fully written to disk.
    pub async fn wait_for_chunk(&self, idx: usize) -> Result<(), ProxyError> {
        self.wait_for_bytes(idx, self.expected_chunk_len(idx))
            .await?;
        Ok(())
    }

    /// Byte range `(start, end_inclusive)` for a given chunk index.
    pub fn chunk_byte_range(&self, idx: usize) -> (u64, u64) {
        let start = idx as u64 * self.chunk_size;
        let end = std::cmp::min(
            start + self.chunk_size - 1,
            self.object_size.saturating_sub(1),
        );
        (start, end)
    }
}

impl std::fmt::Debug for InFlightDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InFlightDownload")
            .field("temp_path", &self.temp_path)
            .field("final_path", &self.final_path)
            .field("object_size", &self.object_size)
            .field("chunk_size", &self.chunk_size)
            .field("chunks", &self.chunk_count())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Background download orchestration
// ---------------------------------------------------------------------------

/// Spawn the parallel download workers for one object.
///
/// This function returns immediately. The download runs in a background task.
/// On success the temp file is fsync'd and atomically renamed to the final
/// cache path. On failure the temp file is removed and the download is marked
/// as failed so waiting readers get an error.
pub fn spawn_download(
    upstream: Arc<dyn Upstream>,
    bucket: String,
    key: String,
    cache_key: String,
    download: Arc<InFlightDownload>,
    manager: Arc<DownloadManager>,
    cache: Arc<CacheStore>,
    concurrency: usize,
    trace: Option<Arc<TraceWriter>>,
) {
    tokio::spawn(async move {
        let result = run_parallel_download(
            upstream,
            &bucket,
            &key,
            download.clone(),
            concurrency,
            trace.clone(),
        )
        .await;

        match result {
            Ok(()) => {
                if let Err(e) = finalize_download(&download).await {
                    error!(key, "finalize failed: {e}");
                    download.mark_failed();
                } else {
                    cache.touch_mtime(&download.final_path).await;
                    info!(key, path = ?download.final_path, "cached");
                }
            }
            Err(e) => {
                error!(key, "download failed: {e}");
                download.mark_failed();
                let _ = tokio::fs::remove_file(&download.temp_path).await;
            }
        }
        manager.remove(&cache_key);
    });
}

/// Drive the parallel download: pre-allocate temp file, spawn `concurrency`
/// worker tasks that pop chunks from the shared queue and stream them from
/// S3 to disk via pwrite.
///
/// Workers never buffer more than one network frame (~8-64 KiB) at a time —
/// data flows from the S3 HTTP response directly to the page cache, keeping
/// memory usage O(concurrency × network_buffer) instead of
/// O(concurrency × chunk_size).
async fn run_parallel_download(
    upstream: Arc<dyn Upstream>,
    bucket: &str,
    key: &str,
    download: Arc<InFlightDownload>,
    concurrency: usize,
    trace: Option<Arc<TraceWriter>>,
) -> Result<(), ProxyError> {
    // Pre-allocate the temp file to the full object size
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(&download.temp_path)
        .map_err(|e| ProxyError::Internal(format!("create temp: {e}")))?;
    file.set_len(download.object_size)
        .map_err(|e| ProxyError::Internal(format!("fallocate: {e}")))?;
    let file = Arc::new(file);

    let workers = concurrency.max(1);
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let upstream = upstream.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let file = file.clone();
        let dl = download.clone();
        let trace = trace.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let Some(idx) = dl.pop_chunk() else {
                    break;
                };
                let (start, end_inclusive) = dl.chunk_byte_range(idx);
                let chunk_t0 = Instant::now();

                trace_log(&trace, || json!({
                    "event": "dl_chunk_start",
                    "worker": worker_id,
                    "chunk": idx,
                    "byte_start": start,
                    "byte_end": end_inclusive,
                }));

                // Stream from S3 to disk — each piece is typically 8-64 KiB,
                // so peak memory per worker is one network frame.
                let mut stream = upstream
                    .get_range_stream(&bucket, &key, start, end_inclusive)
                    .await?;

                trace_log(&trace, || json!({
                    "event": "dl_s3_connected",
                    "worker": worker_id,
                    "chunk": idx,
                    "latency_ms": chunk_t0.elapsed().as_secs_f64() * 1000.0,
                }));

                let mut offset = 0u64;
                while let Some(piece) = stream.next().await {
                    let piece = piece?;
                    pwrite_all(&file, start + offset, &piece)
                        .map_err(|e| ProxyError::Internal(format!("pwrite: {e}")))?;
                    offset += piece.len() as u64;
                    dl.record_written(idx, piece.len() as u64);
                }
                dl.mark_chunk_done(idx);

                let elapsed = chunk_t0.elapsed();
                trace_log(&trace, || json!({
                    "event": "dl_chunk_done",
                    "worker": worker_id,
                    "chunk": idx,
                    "bytes": offset,
                    "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
                    "mbps": if elapsed.as_nanos() > 0 {
                        (offset as f64) / elapsed.as_secs_f64() / 1_000_000.0
                    } else { 0.0 },
                }));
            }
            Ok::<(), ProxyError>(())
        }));
    }

    for handle in handles {
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

    Ok(())
}

/// Fsync the temp file to disk, then atomically rename to the final path.
///
/// The fsync ensures that after a power failure the cache file is either
/// absent (crash before rename) or fully persisted (crash after rename).
/// Without it, the file could exist at the final path with the correct
/// size but contain zeros/stale data — leading to silent corruption on
/// serve.  The cost is a one-time I/O flush per download; it runs in the
/// background finalize task and does not affect the reader streaming path.
async fn finalize_download(dl: &InFlightDownload) -> io::Result<()> {
    let f = tokio::fs::File::open(&dl.temp_path).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(&dl.temp_path, &dl.final_path).await
}

/// Thread-safe positional write (pwrite) — does not affect file offset.
fn pwrite_all(file: &std::fs::File, offset: u64, data: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    let mut pos = offset;
    let mut remaining = data;
    while !remaining.is_empty() {
        let n = file.write_at(remaining, pos)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "pwrite returned 0",
            ));
        }
        remaining = &remaining[n..];
        pos += n as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_byte_ranges() {
        let dl = InFlightDownload::new(
            PathBuf::from("/tmp/a.dl"),
            PathBuf::from("/tmp/a"),
            5000,
            1024,
        );
        assert_eq!(dl.chunk_count(), 5);
        assert_eq!(dl.chunk_byte_range(0), (0, 1023));
        assert_eq!(dl.chunk_byte_range(4), (4096, 4999));
    }

    #[test]
    fn expected_chunk_lengths() {
        let dl = InFlightDownload::new(
            PathBuf::from("/tmp/a.dl"),
            PathBuf::from("/tmp/a"),
            5000,
            1024,
        );
        assert_eq!(dl.expected_chunk_len(0), 1024);
        assert_eq!(dl.expected_chunk_len(3), 1024);
        assert_eq!(dl.expected_chunk_len(4), 5000 - 4096); // last chunk: 904 bytes
    }

    #[test]
    fn prioritize_moves_chunks_to_front() {
        let dl = InFlightDownload::new(
            PathBuf::from("/tmp/b.dl"),
            PathBuf::from("/tmp/b"),
            10240,
            1024,
        );
        // Default order: 0,1,2,3,4,5,6,7,8,9
        dl.prioritize_range(5120, 7167); // chunks 5,6
        let mut order = Vec::new();
        while let Some(idx) = dl.pop_chunk() {
            order.push(idx);
        }
        assert_eq!(order[0], 5);
        assert_eq!(order[1], 6);
        // Remaining in original order
        assert_eq!(&order[2..], &[0, 1, 2, 3, 4, 7, 8, 9]);
    }

    #[test]
    fn zero_size_download() {
        let dl = InFlightDownload::new(
            PathBuf::from("/tmp/z.dl"),
            PathBuf::from("/tmp/z"),
            0,
            1024,
        );
        assert_eq!(dl.chunk_count(), 0);
        assert!(dl.pop_chunk().is_none());
    }

    #[test]
    fn record_written_tracks_progress() {
        let dl = InFlightDownload::new(
            PathBuf::from("/tmp/p.dl"),
            PathBuf::from("/tmp/p"),
            10000,
            5000,
        );
        assert_eq!(dl.bytes_written(0), 0);
        assert!(!dl.is_chunk_done(0));

        dl.record_written(0, 1000);
        assert_eq!(dl.bytes_written(0), 1000);
        assert!(!dl.is_chunk_done(0));

        dl.record_written(0, 4000);
        assert_eq!(dl.bytes_written(0), 5000);
        assert!(dl.is_chunk_done(0));
    }
}
