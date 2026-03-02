use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::error::ProxyError;
use crate::manifest::ID_LEN;
use crate::planner;

/// Minimum bytes between reader notifications.
const NOTIFY_INTERVAL: u64 = 64 * 1024;

// ---------------------------------------------------------------------------
// Download manager: deduplication of concurrent requests for the same object
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct DownloadManager {
    active: Mutex<HashMap<String, Arc<InFlightDownload>>>,
}

impl DownloadManager {
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

    /// Check if there's an in-flight download for the given cache key.
    pub fn get_inflight(&self, cache_key: &str) -> Option<Arc<InFlightDownload>> {
        self.active.lock().get(cache_key).cloned()
    }
}

// ---------------------------------------------------------------------------
// Per-chunk state
// ---------------------------------------------------------------------------

/// State for a single download chunk backed by its own temp file.
pub struct ChunkSlot {
    /// Open file descriptor for the anonymous temp file.
    /// `None` once the chunk has been released.
    file: Mutex<Option<Arc<File>>>,
    /// Bytes written to this chunk's file so far (monotonically increasing).
    bytes_written: AtomicU64,
    /// UUIDv4 identifier for this chunk (immutable, set at construction).
    id: [u8; ID_LEN],
    /// Set once the S3 upload for this chunk completes (sync_all + close).
    pub s3_committed: AtomicBool,
    /// Number of active readers (including the download worker itself).
    reader_count: AtomicUsize,
}

impl ChunkSlot {
    fn new(id: [u8; ID_LEN]) -> Self {
        Self {
            file: Mutex::new(None),
            bytes_written: AtomicU64::new(0),
            id,
            s3_committed: AtomicBool::new(false),
            reader_count: AtomicUsize::new(0),
        }
    }

    pub fn id(&self) -> &[u8; ID_LEN] {
        &self.id
    }

    /// Set the file handle for this chunk (called by the download worker
    /// after opening + unlinking the temp file).
    pub fn set_file(&self, file: Arc<File>) {
        *self.file.lock() = Some(file);
    }

    /// Get a clone of the file handle, if the chunk hasn't been released.
    pub fn get_file(&self) -> Option<Arc<File>> {
        self.file.lock().clone()
    }

    /// Release the file handle.  The kernel reclaims pages once all
    /// Arc references are dropped.
    pub fn release_file(&self) {
        *self.file.lock() = None;
    }

    /// Try to release this chunk's temp file if all readers are done and
    /// the chunk has been committed to S3.
    pub fn try_release(&self) {
        if self.s3_committed.load(Ordering::Acquire)
            && self.reader_count.load(Ordering::Acquire) == 0
        {
            self.release_file();
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Acquire)
    }

    /// Increment the reader count for this chunk.
    pub fn increment_readers(&self) {
        self.reader_count.fetch_add(1, Ordering::AcqRel);
    }

    pub fn decrement_reader(&self) {
        let prev = self.reader_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "reader_count underflow");
        if prev == 1 {
            self.try_release();
        }
    }
}

// ---------------------------------------------------------------------------
// Per-object download state
// ---------------------------------------------------------------------------

pub struct InFlightDownload {
    pub object_size: u64,
    pub chunk_size: u64,
    /// True when the upstream response had no Content-Length (chunked/streaming).
    /// The persist pipeline must wait for stream completion before it can know
    /// the actual number of chunks.
    pub unknown_size: bool,
    chunks: Vec<ChunkSlot>,
    chunk_queue: Mutex<VecDeque<usize>>,
    notify: Notify,
    failed: AtomicU8,
    cancelled: AtomicBool,
    /// Set when the S3 manifest write is fully completed.
    pub s3_upload_complete: AtomicBool,
    /// Number of chunks the client has fully consumed (0 initially).
    /// Used by download workers to stay within a bounded prefetch window.
    consumed_count: AtomicUsize,
    /// Signaled when the consumer advances, unblocking workers waiting
    /// for the prefetch window.
    consumer_notify: Notify,
    /// When true, `notify_consumed` eagerly releases chunk files via
    /// `try_release`.  Only set for cache-hit downloads where there is
    /// no concurrent S3 upload or upstream writer that still needs
    /// the file.
    eager_release: AtomicBool,
    /// Set when the upstream stream has finished (all data received).
    /// Used for unknown-size responses where the actual size is smaller
    /// than `object_size`.  Signals `wait_for_bytes` to return early.
    stream_complete: AtomicBool,
    /// Actual total bytes received when `stream_complete` is true.
    actual_total_bytes: AtomicU64,
}

impl InFlightDownload {
    pub fn new(object_size: u64, chunk_size: u64, chunk_ids: Vec<[u8; ID_LEN]>) -> Self {
        Self::create(object_size, chunk_size, false, chunk_ids)
    }

    pub fn new_unknown_size(effective_size: u64, chunk_size: u64, chunk_ids: Vec<[u8; ID_LEN]>) -> Self {
        Self::create(effective_size, chunk_size, true, chunk_ids)
    }

    /// Create a zero-chunk placeholder (for redirects, errors, etc.)
    pub fn new_placeholder(chunk_size: u64) -> Self {
        Self::create(0, chunk_size, false, Vec::new())
    }

    fn create(object_size: u64, chunk_size: u64, unknown_size: bool, chunk_ids: Vec<[u8; ID_LEN]>) -> Self {
        let num_chunks = chunk_ids.len();
        let queue: VecDeque<usize> = (0..num_chunks).collect();
        let chunks = chunk_ids.into_iter().map(|id| ChunkSlot::new(id)).collect();
        Self {
            object_size,
            chunk_size,
            unknown_size,
            chunks,
            chunk_queue: Mutex::new(queue),
            notify: Notify::new(),
            failed: AtomicU8::new(0),
            cancelled: AtomicBool::new(false),
            s3_upload_complete: AtomicBool::new(false),
            consumed_count: AtomicUsize::new(0),
            consumer_notify: Notify::new(),
            eager_release: AtomicBool::new(false),
            stream_complete: AtomicBool::new(false),
            actual_total_bytes: AtomicU64::new(0),
        }
    }

    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub fn chunk(&self, idx: usize) -> &ChunkSlot {
        &self.chunks[idx]
    }

    pub fn chunk_id(&self, idx: usize) -> &[u8; ID_LEN] {
        self.chunks[idx].id()
    }

    pub fn expected_chunk_len(&self, idx: usize) -> u64 {
        planner::expected_chunk_len(idx, self.chunk_size, self.object_size)
    }

    pub fn chunk_byte_range(&self, idx: usize) -> (u64, u64) {
        planner::chunk_byte_range(idx, self.chunk_size, self.object_size)
    }

    /// Record additional bytes written to a chunk.  Notifies waiting readers
    /// every [`NOTIFY_INTERVAL`] bytes.
    pub fn record_written(&self, idx: usize, bytes: u64) {
        let prev = self.chunks[idx]
            .bytes_written
            .fetch_add(bytes, Ordering::AcqRel);
        let new = prev + bytes;
        if new / NOTIFY_INTERVAL > prev / NOTIFY_INTERVAL {
            self.notify.notify_waiters();
        }
    }

    pub fn mark_chunk_done(&self, idx: usize) {
        self.chunks[idx]
            .bytes_written
            .store(self.expected_chunk_len(idx), Ordering::Release);
        self.notify.notify_waiters();
    }

    pub fn is_chunk_done(&self, idx: usize) -> bool {
        self.chunks[idx].bytes_written() >= self.expected_chunk_len(idx)
    }

    /// Wait until at least `min_bytes` of chunk `idx` are written.
    /// Returns early with the actual bytes written if the upstream stream
    /// has completed (unknown-size responses may have fewer bytes than
    /// `min_bytes`).
    pub async fn wait_for_bytes(
        &self,
        idx: usize,
        min_bytes: u64,
    ) -> Result<u64, ProxyError> {
        loop {
            let written = self.chunks[idx].bytes_written();
            if written >= min_bytes {
                return Ok(written);
            }
            if self.stream_complete.load(Ordering::Acquire) {
                return Ok(written);
            }
            if self.has_failed() {
                return Err(ProxyError::Upstream("upstream download failed".into()));
            }
            if self.is_cancelled() {
                return Err(ProxyError::Internal("download cancelled".into()));
            }
            self.notify.notified().await;
        }
    }

    /// Mark the upstream stream as fully received.  Used for unknown-size
    /// (chunked) responses where `object_size` is an upper bound and the
    /// actual data may be smaller.  Wakes all `wait_for_bytes` waiters so
    /// they can return with the actual bytes available.
    pub fn mark_stream_complete(&self, actual_bytes: u64) {
        self.actual_total_bytes.store(actual_bytes, Ordering::Release);
        self.stream_complete.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub fn is_stream_complete(&self) -> bool {
        self.stream_complete.load(Ordering::Acquire)
    }

    /// Wait until the upstream stream has finished (unknown-size responses).
    /// Returns immediately if the stream is already complete or if the
    /// download failed/was cancelled.
    pub async fn wait_for_stream_complete(&self) {
        loop {
            if self.stream_complete.load(Ordering::Acquire) {
                return;
            }
            if self.has_failed() || self.is_cancelled() {
                return;
            }
            self.notify.notified().await;
        }
    }

    pub fn actual_total_bytes(&self) -> u64 {
        self.actual_total_bytes.load(Ordering::Acquire)
    }

    pub fn mark_failed(&self) {
        self.failed.store(1, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Wake all tasks waiting on any condition (bytes, s3_complete, etc.).
    pub fn wake_waiters(&self) {
        self.notify.notify_waiters();
    }

    pub fn has_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire) != 0
    }

    /// Signal that no readers remain and workers should stop.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
        self.consumer_notify.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Called by the client stream when it finishes reading a chunk.
    /// Updates the consumer position and, when eager_release is enabled
    /// (cache-hit path), releases the chunk's file descriptor immediately.
    /// For cache-miss paths the file is kept alive for the S3 upload; it
    /// will be released by reader refcounting instead.
    pub fn notify_consumed(&self, chunk_idx: usize) {
        self.consumed_count.store(chunk_idx + 1, Ordering::Release);
        if self.eager_release.load(Ordering::Acquire) {
            self.chunks[chunk_idx].release_file();
        }
        self.consumer_notify.notify_waiters();
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
        priority.sort_unstable();
        for idx in priority {
            q.push_back(idx);
        }
        q.append(&mut rest);
    }

    pub fn pop_chunk(&self) -> Option<usize> {
        self.chunk_queue.lock().pop_front()
    }

    /// Wait until the S3 upload is fully completed (manifest written).
    pub async fn wait_for_s3_complete(&self) {
        loop {
            if self.s3_upload_complete.load(Ordering::Acquire) {
                return;
            }
            if self.has_failed() || self.is_cancelled() {
                return;
            }
            self.notify.notified().await;
        }
    }
}

impl std::fmt::Debug for InFlightDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InFlightDownload")
            .field("object_size", &self.object_size)
            .field("chunk_size", &self.chunk_size)
            .field("chunks", &self.chunk_count())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Temp file helpers
// ---------------------------------------------------------------------------

/// Check whether an I/O error is ENOSPC (no space left on device).
pub fn is_enospc(e: &io::Error) -> bool {
    e.raw_os_error() == Some(libc::ENOSPC)
}

/// Create a temp file in `dir`, immediately unlink it, and return the open fd.
/// The file stays alive via the returned `File` handle; the kernel reclaims
/// storage when the last fd is closed.
pub fn create_temp_chunk_file(dir: &Path) -> io::Result<File> {
    let path = dir.join(format!(
        ".xs3.{}.{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(&path)?;
    // Unlink immediately — file stays alive via the fd.
    let _ = std::fs::remove_file(&path);
    Ok(file)
}

/// Thread-safe positional write (pwrite).
pub fn pwrite_all(file: &File, offset: u64, data: &[u8]) -> io::Result<()> {
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

/// Generate `n` random UUIDv4 identifiers for chunks.
///
/// v4 over v7: UUIDv7's time-sorted prefix concentrates S3 writes into the
/// same key-prefix partition, risking 503 SlowDown. Random v4 prefixes spread
/// chunks uniformly across partitions.
pub fn generate_chunk_ids(n: usize) -> Vec<[u8; ID_LEN]> {
    (0..n)
        .map(|_| *uuid::Uuid::new_v4().as_bytes())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_byte_ranges() {
        let ids = generate_chunk_ids(5);
        let dl = InFlightDownload::new(5000, 1024, ids);
        assert_eq!(dl.chunk_count(), 5);
        assert_eq!(dl.chunk_byte_range(0), (0, 1023));
        assert_eq!(dl.chunk_byte_range(4), (4096, 4999));
    }

    #[test]
    fn prioritize_moves_chunks_to_front() {
        let ids = generate_chunk_ids(10);
        let dl = InFlightDownload::new(10240, 1024, ids);
        dl.prioritize_range(5120, 7167); // chunks 5,6
        let mut order = Vec::new();
        while let Some(idx) = dl.pop_chunk() {
            order.push(idx);
        }
        assert_eq!(order[0], 5);
        assert_eq!(order[1], 6);
        assert_eq!(&order[2..], &[0, 1, 2, 3, 4, 7, 8, 9]);
    }

    #[test]
    fn zero_size_download() {
        let dl = InFlightDownload::new(0, 1024, Vec::new());
        assert_eq!(dl.chunk_count(), 0);
        assert!(dl.pop_chunk().is_none());
    }

    #[test]
    fn record_written_tracks_progress() {
        let ids = generate_chunk_ids(2);
        let dl = InFlightDownload::new(10000, 5000, ids);
        assert_eq!(dl.chunk(0).bytes_written(), 0);
        assert!(!dl.is_chunk_done(0));

        dl.record_written(0, 1000);
        assert_eq!(dl.chunk(0).bytes_written(), 1000);

        dl.record_written(0, 4000);
        assert_eq!(dl.chunk(0).bytes_written(), 5000);
        assert!(dl.is_chunk_done(0));
    }

    #[test]
    fn temp_chunk_file_create_and_write() {
        let dir = std::env::temp_dir();
        let file = create_temp_chunk_file(&dir).unwrap();
        pwrite_all(&file, 0, b"hello").unwrap();
        use std::os::unix::fs::FileExt;
        let mut buf = [0u8; 5];
        file.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");
    }
}
