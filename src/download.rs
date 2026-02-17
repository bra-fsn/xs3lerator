use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::error::ProxyError;
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
}

// ---------------------------------------------------------------------------
// Per-chunk state
// ---------------------------------------------------------------------------

/// State for a single download chunk backed by its own temp file.
pub struct ChunkSlot {
    /// Open file descriptor.  `None` once the chunk has been released.
    file: Mutex<Option<Arc<File>>>,
    /// Bytes written to this chunk's file so far (monotonically increasing).
    bytes_written: AtomicU64,
    /// Set once the S3 UploadPart for this chunk completes.
    pub uploaded_to_s3: AtomicBool,
    /// Set once all downstream readers have finished with this chunk.
    pub readers_done: AtomicBool,
}

impl ChunkSlot {
    fn new() -> Self {
        Self {
            file: Mutex::new(None),
            bytes_written: AtomicU64::new(0),
            uploaded_to_s3: AtomicBool::new(false),
            readers_done: AtomicBool::new(false),
        }
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

    /// Try to release this chunk if both consumers are done.
    pub fn try_release(&self) {
        if self.uploaded_to_s3.load(Ordering::Acquire)
            && self.readers_done.load(Ordering::Acquire)
        {
            self.release_file();
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Acquire)
    }
}

// ---------------------------------------------------------------------------
// Per-object download state
// ---------------------------------------------------------------------------

pub struct InFlightDownload {
    pub object_size: u64,
    pub chunk_size: u64,
    chunks: Vec<ChunkSlot>,
    chunk_queue: Mutex<VecDeque<usize>>,
    notify: Notify,
    failed: AtomicU8,
    cancelled: AtomicBool,
    /// Set when the S3 multipart upload is fully completed.
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
}

impl InFlightDownload {
    pub fn new(object_size: u64, chunk_size: u64) -> Self {
        let num_chunks = if object_size == 0 {
            0
        } else {
            object_size.div_ceil(chunk_size) as usize
        };
        let queue: VecDeque<usize> = (0..num_chunks).collect();
        let chunks = (0..num_chunks).map(|_| ChunkSlot::new()).collect();
        Self {
            object_size,
            chunk_size,
            chunks,
            chunk_queue: Mutex::new(queue),
            notify: Notify::new(),
            failed: AtomicU8::new(0),
            cancelled: AtomicBool::new(false),
            s3_upload_complete: AtomicBool::new(false),
            consumed_count: AtomicUsize::new(0),
            consumer_notify: Notify::new(),
            eager_release: AtomicBool::new(false),
        }
    }

    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub fn chunk(&self, idx: usize) -> &ChunkSlot {
        &self.chunks[idx]
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
            if self.has_failed() {
                return Err(ProxyError::Upstream("upstream download failed".into()));
            }
            if self.is_cancelled() {
                return Err(ProxyError::Internal("download cancelled".into()));
            }
            self.notify.notified().await;
        }
    }

    /// Mark all chunks as not needing S3 upload (cache-hit path).
    /// This allows `try_release` to free chunk files as soon as the
    /// consumer finishes reading them, and enables eager release in
    /// `notify_consumed`.
    pub fn mark_no_upload_needed(&self) {
        self.eager_release.store(true, Ordering::Release);
        for chunk in &self.chunks {
            chunk.uploaded_to_s3.store(true, Ordering::Release);
        }
    }

    pub fn mark_failed(&self) {
        self.failed.store(1, Ordering::Release);
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
    /// will be released by the upload completion loop instead.
    pub fn notify_consumed(&self, chunk_idx: usize) {
        self.consumed_count.store(chunk_idx + 1, Ordering::Release);
        self.chunks[chunk_idx].readers_done.store(true, Ordering::Release);
        if self.eager_release.load(Ordering::Acquire) {
            self.chunks[chunk_idx].try_release();
        }
        self.consumer_notify.notify_waiters();
    }

    /// Block until `chunk_idx` is within the prefetch window.
    /// Workers call this before starting each chunk download to avoid
    /// racing too far ahead of the client and blowing out the page cache.
    pub async fn wait_for_consumer_window(&self, chunk_idx: usize, window: usize) {
        loop {
            if self.is_cancelled() || self.has_failed() {
                return;
            }
            let consumed = self.consumed_count.load(Ordering::Acquire);
            if chunk_idx < consumed + window {
                return;
            }
            self.consumer_notify.notified().await;
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
        priority.sort_unstable();
        for idx in priority {
            q.push_back(idx);
        }
        q.append(&mut rest);
    }

    pub fn pop_chunk(&self) -> Option<usize> {
        self.chunk_queue.lock().pop_front()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_byte_ranges() {
        let dl = InFlightDownload::new(5000, 1024);
        assert_eq!(dl.chunk_count(), 5);
        assert_eq!(dl.chunk_byte_range(0), (0, 1023));
        assert_eq!(dl.chunk_byte_range(4), (4096, 4999));
    }

    #[test]
    fn prioritize_moves_chunks_to_front() {
        let dl = InFlightDownload::new(10240, 1024);
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
        let dl = InFlightDownload::new(0, 1024);
        assert_eq!(dl.chunk_count(), 0);
        assert!(dl.pop_chunk().is_none());
    }

    #[test]
    fn record_written_tracks_progress() {
        let dl = InFlightDownload::new(10000, 5000);
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
