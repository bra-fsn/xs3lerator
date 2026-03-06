use std::cmp::{max, min};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

/// Computed download strategy for an object.
#[derive(Debug, Clone, Copy)]
pub struct ChunkPlan {
    #[allow(dead_code)] // validated by planner tests
    pub concurrency: usize,
    pub chunk_size: u64,
    #[allow(dead_code)] // validated by planner tests
    pub num_chunks: usize,
}

/// Maximum S3 multipart upload part size (5 GiB).
const S3_MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// Maximum number of parts in an S3 multipart upload.
const MAX_S3_PARTS: u64 = 10_000;

/// Compute the download plan for an object.
///
/// Chunk size is kept at the configured minimum (`min_chunk`, typically 32 MiB)
/// so that the client can start receiving data quickly — even when many workers
/// share a slow link, each small chunk completes fast and the client can burst
/// through a batch of pre-fetched chunks.
///
/// Chunk size is only bumped when the file would exceed S3's 10,000-part limit
/// or the 5 GiB per-part ceiling.
///
/// Actual download concurrency is managed separately by a worker pool with
/// progressive ramp-up (see `run_s3_download` / `run_adaptive_upstream`).
/// The `concurrency` field here is the *maximum* number of workers.
pub fn compute_chunk_plan(file_size: u64, max_concurrency: usize, min_chunk: u64) -> ChunkPlan {
    if file_size == 0 {
        return ChunkPlan {
            concurrency: 1,
            chunk_size: min_chunk.max(1),
            num_chunks: 0,
        };
    }

    let chunk_size = max(min_chunk, file_size.div_ceil(MAX_S3_PARTS));
    let chunk_size = min(chunk_size, S3_MAX_PART_SIZE);
    let num_chunks = file_size.div_ceil(chunk_size) as usize;
    let concurrency = min(max_concurrency, num_chunks).max(1);

    ChunkPlan {
        concurrency,
        chunk_size,
        num_chunks,
    }
}

/// Byte range `(start, end_inclusive)` for a given chunk index.
pub fn chunk_byte_range(idx: usize, chunk_size: u64, object_size: u64) -> (u64, u64) {
    let start = idx as u64 * chunk_size;
    let end = min(start + chunk_size - 1, object_size.saturating_sub(1));
    (start, end)
}

/// Expected byte count for a given chunk.
pub fn expected_chunk_len(idx: usize, chunk_size: u64, object_size: u64) -> u64 {
    let start = idx as u64 * chunk_size;
    let end = min(start + chunk_size, object_size);
    end - start
}

// ---------------------------------------------------------------------------
// Adaptive concurrency ramp-up controller
// ---------------------------------------------------------------------------
//
// Research across 77 Debian mirrors + HuggingFace showed five server archetypes:
//
//  1. Linear scalers   (annexia, mirhosting)  — 13→218 MiB/s at 16x
//  2. Moderate scalers  (ethz, kaist)          — sweet spot 8-16 streams
//  3. Diminishing       (keystealth, steadfast) — fast 1-stream, <2x gain
//  4. Regressors        (byfly, osuosl)         — slower with parallel (<1x)
//  5. Error-cliff       (HuggingFace @64)       — hard connection cap
//
// The ramp starts at INITIAL_WORKERS, measures aggregate throughput over
// RAMP_WINDOW completed chunks, and doubles workers when throughput
// improved by ≥ SCALE_UP_THRESHOLD (50%).  It freezes when:
//   - throughput gain drops below the threshold (diminishing returns), or
//   - error rate exceeds ERROR_CEILING (10%), or
//   - max_concurrency is reached.
//
// The controller is lock-free on the hot path (worker reads) and uses a
// Mutex only for the infrequent ramp-up decision (once per doubling).

const INITIAL_WORKERS: usize = 4;
const RAMP_WINDOW: usize = 4;
const SCALE_UP_THRESHOLD: f64 = 0.50;
const ERROR_CEILING: f64 = 0.10;
const TIME_TRIGGER_SECS: f64 = 5.0;

/// Per-level throughput snapshot taken when enough chunks complete.
#[derive(Debug, Clone, Copy)]
struct RampSample {
    #[allow(dead_code)]
    concurrency: usize,
    bytes_per_sec: f64,
}

/// Adaptive concurrency controller for upstream HTTP downloads.
///
/// Workers call [`active_workers`] to check whether they should run.
/// The download loop calls [`record_chunk`] after each chunk completes
/// and [`record_error`] on failures; the controller internally decides
/// when to double the worker count.
pub struct ConcurrencyRamp {
    max_concurrency: usize,
    active: AtomicUsize,
    frozen: AtomicUsize, // 0 = not frozen, >0 = frozen at this level

    // Throughput tracking for the current ramp level
    level_bytes: AtomicU64,
    level_chunks: AtomicUsize,
    level_errors: AtomicUsize,
    level_start: Mutex<Instant>,
    samples: Mutex<Vec<RampSample>>,
}

impl ConcurrencyRamp {
    pub fn new(max_concurrency: usize) -> Self {
        let initial = INITIAL_WORKERS.min(max_concurrency).max(1);
        Self {
            max_concurrency,
            active: AtomicUsize::new(initial),
            frozen: AtomicUsize::new(0),
            level_bytes: AtomicU64::new(0),
            level_chunks: AtomicUsize::new(0),
            level_errors: AtomicUsize::new(0),
            level_start: Mutex::new(Instant::now()),
            samples: Mutex::new(Vec::new()),
        }
    }

    /// Current number of workers that should be running.
    pub fn active_workers(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    /// Record a successfully completed chunk of `bytes` length.
    /// Internally triggers a ramp-up evaluation when enough chunks
    /// have completed at the current level.
    pub fn record_chunk(&self, bytes: u64) {
        self.level_bytes.fetch_add(bytes, Ordering::Relaxed);
        let chunks = self.level_chunks.fetch_add(1, Ordering::Relaxed) + 1;

        if self.frozen.load(Ordering::Relaxed) > 0 {
            return;
        }
        if chunks >= RAMP_WINDOW {
            self.evaluate_ramp();
        }
    }

    /// Record a failed chunk (connection error, non-2xx, timeout).
    pub fn record_error(&self) {
        self.level_errors.fetch_add(1, Ordering::Relaxed);

        // Check error rate immediately — don't wait for the window.
        let errors = self.level_errors.load(Ordering::Relaxed);
        let chunks = self.level_chunks.load(Ordering::Relaxed);
        let total = chunks + errors;
        if total >= 2 && errors as f64 / total as f64 > ERROR_CEILING {
            self.freeze();
        }
    }

    fn evaluate_ramp(&self) {
        let mut samples = self.samples.lock();
        let current = self.active.load(Ordering::Acquire);

        let bytes = self.level_bytes.swap(0, Ordering::Relaxed);
        let _chunks = self.level_chunks.swap(0, Ordering::Relaxed);
        let errors = self.level_errors.swap(0, Ordering::Relaxed);

        let elapsed = {
            let mut start = self.level_start.lock();
            let e = start.elapsed();
            *start = Instant::now();
            e
        };

        let secs = elapsed.as_secs_f64();
        if secs < 0.001 {
            return;
        }

        let total_attempts = _chunks + errors;
        if total_attempts > 0 && errors as f64 / total_attempts as f64 > ERROR_CEILING {
            self.frozen.store(current, Ordering::Release);
            return;
        }

        let bps = bytes as f64 / secs;
        samples.push(RampSample {
            concurrency: current,
            bytes_per_sec: bps,
        });

        if current >= self.max_concurrency {
            self.frozen.store(current, Ordering::Release);
            return;
        }

        // Compare to previous level's throughput
        if samples.len() >= 2 {
            let prev = samples[samples.len() - 2].bytes_per_sec;
            if prev > 0.0 {
                let gain = (bps - prev) / prev;
                if gain < SCALE_UP_THRESHOLD {
                    // Throughput plateau — keep current level
                    self.frozen.store(current, Ordering::Release);
                    return;
                }
            }
        }

        // Scale up: double workers (capped at max)
        let next = (current * 2).min(self.max_concurrency);
        self.active.store(next, Ordering::Release);
    }

    fn freeze(&self) {
        let current = self.active.load(Ordering::Acquire);
        // Roll back to previous level if we have one
        let prev = (current / 2).max(INITIAL_WORKERS).max(1);
        self.active.store(prev, Ordering::Release);
        self.frozen.store(prev, Ordering::Release);
    }

    /// True when the controller has settled on a final concurrency level.
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Acquire) > 0
    }

    /// Time-based ramp trigger: call periodically (e.g. from worker sleep
    /// loops).  If TIME_TRIGGER_SECS have elapsed at the current level and
    /// no chunk has completed yet, evaluates ramp-up based on bytes
    /// downloaded so far.  This prevents stalling on slow upstreams where
    /// a single chunk takes minutes.
    pub fn check_time_trigger(&self) {
        if self.frozen.load(Ordering::Relaxed) > 0 {
            return;
        }
        if self.level_chunks.load(Ordering::Relaxed) > 0 {
            return; // chunk-based evaluation will handle it
        }
        let elapsed_secs = self.level_start.lock().elapsed().as_secs_f64();
        if elapsed_secs < TIME_TRIGGER_SECS {
            return;
        }
        let bytes = self.level_bytes.load(Ordering::Relaxed);
        if bytes == 0 {
            // Nothing downloaded yet — still ramp up; more connections might
            // help even if the first ones haven't produced data yet (slow
            // TLS handshake, etc.).
            let current = self.active.load(Ordering::Acquire);
            if current < self.max_concurrency {
                let next = (current * 2).min(self.max_concurrency);
                self.active.store(next, Ordering::Release);
                *self.level_start.lock() = Instant::now();
            }
            return;
        }
        // We have partial data but no completed chunk — evaluate as if
        // this were a completed window so the ramp can grow.
        self.evaluate_ramp();
    }

    /// Summary of throughput samples collected during ramp-up.
    #[allow(dead_code)]
    pub fn samples(&self) -> Vec<(usize, f64)> {
        self.samples
            .lock()
            .iter()
            .map(|s| (s.concurrency, s.bytes_per_sec))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_size_object() {
        let p = compute_chunk_plan(0, 8, 32 << 20);
        assert_eq!(p.concurrency, 1);
        assert_eq!(p.num_chunks, 0);
    }

    #[test]
    fn small_file_single_connection() {
        // 5 MB file, 32 MB min chunk → 1 chunk, 1 connection
        let p = compute_chunk_plan(5 * 1024 * 1024, 8, 32 << 20);
        assert_eq!(p.num_chunks, 1);
        assert_eq!(p.concurrency, 1);
        assert_eq!(p.chunk_size, 32 << 20);
    }

    #[test]
    fn medium_file_ramps_connections() {
        // 64 MB → 2 chunks, 2 connections
        let p = compute_chunk_plan(64 << 20, 8, 32 << 20);
        assert_eq!(p.num_chunks, 2);
        assert_eq!(p.concurrency, 2);
        assert_eq!(p.chunk_size, 32 << 20);
    }

    #[test]
    fn exact_max_concurrency() {
        // 256 MB → 8 chunks of 32 MB, 8 connections
        let p = compute_chunk_plan(256 << 20, 8, 32 << 20);
        assert_eq!(p.num_chunks, 8);
        assert_eq!(p.concurrency, 8);
        assert_eq!(p.chunk_size, 32 << 20);
    }

    #[test]
    fn large_file_keeps_small_chunks() {
        // 512 MB → 16 chunks of 32 MB, capped at 8 concurrent workers
        let p = compute_chunk_plan(512 << 20, 8, 32 << 20);
        assert_eq!(p.num_chunks, 16);
        assert_eq!(p.concurrency, 8);
        assert_eq!(p.chunk_size, 32 << 20);
    }

    #[test]
    fn one_gig_file() {
        // 1 GB → 32 chunks of 32 MB, 8 max concurrent
        let p = compute_chunk_plan(1 << 30, 8, 32 << 20);
        assert_eq!(p.concurrency, 8);
        assert_eq!(p.num_chunks, 32);
        assert_eq!(p.chunk_size, 32 << 20);
    }

    #[test]
    fn huge_file_respects_part_limit() {
        // 48.8 TiB → ceil(48.8T / 10,000) = ~4.88 GB per chunk (under 5 GiB ceiling)
        let size = 48_800_000_000_000u64;
        let p = compute_chunk_plan(size, 8, 32 << 20);
        assert_eq!(p.chunk_size, size.div_ceil(MAX_S3_PARTS));
        assert!(p.chunk_size < S3_MAX_PART_SIZE);
        assert!(p.num_chunks <= 10_000);
    }

    #[test]
    fn extreme_file_hits_s3_part_ceiling() {
        // 60 TiB → ceil(60T / 10,000) = 6 GB > 5 GiB → clamped to 5 GiB
        let size = 60_000_000_000_000u64;
        let p = compute_chunk_plan(size, 8, 32 << 20);
        assert_eq!(p.chunk_size, S3_MAX_PART_SIZE);
    }

    #[test]
    fn s3_concurrency_32() {
        // 1 GB with S3 concurrency 32 → 32 chunks of 32 MB, 32 max concurrent
        let p = compute_chunk_plan(1 << 30, 32, 32 << 20);
        assert_eq!(p.concurrency, 32);
        assert_eq!(p.num_chunks, 32);
        assert_eq!(p.chunk_size, 32 << 20);
    }

    #[test]
    fn huge_file_bumps_chunk_for_part_limit() {
        // 400 GiB: 32 MiB chunks would be 12,800 parts (> 10,000)
        // Planner bumps chunk_size to ceil(400 GiB / 10,000) ≈ 41.9 MiB
        let size = 400u64 * 1024 * 1024 * 1024;
        let p = compute_chunk_plan(size, 32, 32 << 20);
        assert!(p.num_chunks <= 10_000);
        assert!(p.chunk_size > 32 << 20);
    }

    #[test]
    fn chunk_byte_ranges_contiguous() {
        let size = 100_000_000u64;
        let p = compute_chunk_plan(size, 8, 32 << 20);
        let mut prev_end = None;
        for idx in 0..p.num_chunks {
            let (start, end) = chunk_byte_range(idx, p.chunk_size, size);
            if let Some(pe) = prev_end {
                assert_eq!(start, pe + 1);
            } else {
                assert_eq!(start, 0);
            }
            prev_end = Some(end);
        }
        assert_eq!(prev_end.unwrap(), size - 1);
    }

    #[test]
    fn expected_chunk_len_last_chunk() {
        // 65 MB with 32 MB chunks: 2 full + 1 MB remainder
        let size: u64 = 65 << 20;
        let chunk: u64 = 32 << 20;
        let last_idx = (size.div_ceil(chunk) - 1) as usize;
        assert_eq!(expected_chunk_len(0, chunk, size), chunk);
        assert_eq!(expected_chunk_len(last_idx, chunk, size), 1 << 20);
    }

    #[test]
    fn ramp_starts_at_initial_workers() {
        let ramp = ConcurrencyRamp::new(32);
        assert_eq!(ramp.active_workers(), INITIAL_WORKERS);
        assert!(!ramp.is_frozen());
    }

    #[test]
    fn ramp_initial_capped_at_max() {
        let ramp = ConcurrencyRamp::new(2);
        assert_eq!(ramp.active_workers(), 2);
    }

    #[test]
    fn ramp_scales_up_on_good_throughput() {
        let ramp = ConcurrencyRamp::new(32);
        assert_eq!(ramp.active_workers(), 4);

        // First window: 4 chunks at ~100 MB/s baseline
        for _ in 0..RAMP_WINDOW {
            std::thread::sleep(std::time::Duration::from_millis(5));
            ramp.record_chunk(8 << 20);
        }
        // First sample recorded; no previous to compare → scales up
        assert_eq!(ramp.active_workers(), 8);

        // Second window: 4 chunks at much higher throughput → keeps scaling
        for _ in 0..RAMP_WINDOW {
            std::thread::sleep(std::time::Duration::from_millis(2));
            ramp.record_chunk(16 << 20);
        }
        assert_eq!(ramp.active_workers(), 16);
    }

    #[test]
    fn ramp_freezes_on_plateau() {
        let ramp = ConcurrencyRamp::new(64);

        // First window
        for _ in 0..RAMP_WINDOW {
            std::thread::sleep(std::time::Duration::from_millis(5));
            ramp.record_chunk(8 << 20);
        }
        assert_eq!(ramp.active_workers(), 8); // scaled up

        // Second window: same throughput → gain < 50% → freeze
        for _ in 0..RAMP_WINDOW {
            std::thread::sleep(std::time::Duration::from_millis(5));
            ramp.record_chunk(8 << 20);
        }
        assert!(ramp.is_frozen());
        assert_eq!(ramp.active_workers(), 8);
    }

    #[test]
    fn ramp_freezes_on_errors() {
        let ramp = ConcurrencyRamp::new(32);

        // Trigger enough errors to exceed the 10% ceiling
        ramp.record_chunk(8 << 20);
        ramp.record_error();
        ramp.record_error();
        // 1 chunk + 2 errors = 33% error rate → freeze & rollback
        assert!(ramp.is_frozen());
        assert!(ramp.active_workers() <= INITIAL_WORKERS);
    }

    #[test]
    fn ramp_freezes_at_max() {
        let ramp = ConcurrencyRamp::new(8);

        // Window 1: scale 4→8
        for _ in 0..RAMP_WINDOW {
            std::thread::sleep(std::time::Duration::from_millis(3));
            ramp.record_chunk(8 << 20);
        }
        assert_eq!(ramp.active_workers(), 8);

        // Window 2: at max → frozen
        for _ in 0..RAMP_WINDOW {
            std::thread::sleep(std::time::Duration::from_millis(3));
            ramp.record_chunk(16 << 20);
        }
        assert!(ramp.is_frozen());
        assert_eq!(ramp.active_workers(), 8);
    }

    #[test]
    fn time_trigger_ramps_with_no_completed_chunks() {
        let ramp = ConcurrencyRamp::new(32);
        assert_eq!(ramp.active_workers(), 4);

        // Simulate slow upstream: no chunks complete, but 5+ seconds pass
        *ramp.level_start.lock() = Instant::now() - std::time::Duration::from_secs(6);
        ramp.check_time_trigger();
        assert_eq!(ramp.active_workers(), 8);

        // Another 5 seconds, still no chunks → ramp again
        *ramp.level_start.lock() = Instant::now() - std::time::Duration::from_secs(6);
        ramp.check_time_trigger();
        assert_eq!(ramp.active_workers(), 16);
    }

    #[test]
    fn time_trigger_noop_if_chunks_completing() {
        let ramp = ConcurrencyRamp::new(32);
        ramp.record_chunk(8 << 20); // chunk completed → time trigger defers

        *ramp.level_start.lock() = Instant::now() - std::time::Duration::from_secs(6);
        ramp.check_time_trigger();
        // Should stay at 4 because chunk-based path will handle ramp
        assert_eq!(ramp.active_workers(), 4);
    }

    #[test]
    fn time_trigger_noop_before_deadline() {
        let ramp = ConcurrencyRamp::new(32);
        // Only 1 second elapsed, below TIME_TRIGGER_SECS
        ramp.check_time_trigger();
        assert_eq!(ramp.active_workers(), 4);
    }

    #[test]
    fn time_trigger_respects_frozen() {
        let ramp = ConcurrencyRamp::new(32);
        // Force freeze via errors
        ramp.record_chunk(8 << 20);
        ramp.record_error();
        ramp.record_error();
        assert!(ramp.is_frozen());
        let frozen_level = ramp.active_workers();

        *ramp.level_start.lock() = Instant::now() - std::time::Duration::from_secs(10);
        ramp.check_time_trigger();
        assert_eq!(ramp.active_workers(), frozen_level);
    }
}
