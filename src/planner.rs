use std::cmp::{max, min};

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
/// Chunk size is kept at the configured minimum (`min_chunk`, typically 8 MiB)
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
pub fn compute_chunk_plan(
    file_size: u64,
    max_concurrency: usize,
    min_chunk: u64,
) -> ChunkPlan {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_size_object() {
        let p = compute_chunk_plan(0, 8, 8 << 20);
        assert_eq!(p.concurrency, 1);
        assert_eq!(p.num_chunks, 0);
    }

    #[test]
    fn small_file_single_connection() {
        // 5 MB file, 8 MB min chunk → 1 chunk, 1 connection
        let p = compute_chunk_plan(5 * 1024 * 1024, 8, 8 << 20);
        assert_eq!(p.num_chunks, 1);
        assert_eq!(p.concurrency, 1);
        assert_eq!(p.chunk_size, 8 << 20);
    }

    #[test]
    fn medium_file_ramps_connections() {
        // 16 MB → 2 chunks, 2 connections
        let p = compute_chunk_plan(16 << 20, 8, 8 << 20);
        assert_eq!(p.num_chunks, 2);
        assert_eq!(p.concurrency, 2);
        assert_eq!(p.chunk_size, 8 << 20);
    }

    #[test]
    fn exact_max_concurrency() {
        // 64 MB → 8 chunks of 8 MB, 8 connections
        let p = compute_chunk_plan(64 << 20, 8, 8 << 20);
        assert_eq!(p.num_chunks, 8);
        assert_eq!(p.concurrency, 8);
        assert_eq!(p.chunk_size, 8 << 20);
    }

    #[test]
    fn large_file_keeps_small_chunks() {
        // 128 MB → 16 chunks of 8 MB, capped at 8 concurrent workers
        let p = compute_chunk_plan(128 << 20, 8, 8 << 20);
        assert_eq!(p.num_chunks, 16);
        assert_eq!(p.concurrency, 8);
        assert_eq!(p.chunk_size, 8 << 20);
    }

    #[test]
    fn one_gig_file() {
        // 1 GB → 128 chunks of 8 MB, 8 max concurrent
        let p = compute_chunk_plan(1 << 30, 8, 8 << 20);
        assert_eq!(p.concurrency, 8);
        assert_eq!(p.num_chunks, 128);
        assert_eq!(p.chunk_size, 8 << 20);
    }

    #[test]
    fn huge_file_respects_part_limit() {
        // 48.8 TiB → ceil(48.8T / 10,000) = ~4.88 GB per chunk (under 5 GiB ceiling)
        let size = 48_800_000_000_000u64;
        let p = compute_chunk_plan(size, 8, 8 << 20);
        assert_eq!(p.chunk_size, size.div_ceil(MAX_S3_PARTS));
        assert!(p.chunk_size < S3_MAX_PART_SIZE);
        assert!(p.num_chunks <= 10_000);
    }

    #[test]
    fn extreme_file_hits_s3_part_ceiling() {
        // 60 TiB → ceil(60T / 10,000) = 6 GB > 5 GiB → clamped to 5 GiB
        let size = 60_000_000_000_000u64;
        let p = compute_chunk_plan(size, 8, 8 << 20);
        assert_eq!(p.chunk_size, S3_MAX_PART_SIZE);
    }

    #[test]
    fn s3_concurrency_32() {
        // 1 GB with S3 concurrency 32 → 128 chunks of 8 MB, 32 max concurrent
        let p = compute_chunk_plan(1 << 30, 32, 8 << 20);
        assert_eq!(p.concurrency, 32);
        assert_eq!(p.num_chunks, 128);
        assert_eq!(p.chunk_size, 8 << 20);
    }

    #[test]
    fn huge_file_bumps_chunk_for_part_limit() {
        // 100 GiB: 8 MiB chunks would be 12,800 parts (> 10,000)
        // Planner bumps chunk_size to ceil(100 GiB / 10,000) ≈ 10.7 MiB
        let size = 100u64 * 1024 * 1024 * 1024;
        let p = compute_chunk_plan(size, 32, 8 << 20);
        assert!(p.num_chunks <= 10_000);
        assert!(p.chunk_size > 8 << 20);
    }

    #[test]
    fn chunk_byte_ranges_contiguous() {
        let size = 100_000_000u64;
        let p = compute_chunk_plan(size, 8, 8 << 20);
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
        // 65 MB with 8 MB chunks: 8 full + 1 MB remainder
        let size: u64 = 65 << 20;
        let chunk: u64 = 8 << 20;
        let last_idx = (size.div_ceil(chunk) - 1) as usize;
        assert_eq!(expected_chunk_len(0, chunk, size), chunk);
        assert_eq!(expected_chunk_len(last_idx, chunk, size), 1 << 20);
    }
}
