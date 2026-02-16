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

/// Compute the download plan for an object using dual-ramp sizing.
///
/// Both concurrency and chunk size scale together:
///   - For small files, fewer connections are used (one per `min_chunk`-sized piece).
///   - For large files, exactly `max_concurrency` connections each download 1/Nth.
///   - Chunk size is clamped to 5 GiB (S3 multipart part ceiling).
///
/// The same function is used for both S3 and HTTP upstream downloads — only
/// the `max_concurrency` differs (e.g. 32 for S3, 8 for HTTP).
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

    let chunk_size = max(min_chunk, file_size.div_ceil(max_concurrency as u64));
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
    fn large_file_scales_chunk_size() {
        // 128 MB → 8 chunks of 16 MB, 8 connections
        let p = compute_chunk_plan(128 << 20, 8, 8 << 20);
        assert_eq!(p.num_chunks, 8);
        assert_eq!(p.concurrency, 8);
        assert_eq!(p.chunk_size, 16 << 20);
    }

    #[test]
    fn one_gig_file() {
        // 1 GB → 8 chunks of 128 MB, 8 connections
        let p = compute_chunk_plan(1 << 30, 8, 8 << 20);
        assert_eq!(p.concurrency, 8);
        assert_eq!(p.chunk_size, 128 << 20);
    }

    #[test]
    fn huge_file_hits_s3_part_ceiling() {
        // 48.8 TiB → chunk_size clamped to 5 GiB
        let size = 48_800_000_000_000u64;
        let p = compute_chunk_plan(size, 8, 8 << 20);
        assert_eq!(p.chunk_size, S3_MAX_PART_SIZE);
        assert!(p.num_chunks <= 10_000);
    }

    #[test]
    fn s3_concurrency_32() {
        // 1 GB with S3 concurrency 32 → 32 chunks of ~32 MB
        let p = compute_chunk_plan(1 << 30, 32, 8 << 20);
        assert_eq!(p.concurrency, 32);
        assert_eq!(p.num_chunks, 32);
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
