use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;
use tracing::{info, warn};

use crate::cache::CacheStore;

pub struct EvictorConfig {
    pub max_bytes: u64,
    pub watermark_percent: u8,
    pub target_percent: u8,
    pub interval_seconds: u64,
}

/// Background loop that periodically scans the cache directory and evicts
/// the oldest files (by mtime) when total usage crosses the watermark.
///
/// Designed to use minimal memory: it only collects file stats during the
/// scan, sorts by mtime, and deletes until the target is reached. No
/// persistent in-memory index is kept between scans.
pub async fn run_evictor(cache: Arc<CacheStore>, cfg: EvictorConfig) {
    let interval = Duration::from_secs(cfg.interval_seconds.max(1));
    loop {
        if let Err(e) = evict_once(&cache, &cfg).await {
            warn!("cache eviction scan failed: {e}");
        }
        sleep(interval).await;
    }
}

async fn evict_once(
    cache: &CacheStore,
    cfg: &EvictorConfig,
) -> std::io::Result<()> {
    let (total_size, mut files) = cache.scan_files().await?;
    if total_size == 0 {
        return Ok(());
    }

    let watermark =
        cfg.max_bytes.saturating_mul(cfg.watermark_percent as u64) / 100;
    if total_size < watermark {
        return Ok(());
    }

    let target =
        cfg.max_bytes.saturating_mul(cfg.target_percent as u64) / 100;

    // Sort oldest-first by mtime for LRU eviction
    files.sort_by_key(|f| f.modified);

    let mut current = total_size;
    let mut evicted_bytes = 0u64;
    let mut evicted_count = 0u64;

    for entry in &files {
        if current <= target {
            break;
        }
        match tokio::fs::remove_file(&entry.path).await {
            Ok(()) => {
                current = current.saturating_sub(entry.size);
                evicted_bytes += entry.size;
                evicted_count += 1;
            }
            Err(e) => {
                warn!(path = ?entry.path, "evict failed: {e}");
            }
        }
    }

    if evicted_count > 0 {
        info!(
            evicted_count,
            evicted_bytes,
            before = total_size,
            after = current,
            "cache eviction completed"
        );
    }
    Ok(())
}
