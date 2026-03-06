use std::fs::{self, File, OpenOptions};
use std::io::{self};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::manifest::ID_LEN;

const HEX: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
];

const TMP_SUBDIR: &str = ".tmp";

pub struct DiskCache {
    cache_dir: PathBuf,
    /// Set when the cache is full and writes should be skipped.
    degraded: AtomicBool,
}

impl DiskCache {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self {
            cache_dir,
            degraded: AtomicBool::new(false),
        }
    }

    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Relaxed)
    }

    pub fn set_degraded(&self, v: bool) {
        self.degraded.store(v, Ordering::Relaxed);
    }

    /// Create the .tmp staging dir. Leaf directories are created on-demand by `finalize`.
    pub fn preseed_dirs(&self) -> io::Result<usize> {
        fs::create_dir_all(self.cache_dir.join(TMP_SUBDIR))?;
        Ok(0)
    }

    /// Return the cache path for a chunk id, if the file exists on disk.
    pub fn lookup(&self, chunk_id: &[u8; ID_LEN]) -> Option<PathBuf> {
        let path = self.chunk_path(chunk_id);
        if path.exists() {
            Some(path)
        } else {
            None
        }
    }

    /// Build the local cache path for a chunk: `cache_dir/h[0]/h[1]/h[2]/h[3]/<full_hex>`
    /// Mirrors the S3 key layout `data/a/8/6/8/a868…`.
    fn chunk_path(&self, chunk_id: &[u8; ID_LEN]) -> PathBuf {
        let hex: String = chunk_id.iter().map(|b| format!("{b:02x}")).collect();
        self.cache_dir
            .join(&hex[0..1])
            .join(&hex[1..2])
            .join(&hex[2..3])
            .join(&hex[3..4])
            .join(&hex)
    }

    /// Create a new temp file under `cache_dir/.tmp/`.
    pub fn temp_file(&self) -> io::Result<(File, PathBuf)> {
        let name = format!(
            ".xs3.{}.{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = self.cache_dir.join(TMP_SUBDIR).join(name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)?;
        Ok((file, path))
    }

    /// fsync the file, then rename from temp_path to the final chunk path.
    pub fn finalize(&self, temp_path: &Path, chunk_id: &[u8; ID_LEN]) -> io::Result<PathBuf> {
        let final_path = self.chunk_path(chunk_id);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new().read(true).open(temp_path)?;
        file.sync_all()?;
        drop(file);

        fs::rename(temp_path, &final_path)?;
        Ok(final_path)
    }

    /// Spawn a background GC thread that monitors disk usage and evicts files.
    pub fn spawn_gc_thread(self: &Arc<Self>, low_watermark_pct: u8, high_watermark_pct: u8) {
        let cache = self.clone();
        let low = low_watermark_pct;
        let high = high_watermark_pct;

        std::thread::Builder::new()
            .name("disk-cache-gc".into())
            .spawn(move || gc_loop(&cache, low, high))
            .expect("spawn GC thread");
    }
}

/// Compute the percentage of disk usage for the filesystem containing `path`.
fn disk_usage_pct(path: &Path) -> io::Result<u8> {
    let stat =
        nix::sys::statvfs::statvfs(path).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let total = stat.blocks();
    let avail = stat.blocks_available();
    if total == 0 {
        return Ok(0);
    }
    let used_pct = ((total - avail) as f64 / total as f64 * 100.0) as u8;
    Ok(used_pct)
}

fn gc_loop(cache: &DiskCache, low_watermark: u8, high_watermark: u8) {
    loop {
        std::thread::sleep(std::time::Duration::from_secs(30));

        let pct = match disk_usage_pct(cache.cache_dir()) {
            Ok(p) => p,
            Err(e) => {
                warn!("GC: statvfs failed: {e}");
                continue;
            }
        };

        if pct < low_watermark {
            if cache.is_degraded() {
                info!(
                    pct,
                    low_watermark, "GC: disk usage below low watermark, resuming caching"
                );
                cache.set_degraded(false);
            }
            continue;
        }

        if pct >= high_watermark {
            info!(
                pct,
                high_watermark, "GC: above high watermark, random eviction"
            );
            random_evict(cache, low_watermark);
        } else {
            debug!(pct, low_watermark, "GC: above low watermark, LRU eviction");
            lru_evict(cache, low_watermark);
        }
    }
}

/// Iterate all leaf directories in the 4-level hex tree: `cache_dir/a/b/c/d/`.
fn leaf_dirs(base: &Path) -> impl Iterator<Item = PathBuf> + '_ {
    HEX.iter().flat_map(move |&a| {
        HEX.iter().flat_map(move |&b| {
            HEX.iter().flat_map(move |&c| {
                HEX.iter().map(move |&d| {
                    base.join(format!("{a}"))
                        .join(format!("{b}"))
                        .join(format!("{c}"))
                        .join(format!("{d}"))
                })
            })
        })
    })
}

/// Random eviction: quickly delete files until usage drops below low_watermark.
fn random_evict(cache: &DiskCache, target_pct: u8) {
    let mut deleted = 0u64;
    for dir in leaf_dirs(cache.cache_dir()) {
        let Ok(rd) = fs::read_dir(&dir) else { continue };
        for file_entry in rd.flatten() {
            let path = file_entry.path();
            if !path.is_file() {
                continue;
            }
            if let Err(e) = fs::remove_file(&path) {
                warn!(path = %path.display(), "GC: remove failed: {e}");
                continue;
            }
            deleted += 1;

            if deleted % 100 == 0 {
                if let Ok(pct) = disk_usage_pct(cache.cache_dir()) {
                    if pct < target_pct {
                        info!(deleted, pct, "GC: random eviction reached target");
                        return;
                    }
                }
            }
        }
    }
    info!(deleted, "GC: random eviction sweep complete");
}

/// LRU eviction: scan all files, sort by atime, delete oldest first.
/// Processes files in batches to avoid loading all atimes into memory at once.
fn lru_evict(cache: &DiskCache, target_pct: u8) {
    const BATCH_SIZE: usize = 4096;
    let mut files: Vec<(u64, PathBuf)> = Vec::with_capacity(BATCH_SIZE);

    for dir in leaf_dirs(cache.cache_dir()) {
        let Ok(rd) = fs::read_dir(&dir) else { continue };
        for file_entry in rd.flatten() {
            let path = file_entry.path();
            if !path.is_file() {
                continue;
            }
            let Ok(meta) = path.metadata() else { continue };
            let atime = meta.atime() as u64;
            files.push((atime, path));

            if files.len() >= BATCH_SIZE {
                files.sort_unstable_by_key(|(atime, _)| *atime);
                let deleted = evict_batch(&mut files, cache, target_pct);
                if deleted > 0 {
                    if let Ok(pct) = disk_usage_pct(cache.cache_dir()) {
                        if pct < target_pct {
                            info!(pct, "GC: LRU eviction reached target");
                            return;
                        }
                    }
                }
            }
        }
    }

    if !files.is_empty() {
        files.sort_unstable_by_key(|(atime, _)| *atime);
        evict_batch(&mut files, cache, target_pct);
    }
}

fn evict_batch(files: &mut Vec<(u64, PathBuf)>, cache: &DiskCache, target_pct: u8) -> u64 {
    let mut deleted = 0u64;
    while let Some((_, path)) = files.first() {
        if let Err(e) = fs::remove_file(path) {
            warn!(path = %path.display(), "GC: remove failed: {e}");
        } else {
            deleted += 1;
        }
        files.remove(0);

        if deleted % 50 == 0 {
            if let Ok(pct) = disk_usage_pct(cache.cache_dir()) {
                if pct < target_pct {
                    files.clear();
                    return deleted;
                }
            }
        }
    }
    deleted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preseed_creates_tmp_dir() {
        let tmp = std::env::temp_dir().join(format!("xs3-test-{}", std::process::id()));
        fs::create_dir_all(&tmp).unwrap();
        let cache = DiskCache::new(tmp.clone());
        cache.preseed_dirs().unwrap();
        assert!(tmp.join(TMP_SUBDIR).is_dir());
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn temp_file_and_finalize() {
        use std::io::Write;
        let tmp = std::env::temp_dir().join(format!("xs3-fin-{}", std::process::id()));
        fs::create_dir_all(&tmp).unwrap();
        let cache = DiskCache::new(tmp.clone());
        cache.preseed_dirs().unwrap();

        let (mut file, temp_path) = cache.temp_file().unwrap();
        file.write_all(b"hello").unwrap();
        drop(file);

        let id = [0xab; 16];
        let final_path = cache.finalize(&temp_path, &id).unwrap();
        assert!(final_path.exists());
        assert!(!temp_path.exists());
        // 0xab repeated → hex "abab…"; path should be a/b/a/b/<full_hex>
        assert!(final_path.starts_with(tmp.join("a").join("b").join("a").join("b")));

        let found = cache.lookup(&id);
        assert!(found.is_some());
        let _ = fs::remove_dir_all(&tmp);
    }
}
