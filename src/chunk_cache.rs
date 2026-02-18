use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

const TMP_SUBDIR: &str = ".tmp";

/// Optional filesystem-based content-addressed chunk cache with LRU eviction.
pub struct ChunkCache {
    root: PathBuf,
    max_size: u64,
}

impl ChunkCache {
    pub fn new(root: PathBuf, max_size: u64) -> io::Result<Self> {
        fs::create_dir_all(root.join(TMP_SUBDIR))?;
        for i in 0u8..=255 {
            let hex = format!("{i:02x}");
            let p0 = &hex[0..1];
            let dir = root.join(p0);
            let _ = fs::create_dir(&dir);
        }
        Ok(Self { root, max_size })
    }

    /// Try to open a cached chunk for reading.
    pub fn get(&self, hash: &[u8; 32]) -> Option<File> {
        let path = self.cache_path(hash);
        File::open(&path).ok()
    }

    /// Compute the cache path for a given chunk hash (2-level prefix).
    pub fn cache_path(&self, hash: &[u8; 32]) -> PathBuf {
        let (p0, p1, hex) = crate::manifest::hash_to_cache_path(hash);
        self.root.join(p0).join(p1).join(hex)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn max_size(&self) -> u64 {
        self.max_size
    }
}

/// Background LRU eviction task.
pub fn spawn_evictor(cache: Arc<ChunkCache>, interval: Duration) {
    tokio::spawn(async move {
        let jitter = Duration::from_millis(rand_u64() % 5000);
        tokio::time::sleep(jitter).await;

        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = run_eviction(&cache) {
                warn!("chunk cache eviction error: {e}");
            }
        }
    });
}

fn run_eviction(cache: &ChunkCache) -> io::Result<()> {
    let mut entries: Vec<(PathBuf, u64, std::time::SystemTime)> = Vec::new();
    let mut total_size: u64 = 0;

    for p0_entry in fs::read_dir(cache.root())? {
        let p0_entry = p0_entry?;
        if !p0_entry.file_type()?.is_dir() {
            continue;
        }
        let p0_name = p0_entry.file_name();
        if p0_name.to_string_lossy().starts_with('.') {
            continue;
        }
        for p1_entry in fs::read_dir(p0_entry.path())? {
            let p1_entry = p1_entry?;
            if !p1_entry.file_type()?.is_dir() {
                continue;
            }
            for file_entry in fs::read_dir(p1_entry.path())? {
                let file_entry = file_entry?;
                if !file_entry.file_type()?.is_file() {
                    continue;
                }
                let meta = file_entry.metadata()?;
                let size = meta.len();
                let atime = meta.accessed().unwrap_or(meta.modified().unwrap_or(std::time::UNIX_EPOCH));
                total_size += size;
                entries.push((file_entry.path(), size, atime));
            }
        }
    }

    if total_size <= cache.max_size() {
        return Ok(());
    }

    entries.sort_by_key(|(_, _, t)| *t);

    let mut freed: u64 = 0;
    let target = total_size - cache.max_size();
    for (path, size, _) in &entries {
        if freed >= target {
            break;
        }
        match fs::remove_file(path) {
            Ok(()) => freed += size,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => warn!("eviction: failed to remove {}: {e}", path.display()),
        }
    }

    info!(
        total_size,
        freed,
        entries = entries.len(),
        "chunk cache eviction complete"
    );
    Ok(())
}

fn rand_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_none_for_missing() {
        let dir = std::env::temp_dir().join(format!("xs3_cc_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let cache = ChunkCache::new(dir.clone(), 1_000_000).unwrap();
        let hash: [u8; 32] = [0xab; 32];
        assert!(cache.get(&hash).is_none());
        let _ = fs::remove_dir_all(&dir);
    }
}
