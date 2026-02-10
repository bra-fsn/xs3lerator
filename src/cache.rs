use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use filetime::FileTime;
use tokio::fs;
use tracing::warn;

/// Manages the on-disk cache directory: hierarchy creation, lookup,
/// verification, mtime touching, and writability probing.
pub struct CacheStore {
    root: PathBuf,
    hierarchy_level: usize,
    writable: AtomicBool,
}

impl CacheStore {
    pub async fn new(root: PathBuf, hierarchy_level: usize) -> io::Result<Self> {
        fs::create_dir_all(&root).await?;
        let writable = probe_writable(&root).await;
        Ok(Self {
            root,
            hierarchy_level,
            writable: AtomicBool::new(writable),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Compute the on-disk path for a content hash.
    /// E.g. for hash "e8b0..." and level 4 → <root>/e/8/b/0/e8b0...
    pub fn cache_path(&self, hash: &str) -> PathBuf {
        let mut p = self.root.clone();
        for ch in hash.chars().take(self.hierarchy_level) {
            p.push(ch.to_string());
        }
        p.push(hash);
        p
    }

    /// Compute a unique temp file path for an in-progress download.
    pub fn temp_path(&self, hash: &str) -> PathBuf {
        let mut path = self.cache_path(hash);
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        path.set_extension(format!("dl.{}.{}", std::process::id(), nanos));
        path
    }

    /// Pre-create the full hierarchy of directories (16^level leaf dirs).
    pub async fn create_hierarchy(&self) -> io::Result<()> {
        let hex: Vec<char> = "0123456789abcdef".chars().collect();
        let mut prefixes: Vec<PathBuf> = vec![PathBuf::new()];
        for _ in 0..self.hierarchy_level {
            let mut next = Vec::with_capacity(prefixes.len() * 16);
            for prefix in &prefixes {
                for &ch in &hex {
                    let mut p = prefix.clone();
                    p.push(ch.to_string());
                    next.push(p);
                }
            }
            prefixes = next;
        }
        for rel in prefixes {
            fs::create_dir_all(self.root.join(rel)).await?;
        }
        Ok(())
    }

    pub fn is_writable(&self) -> bool {
        self.writable.load(Ordering::Relaxed)
    }

    pub async fn refresh_writable(&self) {
        let ok = probe_writable(&self.root).await;
        self.writable.store(ok, Ordering::Relaxed);
        if !ok {
            warn!("cache directory not writable — passthrough mode");
        }
    }

    /// Check whether a cached file exists and has the expected size.
    /// Returns `Some(true)` on match, `Some(false)` on mismatch, `None` if absent.
    pub async fn verify_cached(&self, path: &Path, expected_size: u64) -> Option<bool> {
        match fs::metadata(path).await {
            Ok(md) => Some(md.len() == expected_size),
            Err(_) => None,
        }
    }

    /// Touch mtime to mark the file as recently used (for LRU ordering).
    pub async fn touch_mtime(&self, path: &Path) {
        let now = FileTime::from_system_time(SystemTime::now());
        if let Err(e) = filetime::set_file_mtime(path, now) {
            warn!(?path, "failed to update mtime: {e}");
        }
    }

    pub async fn ensure_parent_dir(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }

    /// Walk the cache directory and collect all regular files with their sizes
    /// and modification times. Temp download files (*.dl.*) are excluded.
    pub async fn scan_files(&self) -> io::Result<(u64, Vec<CacheFileEntry>)> {
        scan_dir_recursive(&self.root).await
    }
}

#[derive(Debug, Clone)]
pub struct CacheFileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub modified: SystemTime,
}

async fn scan_dir_recursive(root: &Path) -> io::Result<(u64, Vec<CacheFileEntry>)> {
    let mut stack = vec![root.to_path_buf()];
    let mut entries = Vec::new();
    let mut total_size = 0u64;

    while let Some(dir) = stack.pop() {
        let mut rd = match fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(e) => {
                warn!(?dir, "skipping unreadable directory: {e}");
                continue;
            }
        };
        while let Some(entry) = rd.next_entry().await? {
            let md = match entry.metadata().await {
                Ok(md) => md,
                Err(_) => continue,
            };
            let path = entry.path();
            if md.is_dir() {
                stack.push(path);
                continue;
            }
            // Skip in-progress download temp files
            if let Some(ext) = path.extension().and_then(OsStr::to_str) {
                if ext.starts_with("dl.") {
                    continue;
                }
            }
            // Skip probe marker files
            if path
                .file_name()
                .and_then(OsStr::to_str)
                .map_or(false, |n| n.starts_with(".xs3lerator"))
            {
                continue;
            }
            let size = md.len();
            total_size = total_size.saturating_add(size);
            entries.push(CacheFileEntry {
                path,
                size,
                modified: md.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            });
        }
    }

    Ok((total_size, entries))
}

async fn probe_writable(root: &Path) -> bool {
    let marker = root.join(".xs3lerator_probe");
    if fs::write(&marker, b"ok").await.is_err() {
        return false;
    }
    let _ = fs::remove_file(&marker).await;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_path_hierarchy() {
        let store = CacheStore {
            root: PathBuf::from("/cache"),
            hierarchy_level: 4,
            writable: AtomicBool::new(true),
        };
        let hash = "e8b0356886eb5804f25ad799b3db28ca32ae0205a47639d6e7a0afea";
        let path = store.cache_path(hash);
        assert_eq!(
            path,
            PathBuf::from("/cache/e/8/b/0/e8b0356886eb5804f25ad799b3db28ca32ae0205a47639d6e7a0afea")
        );
    }

    #[tokio::test]
    async fn cache_path_level_2() {
        let store = CacheStore {
            root: PathBuf::from("/c"),
            hierarchy_level: 2,
            writable: AtomicBool::new(true),
        };
        let path = store.cache_path("aabb112233");
        assert_eq!(path, PathBuf::from("/c/a/a/aabb112233"));
    }
}
