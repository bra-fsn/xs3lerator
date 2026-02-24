use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tracing::{debug, error, info};

use crate::download::InFlightDownload;
use crate::error::ProxyError;
use crate::es_client::EsClient;
use crate::manifest::{hash_to_chunk_path, Manifest};

/// Copy a single content-addressed chunk from its temp file to the data directory.
/// Idempotent: skips if the target path already exists.
pub fn persist_chunk(
    data_dir: &Path,
    data_prefix: &str,
    hash: &[u8; 32],
    source_file: &std::fs::File,
    source_len: u64,
) -> Result<bool, ProxyError> {
    let rel_path = hash_to_chunk_path(hash, data_prefix);
    let target = data_dir.join(&rel_path);

    if target.exists() {
        debug!(path = %target.display(), "chunk already exists, skipping");
        return Ok(false);
    }

    // Create parent directories
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            ProxyError::Internal(format!("mkdir {}: {e}", parent.display()))
        })?;
    }

    // Copy from source fd to target path using positional reads
    use std::os::unix::fs::FileExt;
    let mut target_file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&target);

    match target_file {
        Ok(ref mut out) => {
            use std::io::Write;
            let mut offset = 0u64;
            let mut buf = vec![0u8; 256 * 1024];
            while offset < source_len {
                let to_read = std::cmp::min(buf.len() as u64, source_len - offset) as usize;
                source_file.read_exact_at(&mut buf[..to_read], offset).map_err(|e| {
                    ProxyError::Internal(format!("read chunk at offset {offset}: {e}"))
                })?;
                out.write_all(&buf[..to_read]).map_err(|e| {
                    ProxyError::Internal(format!("write chunk {}: {e}", target.display()))
                })?;
                offset += to_read as u64;
            }
            Ok(true)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            debug!(path = %target.display(), "chunk already exists (race), skipping");
            Ok(false)
        }
        Err(e) => Err(ProxyError::Internal(format!(
            "create chunk {}: {e}",
            target.display()
        ))),
    }
}

/// Spawn background tasks that persist all chunks of an in-flight download
/// to the data directory, then write the manifest to Elasticsearch.
pub fn spawn_chunk_persist(
    data_dir: PathBuf,
    data_prefix: String,
    cache_key: String,
    download: Arc<InFlightDownload>,
    es_client: Option<Arc<EsClient>>,
) {
    tokio::spawn(async move {
        if let Err(e) = run_chunk_persist(
            &data_dir,
            &data_prefix,
            &cache_key,
            &download,
            es_client.as_deref(),
        )
        .await
        {
            error!(key = cache_key, "chunk persist pipeline failed: {e}");
        }
    });
}

async fn run_chunk_persist(
    data_dir: &Path,
    data_prefix: &str,
    key: &str,
    download: &InFlightDownload,
    es_client: Option<&EsClient>,
) -> Result<(), ProxyError> {
    let num_chunks = if download.is_stream_complete() {
        let total = download.actual_total_bytes();
        if total == 0 {
            return Ok(());
        }
        ((total + download.chunk_size - 1) / download.chunk_size) as usize
    } else {
        download.chunk_count()
    };

    if num_chunks == 0 {
        return Ok(());
    }

    info!(key, num_chunks, "starting content-addressed chunk persist");

    let mut handles = Vec::with_capacity(num_chunks);
    for idx in 0..num_chunks {
        let data_dir = data_dir.to_path_buf();
        let data_prefix = data_prefix.to_string();
        let dl = download as *const InFlightDownload as usize;

        handles.push(tokio::spawn(async move {
            // Safety: download is held alive by the Arc in the spawning scope
            let download = unsafe { &*(dl as *const InFlightDownload) };
            persist_single_chunk(&data_dir, &data_prefix, download, idx).await
        }));
    }

    let mut all_ok = true;
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!(key, "chunk persist error: {e}");
                all_ok = false;
            }
            Err(e) => {
                error!(key, "chunk persist task panicked: {e}");
                all_ok = false;
            }
        }
    }

    if all_ok {
        let mut hashes = Vec::with_capacity(num_chunks);
        for idx in 0..num_chunks {
            match download.chunk(idx).get_hash() {
                Some(h) => hashes.push(h),
                None => {
                    error!(key, idx, "missing hash for chunk, cannot write manifest");
                    all_ok = false;
                    break;
                }
            }
        }

        if all_ok {
            let manifest = Manifest {
                chunk_size: download.chunk_size,
                total_size: if download.is_stream_complete() {
                    download.actual_total_bytes()
                } else {
                    download.object_size
                },
                hashes,
            };
            if let Some(es) = es_client {
                es.put_manifest(key, manifest.serialize()).await?;
            }
            download.s3_upload_complete.store(true, Ordering::Release);
            info!(key, "manifest written");
        }
    }

    // Release chunk file handles
    for idx in 0..num_chunks {
        download.chunk(idx).uploaded_to_s3.store(true, Ordering::Release);
        download.chunk(idx).try_release();
    }

    Ok(())
}

async fn persist_single_chunk(
    data_dir: &Path,
    data_prefix: &str,
    download: &InFlightDownload,
    idx: usize,
) -> Result<(), ProxyError> {
    let expected = download.expected_chunk_len(idx);
    download.wait_for_bytes(idx, expected).await?;

    let hash = download.chunk(idx).get_hash().ok_or_else(|| {
        ProxyError::Internal(format!("chunk {idx}: hash not available after download"))
    })?;

    let actual_written = download.chunk(idx).bytes_written();
    let persist_size = std::cmp::min(expected, actual_written);
    if persist_size == 0 {
        return Ok(());
    }

    let file = match download.chunk(idx).get_file() {
        Some(f) => f,
        None => return Ok(()),
    };

    // Perform the blocking filesystem copy on a blocking thread
    let data_dir = data_dir.to_path_buf();
    let data_prefix = data_prefix.to_string();
    tokio::task::spawn_blocking(move || {
        persist_chunk(&data_dir, &data_prefix, &hash, &file, persist_size)
    })
    .await
    .map_err(|e| ProxyError::Internal(format!("spawn_blocking: {e}")))?
    .map(|_| ())
}
