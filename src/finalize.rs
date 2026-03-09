use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tracing::{debug, error, info, warn};

use crate::download::InFlightDownload;
use crate::error::ProxyError;
use crate::es_client::EsClient;
use crate::manifest::{Manifest, ID_LEN};
use crate::s3_client::S3Client;

/// Grace period before deleting orphaned chunks, giving in-flight readers
/// time to finish streaming the old manifest's data from S3.
const ORPHAN_DELETE_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

/// Spawn a background task that waits for all chunks to be S3-committed,
/// then writes the manifest to Elasticsearch.  If the key already had a
/// manifest, the old chunks that are no longer referenced are deleted from S3
/// after a short grace period.
pub fn spawn_finalize(
    cache_key: String,
    download: Arc<InFlightDownload>,
    es_client: Option<Arc<EsClient>>,
    s3_client: Option<Arc<S3Client>>,
    data_prefix: String,
) {
    tokio::spawn(async move {
        if let Err(e) = run_finalize(
            &cache_key,
            &download,
            es_client.as_deref(),
            s3_client.as_deref(),
            &data_prefix,
        )
        .await
        {
            error!(key = cache_key, "finalize pipeline failed: {e}");
            download.mark_failed();
        }
    });
}

async fn run_finalize(
    key: &str,
    download: &InFlightDownload,
    es_client: Option<&EsClient>,
    s3_client: Option<&S3Client>,
    data_prefix: &str,
) -> Result<(), ProxyError> {
    // For unknown-size (chunked) responses, wait until stream finishes
    // to know the actual number of chunks.
    if download.unknown_size && !download.is_stream_complete() {
        download.wait_for_stream_complete().await;
    }

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

    debug!(key, num_chunks, "waiting for all chunks to be S3-committed");

    // Wait for all chunks to finish downloading and be S3-committed.
    for idx in 0..num_chunks {
        let expected = download.expected_chunk_len(idx);
        download.wait_for_bytes(idx, expected).await?;

        // Spin-wait for s3_committed (set by finalize_chunk_s3 in fetcher)
        loop {
            if download.chunk(idx).s3_committed.load(Ordering::Acquire) {
                break;
            }
            if download.has_failed() || download.is_cancelled() {
                return Err(ProxyError::Internal(
                    "download failed/cancelled during finalize".into(),
                ));
            }
            tokio::task::yield_now().await;
        }
    }

    // Build manifest with UUIDs
    let chunk_ids: Vec<[u8; 16]> = (0..num_chunks).map(|idx| *download.chunk_id(idx)).collect();

    let manifest = Manifest {
        chunk_size: download.chunk_size,
        total_size: if download.is_stream_complete() {
            download.actual_total_bytes()
        } else {
            download.object_size
        },
        chunk_ids,
    };

    // Before writing the new manifest, snapshot the old one so we can clean up
    // orphaned S3 chunks after the swap.
    let old_manifest = if let Some(es) = es_client {
        match es.get_manifest(key).await {
            Ok(m) => m,
            Err(e) => {
                warn!(key, "failed to read old manifest for orphan cleanup: {e}");
                None
            }
        }
    } else {
        None
    };

    if let Some(es) = es_client {
        es.put_manifest(key, manifest.serialize()).await?;
    }

    download.s3_upload_complete.store(true, Ordering::Release);
    download.wake_waiters();
    debug!(key, "manifest written");

    // Decrement reader_count for the writer's share on each chunk.
    // This may trigger temp file release if no other readers remain.
    for idx in 0..num_chunks {
        download.chunk(idx).decrement_reader();
    }

    // Clean up orphaned S3 chunks from the previous manifest.
    if let (Some(old), Some(s3)) = (old_manifest, s3_client) {
        let new_ids: HashSet<[u8; ID_LEN]> = manifest.chunk_ids.iter().copied().collect();
        let orphaned = orphaned_chunk_ids(&old, &new_ids);

        if !orphaned.is_empty() {
            let s3 = s3.clone();
            let prefix = data_prefix.to_string();
            let key_owned = key.to_string();
            tokio::spawn(async move {
                // Wait a grace period so in-flight readers streaming the old
                // chunks have time to finish before we remove them.
                tokio::time::sleep(ORPHAN_DELETE_DELAY).await;
                info!(
                    cache_key = key_owned.as_str(),
                    num_orphaned = orphaned.len(),
                    "deleting orphaned S3 chunks from previous manifest"
                );
                s3.delete_chunks(&orphaned, &prefix, 8).await;
            });
        }
    }

    Ok(())
}

/// Compute chunk IDs present in `old` but absent in `new_ids`.
fn orphaned_chunk_ids(
    old: &Manifest,
    new_ids: &HashSet<[u8; ID_LEN]>,
) -> Vec<[u8; ID_LEN]> {
    old.chunk_ids
        .iter()
        .filter(|id| !new_ids.contains(*id))
        .copied()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manifest(chunk_ids: Vec<[u8; ID_LEN]>) -> Manifest {
        let n = chunk_ids.len();
        Manifest {
            chunk_size: 1024,
            total_size: 1024 * n as u64,
            chunk_ids,
        }
    }

    fn id(byte: u8) -> [u8; ID_LEN] {
        [byte; ID_LEN]
    }

    #[test]
    fn all_chunks_replaced() {
        let old = make_manifest(vec![id(1), id(2), id(3)]);
        let new_ids: HashSet<_> = [id(4), id(5), id(6)].into_iter().collect();
        let orphans = orphaned_chunk_ids(&old, &new_ids);
        assert_eq!(orphans.len(), 3);
        assert!(orphans.contains(&id(1)));
        assert!(orphans.contains(&id(2)));
        assert!(orphans.contains(&id(3)));
    }

    #[test]
    fn no_chunks_replaced_identical_manifests() {
        let old = make_manifest(vec![id(1), id(2)]);
        let new_ids: HashSet<_> = [id(1), id(2)].into_iter().collect();
        let orphans = orphaned_chunk_ids(&old, &new_ids);
        assert!(orphans.is_empty());
    }

    #[test]
    fn partial_overlap() {
        let old = make_manifest(vec![id(1), id(2), id(3)]);
        let new_ids: HashSet<_> = [id(2), id(4)].into_iter().collect();
        let orphans = orphaned_chunk_ids(&old, &new_ids);
        assert_eq!(orphans.len(), 2);
        assert!(orphans.contains(&id(1)));
        assert!(orphans.contains(&id(3)));
        assert!(!orphans.contains(&id(2)));
    }

    #[test]
    fn empty_old_manifest() {
        let old = make_manifest(vec![]);
        let new_ids: HashSet<_> = [id(1)].into_iter().collect();
        let orphans = orphaned_chunk_ids(&old, &new_ids);
        assert!(orphans.is_empty());
    }

    #[test]
    fn new_manifest_superset_of_old() {
        let old = make_manifest(vec![id(1), id(2)]);
        let new_ids: HashSet<_> = [id(1), id(2), id(3)].into_iter().collect();
        let orphans = orphaned_chunk_ids(&old, &new_ids);
        assert!(orphans.is_empty());
    }

    #[test]
    fn single_chunk_replaced() {
        let old = make_manifest(vec![id(0xaa)]);
        let new_ids: HashSet<_> = [id(0xbb)].into_iter().collect();
        let orphans = orphaned_chunk_ids(&old, &new_ids);
        assert_eq!(orphans, vec![id(0xaa)]);
    }
}
