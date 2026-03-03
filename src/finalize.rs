use std::sync::atomic::Ordering;
use std::sync::Arc;

use tracing::{debug, error};

use crate::download::InFlightDownload;
use crate::error::ProxyError;
use crate::fdb_client::FdbClient;
use crate::manifest::Manifest;

/// Spawn a background task that waits for all chunks to be S3-committed,
/// then writes the manifest to FoundationDB.
pub fn spawn_finalize(
    cache_key: String,
    download: Arc<InFlightDownload>,
    fdb_client: Option<Arc<FdbClient>>,
) {
    tokio::spawn(async move {
        if let Err(e) = run_finalize(&cache_key, &download, fdb_client.as_deref()).await {
            error!(key = cache_key, "finalize pipeline failed: {e}");
            download.mark_failed();
        }
    });
}

async fn run_finalize(
    key: &str,
    download: &InFlightDownload,
    fdb_client: Option<&FdbClient>,
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
                return Err(ProxyError::Internal("download failed/cancelled during finalize".into()));
            }
            tokio::task::yield_now().await;
        }
    }

    // Build manifest with UUIDs
    let chunk_ids: Vec<[u8; 16]> = (0..num_chunks)
        .map(|idx| *download.chunk_id(idx))
        .collect();

    let manifest = Manifest {
        chunk_size: download.chunk_size,
        total_size: if download.is_stream_complete() {
            download.actual_total_bytes()
        } else {
            download.object_size
        },
        chunk_ids,
    };

    if let Some(fdb) = fdb_client {
        fdb.put_manifest(key, manifest.serialize()).await?;
    }

    download.s3_upload_complete.store(true, Ordering::Release);
    download.wake_waiters();
    debug!(key, "manifest written");

    // Decrement reader_count for the writer's share on each chunk.
    // This may trigger temp file release if no other readers remain.
    for idx in 0..num_chunks {
        download.chunk(idx).decrement_reader();
    }

    Ok(())
}
