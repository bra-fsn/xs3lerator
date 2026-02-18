use std::sync::atomic::Ordering;
use std::sync::Arc;

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use tracing::{debug, error, info};

use crate::chunk_cache::ChunkCache;
use crate::download::InFlightDownload;
use crate::error::ProxyError;
use crate::manifest::{hash_to_s3_key, Manifest};
use crate::s3::S3Uploader;

/// Source for a chunk upload: either a named file path or in-memory bytes.
pub enum ChunkSource {
    File(std::path::PathBuf),
    Memory(Bytes),
}

/// Upload a single content-addressed chunk to S3 with `If-None-Match: *`.
/// Returns `Ok(true)` if the chunk was uploaded, `Ok(false)` if it already existed.
pub async fn put_chunk(
    uploader: &S3Uploader,
    bucket: &str,
    data_prefix: &str,
    hash: &[u8; 32],
    source: ChunkSource,
) -> Result<bool, ProxyError> {
    let key = hash_to_s3_key(hash, data_prefix);

    let body = match source {
        ChunkSource::File(ref path) => ByteStream::from_path(path)
            .await
            .map_err(|e| ProxyError::Internal(format!("ByteStream from path: {e}")))?,
        ChunkSource::Memory(data) => ByteStream::from(data),
    };

    match uploader.put_object_if_none_match(bucket, &key, body).await {
        Ok(()) => Ok(true),
        Err(ProxyError::PreconditionFailed) => {
            debug!(key, "chunk already exists, skipping upload");
            Ok(false)
        }
        Err(e) => Err(e),
    }
}

/// Upload manifest bytes to S3 at `{map_prefix}{key}`.
pub async fn put_manifest(
    uploader: &S3Uploader,
    bucket: &str,
    map_prefix: &str,
    key: &str,
    manifest_bytes: Vec<u8>,
) -> Result<(), ProxyError> {
    let s3_key = format!("{map_prefix}{key}");
    let body = ByteStream::from(Bytes::from(manifest_bytes));
    uploader.put_object(bucket, &s3_key, body).await
}

/// Fetch a manifest from S3 at `{map_prefix}{key}`.
pub async fn get_manifest(
    uploader: &S3Uploader,
    bucket: &str,
    map_prefix: &str,
    key: &str,
) -> Result<Option<Manifest>, ProxyError> {
    let s3_key = format!("{map_prefix}{key}");
    match uploader.get_object_bytes(bucket, &s3_key).await {
        Ok(data) => {
            let manifest = Manifest::deserialize(&data)?;
            Ok(Some(manifest))
        }
        Err(ProxyError::NotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Spawn background tasks that upload all chunks of an in-flight download
/// to S3 as content-addressed objects, then write the manifest.
pub fn spawn_chunk_uploads(
    uploader: Arc<S3Uploader>,
    bucket: String,
    key: String,
    data_prefix: String,
    map_prefix: String,
    download: Arc<InFlightDownload>,
    chunk_cache: Option<Arc<ChunkCache>>,
) {
    tokio::spawn(async move {
        if let Err(e) = run_chunk_uploads(
            &uploader,
            &bucket,
            &key,
            &data_prefix,
            &map_prefix,
            &download,
            chunk_cache.as_deref(),
        )
        .await
        {
            error!(key, "chunk upload pipeline failed: {e}");
        }
    });
}

async fn run_chunk_uploads(
    uploader: &S3Uploader,
    bucket: &str,
    key: &str,
    data_prefix: &str,
    map_prefix: &str,
    download: &InFlightDownload,
    _chunk_cache: Option<&ChunkCache>,
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

    info!(key, num_chunks, "starting content-addressed chunk uploads");

    let mut handles = Vec::with_capacity(num_chunks);
    for idx in 0..num_chunks {
        let uploader = uploader.clone();
        let bucket = bucket.to_string();
        let data_prefix = data_prefix.to_string();
        let dl = download as *const InFlightDownload as usize;

        handles.push(tokio::spawn(async move {
            // Safety: download is held alive by the Arc in the spawning scope
            let download = unsafe { &*(dl as *const InFlightDownload) };
            upload_single_chunk(&uploader, &bucket, &data_prefix, download, idx).await
        }));
    }

    let mut all_ok = true;
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!(key, "chunk upload error: {e}");
                all_ok = false;
            }
            Err(e) => {
                error!(key, "chunk upload task panicked: {e}");
                all_ok = false;
            }
        }
    }

    if all_ok {
        // Collect hashes and write manifest
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
            put_manifest(uploader, bucket, map_prefix, key, manifest.serialize()).await?;
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

async fn upload_single_chunk(
    uploader: &S3Uploader,
    bucket: &str,
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
    let upload_size = std::cmp::min(expected, actual_written);
    if upload_size == 0 {
        return Ok(());
    }

    // Check if we have a cache path (named file) or need to read into memory
    let source = if let Some(path) = download.chunk(idx).get_cache_path() {
        ChunkSource::File(path)
    } else {
        let file = match download.chunk(idx).get_file() {
            Some(f) => f,
            None => return Ok(()),
        };
        let data = tokio::task::spawn_blocking(move || -> Result<Bytes, std::io::Error> {
            use std::os::unix::fs::FileExt;
            let mut buf = vec![0u8; upload_size as usize];
            file.read_exact_at(&mut buf, 0)?;
            Ok(Bytes::from(buf))
        })
        .await
        .map_err(|e| ProxyError::Internal(format!("spawn_blocking: {e}")))?
        .map_err(|e| ProxyError::Internal(format!("read chunk {idx}: {e}")))?;
        ChunkSource::Memory(data)
    };

    put_chunk(uploader, bucket, data_prefix, &hash, source).await?;
    Ok(())
}
