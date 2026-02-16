use std::sync::atomic::Ordering;
use std::sync::Arc;

use aws_sdk_s3::types::CompletedPart;
use bytes::Bytes;
use parking_lot::Mutex;
use tracing::{error, info, warn};

use crate::download::InFlightDownload;
use crate::s3::S3Uploader;

/// Spawn a background task that uploads all chunks of an in-flight download
/// to S3 using multipart upload.  Each chunk is streamed to S3 concurrently
/// with the download writer — the upload worker follows behind using
/// `wait_for_bytes()`.
///
/// Returns immediately.  The upload runs in a tokio task.
pub fn spawn_s3_upload(
    uploader: Arc<S3Uploader>,
    bucket: String,
    key: String,
    download: Arc<InFlightDownload>,
) {
    tokio::spawn(async move {
        if let Err(e) = run_s3_upload(&uploader, &bucket, &key, &download).await {
            error!(key, "S3 multipart upload failed: {e}");
        }
    });
}

async fn run_s3_upload(
    uploader: &S3Uploader,
    bucket: &str,
    key: &str,
    download: &InFlightDownload,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let num_chunks = download.chunk_count();
    if num_chunks == 0 {
        return Ok(());
    }

    let upload_id = uploader.create_multipart(bucket, key).await?;
    info!(key, upload_id, num_chunks, "S3 multipart upload started");

    let completed_parts: Arc<Mutex<Vec<CompletedPart>>> =
        Arc::new(Mutex::new(Vec::with_capacity(num_chunks)));
    let mut handles = Vec::with_capacity(num_chunks);

    for idx in 0..num_chunks {
        let uploader = uploader.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.clone();
        let dl = download as *const InFlightDownload as usize;
        let parts = completed_parts.clone();

        // Safety: we hold an Arc<InFlightDownload> in the spawning scope and
        // the tasks complete before the download is dropped.
        handles.push(tokio::spawn(async move {
            let download = unsafe { &*(dl as *const InFlightDownload) };
            upload_chunk_to_s3(
                &uploader, &bucket, &key, &upload_id, download, idx, &parts,
            )
            .await
        }));
    }

    let mut all_ok = true;
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!(key, "S3 part upload error: {e}");
                all_ok = false;
            }
            Err(e) => {
                error!(key, "S3 upload task panicked: {e}");
                all_ok = false;
            }
        }
    }

    if all_ok {
        let parts = completed_parts.lock().clone();
        uploader.complete_multipart(bucket, key, &upload_id, parts).await?;
        download.s3_upload_complete.store(true, Ordering::Release);
        info!(key, "S3 multipart upload completed");
    } else {
        warn!(key, "S3 multipart upload aborting due to errors");
        let _ = uploader.abort_multipart(bucket, key, &upload_id).await;
    }

    // Mark all chunks as uploaded (even on failure, so chunk fds are released)
    for idx in 0..num_chunks {
        download.chunk(idx).uploaded_to_s3.store(true, Ordering::Release);
        download.chunk(idx).try_release();
    }

    Ok(())
}

async fn upload_chunk_to_s3(
    uploader: &S3Uploader,
    bucket: &str,
    key: &str,
    upload_id: &str,
    download: &InFlightDownload,
    idx: usize,
    completed_parts: &Mutex<Vec<CompletedPart>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let expected = download.expected_chunk_len(idx);

    // Wait for the full chunk to be downloaded before uploading.
    // (S3 UploadPart needs the complete body; streaming partial
    // data requires a more complex streaming body implementation
    // which we can add later as an optimization.)
    download.wait_for_bytes(idx, expected).await?;

    let file = download.chunk(idx).get_file().ok_or_else(|| {
        Box::<dyn std::error::Error + Send + Sync>::from("chunk file released before S3 upload")
    })?;

    // Read the full chunk from the file (should hit page cache)
    let data = tokio::task::spawn_blocking(move || -> Result<Bytes, std::io::Error> {
        use std::os::unix::fs::FileExt;
        let mut buf = vec![0u8; expected as usize];
        file.read_exact_at(&mut buf, 0)?;
        Ok(Bytes::from(buf))
    })
    .await??;

    let part_number = idx as i32 + 1;
    let part = uploader
        .upload_part(bucket, key, upload_id, part_number, data)
        .await?;

    completed_parts.lock().push(part);
    download.chunk(idx).uploaded_to_s3.store(true, Ordering::Release);
    download.chunk(idx).try_release();

    Ok(())
}
