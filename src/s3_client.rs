use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjPath;
use object_store::{GetResultPayload, ObjectStore, PutPayload};
use tracing::{debug, warn};

use crate::error::ProxyError;
use crate::es_client::EsClient;
use crate::manifest::{id_to_s3_key, ID_LEN};

/// Wrapper around `object_store::aws::AmazonS3` providing chunk-level ops.
#[derive(Clone)]
pub struct S3Client {
    store: Arc<AmazonS3>,
}

impl S3Client {
    pub fn new(bucket: &str, region: &str, endpoint: Option<&str>) -> Result<Self, ProxyError> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region);

        if let Some(ep) = endpoint {
            builder = builder
                .with_endpoint(ep)
                .with_allow_http(true)
                .with_virtual_hosted_style_request(false);
        }

        if let Ok(key_id) = std::env::var("AWS_ACCESS_KEY_ID") {
            builder = builder.with_access_key_id(key_id);
        }
        if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(secret);
        }
        if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
            builder = builder.with_token(token);
        }

        let store = builder
            .build()
            .map_err(|e| ProxyError::Internal(format!("build S3 client: {e}")))?;

        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Stream a chunk from S3, returning a `BoxStream<Result<Bytes>>`.
    /// Returns `Err(NotFound)` if the object does not exist.
    pub async fn get_chunk_stream(
        &self,
        chunk_id: &[u8; ID_LEN],
        prefix: &str,
    ) -> Result<BoxStream<'static, Result<Bytes, ProxyError>>, ProxyError> {
        let path = ObjPath::from(id_to_s3_key(chunk_id, prefix));

        let result = self.store.get(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                ProxyError::NotFound(format!("S3 chunk not found: {path}"))
            }
            other => ProxyError::Internal(format!("S3 get chunk: {other}")),
        })?;

        match result.payload {
            GetResultPayload::Stream(stream) => Ok(stream
                .map(|r| r.map_err(|e| ProxyError::Internal(format!("S3 stream chunk: {e}"))))
                .boxed()),
            GetResultPayload::File(_, _) => Err(ProxyError::Internal(
                "unexpected File payload from S3".into(),
            )),
        }
    }

    /// Upload a chunk to S3 using a single PUT (optimal for 32 MiB chunks).
    pub async fn put_chunk(
        &self,
        chunk_id: &[u8; ID_LEN],
        prefix: &str,
        data: Bytes,
    ) -> Result<(), ProxyError> {
        let path = ObjPath::from(id_to_s3_key(chunk_id, prefix));
        let payload = PutPayload::from_bytes(data);
        self.store
            .put(&path, payload)
            .await
            .map_err(|e| ProxyError::Internal(format!("S3 put chunk: {e}")))?;
        Ok(())
    }

    /// Delete multiple chunks from S3 with bounded concurrency.
    pub async fn delete_chunks(
        &self,
        chunk_ids: &[[u8; ID_LEN]],
        prefix: &str,
        max_concurrent: usize,
    ) {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
        let mut handles = Vec::with_capacity(chunk_ids.len());

        for id in chunk_ids {
            let path = ObjPath::from(id_to_s3_key(id, prefix));
            let store = self.store.clone();
            let sem = semaphore.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await;
                if let Err(e) = store.delete(&path).await {
                    match e {
                        object_store::Error::NotFound { .. } => {}
                        other => warn!(path = %path, "S3 delete chunk failed: {other}"),
                    }
                }
            }));
        }

        for h in handles {
            let _ = h.await;
        }
    }
}

/// Cleanup a corrupt/orphaned manifest: clear from ES and delete all S3 chunks.
pub async fn cleanup_corrupt_manifest(
    es_client: &EsClient,
    s3_client: &S3Client,
    cache_key: &str,
    chunk_ids: &[[u8; ID_LEN]],
    prefix: &str,
) {
    if let Err(e) = es_client.clear_manifest(cache_key).await {
        warn!(cache_key, "failed to clear manifest from ES: {e}");
    }

    debug!(
        cache_key,
        num_chunks = chunk_ids.len(),
        "deleting orphaned S3 chunks"
    );
    s3_client.delete_chunks(chunk_ids, prefix, 8).await;
}
