use std::collections::HashMap;
use std::pin::Pin;

use async_trait::async_trait;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::Client;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio_util::io::ReaderStream;

use crate::error::ProxyError;

/// Metadata returned by a HEAD on an S3 object.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub content_length: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub cache_control: Option<String>,
    pub content_encoding: Option<String>,
    pub metadata: HashMap<String, String>,
}

pub type BoxByteStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, ProxyError>> + Send>>;

/// Abstraction over S3 access, enabling mock implementations for testing.
#[async_trait]
pub trait Upstream: Send + Sync {
    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectMeta, ProxyError>;

    /// Fetch a byte range and return all data at once.
    async fn get_range_bytes(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Bytes, ProxyError>;

    /// Fetch a byte range as an async stream (for passthrough mode).
    async fn get_range_stream(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end_inclusive: u64,
    ) -> Result<BoxByteStream, ProxyError>;
}

/// Production S3 client backed by the official AWS SDK.
#[derive(Clone)]
pub struct AwsUpstream {
    client: Client,
}

impl AwsUpstream {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Upstream for AwsUpstream {
    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectMeta, ProxyError> {
        let output = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(map_head_err)?;

        Ok(ObjectMeta {
            content_length: output
                .content_length()
                .unwrap_or_default()
                .max(0) as u64,
            content_type: output.content_type().map(str::to_owned),
            etag: output.e_tag().map(str::to_owned),
            last_modified: output.last_modified().and_then(|t| {
                // AWS SDK DateTime::to_string() produces ISO 8601, but HTTP
                // requires RFC 7231 format: "Mon, 09 Feb 2026 09:18:32 GMT"
                let epoch = std::time::UNIX_EPOCH
                    + std::time::Duration::from_secs(t.secs().max(0) as u64);
                httpdate::fmt_http_date(epoch).into()
            }),
            cache_control: output.cache_control().map(str::to_owned),
            content_encoding: output.content_encoding().map(str::to_owned),
            metadata: output.metadata().cloned().unwrap_or_default(),
        })
    }

    async fn get_range_bytes(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Bytes, ProxyError> {
        let range = format!("bytes={start}-{end_inclusive}");
        let output = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .map_err(map_get_err)?;

        output
            .body
            .collect()
            .await
            .map(|agg| agg.into_bytes())
            .map_err(|e| ProxyError::Upstream(e.to_string()))
    }

    async fn get_range_stream(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end_inclusive: u64,
    ) -> Result<BoxByteStream, ProxyError> {
        let range = format!("bytes={start}-{end_inclusive}");
        let output = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .map_err(map_get_err)?;

        let reader = output.body.into_async_read();
        let stream = ReaderStream::with_capacity(reader, 256 * 1024)
            .map(|r| r.map_err(|e| ProxyError::Upstream(e.to_string())));
        Ok(Box::pin(stream))
    }
}

fn map_head_err(err: SdkError<HeadObjectError>) -> ProxyError {
    match &err {
        SdkError::ServiceError(se) if se.err().is_not_found() => {
            ProxyError::NotFound("object not found".into())
        }
        _ => ProxyError::Upstream(err.to_string()),
    }
}

fn map_get_err(err: SdkError<GetObjectError>) -> ProxyError {
    match &err {
        SdkError::ServiceError(se) if se.err().is_no_such_key() => {
            ProxyError::NotFound("object not found".into())
        }
        _ => ProxyError::Upstream(err.to_string()),
    }
}
