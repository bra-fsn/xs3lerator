use async_trait::async_trait;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use bytes::Bytes;

use crate::error::ProxyError;

/// Metadata returned by a HEAD on an S3 object.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub content_length: u64,
}

/// Abstraction over S3 access, enabling mock implementations for testing.
#[async_trait]
pub trait Upstream: Send + Sync {
    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectMeta, ProxyError>;

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

    /// Fetch an S3 range as a streaming ByteStream, yielding chunks
    /// incrementally instead of buffering the entire range into memory.
    pub async fn get_range_stream(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end_inclusive: u64,
    ) -> Result<ByteStream, ProxyError> {
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
        Ok(output.body)
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
        })
    }

}

// ---------------------------------------------------------------------------
// S3 Multipart Upload
// ---------------------------------------------------------------------------

/// Handles S3 multipart upload operations.
#[derive(Clone)]
pub struct S3Uploader {
    client: Client,
}

impl S3Uploader {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<String, ProxyError> {
        let output = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| ProxyError::Internal(format!("create multipart: {}", format_sdk_error(&e))))?;

        output
            .upload_id()
            .map(str::to_owned)
            .ok_or_else(|| ProxyError::Internal("no upload_id in response".into()))
    }

    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Bytes,
    ) -> Result<CompletedPart, ProxyError> {
        let output = self
            .client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|e| ProxyError::Internal(format!("upload part {part_number}: {}", format_sdk_error(&e))))?;

        Ok(CompletedPart::builder()
            .part_number(part_number)
            .set_e_tag(output.e_tag().map(str::to_owned))
            .build())
    }

    pub async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        mut parts: Vec<CompletedPart>,
    ) -> Result<(), ProxyError> {
        parts.sort_by_key(|p| p.part_number());
        self.client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .map_err(|e| ProxyError::Internal(format!("complete multipart: {}", format_sdk_error(&e))))?;
        Ok(())
    }

    pub async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), ProxyError> {
        self.client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| ProxyError::Internal(format!("abort multipart: {}", format_sdk_error(&e))))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn map_head_err(err: SdkError<HeadObjectError>) -> ProxyError {
    match &err {
        SdkError::ServiceError(se) if se.err().is_not_found() => {
            ProxyError::NotFound("object not found".into())
        }
        _ => ProxyError::Upstream(format_sdk_error(&err)),
    }
}

fn map_get_err(err: SdkError<GetObjectError>) -> ProxyError {
    match &err {
        SdkError::ServiceError(se) if se.err().is_no_such_key() => {
            ProxyError::NotFound("object not found".into())
        }
        _ => ProxyError::Upstream(format_sdk_error(&err)),
    }
}

fn format_sdk_error(err: &dyn std::error::Error) -> String {
    let mut msg = err.to_string();
    let mut cur: &dyn std::error::Error = err;
    while let Some(src) = cur.source() {
        msg.push_str(": ");
        msg.push_str(&src.to_string());
        cur = src;
    }
    msg
}
