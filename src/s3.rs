use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use crate::error::ProxyError;

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

// ---------------------------------------------------------------------------
// S3 Upload / Storage Operations
// ---------------------------------------------------------------------------

/// Handles S3 upload and storage operations (content-addressed chunks + manifests).
#[derive(Clone)]
pub struct S3Uploader {
    client: Client,
}

impl S3Uploader {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Put an object with `If-None-Match: *` for idempotent content-addressed writes.
    /// Returns `PreconditionFailed` if the object already exists.
    pub async fn put_object_if_none_match(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
    ) -> Result<(), ProxyError> {
        let result = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .if_none_match("*")
            .body(body)
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                let err_str = format_sdk_error(&e);
                if err_str.contains("PreconditionFailed") || err_str.contains("412") || err_str.contains("ConditionalRequestConflict") {
                    Err(ProxyError::PreconditionFailed)
                } else {
                    Err(ProxyError::Internal(format!("put_object {key}: {err_str}")))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

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
