use std::sync::Arc;

use base64::Engine;
use foundationdb::api::NetworkAutoStop;
use foundationdb::{Database, RangeOption};
use tracing::debug;

use crate::error::ProxyError;
use crate::manifest::Manifest;

const MANIFEST_PREFIX: u8 = 0x03;
const SPLIT_VALUE_CHUNK: usize = 90_000;

#[derive(Clone)]
pub struct FdbClient {
    db: Arc<Database>,
}

impl FdbClient {
    pub fn new(cluster_file: Option<&str>) -> Result<Self, ProxyError> {
        let db = match cluster_file {
            Some(path) => Database::new(Some(path)),
            None => Database::default(),
        }
        .map_err(|e| ProxyError::Internal(format!("FDB open database: {e}")))?;

        Ok(Self { db: Arc::new(db) })
    }

    fn manifest_key(key: &str) -> Vec<u8> {
        let mut k = Vec::with_capacity(1 + key.len());
        k.push(MANIFEST_PREFIX);
        k.extend_from_slice(key.as_bytes());
        k
    }

    fn manifest_range_end(key: &str) -> Vec<u8> {
        let mut k = Self::manifest_key(key);
        k.push(0xFF);
        k
    }

    pub async fn get_manifest(&self, key: &str) -> Result<Option<Manifest>, ProxyError> {
        let begin = Self::manifest_key(key);
        let end = Self::manifest_range_end(key);

        let trx = self
            .db
            .create_trx()
            .map_err(|e| ProxyError::Internal(format!("FDB create_trx: {e}")))?;

        let range_opt = RangeOption::from((begin.as_slice(), end.as_slice()));
        let result = trx
            .get_range(&range_opt, 0, false)
            .await
            .map_err(|e| ProxyError::Internal(format!("FDB get_range: {e}")))?;

        let kvs = result.key_values();
        if kvs.is_empty() {
            return Ok(None);
        }

        let total_len: usize = kvs.iter().map(|kv| kv.value().len()).sum();
        let mut data = Vec::with_capacity(total_len);
        for kv in kvs {
            data.extend_from_slice(kv.value());
        }

        Manifest::deserialize(&data).map(Some)
    }

    pub async fn put_manifest(&self, key: &str, manifest_bytes: Vec<u8>) -> Result<(), ProxyError> {
        let begin = Self::manifest_key(key);
        let end = Self::manifest_range_end(key);

        let trx = self
            .db
            .create_trx()
            .map_err(|e| ProxyError::Internal(format!("FDB create_trx: {e}")))?;

        trx.clear_range(begin.as_slice(), end.as_slice());

        let chunks: Vec<&[u8]> = manifest_bytes.chunks(SPLIT_VALUE_CHUNK).collect();
        if chunks.len() == 1 {
            trx.set(&begin, chunks[0]);
        } else {
            trx.set(&begin, chunks[0]);
            for (i, chunk) in chunks.iter().enumerate().skip(1) {
                let mut continuation_key = begin.clone();
                continuation_key.push((i - 1) as u8);
                trx.set(&continuation_key, chunk);
            }
        }

        trx.commit()
            .await
            .map_err(|e| ProxyError::Internal(format!("FDB put_manifest commit: {e}")))?;

        debug!(key, "manifest written to FDB");
        Ok(())
    }

    pub async fn clear_manifest(&self, key: &str) -> Result<(), ProxyError> {
        let begin = Self::manifest_key(key);
        let end = Self::manifest_range_end(key);

        let trx = self
            .db
            .create_trx()
            .map_err(|e| ProxyError::Internal(format!("FDB create_trx: {e}")))?;

        trx.clear_range(begin.as_slice(), end.as_slice());

        trx.commit()
            .await
            .map_err(|e| ProxyError::Internal(format!("FDB clear_manifest commit: {e}")))?;

        debug!(key, "manifest cleared from FDB");
        Ok(())
    }
}

/// Decode a base64 manifest string into a Manifest struct.
/// Used for the X-Xs3lerator-Manifest header (still base64 over HTTP).
pub fn decode_manifest_b64(b64: &str) -> Result<Manifest, ProxyError> {
    let data = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| ProxyError::Internal(format!("base64 decode manifest: {e}")))?;
    Manifest::deserialize(&data)
}

/// Initialize the FDB network. Must be called before any FDB operations.
/// Returns a guard that must be kept alive for the program's lifetime.
///
/// # Safety
/// Must only be called once, before the tokio runtime is fully utilized.
pub unsafe fn boot() -> NetworkAutoStop {
    unsafe { foundationdb::boot() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_key_format() {
        let key = "https/host/hash123";
        let k = FdbClient::manifest_key(key);
        assert_eq!(k[0], MANIFEST_PREFIX);
        assert_eq!(&k[1..], key.as_bytes());
    }

    #[test]
    fn test_manifest_range_end() {
        let key = "test";
        let end = FdbClient::manifest_range_end(key);
        assert_eq!(end.len(), 1 + key.len() + 1);
        assert_eq!(*end.last().unwrap(), 0xFF);
    }

    #[test]
    fn test_decode_manifest_b64_roundtrip() {
        let manifest = Manifest {
            chunk_size: 8_388_608,
            total_size: 20_000_000,
            chunk_ids: vec![[0xab; 16], [0xcd; 16]],
        };
        let bytes = manifest.serialize();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let decoded = decode_manifest_b64(&b64).unwrap();
        assert_eq!(decoded.chunk_size, manifest.chunk_size);
        assert_eq!(decoded.total_size, manifest.total_size);
        assert_eq!(decoded.chunk_ids.len(), manifest.chunk_ids.len());
    }
}
