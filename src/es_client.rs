use base64::Engine;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::error::ProxyError;
use crate::manifest::Manifest;

#[derive(Clone)]
pub struct EsClient {
    http: Client,
    base_url: String,
    index: String,
}

#[derive(Serialize)]
struct UpdateBody {
    doc: ManifestDoc,
    doc_as_upsert: bool,
}

#[derive(Serialize)]
struct ManifestDoc {
    manifest_b64: String,
}

#[derive(Deserialize)]
struct EsGetResponse {
    #[serde(default)]
    found: bool,
    #[serde(default)]
    _source: Option<ManifestDocSource>,
}

#[derive(Deserialize)]
struct ManifestDocSource {
    manifest_b64: Option<String>,
}

impl EsClient {
    pub fn new(base_url: &str, index: &str) -> Self {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("build reqwest client");

        Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            index: index.to_string(),
        }
    }

    fn doc_url(&self, key: &str) -> String {
        let encoded = percent_encode_id(key);
        format!("{}/{}/_doc/{}", self.base_url, self.index, encoded)
    }

    fn update_url(&self, key: &str) -> String {
        let encoded = percent_encode_id(key);
        format!("{}/{}/_update/{}", self.base_url, self.index, encoded)
    }

    pub async fn get_manifest(&self, key: &str) -> Result<Option<Manifest>, ProxyError> {
        let url = self.doc_url(key);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| ProxyError::Internal(format!("ES get_manifest: {e}")))?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ProxyError::Internal(format!(
                "ES get_manifest {status}: {body}"
            )));
        }

        let es_resp: EsGetResponse = resp
            .json()
            .await
            .map_err(|e| ProxyError::Internal(format!("ES get_manifest parse: {e}")))?;

        if !es_resp.found {
            return Ok(None);
        }

        let source = es_resp
            ._source
            .ok_or_else(|| ProxyError::Internal("ES doc missing _source".into()))?;

        let b64 = match source.manifest_b64 {
            Some(v) if !v.is_empty() => v,
            _ => return Ok(None),
        };

        decode_manifest_b64(&b64).map(Some)
    }

    /// Upsert the manifest_b64 field via POST _update with doc_as_upsert,
    /// preserving any other fields in the document written by passsage.
    pub async fn put_manifest(&self, key: &str, manifest_bytes: Vec<u8>) -> Result<(), ProxyError> {
        let b64 = base64::engine::general_purpose::STANDARD.encode(&manifest_bytes);
        let body = UpdateBody {
            doc: ManifestDoc { manifest_b64: b64 },
            doc_as_upsert: true,
        };
        let url = self.update_url(key);

        let resp = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ProxyError::Internal(format!("ES put_manifest: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ProxyError::Internal(format!(
                "ES put_manifest {status}: {body}"
            )));
        }

        debug!(key, "manifest written to ES");
        Ok(())
    }
}

/// Decode a base64 manifest string into a Manifest struct.
pub fn decode_manifest_b64(b64: &str) -> Result<Manifest, ProxyError> {
    let data = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| ProxyError::Internal(format!("base64 decode manifest: {e}")))?;
    Manifest::deserialize(&data)
}

fn percent_encode_id(id: &str) -> String {
    let mut out = String::with_capacity(id.len() * 3);
    for b in id.bytes() {
        match b {
            b'/' => out.push_str("%2F"),
            b'%' => out.push_str("%25"),
            b'+' => out.push_str("%2B"),
            b' ' => out.push_str("%20"),
            b'#' => out.push_str("%23"),
            b'?' => out.push_str("%3F"),
            _ => out.push(b as char),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percent_encode() {
        assert_eq!(
            percent_encode_id("https/pypi.org/abc123+def456"),
            "https%2Fpypi.org%2Fabc123%2Bdef456"
        );
    }

    #[test]
    fn test_doc_url() {
        let client = EsClient::new("http://localhost:9200", "passsage_meta");
        assert_eq!(
            client.doc_url("https/host/hash123"),
            "http://localhost:9200/passsage_meta/_doc/https%2Fhost%2Fhash123"
        );
    }

    #[test]
    fn test_update_url() {
        let client = EsClient::new("http://localhost:9200", "passsage_meta");
        assert_eq!(
            client.update_url("https/host/abc123"),
            "http://localhost:9200/passsage_meta/_update/https%2Fhost%2Fabc123"
        );
    }

    #[test]
    fn test_decode_manifest_b64_roundtrip() {
        let manifest = Manifest {
            chunk_size: 8_388_608,
            total_size: 20_000_000,
            hashes: vec![[0xab; 32], [0xcd; 32]],
        };
        let bytes = manifest.serialize();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let decoded = decode_manifest_b64(&b64).unwrap();
        assert_eq!(decoded.chunk_size, manifest.chunk_size);
        assert_eq!(decoded.total_size, manifest.total_size);
        assert_eq!(decoded.hashes.len(), manifest.hashes.len());
    }
}
