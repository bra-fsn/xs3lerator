use base64::Engine;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::error::ProxyError;
use crate::manifest::Manifest;

#[derive(Clone)]
pub struct EsClient {
    http: Client,
    base_url: String,
    index: String,
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
    manifest_b64: String,
}

#[derive(Serialize)]
struct IndexSettings {
    settings: IndexSettingsInner,
    mappings: IndexMappings,
}

#[derive(Serialize)]
struct IndexSettingsInner {
    number_of_shards: u32,
    number_of_replicas: u32,
    refresh_interval: String,
}

#[derive(Serialize)]
struct IndexMappings {
    dynamic: bool,
    properties: IndexProperties,
}

#[derive(Serialize)]
struct IndexProperties {
    manifest_b64: ManifestFieldMapping,
}

#[derive(Serialize)]
struct ManifestFieldMapping {
    #[serde(rename = "type")]
    field_type: String,
    index: bool,
    doc_values: bool,
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

        let data = base64::engine::general_purpose::STANDARD
            .decode(&source.manifest_b64)
            .map_err(|e| ProxyError::Internal(format!("base64 decode manifest: {e}")))?;

        let manifest = Manifest::deserialize(&data)?;
        Ok(Some(manifest))
    }

    pub async fn put_manifest(&self, key: &str, manifest_bytes: Vec<u8>) -> Result<(), ProxyError> {
        let b64 = base64::engine::general_purpose::STANDARD.encode(&manifest_bytes);
        let doc = ManifestDoc { manifest_b64: b64 };
        let url = self.doc_url(key);

        let resp = self
            .http
            .put(&url)
            .header("Content-Type", "application/json")
            .json(&doc)
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

    pub async fn create_index_if_not_exists(
        &self,
        shards: u32,
        replicas: u32,
    ) -> Result<(), ProxyError> {
        let url = format!("{}/{}", self.base_url, self.index);
        let body = IndexSettings {
            settings: IndexSettingsInner {
                number_of_shards: shards,
                number_of_replicas: replicas,
                refresh_interval: "60s".to_string(),
            },
            mappings: IndexMappings {
                dynamic: false,
                properties: IndexProperties {
                    manifest_b64: ManifestFieldMapping {
                        field_type: "keyword".to_string(),
                        index: false,
                        doc_values: false,
                    },
                },
            },
        };

        let resp = self
            .http
            .put(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ProxyError::Internal(format!("ES create index: {e}")))?;

        if resp.status().is_success() {
            info!(index = %self.index, "ES index created");
        } else {
            let body_text = resp.text().await.unwrap_or_default();
            if body_text.contains("resource_already_exists_exception") {
                debug!(index = %self.index, "ES index already exists");
            } else {
                return Err(ProxyError::Internal(format!(
                    "ES create index failed: {body_text}"
                )));
            }
        }

        Ok(())
    }
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
        let client = EsClient::new("http://localhost:9200", "xs3_manifests");
        assert_eq!(
            client.doc_url("my-bucket/https/host/hash123"),
            "http://localhost:9200/xs3_manifests/_doc/my-bucket%2Fhttps%2Fhost%2Fhash123"
        );
    }
}
