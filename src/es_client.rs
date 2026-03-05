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

#[derive(Serialize)]
struct ClearManifestBody {
    doc: ClearManifestDoc,
}

#[derive(Serialize)]
struct ClearManifestDoc {
    manifest_b64: Option<String>,
}

#[derive(Serialize)]
struct StoredAtUpdateBody {
    doc: StoredAtDoc,
}

#[derive(Serialize)]
struct StoredAtDoc {
    stored_at: String,
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
        let url = format!("{}?retry_on_conflict=3", self.update_url(key));

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

    /// Remove the manifest_b64 field from an ES document, preserving all other
    /// fields written by passsage.  After this, get_manifest() returns None
    /// and passsage won't send the X-Xs3lerator-Manifest header, causing the
    /// next request to trigger a full upstream fetch.
    pub async fn clear_manifest(&self, key: &str) -> Result<(), ProxyError> {
        let body = ClearManifestBody {
            doc: ClearManifestDoc { manifest_b64: None },
        };
        let url = format!("{}?retry_on_conflict=3", self.update_url(key));

        let resp = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ProxyError::Internal(format!("ES clear_manifest: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ProxyError::Internal(format!(
                "ES clear_manifest {status}: {body}"
            )));
        }

        debug!(key, "manifest_b64 cleared from ES");
        Ok(())
    }

    /// Refresh the `stored_at` timestamp to "now" for a cache entry.
    ///
    /// Used by background revalidation: when upstream returns 304 Not Modified,
    /// the content hasn't changed but we reset the freshness clock so passsage
    /// considers the entry fresh on the next request.
    pub async fn refresh_stored_at(&self, key: &str) -> Result<(), ProxyError> {
        use std::time::SystemTime;

        let secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let now = format_unix_as_iso8601(secs);
        let body = StoredAtUpdateBody {
            doc: StoredAtDoc { stored_at: now },
        };
        let url = format!("{}?retry_on_conflict=3", self.update_url(key));

        let resp = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ProxyError::Internal(format!("ES refresh_stored_at: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ProxyError::Internal(format!(
                "ES refresh_stored_at {status}: {body}"
            )));
        }

        debug!(key, "stored_at refreshed in ES");
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

/// Format a UNIX timestamp as an ISO 8601 UTC string (e.g. "2026-03-05T09:50:00+00:00").
/// Compatible with Python's `datetime.fromisoformat()`.
fn format_unix_as_iso8601(epoch_secs: u64) -> String {
    let total_secs = epoch_secs;
    let secs_of_day = (total_secs % 86400) as u32;
    let z = (total_secs / 86400) as i64 + 719468;

    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };

    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}+00:00",
        year, month, day, hour, minute, second
    )
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
            chunk_ids: vec![[0xab; 16], [0xcd; 16]],
        };
        let bytes = manifest.serialize();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let decoded = decode_manifest_b64(&b64).unwrap();
        assert_eq!(decoded.chunk_size, manifest.chunk_size);
        assert_eq!(decoded.total_size, manifest.total_size);
        assert_eq!(decoded.chunk_ids.len(), manifest.chunk_ids.len());
    }

    #[test]
    fn update_body_has_no_retry_on_conflict() {
        let body = UpdateBody {
            doc: ManifestDoc {
                manifest_b64: "dGVzdA==".to_string(),
            },
            doc_as_upsert: true,
        };
        let json = serde_json::to_value(&body).unwrap();
        assert!(
            !json.as_object().unwrap().contains_key("retry_on_conflict"),
            "retry_on_conflict must be a query parameter, not in the JSON body"
        );
        assert_eq!(json["doc_as_upsert"], true);
        assert_eq!(json["doc"]["manifest_b64"], "dGVzdA==");
    }

    #[test]
    fn format_unix_epoch_zero() {
        assert_eq!(format_unix_as_iso8601(0), "1970-01-01T00:00:00+00:00");
    }

    #[test]
    fn format_unix_known_date() {
        // 2026-03-05T09:50:00 UTC = 1772704200
        assert_eq!(
            format_unix_as_iso8601(1772704200),
            "2026-03-05T09:50:00+00:00"
        );
    }

    #[test]
    fn stored_at_update_body_serializes() {
        let body = StoredAtUpdateBody {
            doc: StoredAtDoc {
                stored_at: "2026-03-05T09:50:00+00:00".to_string(),
            },
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["doc"]["stored_at"], "2026-03-05T09:50:00+00:00");
    }
}
