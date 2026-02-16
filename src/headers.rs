use axum::http::HeaderMap;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

/// Parsed contract headers from a client request.
#[derive(Debug, Clone)]
pub struct ContractHeaders {
    /// Base64-decoded real upstream URL.
    pub upstream_url: Option<String>,
    /// When true, skip S3 cache read and go directly to upstream.
    pub cache_skip: bool,
    /// Known object size (from passsage metadata), for S3 range-GETs without HEAD.
    pub object_size: Option<u64>,
    /// Skip TLS certificate verification for this upstream request.
    pub tls_skip_verify: bool,
}

const HEADER_UPSTREAM_URL: &str = "x-xs3lerator-upstream-url";
const HEADER_CACHE_SKIP: &str = "x-xs3lerator-cache-skip";
const HEADER_OBJECT_SIZE: &str = "x-xs3lerator-object-size";
const HEADER_TLS_SKIP_VERIFY: &str = "x-xs3lerator-tls-skip-verify";

/// Contract header prefix — all headers with this prefix are stripped before
/// forwarding to the upstream server.
pub const CONTRACT_PREFIX: &str = "x-xs3lerator-";

/// Response header: whether data was served from S3 cache.
pub const RESP_CACHE_HIT: &str = "x-xs3lerator-cache-hit";
/// Response header: full object size in bytes.
pub const RESP_FULL_SIZE: &str = "x-xs3lerator-full-size";
/// Response header: degraded mode indicator.
pub const RESP_DEGRADED: &str = "x-xs3lerator-degraded";

/// Parse contract headers from an incoming request.
pub fn parse_contract_headers(headers: &HeaderMap) -> ContractHeaders {
    let upstream_url = headers
        .get(HEADER_UPSTREAM_URL)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| BASE64.decode(v).ok())
        .and_then(|bytes| String::from_utf8(bytes).ok());

    let cache_skip = headers
        .get(HEADER_CACHE_SKIP)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "true")
        .unwrap_or(false);

    let object_size = headers
        .get(HEADER_OBJECT_SIZE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    let tls_skip_verify = headers
        .get(HEADER_TLS_SKIP_VERIFY)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "true")
        .unwrap_or(false);

    ContractHeaders {
        upstream_url,
        cache_skip,
        object_size,
        tls_skip_verify,
    }
}

/// Headers to strip before forwarding to the upstream server.
const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "transfer-encoding",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "upgrade",
];

/// Build a filtered header map suitable for forwarding to the upstream server.
///
/// Strips:
///   - All `X-Xs3lerator-*` contract headers
///   - `Host` (replaced by the caller with the upstream host)
///   - `Range` (xs3lerator manages its own range strategy)
///   - Hop-by-hop headers
pub fn filter_upstream_headers(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in headers.iter() {
        let key = name.as_str().to_lowercase();
        if key.starts_with(CONTRACT_PREFIX) {
            continue;
        }
        if key == "host" || key == "range" {
            continue;
        }
        if HOP_BY_HOP_HEADERS.contains(&key.as_str()) {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

/// Parse bucket and S3 key from the URL path.
///
/// Path format: `/<bucket>/<key...>`
/// Returns `(bucket, key)` or None if the path is malformed.
pub fn parse_bucket_key(path: &str) -> Option<(String, String)> {
    let trimmed = path.trim_start_matches('/');
    let slash = trimmed.find('/')?;
    let bucket = &trimmed[..slash];
    let key = &trimmed[slash + 1..];
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some((bucket.to_string(), key.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn parse_bucket_key_normal() {
        let (b, k) = parse_bucket_key("/my-bucket/https/host/a/b/c/hash.iso").unwrap();
        assert_eq!(b, "my-bucket");
        assert_eq!(k, "https/host/a/b/c/hash.iso");
    }

    #[test]
    fn parse_bucket_key_no_key() {
        assert!(parse_bucket_key("/bucket/").is_none());
        assert!(parse_bucket_key("/bucket").is_none());
    }

    #[test]
    fn filter_strips_contract_headers() {
        let mut h = HeaderMap::new();
        h.insert("x-xs3lerator-cache-skip", HeaderValue::from_static("true"));
        h.insert("accept", HeaderValue::from_static("*/*"));
        h.insert("host", HeaderValue::from_static("example.com"));
        h.insert("range", HeaderValue::from_static("bytes=0-100"));
        let filtered = filter_upstream_headers(&h);
        assert!(filtered.get("x-xs3lerator-cache-skip").is_none());
        assert!(filtered.get("host").is_none());
        assert!(filtered.get("range").is_none());
        assert_eq!(filtered.get("accept").unwrap(), "*/*");
    }

    #[test]
    fn parse_contract_headers_full() {
        let mut h = HeaderMap::new();
        let url = BASE64.encode("https://example.com/file.iso");
        h.insert(HEADER_UPSTREAM_URL, url.parse().unwrap());
        h.insert(HEADER_CACHE_SKIP, HeaderValue::from_static("true"));
        h.insert(HEADER_OBJECT_SIZE, HeaderValue::from_static("12345"));
        h.insert(HEADER_TLS_SKIP_VERIFY, HeaderValue::from_static("true"));
        let c = parse_contract_headers(&h);
        assert_eq!(c.upstream_url.as_deref(), Some("https://example.com/file.iso"));
        assert!(c.cache_skip);
        assert_eq!(c.object_size, Some(12345));
        assert!(c.tls_skip_verify);
    }

    #[test]
    fn parse_contract_headers_absent() {
        let h = HeaderMap::new();
        let c = parse_contract_headers(&h);
        assert!(c.upstream_url.is_none());
        assert!(!c.cache_skip);
        assert!(c.object_size.is_none());
        assert!(!c.tls_skip_verify);
    }
}
