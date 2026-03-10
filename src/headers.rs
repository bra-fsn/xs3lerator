use std::time::Duration;

use axum::http::HeaderMap;

/// Parsed contract headers from a client request.
#[derive(Debug, Clone)]
pub struct ContractHeaders {
    /// Cache key for ES manifest / content-addressed storage.
    /// When absent, xs3lerator operates as a pure download accelerator.
    pub cache_key: Option<String>,
    /// Known object size (from passsage metadata), for S3 range-GETs without HEAD.
    pub object_size: Option<u64>,
    /// Skip TLS certificate verification for this upstream request.
    pub tls_skip_verify: bool,
    /// When true, follow upstream HTTP redirects instead of returning the 3xx
    /// response to the caller.  Default false — redirects are returned as-is
    /// so the caller (passsage) can cache them independently per RFC 9111.
    pub follow_redirects: bool,
    /// When true, skip cache read and go directly to upstream even when a
    /// cache key is provided.  The downloaded data is still persisted to cache.
    pub cache_skip: bool,
    /// Pre-supplied manifest (base64) from passsage, allowing xs3lerator to
    /// skip the ES lookup on cache hits.
    pub manifest_b64: Option<String>,
    /// Cached ETag from passsage for conditional revalidation.
    /// xs3lerator forwards this as `If-None-Match` to the upstream.
    pub if_none_match: Option<String>,
    /// Cached Last-Modified from passsage for conditional revalidation.
    /// xs3lerator forwards this as `If-Modified-Since` to the upstream.
    pub if_modified_since: Option<String>,
    /// When true, serve cached content on upstream errors instead of passing
    /// the error through to the caller.
    pub stale_if_error: bool,
    /// When true, serve from cache immediately and revalidate upstream in the
    /// background. Requires `if_none_match` to be set. xs3lerator returns the
    /// cached response right away, then spawns an async task to revalidate.
    pub background_revalidate: bool,
    /// Per-request connect timeout override (seconds). Takes precedence over
    /// the server-wide --upstream-connect-timeout default.
    pub connect_timeout: Option<Duration>,
    /// Per-request read timeout override (seconds). Takes precedence over
    /// the server-wide --upstream-read-timeout default. 0 = no timeout.
    pub read_timeout: Option<Duration>,
}

const HEADER_CACHE_KEY: &str = "x-xs3lerator-cache-key";
const HEADER_CACHE_SKIP: &str = "x-xs3lerator-cache-skip";
const HEADER_OBJECT_SIZE: &str = "x-xs3lerator-object-size";
const HEADER_TLS_SKIP_VERIFY: &str = "x-xs3lerator-tls-skip-verify";
const HEADER_FOLLOW_REDIRECTS: &str = "x-xs3lerator-follow-redirects";
const HEADER_MANIFEST: &str = "x-xs3lerator-manifest";
const HEADER_IF_NONE_MATCH: &str = "x-xs3lerator-if-none-match";
const HEADER_IF_MODIFIED_SINCE: &str = "x-xs3lerator-if-modified-since";
const HEADER_STALE_IF_ERROR: &str = "x-xs3lerator-stale-if-error";
const HEADER_BACKGROUND_REVALIDATE: &str = "x-xs3lerator-background-revalidate";
const HEADER_CONNECT_TIMEOUT: &str = "x-xs3lerator-connect-timeout";
const HEADER_READ_TIMEOUT: &str = "x-xs3lerator-read-timeout";

/// Contract header prefix — all headers with this prefix are stripped before
/// forwarding to the upstream server.
pub const CONTRACT_PREFIX: &str = "x-xs3lerator-";

/// Response header: whether data was served from cache.
pub const RESP_CACHE_HIT: &str = "x-xs3lerator-cache-hit";
/// Response header: full object size in bytes.
pub const RESP_FULL_SIZE: &str = "x-xs3lerator-full-size";
/// Response header: degraded mode indicator.
pub const RESP_DEGRADED: &str = "x-xs3lerator-degraded";
/// Response header: conditional revalidation result.
/// Values: "true" (304 from upstream), "stale-error" (upstream error, served cached).
pub const RESP_REVALIDATED: &str = "x-xs3lerator-revalidated";
/// Response header: background revalidation accepted.
pub const RESP_BACKGROUND_REVALIDATE: &str = "x-xs3lerator-background-revalidate";

/// Parse contract headers from an incoming request.
pub fn parse_contract_headers(headers: &HeaderMap) -> ContractHeaders {
    let cache_key = headers
        .get(HEADER_CACHE_KEY)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(str::to_owned);

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

    let follow_redirects = headers
        .get(HEADER_FOLLOW_REDIRECTS)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "true")
        .unwrap_or(false);

    let manifest_b64 = headers
        .get(HEADER_MANIFEST)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(str::to_owned);

    let if_none_match = headers
        .get(HEADER_IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(str::to_owned);

    let if_modified_since = headers
        .get(HEADER_IF_MODIFIED_SINCE)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(str::to_owned);

    let stale_if_error = headers
        .get(HEADER_STALE_IF_ERROR)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "true")
        .unwrap_or(false);

    let background_revalidate = headers
        .get(HEADER_BACKGROUND_REVALIDATE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "true")
        .unwrap_or(false);

    let connect_timeout = headers
        .get(HEADER_CONNECT_TIMEOUT)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|&v| v > 0.0)
        .map(Duration::from_secs_f64);

    let read_timeout = headers
        .get(HEADER_READ_TIMEOUT)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| {
            if v <= 0.0 {
                Duration::ZERO
            } else {
                Duration::from_secs_f64(v)
            }
        });

    ContractHeaders {
        cache_key,
        cache_skip,
        object_size,
        tls_skip_verify,
        follow_redirects,
        manifest_b64,
        if_none_match,
        if_modified_since,
        stale_if_error,
        background_revalidate,
        connect_timeout,
        read_timeout,
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

/// Like [`filter_upstream_headers`] but also strips client conditional headers
/// (`If-None-Match`, `If-Modified-Since`).  Use when xs3lerator owns the
/// caching layer — conditional revalidation is driven exclusively by the
/// contract headers (`X-Xs3lerator-If-None-Match` / `If-Modified-Since`).
/// Forwarding the client's own values would trigger a 304 from upstream that
/// xs3lerator cannot serve when its cache is empty.
pub fn filter_upstream_headers_no_conditionals(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in headers.iter() {
        let key = name.as_str().to_lowercase();
        if key.starts_with(CONTRACT_PREFIX) {
            continue;
        }
        if key == "host" || key == "range" {
            continue;
        }
        if key == "if-none-match" || key == "if-modified-since" {
            continue;
        }
        if HOP_BY_HOP_HEADERS.contains(&key.as_str()) {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

/// Extract the upstream URL from the request URI.
///
/// The upstream URL is encoded in the path+query of the incoming request:
/// `GET /https://example.com/path?q=1` → upstream URL is `https://example.com/path?q=1`
///
/// Returns None if the path is empty or just `/`.
pub fn parse_upstream_url(uri: &axum::http::Uri) -> Option<String> {
    let pq = uri.path_and_query()?;
    let raw = pq.as_str().strip_prefix('/')?;
    if raw.is_empty() {
        return None;
    }
    Some(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, Uri};

    #[test]
    fn parse_upstream_url_normal() {
        let uri: Uri = "/https://example.com/file.iso".parse().unwrap();
        assert_eq!(
            parse_upstream_url(&uri).unwrap(),
            "https://example.com/file.iso"
        );
    }

    #[test]
    fn parse_upstream_url_with_query() {
        let uri: Uri = "/https://example.com/file?token=abc&v=1".parse().unwrap();
        assert_eq!(
            parse_upstream_url(&uri).unwrap(),
            "https://example.com/file?token=abc&v=1"
        );
    }

    #[test]
    fn parse_upstream_url_empty() {
        let uri: Uri = "/".parse().unwrap();
        assert!(parse_upstream_url(&uri).is_none());
    }

    #[test]
    fn filter_strips_contract_headers() {
        let mut h = HeaderMap::new();
        h.insert("x-xs3lerator-cache-key", HeaderValue::from_static("key1"));
        h.insert("accept", HeaderValue::from_static("*/*"));
        h.insert("host", HeaderValue::from_static("example.com"));
        h.insert("range", HeaderValue::from_static("bytes=0-100"));
        let filtered = filter_upstream_headers(&h);
        assert!(filtered.get("x-xs3lerator-cache-key").is_none());
        assert!(filtered.get("host").is_none());
        assert!(filtered.get("range").is_none());
        assert_eq!(filtered.get("accept").unwrap(), "*/*");
    }

    #[test]
    fn parse_contract_headers_full() {
        let mut h = HeaderMap::new();
        h.insert(
            HEADER_CACHE_KEY,
            HeaderValue::from_static("https/host/abc123"),
        );
        h.insert(HEADER_CACHE_SKIP, HeaderValue::from_static("true"));
        h.insert(HEADER_OBJECT_SIZE, HeaderValue::from_static("12345"));
        h.insert(HEADER_TLS_SKIP_VERIFY, HeaderValue::from_static("true"));
        h.insert(HEADER_FOLLOW_REDIRECTS, HeaderValue::from_static("true"));
        let c = parse_contract_headers(&h);
        assert_eq!(c.cache_key.as_deref(), Some("https/host/abc123"));
        assert!(c.cache_skip);
        assert_eq!(c.object_size, Some(12345));
        assert!(c.tls_skip_verify);
        assert!(c.follow_redirects);
    }

    #[test]
    fn parse_contract_headers_absent() {
        let h = HeaderMap::new();
        let c = parse_contract_headers(&h);
        assert!(c.cache_key.is_none());
        assert!(!c.cache_skip);
        assert!(c.object_size.is_none());
        assert!(!c.tls_skip_verify);
        assert!(!c.follow_redirects);
        assert!(c.manifest_b64.is_none());
        assert!(c.if_none_match.is_none());
        assert!(c.if_modified_since.is_none());
        assert!(!c.stale_if_error);
        assert!(!c.background_revalidate);
    }

    #[test]
    fn parse_contract_headers_conditional_revalidation() {
        let mut h = HeaderMap::new();
        h.insert(HEADER_CACHE_KEY, HeaderValue::from_static("https/host/abc"));
        h.insert(HEADER_IF_NONE_MATCH, HeaderValue::from_static("\"abc123\""));
        h.insert(
            HEADER_IF_MODIFIED_SINCE,
            HeaderValue::from_static("Mon, 01 Jan 2024 00:00:00 GMT"),
        );
        h.insert(HEADER_STALE_IF_ERROR, HeaderValue::from_static("true"));
        let c = parse_contract_headers(&h);
        assert_eq!(c.if_none_match.as_deref(), Some("\"abc123\""));
        assert_eq!(
            c.if_modified_since.as_deref(),
            Some("Mon, 01 Jan 2024 00:00:00 GMT")
        );
        assert!(c.stale_if_error);
        assert!(!c.cache_skip);
    }

    #[test]
    fn parse_contract_headers_no_cache_key_is_passthrough() {
        let mut h = HeaderMap::new();
        h.insert(HEADER_TLS_SKIP_VERIFY, HeaderValue::from_static("true"));
        let c = parse_contract_headers(&h);
        assert!(c.cache_key.is_none());
    }
}
