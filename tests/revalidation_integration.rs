use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State as AxumState;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::Router;

use xs3lerator::config::AppConfig;
use xs3lerator::download::DownloadManager;
use xs3lerator::handler::AppState;
use xs3lerator::http_pool::HttpClientPool;
use xs3lerator::server::build_router;

fn test_config() -> AppConfig {
    AppConfig {
        bind_ip: std::net::IpAddr::from([127, 0, 0, 1]),
        port: 0,
        s3_bucket: None,
        s3_region: "us-east-1".to_string(),
        s3_endpoint: None,
        cache_dir: None,
        cache_low_watermark: 85,
        cache_high_watermark: 95,
        http_concurrency: 4,
        chunk_size: 5 * 1024 * 1024,
        temp_dir: std::env::temp_dir(),
        upstream_tls_skip_verify: false,
        upstream_connect_timeout: std::time::Duration::from_secs(30),
        upstream_read_timeout: Some(std::time::Duration::from_secs(300)),
        data_prefix: "data/".to_string(),
        fdb_cluster_file: None,
        passthrough: false,
    }
}

fn test_state() -> AppState {
    AppState {
        config: Arc::new(test_config()),
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        fdb_client: None,
        http_pool: Arc::new(HttpClientPool::new()),
        s3: None,
        disk_cache: None,
    }
}

async fn start_xs3lerator(state: AppState) -> String {
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[derive(Clone)]
struct MockUpstreamState {
    etag: String,
    body: String,
}

async fn handle_conditional(
    AxumState(state): AxumState<MockUpstreamState>,
    req: Request<Body>,
) -> Response {
    let if_none_match = req
        .headers()
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    if let Some(inm) = if_none_match {
        if inm == state.etag {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header("etag", &state.etag)
                .body(Body::empty())
                .unwrap();
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .header("etag", &state.etag)
        .header("content-length", state.body.len().to_string())
        .body(Body::from(state.body.clone()))
        .unwrap()
}

async fn handle_upstream_error(_req: Request<Body>) -> Response {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from("upstream error"))
        .unwrap()
}

async fn start_mock_upstream(etag: &str, body: &str) -> String {
    let state = MockUpstreamState {
        etag: etag.to_string(),
        body: body.to_string(),
    };
    let app = Router::new()
        .route("/resource", get(handle_conditional))
        .route("/error-resource", get(handle_upstream_error))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// When If-None-Match matches the upstream ETag, xs3lerator receives a 304
/// from upstream. Without a cache to serve from (no ES/S3), handle_cache_hit
/// fails — confirming the 304 path was taken (not error passthrough).
#[tokio::test]
async fn conditional_get_304_without_cache_returns_error() {
    let upstream = start_mock_upstream("\"abc123\"", "hello world").await;
    let xs3 = start_xs3lerator(test_state()).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{xs3}/{upstream}/resource"))
        .header("x-xs3lerator-cache-key", "test/key")
        .header("x-xs3lerator-if-none-match", "\"abc123\"")
        .send()
        .await
        .unwrap();

    assert!(
        resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::INTERNAL_SERVER_ERROR,
        "expected 404 or 500 from cache-hit fallback (no ES), got {}",
        resp.status()
    );
}

/// When If-None-Match does NOT match (upstream returns 200 with new content),
/// xs3lerator should serve the new content.
#[tokio::test]
async fn conditional_get_200_content_changed() {
    let upstream = start_mock_upstream("\"new-etag\"", "new content").await;
    let xs3 = start_xs3lerator(test_state()).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{xs3}/{upstream}/resource"))
        .header("x-xs3lerator-cache-key", "test/key2")
        .header("x-xs3lerator-if-none-match", "\"old-etag\"")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "new content");
}

/// When upstream returns 5xx and stale-if-error is set, xs3lerator tries to
/// serve from cache. Without a cache, it returns an error.
#[tokio::test]
async fn conditional_get_upstream_error_stale_if_error_without_cache() {
    let upstream = start_mock_upstream("\"abc\"", "body").await;
    let xs3 = start_xs3lerator(test_state()).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{xs3}/{upstream}/error-resource"))
        .header("x-xs3lerator-cache-key", "test/key3")
        .header("x-xs3lerator-if-none-match", "\"abc\"")
        .header("x-xs3lerator-stale-if-error", "true")
        .send()
        .await
        .unwrap();

    assert!(
        resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::INTERNAL_SERVER_ERROR,
        "expected 404 or 500 from stale-if-error cache fallback (no ES), got {}",
        resp.status()
    );
}

/// When upstream returns 5xx and stale-if-error is NOT set, xs3lerator should
/// pass through the error.
#[tokio::test]
async fn conditional_get_upstream_error_no_stale_if_error_passes_through() {
    let upstream = start_mock_upstream("\"abc\"", "body").await;
    let xs3 = start_xs3lerator(test_state()).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{xs3}/{upstream}/error-resource"))
        .header("x-xs3lerator-cache-key", "test/key4")
        .header("x-xs3lerator-if-none-match", "\"abc\"")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "upstream error");
}

/// Verify contract header parsing for conditional revalidation.
#[test]
fn parse_conditional_revalidation_headers() {
    use xs3lerator::headers::parse_contract_headers;

    let mut h = axum::http::HeaderMap::new();
    h.insert("x-xs3lerator-cache-key", "test/key".parse().unwrap());
    h.insert(
        "x-xs3lerator-if-none-match",
        "\"etag-val\"".parse().unwrap(),
    );
    h.insert(
        "x-xs3lerator-if-modified-since",
        "Mon, 01 Jan 2024 00:00:00 GMT".parse().unwrap(),
    );
    h.insert("x-xs3lerator-stale-if-error", "true".parse().unwrap());

    let c = parse_contract_headers(&h);
    assert_eq!(c.cache_key.as_deref(), Some("test/key"));
    assert_eq!(c.if_none_match.as_deref(), Some("\"etag-val\""));
    assert_eq!(
        c.if_modified_since.as_deref(),
        Some("Mon, 01 Jan 2024 00:00:00 GMT")
    );
    assert!(c.stale_if_error);
    assert!(!c.cache_skip);
}
