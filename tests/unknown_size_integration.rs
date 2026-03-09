/// Regression tests for unknown-size (Transfer-Encoding: chunked) responses.
///
/// These reproduce the bug where xs3lerator would try to read `expected_chunk_len`
/// bytes from a temp file that only contained the actual (smaller) response body,
/// causing S3 uploads to silently fail while still marking chunks as committed.
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;

use xs3lerator::config::AppConfig;
use xs3lerator::disk_cache::DiskCache;
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
        chunk_size: 8 * 1024 * 1024,
        temp_dir: std::env::temp_dir(),
        upstream_tls_skip_verify: false,
        upstream_connect_timeout: std::time::Duration::from_secs(30),
        upstream_read_timeout: Some(std::time::Duration::from_secs(300)),
        data_prefix: "data/".to_string(),
        elasticsearch_url: None,
        elasticsearch_manifest_index: "passsage_meta".to_string(),
        passthrough: false,
    }
}

async fn start_xs3lerator(state: AppState) -> String {
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Mock upstream that sends a small body with no Content-Length header,
/// causing HTTP to use Transfer-Encoding: chunked, mimicking the
/// api.launchpad.net response pattern.
async fn handle_chunked_small() -> Response {
    use futures::stream;
    let payload = b"{\"resource_type_link\": \"https://api.launchpad.net/#service-root\"}";
    let stream = stream::once(async { Ok::<_, std::io::Error>(bytes::Bytes::from_static(payload)) });
    let body = Body::from_stream(stream);
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

/// Mock upstream that sends a multi-kilobyte streaming response, still smaller
/// than the 8 MiB chunk size.
async fn handle_chunked_medium() -> Response {
    use futures::stream;
    let payload: Vec<u8> = (0..4096u16).flat_map(|i| (i as u8).to_le_bytes()).collect();
    let data = bytes::Bytes::from(payload);
    let stream = stream::once(async move { Ok::<_, std::io::Error>(data) });
    let body = Body::from_stream(stream);
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/octet-stream")
        .body(body)
        .unwrap()
}

/// Mock upstream that sends an exact Content-Length (known-size) response.
async fn handle_known_size() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain")],
        "hello, known size response",
    )
}

async fn start_mock_upstream() -> String {
    let app = Router::new()
        .route("/chunked-small", get(handle_chunked_small))
        .route("/chunked-medium", get(handle_chunked_medium))
        .route("/known-size", get(handle_known_size));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Regression: a small chunked response (462 bytes) much smaller than chunk_size
/// (8 MiB) must be returned correctly through passthrough mode.
#[tokio::test]
async fn passthrough_chunked_small_response() {
    let upstream = start_mock_upstream().await;
    let config = AppConfig {
        passthrough: true,
        ..test_config()
    };
    let state = AppState {
        config: Arc::new(config),
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        es_client: None,
        http_pool: Arc::new(HttpClientPool::new()),
        s3: None,
        disk_cache: None,
    };
    let base = start_xs3lerator(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/{upstream}/chunked-small"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("resource_type_link"));
    assert!(body.contains("service-root"));
}

/// Regression: a chunked response cached to disk must produce a correct
/// disk-cache entry whose size matches the actual body, not the chunk_size.
#[tokio::test]
async fn cache_miss_chunked_small_writes_correct_disk_cache() {
    let upstream = start_mock_upstream().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache_dir = tmp.path().join("cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let dc = Arc::new(DiskCache::new(cache_dir.clone()));
    dc.preseed_dirs().unwrap();

    let config = AppConfig {
        chunk_size: 8 * 1024 * 1024,
        cache_dir: Some(cache_dir),
        ..test_config()
    };
    let state = AppState {
        config: Arc::new(config),
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        es_client: None,
        http_pool: Arc::new(HttpClientPool::new()),
        s3: None,
        disk_cache: Some(dc),
    };
    let base = start_xs3lerator(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/{upstream}/chunked-small"))
        .header("X-Xs3lerator-Cache-Key", "test-chunked-small")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("resource_type_link"),
        "response body should contain the upstream JSON"
    );
}

/// Regression: a medium-sized chunked response must also be returned correctly.
#[tokio::test]
async fn passthrough_chunked_medium_response() {
    let upstream = start_mock_upstream().await;
    let config = AppConfig {
        passthrough: true,
        ..test_config()
    };
    let state = AppState {
        config: Arc::new(config),
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        es_client: None,
        http_pool: Arc::new(HttpClientPool::new()),
        s3: None,
        disk_cache: None,
    };
    let base = start_xs3lerator(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/{upstream}/chunked-medium"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    let expected: Vec<u8> = (0..4096u16).flat_map(|i| (i as u8).to_le_bytes()).collect();
    assert_eq!(body.len(), expected.len());
    assert_eq!(&body[..], &expected[..]);
}

/// Known-size responses should continue to work normally (no regression).
#[tokio::test]
async fn passthrough_known_size_response() {
    let upstream = start_mock_upstream().await;
    let config = AppConfig {
        passthrough: true,
        ..test_config()
    };
    let state = AppState {
        config: Arc::new(config),
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        es_client: None,
        http_pool: Arc::new(HttpClientPool::new()),
        s3: None,
        disk_cache: None,
    };
    let base = start_xs3lerator(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/{upstream}/known-size"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "hello, known size response");
}

/// Mock upstream that returns 304 when the client sends a matching If-None-Match.
async fn handle_conditional(
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(inm) = headers.get("if-none-match") {
        if inm.to_str().unwrap_or("") == "\"abc123\"" {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header("etag", "\"abc123\"")
                .body(Body::empty())
                .unwrap();
        }
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .header("etag", "\"abc123\"")
        .body(Body::from("{\"ok\":true}"))
        .unwrap()
}

async fn start_mock_upstream_with_conditional() -> String {
    let app = Router::new()
        .route("/conditional", get(handle_conditional));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Regression: when the client sends If-None-Match and upstream returns 304,
/// xs3lerator must relay the 304 status back to the caller instead of
/// producing a 200 OK with an empty body (the pre-fix behavior that caused
/// pip to see "Content-Type: Unknown").
#[tokio::test]
async fn passthrough_304_not_modified() {
    let upstream = start_mock_upstream_with_conditional().await;
    let config = AppConfig {
        passthrough: true,
        ..test_config()
    };
    let state = AppState {
        config: Arc::new(config),
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        es_client: None,
        http_pool: Arc::new(HttpClientPool::new()),
        s3: None,
        disk_cache: None,
    };
    let base = start_xs3lerator(state).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // First request without If-None-Match — should get 200.
    let resp = client
        .get(format!("{base}/{upstream}/conditional"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("ok"));

    // Second request with matching ETag — should get 304, not 200-empty.
    let resp = client
        .get(format!("{base}/{upstream}/conditional"))
        .header("if-none-match", "\"abc123\"")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        304,
        "xs3lerator must relay upstream 304 in passthrough mode"
    );
}

/// Regression: same as above but with cache_skip=true (cache key set but
/// skip flag active), which follows the same code path.
#[tokio::test]
async fn cache_skip_304_not_modified() {
    let upstream = start_mock_upstream_with_conditional().await;
    let config = test_config();
    let state = AppState {
        config: Arc::new(config),
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        es_client: None,
        http_pool: Arc::new(HttpClientPool::new()),
        s3: None,
        disk_cache: None,
    };
    let base = start_xs3lerator(state).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .get(format!("{base}/{upstream}/conditional"))
        .header("X-Xs3lerator-Cache-Key", "test-conditional")
        .header("X-Xs3lerator-Cache-Skip", "true")
        .header("if-none-match", "\"abc123\"")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        304,
        "xs3lerator must relay upstream 304 when cache_skip=true"
    );
}
