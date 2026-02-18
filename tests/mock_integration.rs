use std::net::SocketAddr;
use std::sync::Arc;

use xs3lerator::config::AppConfig;
use xs3lerator::download::DownloadManager;
use xs3lerator::handler::AppState;
use xs3lerator::manifest::ManifestCache;
use xs3lerator::s3::{AwsUpstream, S3Uploader};
use xs3lerator::server::build_router;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn test_config() -> AppConfig {
    AppConfig {
        bind_ip: std::net::IpAddr::from([127, 0, 0, 1]),
        port: 0,
        region: None,
        s3_endpoint_url: None,
        s3_force_path_style: false,
        s3_concurrency: 4,
        http_concurrency: 4,
        min_chunk_size: 5 * 1024 * 1024,
        temp_dir: std::env::temp_dir(),
        upstream_tls_skip_verify: false,
        data_prefix: "data/".to_string(),
        map_prefix: "_map/".to_string(),
        manifest_cache_size: 100,
        chunk_cache_dir: None,
        chunk_cache_max_size: 100 * 1024 * 1024 * 1024,
    }
}

async fn start_server(state: AppState) -> String {
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

// ---------------------------------------------------------------------------
// Unit tests for planner, headers, etc. are in their respective modules.
// Integration tests require a real or mock S3 backend, which we'll build
// incrementally.  For now, test the basic routing and error handling.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn healthz_returns_ok() {
    // Build a minimal state -- S3 client won't be called for healthz
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .load()
        .await;
    let s3_client = aws_sdk_s3::Client::new(&sdk_config);

    let state = AppState {
        config: Arc::new(test_config()),
        s3_upstream: Arc::new(AwsUpstream::new(s3_client.clone())),
        s3_uploader: Arc::new(S3Uploader::new(s3_client)),
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        manifest_cache: Arc::new(ManifestCache::new(100)),
        chunk_cache: None,
    };

    let base = start_server(state).await;
    let resp = reqwest::get(format!("{base}/healthz")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

#[tokio::test]
async fn non_get_returns_405() {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .load()
        .await;
    let s3_client = aws_sdk_s3::Client::new(&sdk_config);

    let state = AppState {
        config: Arc::new(test_config()),
        s3_upstream: Arc::new(AwsUpstream::new(s3_client.clone())),
        s3_uploader: Arc::new(S3Uploader::new(s3_client)),
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        manifest_cache: Arc::new(ManifestCache::new(100)),
        chunk_cache: None,
    };

    let base = start_server(state).await;
    let client = reqwest::Client::new();

    // POST without the manifest alias header returns 500 (missing header)
    let resp = client
        .post(format!("{base}/test-bucket/test-key"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 500);

    // PUT should return 405
    let resp = client
        .put(format!("{base}/test-bucket/test-key"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);

    // DELETE should return 405
    let resp = client
        .delete(format!("{base}/test-bucket/test-key"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);

    // HEAD should return 405
    let resp = client
        .head(format!("{base}/test-bucket/test-key"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);
}

#[tokio::test]
async fn get_without_upstream_url_returns_error() {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .load()
        .await;
    let s3_client = aws_sdk_s3::Client::new(&sdk_config);

    let state = AppState {
        config: Arc::new(test_config()),
        s3_upstream: Arc::new(AwsUpstream::new(s3_client.clone())),
        s3_uploader: Arc::new(S3Uploader::new(s3_client)),
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        manifest_cache: Arc::new(ManifestCache::new(100)),
        chunk_cache: None,
    };

    let base = start_server(state).await;
    let client = reqwest::Client::new();

    // GET with cache-skip but no upstream URL should error
    let resp = client
        .get(format!("{base}/test-bucket/test-key"))
        .header("X-Xs3lerator-Cache-Skip", "true")
        .send()
        .await
        .unwrap();
    // Should get an error (502 Bad Gateway) because S3 fetch fails
    // (no real S3) and upstream URL is missing
    assert!(resp.status().is_client_error() || resp.status().is_server_error());
}
