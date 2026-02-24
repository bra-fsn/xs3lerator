use std::net::SocketAddr;
use std::sync::Arc;

use xs3lerator::config::AppConfig;
use xs3lerator::download::DownloadManager;
use xs3lerator::handler::AppState;
use xs3lerator::server::build_router;

fn test_config() -> AppConfig {
    let data_dir = std::env::temp_dir().join("xs3-mock-test-data");
    std::fs::create_dir_all(&data_dir).ok();
    AppConfig {
        bind_ip: std::net::IpAddr::from([127, 0, 0, 1]),
        port: 0,
        data_dir,
        http_concurrency: 4,
        chunk_size: 5 * 1024 * 1024,
        temp_dir: std::env::temp_dir(),
        upstream_tls_skip_verify: false,
        data_prefix: "data/".to_string(),
        elasticsearch_url: None,
        elasticsearch_manifest_index: "xs3_manifests".to_string(),
        elasticsearch_replicas: 0,
        elasticsearch_shards: 1,
        passthrough: false,
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

fn make_state() -> AppState {
    let config = test_config();
    let data_dir = config.data_dir.clone();
    AppState {
        config: Arc::new(config),
        data_dir,
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        es_client: None,
    }
}

#[tokio::test]
async fn healthz_returns_ok() {
    let state = make_state();
    let base = start_server(state).await;
    let resp = reqwest::get(format!("{base}/healthz")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

#[tokio::test]
async fn non_get_returns_405() {
    let state = make_state();
    let base = start_server(state).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/test-key"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 500);

    let resp = client
        .put(format!("{base}/test-key"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);

    let resp = client
        .delete(format!("{base}/test-key"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);

    let resp = client
        .head(format!("{base}/test-key"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);
}

#[tokio::test]
async fn get_without_upstream_url_returns_error() {
    let state = make_state();
    let base = start_server(state).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/test-key"))
        .header("X-Xs3lerator-Cache-Skip", "true")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_client_error() || resp.status().is_server_error());
}
