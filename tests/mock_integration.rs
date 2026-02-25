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
        elasticsearch_manifest_index: "passsage_meta".to_string(),
        passthrough: false,
        open_parallelism: 8,
    }
}

fn test_config_with_dir(data_dir: std::path::PathBuf) -> AppConfig {
    AppConfig {
        data_dir,
        ..test_config()
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
        http_pool: Arc::new(xs3lerator::http_pool::HttpClientPool::new()),
    }
}

fn make_state_with_config(config: AppConfig) -> AppState {
    let data_dir = config.data_dir.clone();
    AppState {
        config: Arc::new(config),
        data_dir,
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
        es_client: None,
        http_pool: Arc::new(xs3lerator::http_pool::HttpClientPool::new()),
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

    // HEAD is now a supported method (passthrough to upstream),
    // but "test-key" is not a valid upstream URL so it returns 502.
    let resp = client
        .head(format!("{base}/test-key"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);
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

#[tokio::test]
async fn cache_hit_multi_chunk_prefetch_pipeline() {
    use base64::Engine;
    use sha2::{Digest, Sha256};
    use xs3lerator::manifest::{hash_to_chunk_path, Manifest};

    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let chunk_size: u64 = 1024;
    let num_chunks = 3;
    let last_chunk_len: u64 = 500;
    let total_size = chunk_size * (num_chunks - 1) + last_chunk_len;

    let mut expected_bytes = Vec::new();
    let mut hashes: Vec<[u8; 32]> = Vec::new();
    let prefix = "data/";

    for i in 0..num_chunks as u8 {
        let len = if (i as u64) == num_chunks - 1 {
            last_chunk_len as usize
        } else {
            chunk_size as usize
        };
        let data: Vec<u8> = (0..len).map(|j| i.wrapping_add(j as u8)).collect();
        expected_bytes.extend_from_slice(&data);

        let hash: [u8; 32] = Sha256::digest(&data).into();
        let rel_path = hash_to_chunk_path(&hash, prefix);
        let full_path = data_dir.join(&rel_path);
        std::fs::create_dir_all(full_path.parent().unwrap()).unwrap();
        std::fs::write(&full_path, &data).unwrap();
        hashes.push(hash);
    }

    let manifest = Manifest {
        chunk_size,
        total_size,
        hashes,
    };
    let manifest_b64 = base64::engine::general_purpose::STANDARD.encode(manifest.serialize());

    let config = test_config_with_dir(data_dir);
    let state = make_state_with_config(AppConfig {
        chunk_size,
        ..config
    });
    let base = start_server(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/http://fake-upstream/file.bin"))
        .header("X-Xs3lerator-Cache-Key", "test-multi-chunk")
        .header("X-Xs3lerator-Manifest", &manifest_b64)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-xs3lerator-cache-hit")
            .unwrap()
            .to_str()
            .unwrap(),
        "true"
    );
    assert_eq!(
        resp.headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        total_size.to_string()
    );

    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), total_size as usize);
    assert_eq!(&body[..], &expected_bytes[..]);
}

#[tokio::test]
async fn cache_hit_range_request() {
    use base64::Engine;
    use sha2::{Digest, Sha256};
    use xs3lerator::manifest::{hash_to_chunk_path, Manifest};

    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let chunk_size: u64 = 1024;
    let total_size = chunk_size * 2;

    let mut all_bytes = Vec::new();
    let mut hashes: Vec<[u8; 32]> = Vec::new();
    let prefix = "data/";

    for i in 0..2u8 {
        let data: Vec<u8> = (0..chunk_size as usize).map(|j| i.wrapping_add(j as u8)).collect();
        all_bytes.extend_from_slice(&data);

        let hash: [u8; 32] = Sha256::digest(&data).into();
        let rel_path = hash_to_chunk_path(&hash, prefix);
        let full_path = data_dir.join(&rel_path);
        std::fs::create_dir_all(full_path.parent().unwrap()).unwrap();
        std::fs::write(&full_path, &data).unwrap();
        hashes.push(hash);
    }

    let manifest = Manifest {
        chunk_size,
        total_size,
        hashes,
    };
    let manifest_b64 = base64::engine::general_purpose::STANDARD.encode(manifest.serialize());

    let config = test_config_with_dir(data_dir);
    let state = make_state_with_config(AppConfig {
        chunk_size,
        ..config
    });
    let base = start_server(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/http://fake-upstream/file.bin"))
        .header("X-Xs3lerator-Cache-Key", "test-range")
        .header("X-Xs3lerator-Manifest", &manifest_b64)
        .header("Range", "bytes=512-1535")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 206);
    assert_eq!(
        resp.headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        "1024"
    );

    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 1024);
    assert_eq!(&body[..], &all_bytes[512..1536]);
}
