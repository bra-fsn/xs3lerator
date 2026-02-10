use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tempfile::tempdir;

use xs3lerator::cache::CacheStore;
use xs3lerator::config::AppConfig;
use xs3lerator::download::DownloadManager;
use xs3lerator::error::ProxyError;
use xs3lerator::handler::ProxyState;
use xs3lerator::s3::{BoxByteStream, ObjectMeta, Upstream};
use xs3lerator::server::build_router;

// ---------------------------------------------------------------------------
// Mock S3 upstream
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MockUpstream {
    data: Arc<Vec<u8>>,
    sha224: String,
    content_type: String,
}

#[async_trait]
impl Upstream for MockUpstream {
    async fn head_object(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Result<ObjectMeta, ProxyError> {
        let mut metadata = HashMap::new();
        if !self.sha224.is_empty() {
            metadata.insert("sha224".to_string(), self.sha224.clone());
        }
        Ok(ObjectMeta {
            content_length: self.data.len() as u64,
            content_type: Some(self.content_type.clone()),
            etag: Some("\"mock-etag-1\"".into()),
            last_modified: None,
            cache_control: None,
            content_encoding: None,
            metadata,
        })
    }

    async fn get_range_bytes(
        &self,
        _bucket: &str,
        _key: &str,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Bytes, ProxyError> {
        Ok(Bytes::copy_from_slice(
            &self.data[start as usize..=end_inclusive as usize],
        ))
    }

    async fn get_range_stream(
        &self,
        _bucket: &str,
        _key: &str,
        start: u64,
        end_inclusive: u64,
    ) -> Result<BoxByteStream, ProxyError> {
        let chunk = Bytes::copy_from_slice(
            &self.data[start as usize..=end_inclusive as usize],
        );
        Ok(Box::pin(stream::once(async { Ok(chunk) })))
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn test_config(cache_dir: std::path::PathBuf) -> AppConfig {
    AppConfig {
        bind_ip: std::net::IpAddr::from([127, 0, 0, 1]),
        port: 0,
        bucket: "test-bucket".into(),
        region: None,
        s3_endpoint_url: None,
        s3_force_path_style: false,
        cache_dir,
        max_cache_size: 1 << 30,
        cache_hierarchy_level: 2,
        max_concurrency: 4,
        min_chunk_size: 64 * 1024,
        max_chunk_size: 4 << 20,
        gc_interval_seconds: 3600,
        gc_watermark_percent: 95,
        gc_target_percent: 90,
    }
}

async fn start_server(state: ProxyState) -> String {
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

fn make_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_get_and_cache_hit() {
    let tmp = tempdir().unwrap();
    let payload = make_payload(512 * 1024);
    let sha = "aabbccddee112233445566778899001122334455667788aabbccdd";

    let cache = Arc::new(
        CacheStore::new(tmp.path().to_path_buf(), 2).await.unwrap(),
    );
    cache.create_hierarchy().await.unwrap();

    let state = ProxyState {
        config: Arc::new(test_config(tmp.path().to_path_buf())),
        upstream: Arc::new(MockUpstream {
            data: Arc::new(payload.clone()),
            sha224: sha.into(),
            content_type: "application/octet-stream".into(),
        }),
        cache,
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
    };

    let base = start_server(state).await;

    // First GET — cache miss, downloads from upstream
    let resp = reqwest::get(format!("{base}/test-object")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), payload.as_slice());

    // Second GET — should be served from cache
    let resp = reqwest::get(format!("{base}/test-object")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), payload.as_slice());
}

#[tokio::test]
async fn range_request() {
    let tmp = tempdir().unwrap();
    let payload = make_payload(512 * 1024);

    let cache = Arc::new(
        CacheStore::new(tmp.path().to_path_buf(), 2).await.unwrap(),
    );
    cache.create_hierarchy().await.unwrap();

    let state = ProxyState {
        config: Arc::new(test_config(tmp.path().to_path_buf())),
        upstream: Arc::new(MockUpstream {
            data: Arc::new(payload.clone()),
            sha224: "deadbeef00112233".into(),
            content_type: "application/octet-stream".into(),
        }),
        cache,
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
    };

    let base = start_server(state).await;

    let client = reqwest::Client::new();

    // Explicit range
    let resp = client
        .get(format!("{base}/ranged"))
        .header("Range", "bytes=1000-1999")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &payload[1000..=1999]);

    // Suffix range
    let resp = client
        .get(format!("{base}/ranged"))
        .header("Range", "bytes=-200")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &payload[payload.len() - 200..]);

    // Open-ended range
    let resp = client
        .get(format!("{base}/ranged"))
        .header("Range", "bytes=524087-")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &payload[524087..]);
}

#[tokio::test]
async fn head_request() {
    let tmp = tempdir().unwrap();
    let payload = make_payload(4096);

    let cache = Arc::new(
        CacheStore::new(tmp.path().to_path_buf(), 2).await.unwrap(),
    );

    let state = ProxyState {
        config: Arc::new(test_config(tmp.path().to_path_buf())),
        upstream: Arc::new(MockUpstream {
            data: Arc::new(payload),
            sha224: "aabb".into(),
            content_type: "text/plain".into(),
        }),
        cache,
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
    };

    let base = start_server(state).await;
    let client = reqwest::Client::new();

    let resp = client
        .head(format!("{base}/head-test"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("server").unwrap().to_str().unwrap(),
        "xs3lerator/0.1.0"
    );
    assert_eq!(
        resp.headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        "4096"
    );
    assert_eq!(
        resp.headers()
            .get("accept-ranges")
            .unwrap()
            .to_str()
            .unwrap(),
        "bytes"
    );
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "text/plain"
    );
    // Body should be empty for HEAD
    let body = resp.bytes().await.unwrap();
    assert!(body.is_empty());
}

#[tokio::test]
async fn concurrent_requests_same_object() {
    let tmp = tempdir().unwrap();
    let payload = make_payload(1024 * 1024);

    let cache = Arc::new(
        CacheStore::new(tmp.path().to_path_buf(), 2).await.unwrap(),
    );
    cache.create_hierarchy().await.unwrap();

    let state = ProxyState {
        config: Arc::new(test_config(tmp.path().to_path_buf())),
        upstream: Arc::new(MockUpstream {
            data: Arc::new(payload.clone()),
            sha224: "concurrent1234567890abcdef".into(),
            content_type: "application/octet-stream".into(),
        }),
        cache,
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
    };

    let base = start_server(state).await;

    let mut handles = Vec::new();
    for _ in 0..8 {
        let url = format!("{base}/concurrent");
        handles.push(tokio::spawn(async move {
            let resp = reqwest::get(&url).await.unwrap();
            (resp.status().as_u16(), resp.bytes().await.unwrap())
        }));
    }

    for handle in handles {
        let (status, body) = handle.await.unwrap();
        assert_eq!(status, 200);
        assert_eq!(body.as_ref(), payload.as_slice());
    }
}

#[tokio::test]
async fn sha224_fallback_when_header_missing() {
    let tmp = tempdir().unwrap();
    let payload = make_payload(1024);

    let cache = Arc::new(
        CacheStore::new(tmp.path().to_path_buf(), 2).await.unwrap(),
    );
    cache.create_hierarchy().await.unwrap();

    let state = ProxyState {
        config: Arc::new(test_config(tmp.path().to_path_buf())),
        upstream: Arc::new(MockUpstream {
            data: Arc::new(payload.clone()),
            sha224: String::new(), // empty → fallback
            content_type: "application/octet-stream".into(),
        }),
        cache,
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
    };

    let base = start_server(state).await;

    let resp = reqwest::get(format!("{base}/fallback-key")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), payload.as_slice());

    // Second request: cache hit
    let resp = reqwest::get(format!("{base}/fallback-key")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), payload.as_slice());
}

#[tokio::test]
async fn not_found_returns_404() {
    let tmp = tempdir().unwrap();

    #[derive(Clone)]
    struct NotFoundUpstream;

    #[async_trait]
    impl Upstream for NotFoundUpstream {
        async fn head_object(
            &self,
            _bucket: &str,
            _key: &str,
        ) -> Result<ObjectMeta, ProxyError> {
            Err(ProxyError::NotFound("no such key".into()))
        }

        async fn get_range_bytes(
            &self,
            _: &str,
            _: &str,
            _: u64,
            _: u64,
        ) -> Result<Bytes, ProxyError> {
            Err(ProxyError::NotFound("no such key".into()))
        }

        async fn get_range_stream(
            &self,
            _: &str,
            _: &str,
            _: u64,
            _: u64,
        ) -> Result<BoxByteStream, ProxyError> {
            Err(ProxyError::NotFound("no such key".into()))
        }
    }

    let cache = Arc::new(
        CacheStore::new(tmp.path().to_path_buf(), 2).await.unwrap(),
    );

    let state = ProxyState {
        config: Arc::new(test_config(tmp.path().to_path_buf())),
        upstream: Arc::new(NotFoundUpstream),
        cache,
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
    };

    let base = start_server(state).await;

    let resp = reqwest::get(format!("{base}/nonexistent")).await.unwrap();
    assert_eq!(resp.status(), 404);
}
