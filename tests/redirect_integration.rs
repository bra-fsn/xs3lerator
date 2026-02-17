use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

use xs3lerator::config::AppConfig;
use xs3lerator::download::DownloadManager;
use xs3lerator::handler::AppState;
use xs3lerator::s3::{AwsUpstream, S3Uploader};
use xs3lerator::server::build_router;

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

async fn start_mock_upstream() -> String {
    let app = Router::new()
        .route("/redirect/301", get(handle_301))
        .route("/redirect/302", get(handle_302))
        .route("/redirect/307", get(handle_307))
        .route("/redirect/308", get(handle_308))
        .route("/redirect/301-with-cache-control", get(handle_301_cached))
        .route("/redirect/chain-step1", get(handle_chain_step1))
        .route("/redirect/chain-step2", get(handle_chain_step2))
        .route("/final-target", get(handle_final_target));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn handle_301() -> Response {
    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header("location", "https://example.com/new-location")
        .header("x-custom-upstream", "test-value")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn handle_302() -> Response {
    Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "https://example.com/temporary")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn handle_307() -> Response {
    Response::builder()
        .status(StatusCode::TEMPORARY_REDIRECT)
        .header("location", "https://example.com/temp-redirect")
        .header("retry-after", "120")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn handle_308() -> Response {
    Response::builder()
        .status(StatusCode::PERMANENT_REDIRECT)
        .header("location", "https://example.com/permanent-new")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn handle_301_cached() -> Response {
    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header("location", "https://example.com/cached-redirect")
        .header("cache-control", "public, max-age=86400")
        .header("expires", "Thu, 01 Jan 2099 00:00:00 GMT")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn handle_chain_step1() -> Response {
    Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "/redirect/chain-step2")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn handle_chain_step2() -> Response {
    Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "/final-target")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn handle_final_target() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain")],
        "hello from final target",
    )
}

async fn make_state() -> AppState {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .load()
        .await;
    let s3_client = aws_sdk_s3::Client::new(&sdk_config);

    AppState {
        config: Arc::new(test_config()),
        s3_upstream: Arc::new(AwsUpstream::new(s3_client.clone())),
        s3_uploader: Arc::new(S3Uploader::new(s3_client)),
        downloads: Arc::new(DownloadManager::default()),
        trace: None,
    }
}

/// Helper: send a GET through xs3lerator to the given upstream path.
async fn proxy_get(
    client: &reqwest::Client,
    xs3_base: &str,
    upstream_url: &str,
    follow_redirects: bool,
) -> reqwest::Response {
    let upstream_b64 = BASE64.encode(upstream_url);
    let mut req = client
        .get(format!("{xs3_base}/test-bucket/test-key"))
        .header("X-Xs3lerator-Upstream-Url", &upstream_b64)
        .header("X-Xs3lerator-Cache-Skip", "true");
    if follow_redirects {
        req = req.header("X-Xs3lerator-Follow-Redirects", "true");
    }
    req.send().await.unwrap()
}

// -----------------------------------------------------------------------
// Tests: redirect passthrough (default, follow_redirects = false)
// -----------------------------------------------------------------------

#[tokio::test]
async fn redirect_301_passed_through() {
    let upstream = start_mock_upstream().await;
    let state = make_state().await;
    let xs3 = start_xs3lerator(state).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = proxy_get(&client, &xs3, &format!("{upstream}/redirect/301"), false).await;

    assert_eq!(resp.status(), 301);
    assert_eq!(
        resp.headers().get("location").unwrap().to_str().unwrap(),
        "https://example.com/new-location"
    );
    assert_eq!(
        resp.headers()
            .get("x-custom-upstream")
            .unwrap()
            .to_str()
            .unwrap(),
        "test-value"
    );
    assert_eq!(
        resp.headers()
            .get("x-xs3lerator-cache-hit")
            .unwrap()
            .to_str()
            .unwrap(),
        "false"
    );
}

#[tokio::test]
async fn redirect_302_passed_through() {
    let upstream = start_mock_upstream().await;
    let state = make_state().await;
    let xs3 = start_xs3lerator(state).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = proxy_get(&client, &xs3, &format!("{upstream}/redirect/302"), false).await;

    assert_eq!(resp.status(), 302);
    assert_eq!(
        resp.headers().get("location").unwrap().to_str().unwrap(),
        "https://example.com/temporary"
    );
}

#[tokio::test]
async fn redirect_307_preserves_retry_after() {
    let upstream = start_mock_upstream().await;
    let state = make_state().await;
    let xs3 = start_xs3lerator(state).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = proxy_get(&client, &xs3, &format!("{upstream}/redirect/307"), false).await;

    assert_eq!(resp.status(), 307);
    assert_eq!(
        resp.headers().get("location").unwrap().to_str().unwrap(),
        "https://example.com/temp-redirect"
    );
    assert_eq!(
        resp.headers().get("retry-after").unwrap().to_str().unwrap(),
        "120"
    );
}

#[tokio::test]
async fn redirect_308_passed_through() {
    let upstream = start_mock_upstream().await;
    let state = make_state().await;
    let xs3 = start_xs3lerator(state).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = proxy_get(&client, &xs3, &format!("{upstream}/redirect/308"), false).await;

    assert_eq!(resp.status(), 308);
    assert_eq!(
        resp.headers().get("location").unwrap().to_str().unwrap(),
        "https://example.com/permanent-new"
    );
}

#[tokio::test]
async fn redirect_301_preserves_cache_headers() {
    let upstream = start_mock_upstream().await;
    let state = make_state().await;
    let xs3 = start_xs3lerator(state).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = proxy_get(
        &client,
        &xs3,
        &format!("{upstream}/redirect/301-with-cache-control"),
        false,
    )
    .await;

    assert_eq!(resp.status(), 301);
    assert_eq!(
        resp.headers().get("location").unwrap().to_str().unwrap(),
        "https://example.com/cached-redirect"
    );
    assert_eq!(
        resp.headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "public, max-age=86400"
    );
    assert_eq!(
        resp.headers().get("expires").unwrap().to_str().unwrap(),
        "Thu, 01 Jan 2099 00:00:00 GMT"
    );
}

#[tokio::test]
async fn redirect_strips_contract_headers() {
    let upstream = start_mock_upstream().await;
    let state = make_state().await;
    let xs3 = start_xs3lerator(state).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = proxy_get(&client, &xs3, &format!("{upstream}/redirect/301"), false).await;

    assert_eq!(resp.status(), 301);
    // x-xs3lerator-cache-hit is an xs3lerator response header (expected)
    assert!(resp.headers().get("x-xs3lerator-cache-hit").is_some());
    // But upstream x-xs3lerator-* headers should be stripped from the
    // redirect response.  The mock doesn't send any, so just verify
    // no unexpected contract headers leaked through.
    for (name, _) in resp.headers().iter() {
        if name.as_str().starts_with("x-xs3lerator-") && name.as_str() != "x-xs3lerator-cache-hit"
        {
            panic!(
                "unexpected contract header in redirect response: {}",
                name.as_str()
            );
        }
    }
}

// -----------------------------------------------------------------------
// Tests: follow_redirects = true
// -----------------------------------------------------------------------

#[tokio::test]
async fn follow_redirects_follows_single_redirect() {
    let upstream = start_mock_upstream().await;
    let state = make_state().await;
    let xs3 = start_xs3lerator(state).await;
    // Client doesn't follow redirects — we want to see what xs3lerator returns
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // With follow_redirects, xs3lerator should follow the chain and return
    // the final target's response (200). The chain redirect's Location
    // points to a relative path on the same mock server.
    let resp = proxy_get(
        &client,
        &xs3,
        &format!("{upstream}/redirect/chain-step1"),
        true,
    )
    .await;

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "hello from final target");
}

#[tokio::test]
async fn without_follow_redirects_chain_returns_first_redirect() {
    let upstream = start_mock_upstream().await;
    let state = make_state().await;
    let xs3 = start_xs3lerator(state).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // Without follow_redirects, only the first 302 is returned
    let resp = proxy_get(
        &client,
        &xs3,
        &format!("{upstream}/redirect/chain-step1"),
        false,
    )
    .await;

    assert_eq!(resp.status(), 302);
    assert_eq!(
        resp.headers().get("location").unwrap().to_str().unwrap(),
        "/redirect/chain-step2"
    );
}

// -----------------------------------------------------------------------
// Tests: non-redirect responses still work
// -----------------------------------------------------------------------

#[tokio::test]
async fn non_redirect_200_unaffected() {
    let upstream = start_mock_upstream().await;
    let state = make_state().await;
    let xs3 = start_xs3lerator(state).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = proxy_get(
        &client,
        &xs3,
        &format!("{upstream}/final-target"),
        false,
    )
    .await;

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "hello from final target");
}
