use std::sync::atomic::{AtomicU32, Ordering};

use crate::handler::{handle_get, handle_head, handle_post, healthz, method_not_allowed, AppState};
use axum::body::Body;
use axum::extract::State;
use axum::http::{Method, Request};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tower_http::trace::TraceLayer;

static TXN_COUNTER: AtomicU32 = AtomicU32::new(0);

fn next_txn_id() -> String {
    let id = TXN_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{id:06x}")
}

async fn catch_all(State(state): State<AppState>, req: Request<Body>) -> Response {
    match *req.method() {
        Method::GET => match handle_get(State(state), req).await {
            Ok(resp) => resp,
            Err(err) => err.into_response(),
        },
        Method::HEAD => match handle_head(State(state), req).await {
            Ok(resp) => resp,
            Err(err) => err.into_response(),
        },
        Method::POST => match handle_post(State(state), req).await {
            Ok(resp) => resp,
            Err(err) => err.into_response(),
        },
        _ => method_not_allowed().await.into_response(),
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .fallback(catch_all)
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request<Body>| {
                let txn = next_txn_id();
                tracing::debug_span!(
                    "request",
                    txn = %txn,
                    method = %request.method(),
                    uri = %request.uri(),
                )
            }),
        )
        .with_state(state)
}
