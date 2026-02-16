use axum::body::Body;
use axum::extract::State;
use axum::http::{Method, Request};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tower_http::trace::TraceLayer;

use crate::handler::{handle_get, healthz, method_not_allowed, AppState};

async fn catch_all(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Response {
    if req.method() == Method::GET {
        match handle_get(State(state), req).await {
            Ok(resp) => resp,
            Err(err) => err.into_response(),
        }
    } else {
        method_not_allowed().await.into_response()
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .fallback(catch_all)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
