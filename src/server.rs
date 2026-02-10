use axum::routing::get;
use axum::Router;

use crate::handler::{handle_get, handle_head, ProxyState};

/// Build the axum router with GET and HEAD handlers for all paths.
pub fn build_router(state: ProxyState) -> Router {
    Router::new()
        .route("/*key", get(handle_get).head(handle_head))
        .with_state(state)
}
