use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("range not satisfiable")]
    RangeNotSatisfiable,

    #[error("upstream error: {0}")]
    Upstream(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let code = match &self {
            ProxyError::NotFound(_) => StatusCode::NOT_FOUND,
            ProxyError::RangeNotSatisfiable => StatusCode::RANGE_NOT_SATISFIABLE,
            ProxyError::Upstream(_) => StatusCode::BAD_GATEWAY,
            ProxyError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (code, self.to_string()).into_response()
    }
}

pub type ProxyResult<T> = Result<T, ProxyError>;
