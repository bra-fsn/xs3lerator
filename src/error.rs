use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("range not satisfiable")]
    RangeNotSatisfiable(Option<u64>),

    #[error("upstream error: {0}")]
    Upstream(String),

    #[error("upstream returned {status}")]
    UpstreamStatus { status: u16, message: String },

    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let (code, extra_headers) = match &self {
            ProxyError::NotFound(_) => (StatusCode::NOT_FOUND, None),
            ProxyError::RangeNotSatisfiable(total) => {
                let mut h = HeaderMap::new();
                if let Some(t) = total {
                    if let Ok(v) = HeaderValue::from_str(&format!("bytes */{t}")) {
                        h.insert("content-range", v);
                    }
                }
                (StatusCode::RANGE_NOT_SATISFIABLE, Some(h))
            }
            ProxyError::Upstream(_) => (StatusCode::BAD_GATEWAY, None),
            ProxyError::UpstreamStatus { status, .. } => (
                StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_GATEWAY),
                None,
            ),
            ProxyError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, None),
        };
        let mut resp = (code, self.to_string()).into_response();
        if let Some(headers) = extra_headers {
            resp.headers_mut().extend(headers);
        }
        resp
    }
}

pub type ProxyResult<T> = Result<T, ProxyError>;
