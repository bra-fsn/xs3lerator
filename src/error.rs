use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use thiserror::Error;
use tracing::{error, warn};

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

    #[error("precondition failed")]
    PreconditionFailed,
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let (code, extra_headers) = match &self {
            ProxyError::NotFound(msg) => {
                warn!(status = 404, error = %msg, "not found");
                (StatusCode::NOT_FOUND, None)
            }
            ProxyError::RangeNotSatisfiable(total) => {
                let mut h = HeaderMap::new();
                if let Some(t) = total {
                    if let Ok(v) = HeaderValue::from_str(&format!("bytes */{t}")) {
                        h.insert("content-range", v);
                    }
                }
                warn!(status = 416, "range not satisfiable");
                (StatusCode::RANGE_NOT_SATISFIABLE, Some(h))
            }
            ProxyError::Upstream(msg) => {
                error!(status = 502, error = %msg, "upstream error");
                (StatusCode::BAD_GATEWAY, None)
            }
            ProxyError::UpstreamStatus { status, message } => {
                warn!(status, error = %message, "upstream status error");
                (
                    StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_GATEWAY),
                    None,
                )
            }
            ProxyError::Internal(msg) => {
                error!(status = 500, error = %msg, "internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, None)
            }
            ProxyError::PreconditionFailed => {
                (StatusCode::PRECONDITION_FAILED, None)
            }
        };
        let mut resp = (code, self.to_string()).into_response();
        if let Some(headers) = extra_headers {
            resp.headers_mut().extend(headers);
        }
        resp
    }
}

pub type ProxyResult<T> = Result<T, ProxyError>;
