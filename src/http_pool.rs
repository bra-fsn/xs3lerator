use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::Duration;

/// Key for the HTTP client pool: the per-request booleans + timeouts that
/// affect `reqwest::Client` construction. In practice only a handful of
/// combinations appear (the server defaults + per-request overrides).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct ClientKey {
    skip_tls: bool,
    follow_redirects: bool,
    connect_timeout_ms: u64,
    read_timeout_ms: Option<u64>,
}

/// Persistent pool of `reqwest::Client` instances, keyed by the TLS /
/// redirect / timeout configuration.  Because reqwest internally maintains a
/// per-host connection pool (including TLS session tickets), reusing
/// the same `Client` across requests eliminates repeated TLS handshakes
/// for frequently accessed upstreams.
pub struct HttpClientPool {
    clients: Mutex<HashMap<ClientKey, reqwest::Client>>,
}

impl HttpClientPool {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::with_capacity(8)),
        }
    }

    /// Get or create a `reqwest::Client` for the given configuration.
    /// The client is cached and reused for subsequent requests with the
    /// same (skip_tls, follow_redirects, timeouts) combination.
    pub fn get(
        &self,
        skip_tls: bool,
        follow_redirects: bool,
        connect_timeout: Duration,
        read_timeout: Option<Duration>,
    ) -> Result<reqwest::Client, reqwest::Error> {
        let key = ClientKey {
            skip_tls,
            follow_redirects,
            connect_timeout_ms: connect_timeout.as_millis() as u64,
            read_timeout_ms: read_timeout.map(|d| d.as_millis() as u64),
        };
        let mut map = self.clients.lock();
        if let Some(client) = map.get(&key) {
            return Ok(client.clone());
        }

        let redirect_policy = if follow_redirects {
            reqwest::redirect::Policy::default()
        } else {
            reqwest::redirect::Policy::none()
        };
        let mut builder = reqwest::Client::builder()
            .danger_accept_invalid_certs(skip_tls)
            .redirect(redirect_policy)
            .no_gzip()
            .no_deflate()
            .no_brotli()
            .no_zstd()
            .pool_max_idle_per_host(32)
            .connect_timeout(connect_timeout);

        if let Some(rt) = read_timeout {
            builder = builder.read_timeout(rt);
        }

        let client = builder.build()?;
        map.insert(key, client.clone());
        Ok(client)
    }
}
