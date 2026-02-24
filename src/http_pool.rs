use parking_lot::Mutex;
use std::collections::HashMap;

/// Key for the HTTP client pool: the two per-request booleans that affect
/// `reqwest::Client` construction. Only 4 combinations exist, so this is
/// effectively a tiny lookup table that keeps connections alive across requests
/// to the same upstream hosts.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct ClientKey {
    skip_tls: bool,
    follow_redirects: bool,
}

/// Persistent pool of `reqwest::Client` instances, keyed by the TLS /
/// redirect configuration.  Because reqwest internally maintains a
/// per-host connection pool (including TLS session tickets), reusing
/// the same `Client` across requests eliminates repeated TLS handshakes
/// for frequently accessed upstreams.
pub struct HttpClientPool {
    clients: Mutex<HashMap<ClientKey, reqwest::Client>>,
}

impl HttpClientPool {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::with_capacity(4)),
        }
    }

    /// Get or create a `reqwest::Client` for the given configuration.
    /// The client is cached and reused for subsequent requests with the
    /// same (skip_tls, follow_redirects) combination.
    pub fn get(
        &self,
        skip_tls: bool,
        follow_redirects: bool,
    ) -> Result<reqwest::Client, reqwest::Error> {
        let key = ClientKey {
            skip_tls,
            follow_redirects,
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
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(skip_tls)
            .redirect(redirect_policy)
            .no_gzip()
            .no_deflate()
            .no_brotli()
            .no_zstd()
            .pool_max_idle_per_host(32)
            .build()?;

        map.insert(key, client.clone());
        Ok(client)
    }
}
