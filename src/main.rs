use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tracing::info;

mod config;
mod download;
mod error;
mod es_client;
mod finalize;
mod handler;
mod headers;
mod http_pool;
mod manifest;
mod planner;
mod range;
mod server;
mod trace;
mod upstream_fetcher;

use config::{AppConfig, CliArgs};
use handler::AppState;
use trace::TraceWriter;

const HEX_CHARS: [char; 16] = ['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'];
const DIR_PLACEHOLDER: &str = ".xs3lerator";
const PRESEED_THREADS: usize = 32;

/// Pre-create all 65,536 leaf directories in the S3-mounted data_dir by writing
/// a placeholder file into each one.  S3 has no concept of directories — the
/// only way to ensure mountpoint-s3 doesn't need a GetObject / HeadObject per
/// mkdir at request time is to have a real object under each prefix already.
fn preseed_s3_directories(data_dir: &std::path::Path, data_prefix: &str) -> anyhow::Result<usize> {
    let pairs: Vec<(char, char)> = HEX_CHARS.iter()
        .flat_map(|&a| HEX_CHARS.iter().map(move |&b| (a, b)))
        .collect();

    let chunk_size = (pairs.len() + PRESEED_THREADS - 1) / PRESEED_THREADS;
    let created = std::sync::atomic::AtomicUsize::new(0);
    let error: std::sync::Mutex<Option<std::io::Error>> = std::sync::Mutex::new(None);

    std::thread::scope(|s| {
        for chunk in pairs.chunks(chunk_size) {
            let created = &created;
            let error = &error;
            let chunk = chunk.to_vec();
            s.spawn(move || {
                use std::io::Write;
                for (a, b) in chunk {
                    for &c in &HEX_CHARS {
                        for &d in &HEX_CHARS {
                            if error.lock().unwrap().is_some() {
                                return;
                            }
                            let dir = data_dir.join(format!("{data_prefix}{a}/{b}/{c}/{d}"));
                            let placeholder = dir.join(DIR_PLACEHOLDER);
                            if placeholder.exists() {
                                continue;
                            }
                            let result = (|| -> std::io::Result<()> {
                                std::fs::create_dir_all(&dir)?;
                                let mut f = std::fs::File::create(&placeholder)?;
                                f.write_all(b"")?;
                                f.sync_all()?;
                                Ok(())
                            })();
                            match result {
                                Ok(()) => { created.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                                Err(e) => { *error.lock().unwrap() = Some(e); return; }
                            }
                        }
                    }
                }
            });
        }
    });

    if let Some(e) = error.into_inner().unwrap() {
        return Err(e.into());
    }
    Ok(created.load(std::sync::atomic::Ordering::Relaxed))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!(
        version = concat!(env!("CARGO_PKG_VERSION"), env!("GIT_SHA")),
        "starting xs3lerator"
    );

    let args = CliArgs::parse();
    let debug_trace = args.debug_trace.clone();
    let config = AppConfig::try_from(args)?;

    let trace_writer = debug_trace.map(|path| {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open trace file");
        Arc::new(TraceWriter::new(Box::new(file)))
    });

    let es_client = if config.passthrough {
        None
    } else if let Some(ref es_url) = config.elasticsearch_url {
        let client = es_client::EsClient::new(es_url, &config.elasticsearch_manifest_index);
        info!(
            url = es_url,
            index = config.elasticsearch_manifest_index,
            "Elasticsearch client initialized (index managed by passsage)"
        );
        Some(Arc::new(client))
    } else {
        None
    };

    let data_dir = config.data_dir.clone();

    if !config.passthrough {
        let dd = data_dir.clone();
        let dp = config.data_prefix.clone();
        let t0 = std::time::Instant::now();
        let created = tokio::task::spawn_blocking(move || preseed_s3_directories(&dd, &dp))
            .await??;
        let elapsed = t0.elapsed();
        info!(
            created,
            total = 65536,
            elapsed_secs = elapsed.as_secs_f64(),
            "S3 directory pre-seeding complete"
        );
    }

    let state = AppState {
        config: Arc::new(config.clone()),
        data_dir,
        downloads: Arc::new(download::DownloadManager::default()),
        trace: trace_writer,
        es_client,
        http_pool: Arc::new(http_pool::HttpClientPool::new()),
    };

    let addr = SocketAddr::from((config.bind_ip, config.port));
    info!("listening on {addr}");
    info!(
        http_concurrency = config.http_concurrency,
        chunk_size = config.chunk_size,
        data_dir = %config.data_dir.display(),
        temp_dir = %config.temp_dir.display(),
        passthrough = config.passthrough,
        "configuration loaded"
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, server::build_router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => info!("received SIGTERM, shutting down"),
        _ = sigint.recv() => info!("received SIGINT, shutting down"),
    }
}
