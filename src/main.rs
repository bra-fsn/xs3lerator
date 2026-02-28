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
