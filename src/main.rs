use std::net::SocketAddr;
use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use clap::Parser;
use tracing::info;

mod chunk_cache;
mod chunk_upload;
mod config;
mod download;
mod error;
mod handler;
mod headers;
mod manifest;
mod planner;
mod range;
mod s3;
mod s3_upload;
mod server;
mod trace;
mod upstream_fetcher;

use config::{AppConfig, CliArgs};
use handler::AppState;
use manifest::ManifestCache;
use s3::{AwsUpstream, S3Uploader};
use trace::TraceWriter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = CliArgs::parse();
    let debug_trace = args.debug_trace.clone();
    let config = AppConfig::try_from(args)?;

    // Build S3 client
    let mut s3_config_builder = aws_config::defaults(BehaviorVersion::latest());
    if let Some(ref region) = config.region {
        s3_config_builder = s3_config_builder.region(Region::new(region.clone()));
    }
    let sdk_config = s3_config_builder.load().await;

    let mut s3_conf = aws_sdk_s3::config::Builder::from(&sdk_config);
    if let Some(ref endpoint) = config.s3_endpoint_url {
        s3_conf = s3_conf.endpoint_url(endpoint);
    }
    if config.s3_force_path_style {
        s3_conf = s3_conf.force_path_style(true);
    }
    let s3_client = aws_sdk_s3::Client::from_conf(s3_conf.build());

    let s3_upstream = Arc::new(AwsUpstream::new(s3_client.clone()));
    let s3_uploader = Arc::new(S3Uploader::new(s3_client));

    let trace_writer = debug_trace.map(|path| {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open trace file");
        Arc::new(TraceWriter::new(Box::new(file)))
    });

    let manifest_cache = Arc::new(ManifestCache::new(config.manifest_cache_size));

    let chunk_cache_arc = if let Some(ref dir) = config.chunk_cache_dir {
        let cc = chunk_cache::ChunkCache::new(
            dir.clone(),
            config.chunk_cache_max_size,
            config.chunk_cache_max_object_size,
        )
        .expect("initialize chunk cache");
        let cc = Arc::new(cc);
        chunk_cache::spawn_evictor(cc.clone(), std::time::Duration::from_secs(45));
        info!(
            dir = %dir.display(),
            max_size = config.chunk_cache_max_size,
            max_object_size = config.chunk_cache_max_object_size,
            "local chunk cache enabled"
        );
        Some(cc)
    } else {
        None
    };

    let state = AppState {
        config: Arc::new(config.clone()),
        s3_upstream,
        s3_uploader,
        downloads: Arc::new(download::DownloadManager::default()),
        trace: trace_writer,
        manifest_cache,
        chunk_cache: chunk_cache_arc,
    };

    let addr = SocketAddr::from((config.bind_ip, config.port));
    info!("listening on {addr}");
    info!(
        s3_concurrency = config.s3_concurrency,
        http_concurrency = config.http_concurrency,
        min_chunk_size = config.min_chunk_size,
        temp_dir = %config.temp_dir.display(),
        "configuration loaded"
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, server::build_router(state)).await?;

    Ok(())
}
