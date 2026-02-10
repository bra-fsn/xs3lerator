use std::net::SocketAddr;
use std::sync::Arc;

use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use clap::Parser;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;

use xs3lerator::cache::CacheStore;
use xs3lerator::config::{AppConfig, CliArgs, OperatingMode};
use xs3lerator::download::DownloadManager;
use xs3lerator::evictor::{run_evictor, EvictorConfig};
use xs3lerator::handler::ProxyState;
use xs3lerator::s3::AwsUpstream;
use xs3lerator::server::build_router;
use xs3lerator::trace::TraceWriter;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let args = CliArgs::parse();
    let trace = args
        .debug_trace
        .as_deref()
        .map(|p| {
            let tw = TraceWriter::new(std::path::Path::new(p))
                .expect("failed to create debug trace file");
            info!(path = p, "debug trace enabled");
            Arc::new(tw)
        });
    let config = Arc::new(AppConfig::try_from(args)?);

    // ---------- cache + evictor (cache mode only) ----------
    let (cache, downloads) = match &config.mode {
        OperatingMode::Cache {
            cache_dir,
            cache_hierarchy_level,
            max_cache_size,
            gc_watermark_percent,
            gc_target_percent,
            gc_interval_seconds,
            ..
        } => {
            let cache = Arc::new(
                CacheStore::new(cache_dir.clone(), *cache_hierarchy_level).await?,
            );
            cache.create_hierarchy().await?;
            info!(dir = ?cache_dir, levels = cache_hierarchy_level,
                  "cache hierarchy created");

            let evictor_cache = cache.clone();
            let evictor_cfg = EvictorConfig {
                max_bytes: *max_cache_size,
                watermark_percent: *gc_watermark_percent,
                target_percent: *gc_target_percent,
                interval_seconds: *gc_interval_seconds,
            };
            tokio::spawn(run_evictor(evictor_cache, evictor_cfg));

            (Some(cache), Some(Arc::new(DownloadManager::default())))
        }
        OperatingMode::Mount { mount_path } => {
            info!(path = ?mount_path, "mount mode — serving from mount-s3 path");
            (None, None)
        }
    };

    // ---------- AWS S3 client ----------
    let region_provider = match &config.region {
        Some(r) => RegionProviderChain::first_try(Some(aws_config::Region::new(r.clone())))
            .or_default_provider(),
        None => RegionProviderChain::default_provider(),
    };
    let aws_cfg = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;

    let mut s3_builder = aws_sdk_s3::config::Builder::from(&aws_cfg);
    if let Some(url) = &config.s3_endpoint_url {
        s3_builder = s3_builder.endpoint_url(url);
    }
    if config.s3_force_path_style {
        s3_builder = s3_builder.force_path_style(true);
    }
    let s3_client = aws_sdk_s3::Client::from_conf(s3_builder.build());

    // ---------- application state ----------
    let state = ProxyState {
        config: config.clone(),
        upstream: Arc::new(AwsUpstream::new(s3_client)),
        cache,
        downloads,
        trace,
    };

    // ---------- HTTP server ----------
    let app = build_router(state).layer(TraceLayer::new_for_http());
    let addr = SocketAddr::new(config.bind_ip, config.port);
    let listener = TcpListener::bind(addr).await?;
    info!("xs3lerator listening on http://{addr}");
    axum::serve(listener, app).await?;

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "xs3lerator=info,tower_http=info".into()
            }),
        )
        .with_target(false)
        .compact()
        .init();
}
