use std::net::IpAddr;
use std::path::PathBuf;

use byte_unit::Byte;
use clap::Parser;

const DEFAULT_PORT: u16 = 8080;
const DEFAULT_HIERARCHY_LEVEL: usize = 4;
const DEFAULT_MAX_CONCURRENCY: usize = 32;
const DEFAULT_MIN_CHUNK: &str = "1MiB";
const DEFAULT_MAX_CHUNK: &str = "256MiB";
const DEFAULT_GC_INTERVAL: u64 = 15;
const DEFAULT_GC_WATERMARK: u8 = 95;
const DEFAULT_GC_TARGET: u8 = 90;

/// High-performance S3 HTTP proxy cache with parallel chunked downloads,
/// content-addressed caching, and background LRU eviction.
#[derive(Debug, Clone, Parser)]
#[command(name = "xs3lerator", version, about, long_about = None)]
pub struct CliArgs {
    /// IP address to bind on. Omit to bind all interfaces.
    #[arg(long, env = "XS3_BIND_IP", help = "Bind IP address [default: 0.0.0.0]")]
    pub bind_ip: Option<IpAddr>,

    /// TCP port to listen on.
    #[arg(long, env = "XS3_PORT", default_value_t = DEFAULT_PORT)]
    pub port: u16,

    /// S3 bucket name (required).
    #[arg(long, env = "XS3_BUCKET")]
    pub bucket: String,

    /// AWS region. Falls back to SDK default resolution chain.
    #[arg(long, env = "XS3_REGION")]
    pub region: Option<String>,

    /// Override S3 endpoint URL (for LocalStack or S3-compatible services).
    #[arg(long, env = "XS3_S3_ENDPOINT_URL")]
    pub s3_endpoint_url: Option<String>,

    /// Force path-style S3 addressing (required for LocalStack).
    #[arg(long, env = "XS3_S3_FORCE_PATH_STYLE", default_value_t = false)]
    pub s3_force_path_style: bool,

    /// Local cache directory (required).
    #[arg(long, env = "XS3_CACHE_DIR")]
    pub cache_dir: PathBuf,

    /// Maximum total cache size, e.g. 100GiB (required).
    #[arg(long, env = "XS3_MAX_CACHE_SIZE", value_parser = parse_byte_size)]
    pub max_cache_size: u64,

    /// Cache directory hierarchy depth. Each level uses one hex nibble.
    /// Level 4 → e/8/b/0/<hash>
    #[arg(
        long,
        env = "XS3_CACHE_HIERARCHY_LEVEL",
        default_value_t = DEFAULT_HIERARCHY_LEVEL
    )]
    pub cache_hierarchy_level: usize,

    /// Maximum parallel range-request streams per object download.
    #[arg(long, env = "XS3_MAX_CONCURRENCY", default_value_t = DEFAULT_MAX_CONCURRENCY)]
    pub max_concurrency: usize,

    /// Minimum upstream chunk size, e.g. 1MiB.
    #[arg(
        long,
        env = "XS3_MIN_CHUNK_SIZE",
        default_value = DEFAULT_MIN_CHUNK,
        value_parser = parse_byte_size
    )]
    pub min_chunk_size: u64,

    /// Maximum upstream chunk size, e.g. 256MiB.
    #[arg(
        long,
        env = "XS3_MAX_CHUNK_SIZE",
        default_value = DEFAULT_MAX_CHUNK,
        value_parser = parse_byte_size
    )]
    pub max_chunk_size: u64,

    /// Background cache GC scan interval in seconds.
    #[arg(
        long,
        env = "XS3_GC_INTERVAL_SECONDS",
        default_value_t = DEFAULT_GC_INTERVAL
    )]
    pub gc_interval_seconds: u64,

    /// GC triggers when cache usage reaches this percentage of --max-cache-size.
    #[arg(
        long,
        env = "XS3_GC_WATERMARK_PERCENT",
        default_value_t = DEFAULT_GC_WATERMARK
    )]
    pub gc_watermark_percent: u8,

    /// GC evicts files until cache usage drops to this percentage of --max-cache-size.
    #[arg(
        long,
        env = "XS3_GC_TARGET_PERCENT",
        default_value_t = DEFAULT_GC_TARGET
    )]
    pub gc_target_percent: u8,

    /// Path to write JSONL debug trace (download/reader timing, S3 latency,
    /// chunk progress).  Disabled when omitted.
    #[arg(long, env = "XS3_DEBUG_TRACE")]
    pub debug_trace: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bind_ip: IpAddr,
    pub port: u16,
    pub bucket: String,
    pub region: Option<String>,
    pub s3_endpoint_url: Option<String>,
    pub s3_force_path_style: bool,
    pub cache_dir: PathBuf,
    pub max_cache_size: u64,
    pub cache_hierarchy_level: usize,
    pub max_concurrency: usize,
    pub min_chunk_size: u64,
    pub max_chunk_size: u64,
    pub gc_interval_seconds: u64,
    pub gc_watermark_percent: u8,
    pub gc_target_percent: u8,
}

impl TryFrom<CliArgs> for AppConfig {
    type Error = anyhow::Error;

    fn try_from(args: CliArgs) -> Result<Self, Self::Error> {
        anyhow::ensure!(
            args.cache_hierarchy_level >= 1 && args.cache_hierarchy_level <= 12,
            "cache-hierarchy-level must be in 1..=12"
        );
        anyhow::ensure!(args.max_concurrency >= 1, "max-concurrency must be >= 1");
        anyhow::ensure!(
            args.min_chunk_size > 0 && args.max_chunk_size > 0,
            "chunk sizes must be > 0"
        );
        anyhow::ensure!(
            args.min_chunk_size <= args.max_chunk_size,
            "min-chunk-size must be <= max-chunk-size"
        );
        anyhow::ensure!(
            args.gc_target_percent > 0
                && args.gc_target_percent < args.gc_watermark_percent
                && args.gc_watermark_percent <= 100,
            "require 0 < gc-target-percent < gc-watermark-percent <= 100"
        );

        Ok(Self {
            bind_ip: args.bind_ip.unwrap_or_else(|| IpAddr::from([0, 0, 0, 0])),
            port: args.port,
            bucket: args.bucket,
            region: args.region,
            s3_endpoint_url: args.s3_endpoint_url,
            s3_force_path_style: args.s3_force_path_style,
            cache_dir: args.cache_dir,
            max_cache_size: args.max_cache_size,
            cache_hierarchy_level: args.cache_hierarchy_level,
            max_concurrency: args.max_concurrency,
            min_chunk_size: args.min_chunk_size,
            max_chunk_size: args.max_chunk_size,
            gc_interval_seconds: args.gc_interval_seconds,
            gc_watermark_percent: args.gc_watermark_percent,
            gc_target_percent: args.gc_target_percent,
        })
    }
}

fn parse_byte_size(input: &str) -> Result<u64, String> {
    Byte::parse_str(input, true)
        .map(|b| b.as_u64())
        .map_err(|e| format!("invalid byte size: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_common_sizes() {
        assert_eq!(parse_byte_size("1MiB").unwrap(), 1_048_576);
        assert_eq!(parse_byte_size("256MiB").unwrap(), 268_435_456);
        assert_eq!(parse_byte_size("1GiB").unwrap(), 1_073_741_824);
        assert_eq!(parse_byte_size("100GiB").unwrap(), 107_374_182_400);
        assert_eq!(parse_byte_size("500GB").unwrap(), 500_000_000_000);
    }

    #[test]
    fn validation_rejects_bad_gc_config() {
        let mut args = CliArgs::parse_from([
            "xs3lerator",
            "--bucket",
            "b",
            "--cache-dir",
            "/tmp",
            "--max-cache-size",
            "1GiB",
        ]);
        args.gc_target_percent = 95;
        args.gc_watermark_percent = 90;
        assert!(AppConfig::try_from(args).is_err());
    }
}
