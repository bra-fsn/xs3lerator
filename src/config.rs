use std::net::IpAddr;
use std::path::PathBuf;

use byte_unit::Byte;
use clap::Parser;

const DEFAULT_PORT: u16 = 8080;
const DEFAULT_S3_CONCURRENCY: usize = 32;
const DEFAULT_HTTP_CONCURRENCY: usize = 8;
const DEFAULT_MIN_CHUNK: &str = "8MiB";
const S3_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// High-performance HTTP caching proxy with parallel downloads and S3-backed storage.
///
/// Accepts GET requests where the URL path encodes `/<bucket>/<s3_key>`.
/// The real upstream URL is passed in the `X-Xs3lerator-Upstream-Url` header
/// (base64-encoded).  Depending on the `X-Xs3lerator-Cache-Skip` header,
/// xs3lerator either serves the object from S3 (parallel range-GETs) or
/// fetches it from the upstream, streams it to the client, and simultaneously
/// uploads it to S3 via multipart upload.
#[derive(Debug, Clone, Parser)]
#[command(name = "xs3lerator", version, about, long_about = None)]
pub struct CliArgs {
    /// IP address to bind on. Omit to bind all interfaces.
    #[arg(long, env = "XS3_BIND_IP", help = "Bind IP address [default: 0.0.0.0]")]
    pub bind_ip: Option<IpAddr>,

    /// TCP port to listen on.
    #[arg(long, env = "XS3_PORT", default_value_t = DEFAULT_PORT)]
    pub port: u16,

    /// AWS region. Falls back to SDK default resolution chain.
    #[arg(long, env = "XS3_REGION")]
    pub region: Option<String>,

    /// Override S3 endpoint URL (for LocalStack or S3-compatible services).
    #[arg(long, env = "XS3_S3_ENDPOINT_URL")]
    pub s3_endpoint_url: Option<String>,

    /// Force path-style S3 addressing (required for LocalStack).
    #[arg(long, env = "XS3_S3_FORCE_PATH_STYLE", default_value_t = false)]
    pub s3_force_path_style: bool,

    /// Maximum parallel range-request streams per S3 object download.
    #[arg(long, env = "XS3_S3_CONCURRENCY", default_value_t = DEFAULT_S3_CONCURRENCY)]
    pub s3_concurrency: usize,

    /// Maximum parallel connections per upstream HTTP download.
    #[arg(long, env = "XS3_HTTP_CONCURRENCY", default_value_t = DEFAULT_HTTP_CONCURRENCY)]
    pub http_concurrency: usize,

    /// Minimum chunk size for parallel downloads.  Also the minimum S3
    /// multipart upload part size (must be >= 5 MiB).
    #[arg(
        long,
        env = "XS3_MIN_CHUNK_SIZE",
        default_value = DEFAULT_MIN_CHUNK,
        value_parser = parse_byte_size
    )]
    pub min_chunk_size: u64,

    /// Directory for temporary chunk files.  Defaults to the system temp dir.
    /// Set to /dev/shm for pure-RAM buffering on EBS-constrained environments.
    #[arg(long, env = "XS3_TEMP_DIR")]
    pub temp_dir: Option<PathBuf>,

    /// Skip TLS certificate verification for upstream HTTP requests (global).
    /// Can also be set per-request via the X-Xs3lerator-Tls-Skip-Verify header.
    #[arg(long, env = "XS3_UPSTREAM_TLS_SKIP_VERIFY", default_value_t = false)]
    pub upstream_tls_skip_verify: bool,

    /// Path to write JSONL debug trace (download/reader timing, S3 latency,
    /// chunk progress).  Disabled when omitted.
    #[arg(long, env = "XS3_DEBUG_TRACE")]
    pub debug_trace: Option<String>,

    /// S3 prefix for content-addressed data chunks.
    #[arg(long, env = "XS3_DATA_PREFIX", default_value = "data/")]
    pub data_prefix: String,

    /// Elasticsearch URL for manifest storage.
    #[arg(long, env = "XS3_ELASTICSEARCH_URL")]
    pub elasticsearch_url: Option<String>,

    /// Elasticsearch index name for manifests.
    #[arg(long, env = "XS3_ELASTICSEARCH_MANIFEST_INDEX", default_value = "xs3_manifests")]
    pub elasticsearch_manifest_index: String,

    /// Number of Elasticsearch index replicas.
    #[arg(long, env = "XS3_ELASTICSEARCH_REPLICAS", default_value_t = 1)]
    pub elasticsearch_replicas: u32,

    /// Number of Elasticsearch index shards.
    #[arg(long, env = "XS3_ELASTICSEARCH_SHARDS", default_value_t = 9)]
    pub elasticsearch_shards: u32,

    /// In-memory LRU cache capacity for manifests.
    #[arg(long, env = "XS3_MANIFEST_CACHE_SIZE", default_value_t = 10_000)]
    pub manifest_cache_size: usize,

    /// Enable local filesystem chunk cache at this directory.
    #[arg(long, env = "XS3_CHUNK_CACHE_DIR")]
    pub chunk_cache_dir: Option<PathBuf>,

    /// Maximum total size of the local chunk cache.
    #[arg(
        long,
        env = "XS3_CHUNK_CACHE_MAX_SIZE",
        default_value = "100GiB",
        value_parser = parse_byte_size
    )]
    pub chunk_cache_max_size: u64,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bind_ip: IpAddr,
    pub port: u16,
    pub region: Option<String>,
    pub s3_endpoint_url: Option<String>,
    pub s3_force_path_style: bool,
    pub s3_concurrency: usize,
    pub http_concurrency: usize,
    pub min_chunk_size: u64,
    pub temp_dir: PathBuf,
    pub upstream_tls_skip_verify: bool,
    pub data_prefix: String,
    pub elasticsearch_url: Option<String>,
    pub elasticsearch_manifest_index: String,
    pub elasticsearch_replicas: u32,
    pub elasticsearch_shards: u32,
    pub manifest_cache_size: usize,
    pub chunk_cache_dir: Option<PathBuf>,
    pub chunk_cache_max_size: u64,
}

impl TryFrom<CliArgs> for AppConfig {
    type Error = anyhow::Error;

    fn try_from(args: CliArgs) -> Result<Self, Self::Error> {
        anyhow::ensure!(args.s3_concurrency >= 1, "s3-concurrency must be >= 1");
        anyhow::ensure!(args.http_concurrency >= 1, "http-concurrency must be >= 1");
        anyhow::ensure!(
            args.min_chunk_size >= S3_MIN_PART_SIZE,
            "min-chunk-size must be >= 5 MiB (S3 multipart minimum part size)"
        );

        let temp_dir = args.temp_dir.unwrap_or_else(std::env::temp_dir);
        anyhow::ensure!(
            temp_dir.is_dir(),
            "temp-dir {:?} is not an existing directory",
            temp_dir
        );

        if let Some(ref dir) = args.chunk_cache_dir {
            anyhow::ensure!(
                dir.is_dir() || !dir.exists(),
                "chunk-cache-dir {:?} exists but is not a directory",
                dir
            );
        }

        Ok(Self {
            bind_ip: args.bind_ip.unwrap_or_else(|| IpAddr::from([0, 0, 0, 0])),
            port: args.port,
            region: args.region,
            s3_endpoint_url: args.s3_endpoint_url,
            s3_force_path_style: args.s3_force_path_style,
            s3_concurrency: args.s3_concurrency,
            http_concurrency: args.http_concurrency,
            min_chunk_size: args.min_chunk_size,
            temp_dir,
            upstream_tls_skip_verify: args.upstream_tls_skip_verify,
            data_prefix: args.data_prefix,
            elasticsearch_url: args.elasticsearch_url,
            elasticsearch_manifest_index: args.elasticsearch_manifest_index,
            elasticsearch_replicas: args.elasticsearch_replicas,
            elasticsearch_shards: args.elasticsearch_shards,
            manifest_cache_size: args.manifest_cache_size,
            chunk_cache_dir: args.chunk_cache_dir,
            chunk_cache_max_size: args.chunk_cache_max_size,
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
        assert_eq!(parse_byte_size("8MiB").unwrap(), 8_388_608);
        assert_eq!(parse_byte_size("256MiB").unwrap(), 268_435_456);
        assert_eq!(parse_byte_size("1GiB").unwrap(), 1_073_741_824);
    }

    #[test]
    fn validation_rejects_small_chunk() {
        let args = CliArgs::parse_from([
            "xs3lerator",
            "--min-chunk-size",
            "1MiB",
        ]);
        let err = AppConfig::try_from(args).unwrap_err();
        assert!(err.to_string().contains("5 MiB"));
    }

    #[test]
    fn defaults_produce_valid_config() {
        let args = CliArgs::parse_from(["xs3lerator"]);
        let config = AppConfig::try_from(args).unwrap();
        assert_eq!(config.s3_concurrency, 32);
        assert_eq!(config.http_concurrency, 8);
        assert_eq!(config.min_chunk_size, 8_388_608);
        assert!(!config.upstream_tls_skip_verify);
    }
}
