use std::net::IpAddr;
use std::path::PathBuf;

use byte_unit::Byte;
use clap::Parser;

const DEFAULT_PORT: u16 = 8080;
const DEFAULT_HTTP_CONCURRENCY: usize = 8;
const DEFAULT_CHUNK_SIZE: &str = "8MiB";
const DEFAULT_PREFETCH_WINDOW: usize = 16;
const DEFAULT_OPEN_PARALLELISM: usize = 8;

/// High-performance HTTP download accelerator and caching proxy with
/// content-addressed S3 storage and parallel chunked downloads.
///
/// The URL path encodes the upstream URL to fetch.  An optional
/// `X-Xs3lerator-Cache-Key` header enables the caching layer
/// (Elasticsearch manifest + content-addressed data directory).
/// Without it, xs3lerator acts as a pure download accelerator.
///
/// In `--passthrough` mode, the caching layer is fully disabled:
/// no Elasticsearch connection, no data directory required.
#[derive(Debug, Clone, Parser)]
#[command(name = "xs3lerator", version, about, long_about = None)]
pub struct CliArgs {
    /// IP address to bind on. Omit to bind all interfaces.
    #[arg(long, env = "XS3_BIND_IP", help = "Bind IP address [default: 0.0.0.0]")]
    pub bind_ip: Option<IpAddr>,

    /// TCP port to listen on.
    #[arg(long, env = "XS3_PORT", default_value_t = DEFAULT_PORT)]
    pub port: u16,

    /// Root directory for content-addressed chunk storage (typically a mount-s3
    /// mountpoint, e.g. /data).  Not required in passthrough mode.
    #[arg(long, env = "XS3_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Maximum parallel connections per upstream HTTP download.
    #[arg(long, env = "XS3_HTTP_CONCURRENCY", default_value_t = DEFAULT_HTTP_CONCURRENCY)]
    pub http_concurrency: usize,

    /// Fixed chunk size for content-addressed storage.
    #[arg(
        long,
        env = "XS3_CHUNK_SIZE",
        default_value = DEFAULT_CHUNK_SIZE,
        value_parser = parse_byte_size
    )]
    pub chunk_size: u64,

    /// Directory for temporary chunk files.  Defaults to the system temp dir.
    /// Set to /dev/shm for pure-RAM buffering on EBS-constrained environments.
    #[arg(long, env = "XS3_TEMP_DIR")]
    pub temp_dir: Option<PathBuf>,

    /// Skip TLS certificate verification for upstream HTTP requests (global).
    /// Can also be set per-request via the X-Xs3lerator-Tls-Skip-Verify header.
    #[arg(long, env = "XS3_UPSTREAM_TLS_SKIP_VERIFY", default_value_t = false)]
    pub upstream_tls_skip_verify: bool,

    /// Path to write JSONL debug trace (download/reader timing, chunk
    /// progress).  Disabled when omitted.
    #[arg(long, env = "XS3_DEBUG_TRACE")]
    pub debug_trace: Option<String>,

    /// Subdirectory under data-dir for content-addressed data chunks.
    #[arg(long, env = "XS3_DATA_PREFIX", default_value = "data/")]
    pub data_prefix: String,

    /// Elasticsearch URL for manifest storage.
    #[arg(long, env = "XS3_ELASTICSEARCH_URL")]
    pub elasticsearch_url: Option<String>,

    /// Elasticsearch index name.
    #[arg(long, env = "XS3_ELASTICSEARCH_INDEX", default_value = "passsage_meta")]
    pub elasticsearch_manifest_index: String,

    /// Passthrough-only mode: disable all caching infrastructure.
    /// No Elasticsearch, no data directory, no S3 writes.
    /// xs3lerator acts as a pure parallel download accelerator.
    #[arg(long, env = "XS3_PASSTHROUGH", default_value_t = false)]
    pub passthrough: bool,

    /// How many chunk files to buffer ahead of the streaming cursor when
    /// serving from cache.  Each slot holds an open file handle with
    /// fadvise(WILLNEED) already issued, so the page-cache is warm by
    /// the time the reader reaches that chunk.
    #[arg(long, env = "XS3_PREFETCH_WINDOW", default_value_t = DEFAULT_PREFETCH_WINDOW)]
    pub prefetch_window: usize,

    /// Maximum concurrent open() + fadvise() calls in the prefetch pipeline.
    /// On FUSE/mount-s3, each open() triggers an S3 HeadObject (~30 ms),
    /// so parallelism here directly reduces the per-chunk latency.
    #[arg(long, env = "XS3_OPEN_PARALLELISM", default_value_t = DEFAULT_OPEN_PARALLELISM)]
    pub open_parallelism: usize,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bind_ip: IpAddr,
    pub port: u16,
    pub data_dir: PathBuf,
    pub http_concurrency: usize,
    pub chunk_size: u64,
    pub temp_dir: PathBuf,
    pub upstream_tls_skip_verify: bool,
    pub data_prefix: String,
    pub elasticsearch_url: Option<String>,
    pub elasticsearch_manifest_index: String,
    pub passthrough: bool,
    pub prefetch_window: usize,
    pub open_parallelism: usize,
}

impl TryFrom<CliArgs> for AppConfig {
    type Error = anyhow::Error;

    fn try_from(args: CliArgs) -> Result<Self, Self::Error> {
        anyhow::ensure!(args.http_concurrency >= 1, "http-concurrency must be >= 1");
        anyhow::ensure!(args.chunk_size >= 1024, "chunk-size must be >= 1 KiB");

        if args.passthrough {
            anyhow::ensure!(
                args.elasticsearch_url.is_none(),
                "--elasticsearch-url is incompatible with --passthrough"
            );
        }

        let data_dir = if args.passthrough {
            args.data_dir.unwrap_or_else(std::env::temp_dir)
        } else {
            let dir = args
                .data_dir
                .ok_or_else(|| anyhow::anyhow!("--data-dir is required (unless --passthrough)"))?;
            anyhow::ensure!(
                dir.is_dir(),
                "data-dir {:?} is not an existing directory",
                dir
            );
            dir
        };

        let temp_dir = args.temp_dir.unwrap_or_else(std::env::temp_dir);
        anyhow::ensure!(
            temp_dir.is_dir(),
            "temp-dir {:?} is not an existing directory",
            temp_dir
        );

        Ok(Self {
            bind_ip: args.bind_ip.unwrap_or_else(|| IpAddr::from([0, 0, 0, 0])),
            port: args.port,
            data_dir,
            http_concurrency: args.http_concurrency,
            chunk_size: args.chunk_size,
            temp_dir,
            upstream_tls_skip_verify: args.upstream_tls_skip_verify,
            data_prefix: args.data_prefix,
            elasticsearch_url: args.elasticsearch_url,
            elasticsearch_manifest_index: args.elasticsearch_manifest_index,
            passthrough: args.passthrough,
            prefetch_window: args.prefetch_window,
            open_parallelism: args.open_parallelism,
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
    fn defaults_produce_valid_config() {
        let dir = std::env::temp_dir();
        let args = CliArgs::parse_from(["xs3lerator", "--data-dir", dir.to_str().unwrap()]);
        let config = AppConfig::try_from(args).unwrap();
        assert_eq!(config.http_concurrency, 8);
        assert_eq!(config.chunk_size, 8_388_608);
        assert!(!config.upstream_tls_skip_verify);
        assert!(!config.passthrough);
    }

    #[test]
    fn passthrough_mode_no_data_dir() {
        let args = CliArgs::parse_from(["xs3lerator", "--passthrough"]);
        let config = AppConfig::try_from(args).unwrap();
        assert!(config.passthrough);
    }

    #[test]
    fn passthrough_rejects_elasticsearch() {
        let args = CliArgs::parse_from([
            "xs3lerator",
            "--passthrough",
            "--elasticsearch-url",
            "http://localhost:9200",
        ]);
        assert!(AppConfig::try_from(args).is_err());
    }
}
