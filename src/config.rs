//! Configuration paths are resolved relative to the configuration file, not the process cwd.
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub server: Server,
    pub storage: Storage,
    pub segments: Segments,
    pub writes: Writes,
    pub reads: Reads,
    pub resources: Resources,
    pub cache: Cache,
    pub streaming: Streaming,
    pub checkpoint: Checkpoint,
    pub logging: Logging,
    pub mcp: Mcp,
    pub verification: Verification,
    pub gc: Gc,
}

/// Resource controls shared by online and offline integrity scans.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Verification {
    /// Maximum number of verification scans allowed to run at once.
    pub max_concurrency: usize,
    /// Maximum scan read rate in MiB/s; zero disables throttling.
    pub max_bytes_per_second_mib: u64,
}
impl Default for Verification {
    fn default() -> Self {
        Self {
            max_concurrency: 1,
            max_bytes_per_second_mib: 0,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Server {
    pub listen: String,
    pub admin_listen: Option<String>,
    pub shutdown_timeout_seconds: u64,
    pub request_timeout_seconds: u64,
}
impl Default for Server {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:7788".into(),
            admin_listen: None,
            shutdown_timeout_seconds: 30,
            request_timeout_seconds: 120,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Storage {
    pub root: PathBuf,
    /// Minimum free bytes reserve, in MiB, retained before any store upload.
    pub min_free_mib: u64,
}
impl Default for Storage {
    fn default() -> Self {
        Self {
            root: PathBuf::from("./data"),
            min_free_mib: 256,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Segments {
    pub max_size_bytes: u64,
    pub growth_factor: u32,
}
impl Default for Segments {
    fn default() -> Self {
        Self {
            max_size_bytes: 8 * 1024 * 1024 * 1024,
            growth_factor: 1,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Writes {
    /// Independent append lanes; publication remains a shared durable sequence.
    pub writer_lanes: usize,
    /// Maximum publication-coalescing wait for multiple lanes. One lane ignores
    /// this window; sparse or skewed multi-lane traffic may incur this extra wait.
    pub commit_max_delay_ms: u64,
    pub batch_max_records: usize,
    pub batch_max_delay_ms: u64,
    pub max_pending_mib: usize,
    pub batch_max_bytes_mib: usize,
}
impl Default for Writes {
    fn default() -> Self {
        Self {
            writer_lanes: 1,
            commit_max_delay_ms: 10,
            batch_max_records: 512,
            batch_max_delay_ms: 1,
            max_pending_mib: 256,
            batch_max_bytes_mib: 16,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Reads {
    pub policy: String,
    pub max_concurrency: usize,
    pub hdd_window_ms: u64,
    pub queue_items: usize,
}
impl Default for Reads {
    fn default() -> Self {
        Self {
            policy: "ssd".into(),
            max_concurrency: 32,
            hdd_window_ms: 4,
            queue_items: 1024,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Resources {
    pub max_object_mib: usize,
    pub max_inflight_read_mib: usize,
    pub max_open_segments: usize,
    pub max_requests: usize,
    pub max_batch_items: usize,
    pub admission_timeout_ms: u64,
    pub max_inflight_per_batch: usize,
    pub admin_max_requests: usize,
}
impl Default for Resources {
    fn default() -> Self {
        Self {
            max_object_mib: 64,
            max_inflight_read_mib: 256,
            max_open_segments: 256,
            max_requests: 128,
            max_batch_items: 4096,
            admission_timeout_ms: 5000,
            max_inflight_per_batch: 32,
            admin_max_requests: 8,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Cache {
    pub payload_mib: usize,
    pub index_mib: usize,
    pub admission_entries: usize,
    pub admission_window_seconds: u64,
    /// Resident payload entry cap, separate from the admission history cap.
    pub max_entries: usize,
    pub max_object_mib: usize,
    /// Conservative index/history metadata cap. With the default 64 MiB this
    /// limits resident entries to roughly one hundred thousand even though
    /// max_entries permits up to sixteen million.
    pub metadata_mib: usize,
    /// Power of two from 1 to 64. Byte budgets are divided per shard, so a
    /// skewed hash distribution can leave capacity unused in another shard.
    pub shards: usize,
}
impl Default for Cache {
    fn default() -> Self {
        Self {
            payload_mib: 256,
            index_mib: 128,
            admission_entries: 65536,
            admission_window_seconds: 300,
            max_entries: 1_000_000,
            max_object_mib: 8,
            metadata_mib: 64,
            shards: 16,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Streaming {
    pub enabled: bool,
    pub threshold_mib: usize,
    pub max_object_mib: usize,
    pub chunk_kib: usize,
    pub max_uploads: usize,
    pub max_reads: usize,
    pub checksum_cache_mib: usize,
    pub transfer_timeout_seconds: u64,
    pub idle_timeout_seconds: u64,
    pub min_free_mib: usize,
}
impl Default for Streaming {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold_mib: 8,
            max_object_mib: 4096,
            chunk_kib: 1024,
            max_uploads: 2,
            max_reads: 4,
            checksum_cache_mib: 16,
            transfer_timeout_seconds: 3600,
            idle_timeout_seconds: 30,
            min_free_mib: 256,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Checkpoint {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub min_journal_mib: usize,
    pub max_catchup_mib: usize,
    pub max_tail_kib: usize,
    pub batch_records: usize,
}
impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: 300,
            min_journal_mib: 256,
            max_catchup_mib: 16,
            max_tail_kib: 256,
            batch_records: 512,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Logging {
    pub level: String,
}
impl Default for Logging {
    fn default() -> Self {
        Self {
            level: "info".into(),
        }
    }
}

/// Conservative, tunable maintenance defaults; automatic collection is opt-in.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Gc {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub min_garbage_ratio: f64,
    pub delete_grace_seconds: u64,
    pub cooldown_seconds: u64,
    pub max_copy_mib: u64,
    pub max_bytes_per_second_mib: u64,
    pub low_space_percent: u64,
    pub target_free_percent: u64,
    pub min_free_mib: u64,
}
impl Default for Gc {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: 1800,
            min_garbage_ratio: 0.5,
            delete_grace_seconds: 86400,
            cooldown_seconds: 86400,
            max_copy_mib: 1024,
            max_bytes_per_second_mib: 25,
            low_space_percent: 20,
            target_free_percent: 30,
            min_free_mib: 256,
        }
    }
}
impl Gc {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.min_garbage_ratio.is_finite() && (0.0..=1.0).contains(&self.min_garbage_ratio),
            "gc.min_garbage_ratio must be finite and in 0..=1"
        );
        ensure!(
            self.interval_seconds > 0 && self.max_copy_mib > 0 && self.max_bytes_per_second_mib > 0,
            "gc interval, copy budget and rate must be positive"
        );
        ensure!(
            self.low_space_percent < self.target_free_percent && self.target_free_percent <= 100,
            "gc free-space percentages must satisfy low < target <= 100"
        );
        for mib in [
            self.max_copy_mib,
            self.max_bytes_per_second_mib,
            self.min_free_mib,
        ] {
            ensure!(
                mib.checked_mul(1024 * 1024).is_some(),
                "gc MiB value overflows bytes"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Mcp {
    pub enabled: bool,
    pub http_path: String,
    pub read_only: bool,
    pub allow_verification: bool,
    pub max_inline_object_kib: usize,
    pub max_put_object_kib: usize,
    pub max_list_page_size: usize,
    pub max_concurrent_requests: usize,
    pub max_encoded_response_kib: usize,
    pub max_response_memory_mib: usize,
    pub max_request_kib: usize,
    pub max_verification_jobs: usize,
    pub max_retained_jobs: usize,
    pub tool_timeout_seconds: u64,
    pub verification_timeout_seconds: u64,
    pub requests_per_minute: u32,
    pub auth_gateway: bool,
    pub external_url: Option<String>,
    pub allowed_origins: Vec<String>,
}
impl Default for Mcp {
    fn default() -> Self {
        Self {
            enabled: true,
            http_path: "/mcp".into(),
            read_only: true,
            allow_verification: false,
            max_inline_object_kib: 128,
            max_put_object_kib: 128,
            max_list_page_size: 100,
            max_concurrent_requests: 16,
            max_encoded_response_kib: 512,
            max_response_memory_mib: 16,
            max_request_kib: 256,
            max_verification_jobs: 1,
            max_retained_jobs: 64,
            tool_timeout_seconds: 30,
            verification_timeout_seconds: 300,
            requests_per_minute: 120,
            auth_gateway: false,
            external_url: None,
            allowed_origins: vec![],
        }
    }
}
impl Mcp {
    pub fn validate(&self, max_object_bytes: usize) -> Result<()> {
        ensure!(
            self.http_path.starts_with('/')
                && self.http_path.len() >= 2
                && self.http_path.len() <= 64
                && self.http_path[1..]
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
            "mcp.http_path must be a single /name component"
        );
        ensure!(
            (1..=1024).contains(&self.max_inline_object_kib)
                && self.max_inline_object_kib * 1024 <= max_object_bytes,
            "MCP inline limit must be 1..1024 KiB and fit object limit"
        );
        ensure!(
            (1..=1024).contains(&self.max_put_object_kib)
                && self.max_put_object_kib * 1024 <= max_object_bytes,
            "MCP PUT limit must be 1..1024 KiB and fit object limit"
        );
        ensure!(
            (1..=1000).contains(&self.max_list_page_size),
            "MCP listing limit must be 1..1000"
        );
        ensure!(
            (1..=256).contains(&self.max_concurrent_requests),
            "MCP concurrency must be 1..256"
        );
        ensure!(
            (16..=8192).contains(&self.max_encoded_response_kib),
            "MCP encoded response limit must be 16..8192 KiB"
        );
        ensure!(
            (1..=1024).contains(&self.max_response_memory_mib)
                && self.max_response_memory_mib * 1024 >= self.max_encoded_response_kib,
            "MCP response budget must fit one maximum response"
        );
        ensure!(
            (16..=4096).contains(&self.max_request_kib)
                && self.max_request_kib * 1024 >= self.max_put_object_kib * 1024 * 4 / 3 + 1024,
            "MCP request limit must fit base64 PUT envelope and be <=4096 KiB"
        );
        ensure!(
            self.max_encoded_response_kib * 1024
                >= self.max_inline_object_kib * 1024 * 8 / 3 + 4096,
            "MCP encoded limit must fit inline tool result and envelope"
        );
        ensure!(
            (1..=16).contains(&self.max_verification_jobs)
                && (self.max_verification_jobs..=4096).contains(&self.max_retained_jobs),
            "MCP verification job limits invalid"
        );
        ensure!(
            (1..=300).contains(&self.tool_timeout_seconds)
                && (1..=3600).contains(&self.verification_timeout_seconds),
            "MCP timeouts invalid"
        );
        ensure!(
            (1..=100000).contains(&self.requests_per_minute),
            "MCP request rate must be 1..100000/minute"
        );
        ensure!(self.allowed_origins.len() <= 64, "too many MCP origins");
        for origin in &self.allowed_origins {
            let url = reqwest::Url::parse(origin).context("invalid MCP allowed origin")?;
            ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "MCP origins must be HTTP(S) origins without credentials/path/query"
            );
        }
        if let Some(external) = &self.external_url {
            let url = reqwest::Url::parse(external).context("invalid MCP external_url")?;
            ensure!(
                url.scheme() == "https"
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "MCP external_url must be an HTTPS URL without credentials/query/fragment"
            );
        }
        ensure!(
            !self.auth_gateway || self.external_url.is_some(),
            "MCP auth_gateway requires external_url"
        );
        Ok(())
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read configuration {}", path.display()))?;
        let mut config: Self = toml::from_str(&text).context("parse configuration")?;
        if config.storage.root.is_relative() {
            config.storage.root = path
                .parent()
                .unwrap_or(Path::new("."))
                .join(&config.storage.root);
        }
        config.apply_env()?;
        config.validate()?;
        Ok(config)
    }
    pub fn save(&self, path: &Path) -> Result<()> {
        self.validate()?;
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, toml::to_string_pretty(self)?)?;
        Ok(())
    }
    pub fn apply_env(&mut self) -> Result<()> {
        if let Ok(value) = std::env::var("SKERRY_LISTEN") {
            self.server.listen = value;
        }
        if let Some(value) = std::env::var_os("SKERRY_ROOT") {
            self.storage.root = value.into();
        }
        if let Ok(value) = std::env::var("SKERRY_MCP_ENABLED") {
            self.mcp.enabled = value
                .parse()
                .context("SKERRY_MCP_ENABLED must be true or false")?;
        }
        Ok(())
    }
    pub fn root(&self) -> &Path {
        &self.storage.root
    }
    /// Secrets deliberately never enter the serialized configuration or status.
    pub fn token(&self) -> Option<String> {
        std::env::var("SKERRY_TOKEN").ok().filter(|v| !v.is_empty())
    }
    pub fn max_object_bytes(&self) -> usize {
        self.resources.max_object_mib * 1024 * 1024
    }
    pub fn validate(&self) -> Result<()> {
        let _: SocketAddr = self
            .server
            .listen
            .parse()
            .context("server.listen must be an IP address and port")?;
        ensure!(
            !self.storage.root.as_os_str().is_empty(),
            "storage.root is empty"
        );
        ensure!(
            self.reads.policy == "ssd" || self.reads.policy == "hdd",
            "reads.policy must be ssd or hdd"
        );
        ensure!(
            (1..=1024).contains(&self.reads.max_concurrency),
            "read concurrency must be 1..1024"
        );
        ensure!(
            (1..=65536).contains(&self.reads.queue_items),
            "reads.queue_items must be 1..65536"
        );
        ensure!(
            matches!(self.writes.writer_lanes, 1 | 2 | 4 | 8),
            "writes.writer_lanes must be 1, 2, 4, or 8"
        );
        ensure!(
            (1..=4096).contains(&self.writes.batch_max_records),
            "batch_max_records must be 1..4096"
        );
        ensure!(
            self.writes.batch_max_delay_ms <= 1000
                && self.writes.commit_max_delay_ms <= 1000
                && self.reads.hdd_window_ms <= 1000,
            "scheduling windows must be at most 1000 ms"
        );
        for (name, mib) in [
            ("max_object_mib", self.resources.max_object_mib),
            ("max_pending_mib", self.writes.max_pending_mib),
            (
                "max_inflight_read_mib",
                self.resources.max_inflight_read_mib,
            ),
        ] {
            ensure!((1..=2047).contains(&mib), "{name} must be 1..2047 MiB");
        }
        ensure!(
            self.writes.max_pending_mib >= self.resources.max_object_mib,
            "write budget must fit one maximum object"
        );
        ensure!(
            self.resources.max_inflight_read_mib >= self.resources.max_object_mib,
            "read budget must fit one maximum object"
        );
        ensure!(
            self.segments.max_size_bytes >= self.max_object_bytes() as u64 + 1024,
            "segment capacity must fit maximum object plus headers/footer"
        );
        ensure!(
            matches!(self.segments.growth_factor, 1 | 2),
            "growth_factor must be 1 (disabled) or 2"
        );
        for (name, count) in [
            ("max_open_segments", self.resources.max_open_segments),
            ("max_requests", self.resources.max_requests),
            ("max_batch_items", self.resources.max_batch_items),
        ] {
            ensure!((1..=65536).contains(&count), "{name} must be 1..65536");
        }
        ensure!(
            self.cache.payload_mib <= 65536
                && self.cache.index_mib <= 65536
                && self.cache.metadata_mib <= 65536,
            "cache budget too large"
        );
        ensure!(
            self.storage.min_free_mib <= 65536,
            "storage.min_free_mib must be <= 65536 MiB"
        );
        ensure!(
            self.cache.max_entries <= 16_000_000,
            "cache.max_entries must be <= 16000000"
        );
        ensure!(
            self.cache.max_object_mib <= 65536,
            "cache.max_object_mib too large"
        );
        ensure!(
            self.cache.shards.is_power_of_two() && (1..=64).contains(&self.cache.shards),
            "cache.shards must be a power of two in 1..=64"
        );
        ensure!(
            self.cache.admission_entries <= 1_000_000,
            "admission tracker too large"
        );
        if self.cache.payload_mib > 0 && self.cache.admission_entries == 0 {
            bail!("payload caching requires admission_entries > 0");
        }
        if let Some(addr) = &self.server.admin_listen {
            addr.parse::<SocketAddr>()
                .context("server.admin_listen must be a socket address")?;
        }
        ensure!(
            self.writes.batch_max_bytes_mib > 0 && self.writes.batch_max_bytes_mib <= 2047,
            "writes.batch_max_bytes_mib must be 1..=2047"
        );
        ensure!(
            (1..=4096).contains(&self.streaming.chunk_kib),
            "streaming.chunk_kib must be 1..=4096"
        );
        ensure!(
            self.streaming.max_object_mib <= 8192,
            "streaming.max_object_mib must be <= 8192 MiB"
        );
        let chunk_bytes = self
            .streaming
            .chunk_kib
            .checked_mul(1024)
            .context("streaming.chunk_kib overflows bytes")?;
        ensure!(
            chunk_bytes <= self.writes.max_pending_mib.saturating_mul(1024 * 1024),
            "streaming chunk must fit within the write byte budget"
        );
        ensure!(
            chunk_bytes
                <= self
                    .resources
                    .max_inflight_read_mib
                    .saturating_mul(1024 * 1024),
            "streaming chunk must fit within the in-flight read byte budget"
        );
        ensure!(
            !self.streaming.enabled
                || (self.streaming.max_object_mib > 0
                    && self.streaming.max_uploads > 0
                    && self.streaming.max_reads > 0
                    && self.streaming.max_uploads <= 65536
                    && self.streaming.max_reads <= 65536),
            "enabled streaming requires positive object, upload, and read limits"
        );
        ensure!(
            self.streaming.idle_timeout_seconds > 0 && self.streaming.idle_timeout_seconds <= 86400,
            "streaming.idle_timeout_seconds must be 1..86400"
        );
        ensure!(
            self.streaming.transfer_timeout_seconds > 0
                && self.streaming.transfer_timeout_seconds <= 86400,
            "streaming.transfer_timeout_seconds must be 1..86400"
        );
        ensure!(
            self.streaming.checksum_cache_mib <= 65536,
            "streaming.checksum_cache_mib must be <= 65536 MiB"
        );
        let max_bytes = (self.streaming.max_object_mib as u64)
            .checked_mul(1024 * 1024)
            .context("streaming maximum object size overflows bytes")?;
        let max_chunk_count = max_bytes.div_ceil(chunk_bytes as u64).max(1);
        let one_table_bytes = max_chunk_count
            .checked_mul(32)
            .and_then(|n| n.checked_add(512))
            .context("streaming checksum table size overflows")?;
        if self.streaming.checksum_cache_mib > 0 {
            let checksum_budget = (self.streaming.checksum_cache_mib as u64)
                .checked_mul(1024 * 1024)
                .context("streaming checksum cache size overflows")?;
            ensure!(
                one_table_bytes <= checksum_budget,
                "streaming.checksum_cache_mib must fit the checksum table for one maximum object"
            );
        } else {
            let inflight_checksum_bytes = one_table_bytes
                .checked_mul(self.streaming.max_reads as u64)
                .context("in-flight checksum metadata budget overflows")?;
            ensure!(
                inflight_checksum_bytes <= 64 * 1024 * 1024 * 1024,
                "in-flight checksum metadata budget exceeds 65536 MiB"
            );
        }
        ensure!(
            self.checkpoint.interval_seconds > 0
                && self.checkpoint.batch_records > 0
                && self.checkpoint.min_journal_mib > 0
                && self.checkpoint.max_catchup_mib > 0
                && self.checkpoint.max_tail_kib > 0,
            "checkpoint interval, journal, catch-up, tail and batch limits must be positive"
        );
        ensure!(
            self.checkpoint.max_tail_kib <= self.checkpoint.max_catchup_mib.saturating_mul(1024),
            "checkpoint.max_tail_kib must not exceed max_catchup_mib"
        );
        ensure!(
            self.server.shutdown_timeout_seconds > 0 && self.server.request_timeout_seconds > 0,
            "timeouts must be positive"
        );
        ensure!(
            (1..=16).contains(&self.verification.max_concurrency),
            "verification.max_concurrency must be 1..16"
        );
        ensure!(
            self.verification
                .max_bytes_per_second_mib
                .checked_mul(1024 * 1024)
                .is_some(),
            "verification bandwidth limit overflows bytes"
        );
        self.mcp.validate(self.max_object_bytes())?;
        self.gc.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn gc_defaults_and_validation() {
        let parsed: Config = toml::from_str("[storage]\nroot='data'").unwrap();
        assert!(!parsed.gc.enabled);
        assert_eq!(parsed.gc.interval_seconds, 1800);
        let mut c = Config::default();
        c.gc.min_garbage_ratio = f64::NAN;
        assert!(c.validate().is_err());
        c.gc.min_garbage_ratio = 0.5;
        c.gc.low_space_percent = c.gc.target_free_percent;
        assert!(c.validate().is_err());
        c.gc.low_space_percent = 20;
        c.gc.max_copy_mib = u64::MAX;
        assert!(c.validate().is_err());
    }
    #[test]
    fn config_roundtrip_and_limits() {
        let dir = tempfile::tempdir().unwrap();
        let c = Config::default();
        c.save(&dir.path().join("config.toml")).unwrap();
        let parsed = Config::load(&dir.path().join("config.toml")).unwrap();
        assert_eq!(parsed.root(), dir.path().join("./data"));
        let mut invalid = c;
        invalid.resources.max_inflight_read_mib = 1;
        assert!(invalid.validate().is_err());
        assert!(toml::from_str::<Config>("[reads]\npolciy='ssd'").is_err());
    }
    #[test]
    fn large_cache_entry_capacity_is_validated() {
        let mut config = Config::default();
        config.cache.max_entries = 1_000_001;
        assert!(config.validate().is_ok());
        config.cache.max_entries = 16_000_001;
        assert!(config.validate().is_err());
        config.cache.max_entries = 1_000_000;
        config.cache.shards = 3;
        assert!(config.validate().is_err());
    }
    #[test]
    fn streaming_chunk_and_checksum_budgets_are_coherent() {
        let mut config = Config::default();
        config.cache.payload_mib = 0;
        assert!(config.validate().is_ok());

        config.resources.max_object_mib = 1;
        config.resources.max_inflight_read_mib = 1;
        config.writes.max_pending_mib = 1;
        config.streaming.chunk_kib = 2048;
        assert!(
            config.validate().is_err(),
            "chunk must fit both byte budgets"
        );

        config.streaming.chunk_kib = 1;
        config.streaming.max_object_mib = 4096;
        config.streaming.checksum_cache_mib = 1;
        assert!(
            config.validate().is_err(),
            "checksum cache must hold one maximum object table"
        );

        config.streaming.checksum_cache_mib = 0;
        config.streaming.max_reads = 65536;
        assert!(
            config.validate().is_err(),
            "uncached in-flight table budget must remain bounded"
        );
    }
    #[test]
    fn checkpoint_tail_must_fit_catchup_budget() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());
        config.checkpoint.max_catchup_mib = 1;
        config.checkpoint.max_tail_kib = 1025;
        assert!(config.validate().is_err());
    }
    #[test]
    fn verification_limits_default_and_validate() {
        let mut config = Config::default();
        assert_eq!(config.verification.max_concurrency, 1);
        assert_eq!(config.verification.max_bytes_per_second_mib, 0);
        config.verification.max_concurrency = 0;
        assert!(config.validate().is_err());
        config.verification.max_concurrency = 17;
        assert!(config.validate().is_err());
        config.verification.max_concurrency = 2;
        config.verification.max_bytes_per_second_mib = u64::MAX;
        assert!(config.validate().is_err());
    }
    #[test]
    fn writer_lane_counts_are_explicit_and_bounded() {
        let mut config = Config::default();
        assert_eq!(config.writes.writer_lanes, 1);
        for lanes in [1, 2, 4, 8] {
            config.writes.writer_lanes = lanes;
            config.validate().unwrap();
        }
        for lanes in [0, 3, 16, usize::MAX] {
            config.writes.writer_lanes = lanes;
            assert!(config.validate().is_err());
        }
    }
    #[test]
    fn multi_writer_commit_window_is_bounded() {
        let mut config = Config::default();
        assert_eq!(config.writes.commit_max_delay_ms, 10);
        config.writes.writer_lanes = 4;
        for delay in [0, 10, 1000] {
            config.writes.commit_max_delay_ms = delay;
            config.validate().unwrap();
        }
        config.writes.commit_max_delay_ms = 1001;
        assert!(config.validate().is_err());
    }
}
