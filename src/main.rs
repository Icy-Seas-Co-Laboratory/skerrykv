use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use skerrykv::{config::Config, engine::Store};
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{filter::filter_fn, prelude::*};

#[derive(Parser)]
#[command(
    name = "skerry",
    version,
    about = "Durable content-addressed object storage"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a configuration and initialize a storage directory.
    Init {
        #[arg(long, default_value = ".")]
        path: PathBuf,
    },
    /// Run the SkerryKV HTTP service.
    Serve {
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
        #[arg(long)]
        tui: bool,
    },
    /// Attach the terminal dashboard to a running service.
    Tui {
        #[arg(
            long = "connect",
            alias = "url",
            default_value = "http://127.0.0.1:7788"
        )]
        connect: String,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Probe service health; exits unsuccessfully when the service is unhealthy.
    Health {
        #[arg(
            long = "connect",
            alias = "url",
            default_value = "http://127.0.0.1:7788"
        )]
        connect: String,
    },
    /// Probe whether storage is ready to serve requests.
    Ready {
        #[arg(
            long = "connect",
            alias = "url",
            default_value = "http://127.0.0.1:7788"
        )]
        connect: String,
    },
    /// List persisted index generations.
    Indexes {
        #[arg(
            long = "connect",
            alias = "url",
            default_value = "http://127.0.0.1:7788"
        )]
        connect: String,
    },
    /// Show the health and measured counters of a running service.
    Status {
        #[arg(
            long = "connect",
            alias = "url",
            default_value = "http://127.0.0.1:7788"
        )]
        connect: String,
        #[arg(long)]
        json: bool,
    },
    /// Request a graceful shutdown of a running service.
    Stop {
        #[arg(
            long = "connect",
            alias = "url",
            default_value = "http://127.0.0.1:7788"
        )]
        connect: String,
    },
    /// Delete an object using the service administrator credential.
    Delete {
        hash: String,
        #[arg(
            long = "connect",
            alias = "url",
            default_value = "http://127.0.0.1:7788"
        )]
        connect: String,
    },
    /// Run one online garbage collection pass with the service administrator credential.
    Gc {
        #[arg(long)]
        force: bool,
        #[arg(
            long = "connect",
            alias = "url",
            default_value = "http://127.0.0.1:7788"
        )]
        connect: String,
    },
    /// Upgrade an offline store to the current mutation format.
    Upgrade {
        #[arg(long, default_value = "./data")]
        path: PathBuf,
    },
    /// Rewrite an offline store to reclaim obsolete data.
    Vacuum {
        #[arg(long, default_value = "./data")]
        path: PathBuf,
        #[arg(long)]
        dry_run: bool,
    },
    /// Verify segment integrity offline.
    Verify {
        #[arg(long, default_value = "./data")]
        path: PathBuf,
    },
    /// Rebuild the hash index from stored segments.
    RebuildIndex {
        #[arg(long, default_value = "./data")]
        path: PathBuf,
        #[arg(long)]
        salvage: bool,
    },
    /// Import supported segment data into this store.
    ImportSegments {
        #[arg(long)]
        source: PathBuf,
        #[arg(long, default_value = "./data")]
        path: PathBuf,
    },
    /// Run an in-process storage workload benchmark.
    Bench {
        #[arg(long, default_value = "./skerry-bench-data")]
        path: PathBuf,
        #[arg(long, default_value_t = 16)]
        objects: usize,
        #[arg(long, default_value_t = 20 * 1024)]
        bytes: usize,
        #[arg(long, default_value_t = 4)]
        workers: usize,
        #[arg(long)]
        mixed: bool,
        #[arg(long)]
        suite: bool,
    },
    /// Show live cache counters.
    Cache {
        #[command(subcommand)]
        command: CacheCommand,
        #[arg(
            long = "connect",
            alias = "url",
            default_value = "http://127.0.0.1:7788"
        )]
        connect: String,
    },
    /// Connect MCP clients to a running SkerryKV service.
    Mcp {
        #[command(subcommand)]
        command: McpCommand,
    },
}
#[derive(Subcommand)]
enum McpCommand {
    /// Bridge MCP stdio to the service's Streamable HTTP MCP endpoint.
    Stdio {
        #[arg(long = "connect", default_value = "http://127.0.0.1:7788")]
        connect: String,
        #[arg(long = "http-path", default_value = "/mcp")]
        http_path: String,
    },
    /// Show measured MCP service counters.
    Status {
        #[arg(long = "connect", default_value = "http://127.0.0.1:7788")]
        connect: String,
    },
}
#[derive(Subcommand)]
enum CacheCommand {
    Stats,
    Clear,
}

fn safe_diagnostic(metadata: &tracing::Metadata<'_>) -> bool {
    ![
        "rmcp",
        "reqwest",
        "hyper",
        "h2",
        "tower_http",
        "fjall",
        "lsm_tree",
        "lsm-tree",
    ]
    .iter()
    .any(|prefix| metadata.target().starts_with(prefix))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let (configured_level, log_file) = logging_setup(&cli.command)?;
    let filter = if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    } else {
        tracing_subscriber::EnvFilter::try_new(configured_level)
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };
    // Payload-bearing dependency logs are forbidden even when a more specific
    // RUST_LOG directive asks for them. This layer cannot be overridden by it.
    match log_file {
        Some(writer) => tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(false)
                    .with_ansi(false)
                    .with_writer(writer)
                    .with_filter(filter_fn(safe_diagnostic)),
            )
            .init(),
        None if matches!(
            &cli.command,
            Command::Mcp {
                command: McpCommand::Stdio { .. }
            }
        ) =>
        {
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_target(false)
                        .with_writer(io::stderr)
                        .with_filter(filter_fn(safe_diagnostic)),
                )
                .init()
        }
        None => tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(false)
                    .with_filter(filter_fn(safe_diagnostic)),
            )
            .init(),
    }
    match cli.command {
        Command::Init { path } => init(path).await?,
        Command::Serve { config, tui } => serve(config, tui).await?,
        Command::Tui { connect, config } => {
            skerrykv::tui::run_with_config(&connect, token(), None, config.as_deref()).await?
        }
        Command::Health { connect } => {
            let v = request_json(&connect, "/v1/health", reqwest::Method::GET).await?;
            output_json(v, false);
        }
        Command::Ready { connect } => {
            let v = request_json(&connect, "/v1/ready", reqwest::Method::GET).await?;
            output_json(v, false);
        }
        Command::Indexes { connect } => {
            let v = request_json(&connect, "/v1/indexes", reqwest::Method::GET).await?;
            output_json(v, true);
        }
        Command::Status { connect, json } => {
            let v = request_json(&connect, "/v1/status", reqwest::Method::GET).await?;
            output_json(v, json);
        }
        Command::Stop { connect } => {
            let v = request_json(&connect, "/v1/admin/shutdown", reqwest::Method::POST).await?;
            println!("{}", v);
        }
        Command::Delete { hash, connect } => {
            let mut digest = [0u8; 32];
            if hash.len() != 64 || hex::decode_to_slice(&hash, &mut digest).is_err() {
                bail!("expected a 64-character BLAKE3 hex digest");
            }
            request_json(
                &connect,
                &format!("/v1/objects/{}", hex::encode(digest)),
                reqwest::Method::DELETE,
            )
            .await?;
            println!("Delete acknowledged");
        }
        Command::Gc { force, connect } => {
            let path = if force {
                "/v1/admin/gc?force=true"
            } else {
                "/v1/admin/gc"
            };
            println!(
                "{}",
                request_json_with_timeout(
                    &connect,
                    path,
                    reqwest::Method::POST,
                    token(),
                    Duration::from_secs(3600)
                )
                .await?
            );
        }
        Command::Upgrade { path } => {
            let cfg = offline_config(&path)?;
            println!(
                "{}",
                tokio::task::spawn_blocking(move || Store::upgrade(&cfg)).await??
            );
        }
        Command::Vacuum { path, dry_run } => {
            let cfg = offline_config(&path)?;
            println!(
                "{}",
                tokio::task::spawn_blocking(move || Store::vacuum(&cfg, dry_run)).await??
            );
        }
        Command::Verify { path } => {
            let cfg = offline_config(&path)?;
            let progress = Arc::new(skerrykv::progress::Progress::new());
            let work_progress = progress.clone();
            let mut task = tokio::task::spawn_blocking(move || {
                Store::verify_with_progress(&cfg, &work_progress)
            });
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let result = loop {
                tokio::select! {
                    result = &mut task => break result?,
                    _ = shutdown_signal() => { progress.cancel(); break task.await?; },
                    _ = interval.tick() => eprintln!("verify progress: {}", serde_json::to_string(&progress.snapshot())?),
                }
            };
            eprintln!(
                "verify progress: {}",
                serde_json::to_string(&progress.snapshot())?
            );
            println!("{}", result?);
        }
        Command::RebuildIndex { path, salvage } => {
            let cfg = offline_config(&path)?;
            println!(
                "{}",
                tokio::task::spawn_blocking(move || Store::rebuild(&cfg, salvage)).await??
            );
        }
        Command::ImportSegments { source, path } => {
            let cfg = offline_config(&path)?;
            println!(
                "{}",
                tokio::task::spawn_blocking(move || Store::import(&cfg, &source)).await??
            );
        }
        Command::Bench {
            path,
            objects,
            bytes,
            workers,
            mixed,
            suite,
        } => {
            let cfg = offline_config(&path)?;
            if suite {
                skerrykv::bench::suite(&cfg, objects, workers).await?;
            } else {
                skerrykv::bench::run(&cfg, objects, bytes, workers, mixed).await?;
            }
        }
        Command::Cache { command, connect } => match command {
            CacheCommand::Stats => println!(
                "{}",
                request_json(&connect, "/v1/cache/stats", reqwest::Method::GET).await?
            ),
            CacheCommand::Clear => println!(
                "{}",
                request_json(&connect, "/v1/admin/cache-clear", reqwest::Method::POST).await?
            ),
        },
        Command::Mcp {
            command: McpCommand::Stdio { connect, http_path },
        } => {
            skerrykv::mcp_stdio::run(&connect, &http_path).await?;
        }
        Command::Mcp {
            command: McpCommand::Status { connect },
        } => {
            let v = request_json_with_token(
                &connect,
                "/v1/mcp/status",
                reqwest::Method::GET,
                mcp_token(),
            )
            .await?;
            output_json(v, false);
        }
    }
    Ok(())
}

fn logging_setup(command: &Command) -> Result<(String, Option<SharedLogWriter>)> {
    let Command::Serve { config: path, tui } = command else {
        return Ok(("info".to_owned(), None));
    };
    let config = if path.exists() {
        Config::load(path)?
    } else {
        let mut config = Config::default();
        config.apply_env()?;
        if std::env::var_os("SKERRY_ROOT").is_none() {
            config.storage.root = absolute_parent(path)?.join("data");
        } else if config.storage.root.is_relative() {
            config.storage.root = std::env::current_dir()?.join(&config.storage.root);
        }
        config.validate()?;
        config
    };
    let level = config.logging.level.clone();
    if !tui {
        return Ok((level, None));
    }
    let directory = config.storage.root.join("logs");
    std::fs::create_dir_all(&directory)
        .with_context(|| format!("creating log directory {}", directory.display()))?;
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.join("skerry.log"))
        .with_context(|| format!("opening log file in {}", directory.display()))?;
    Ok((level, Some(SharedLogWriter(Arc::new(Mutex::new(file))))))
}

#[derive(Clone)]
struct SharedLogWriter(Arc<Mutex<File>>);
struct LogWriter(Arc<Mutex<File>>);
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedLogWriter {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(Arc::clone(&self.0))
    }
}
impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .flush()
    }
}

async fn init(path: PathBuf) -> Result<()> {
    std::fs::create_dir_all(&path)?;
    let directory =
        std::fs::canonicalize(&path).with_context(|| format!("resolving {}", path.display()))?;
    let config_path = directory.join("config.toml");
    if config_path.exists() {
        bail!(
            "{} already exists; refusing to overwrite it",
            config_path.display()
        );
    }
    let mut config = Config::default();
    config.storage.root = PathBuf::from(".");
    let mut runtime_config = config.clone();
    runtime_config.storage.root = directory;
    let store = tokio::task::spawn_blocking(move || Store::open(&runtime_config)).await??;
    config.save(&config_path)?;
    drop(store);
    println!("Initialized {}", config_path.display());
    Ok(())
}

async fn serve(path: PathBuf, show_tui: bool) -> Result<()> {
    let cfg = if path.exists() {
        Config::load(&path)?
    } else {
        let mut cfg = Config::default();
        cfg.apply_env()?;
        let config_dir = absolute_parent(&path)?;
        if std::env::var_os("SKERRY_ROOT").is_none() {
            cfg.storage.root = config_dir.join("data");
        } else if cfg.storage.root.is_relative() {
            cfg.storage.root = std::env::current_dir()?.join(&cfg.storage.root);
        }
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        cfg.save(&path)?;
        cfg
    };
    cfg.validate()?;
    if needs_token(&cfg.server.listen) && cfg.token().is_none() {
        bail!("SKERRY_TOKEN must be set when listening on a non-loopback address");
    }
    let credentials = skerrykv::mcp::Credentials::from_env();
    skerrykv::server::validate_bootstrap(&cfg, &credentials)?;
    let listen = cfg.server.listen.clone();
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .context("cannot bind HTTP listener")?;
    let progress = Arc::new(skerrykv::progress::Progress::new());
    let cancel = CancellationToken::new();
    let server_cancel = cancel.clone();
    let mut server = tokio::spawn(async move {
        skerrykv::server::serve_bootstrap(cfg, credentials, progress, server_cancel, listener).await
    });
    eprintln!("SkerryKV listening at http://{listen}");
    if show_tui {
        let url = local_url(&listen);
        tokio::select! {
            result = skerrykv::tui::run(&url, token(), Some(cancel.clone())) => {
                if let Err(err) = result { eprintln!("Dashboard closed: {err:#}"); }
            }
            _ = shutdown_signal() => {
                cancel.cancel();
                server.await.context("server task failed")??;
                return Ok(());
            }
            result = &mut server => {
                result.context("server task failed")??;
                return Ok(());
            }
        }
    }
    tokio::select! {
        result = &mut server => result.context("server task failed")??,
        _ = shutdown_signal() => { cancel.cancel(); server.await.context("server task failed")??; }
    }
    Ok(())
}
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("register SIGTERM")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}
fn absolute_parent(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if parent.is_absolute() {
        Ok(parent.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(parent))
    }
}
fn token() -> Option<String> {
    std::env::var("SKERRY_TOKEN").ok()
}
fn local_url(listen: &str) -> String {
    let (host, port) = listen.rsplit_once(':').unwrap_or((listen, "7788"));
    let host = host.trim_matches(['[', ']']);
    let local = host
        .parse::<IpAddr>()
        .ok()
        .filter(|ip| ip.is_unspecified())
        .map(|ip| if ip.is_ipv4() { "127.0.0.1" } else { "::1" })
        .unwrap_or(host);
    if local.contains(':') {
        format!("http://[{local}]:{port}")
    } else {
        format!("http://{local}:{port}")
    }
}
fn needs_token(listen: &str) -> bool {
    let host = listen
        .rsplit_once(':')
        .map(|(host, _)| host.trim_matches(['[', ']']))
        .unwrap_or(listen);
    host.parse::<IpAddr>()
        .map(|ip| !ip.is_loopback())
        .unwrap_or_else(|_| !matches!(host, "localhost" | "127.0.0.1" | "::1"))
}
fn offline_config(path: &Path) -> Result<Config> {
    let config_path = path.join("config.toml");
    if path.is_dir() && config_path.is_file() {
        let mut config = Config::load(&config_path)?;
        config.storage.root = std::fs::canonicalize(path)?;
        config.validate()?;
        return Ok(config);
    }
    if path.extension().is_some_and(|e| e == "toml") && path.is_file() {
        return Config::load(path);
    }
    let mut config = Config::default();
    config.storage.root = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(config)
}
async fn request_json(url: &str, path: &str, method: reqwest::Method) -> Result<serde_json::Value> {
    request_json_with_token(url, path, method, token()).await
}
async fn request_json_with_token(
    url: &str,
    path: &str,
    method: reqwest::Method,
    auth_token: Option<String>,
) -> Result<serde_json::Value> {
    request_json_with_timeout(url, path, method, auth_token, Duration::from_secs(10)).await
}
async fn request_json_with_timeout(
    url: &str,
    path: &str,
    method: reqwest::Method,
    auth_token: Option<String>,
    timeout: Duration,
) -> Result<serde_json::Value> {
    let client = reqwest::Client::builder().timeout(timeout).build()?;
    let mut req = client.request(method, format!("{}{}", url.trim_end_matches('/'), path));
    if let Some(t) = auth_token {
        req = req.bearer_auth(t);
    }
    let response = req.send().await.context("connecting to SkerryKV")?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        bail!("service returned {status}: {body}");
    }
    serde_json::from_str(&body).or_else(|_| Ok(serde_json::json!({"message":body})))
}
fn mcp_token() -> Option<String> {
    std::env::var("SKERRY_MCP_TOKEN").ok().or_else(token)
}
fn output_json(value: serde_json::Value, json: bool) {
    if json {
        println!("{}", value);
    } else if let Some(o) = value.as_object() {
        for (k, v) in o {
            println!("{k}: {v}");
        }
    } else {
        println!("{value}");
    }
}

#[cfg(test)]
mod maintenance_cli_tests {
    use super::*;

    #[test]
    fn parses_maintenance_commands() {
        assert!(matches!(
            Cli::try_parse_from(["skerry", "delete", "abcd"])
                .unwrap()
                .command,
            Command::Delete { .. }
        ));
        assert!(matches!(
            Cli::try_parse_from(["skerry", "gc", "--force"])
                .unwrap()
                .command,
            Command::Gc { force: true, .. }
        ));
        assert!(matches!(
            Cli::try_parse_from(["skerry", "upgrade", "--path", "data"])
                .unwrap()
                .command,
            Command::Upgrade { .. }
        ));
        assert!(matches!(
            Cli::try_parse_from(["skerry", "vacuum", "--dry-run"])
                .unwrap()
                .command,
            Command::Vacuum { dry_run: true, .. }
        ));
    }
}
