use anyhow::{Context, Result};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Wrap},
};
use serde_json::Value;
use std::{
    io,
    net::IpAddr,
    path::Path,
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::process::Child;
use tokio_util::sync::CancellationToken;

struct TerminalGuard;
impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        if let Err(error) = execute!(io::stdout(), EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        Ok(Self)
    }
}
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

/// Attach a dashboard. Press `q` to detach; a launched service stays running.
pub async fn run(
    url: &str,
    token: Option<String>,
    shutdown: Option<CancellationToken>,
) -> Result<()> {
    run_with_config(url, token, shutdown, None).await
}

/// Attach a dashboard and optionally offer to launch a service from a config file.
pub async fn run_with_config(
    url: &str,
    token: Option<String>,
    shutdown: Option<CancellationToken>,
    config: Option<&Path>,
) -> Result<()> {
    let _guard = TerminalGuard::enter().context("entering terminal dashboard")?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let mut endpoint = url.to_owned();
    let mut snapshot: Option<Value> = None;
    let mut rates = None;
    let mut mcp_rate = None;
    let mut previous_mcp: Option<(Instant, u64)> = None;
    let mut previous: Option<(Instant, u64, u64, u64, u64)> = None;
    let mut error: Option<String> = None;
    let mut tab = 0usize;
    let mut help = false;
    let mut confirm_stop = false;
    let mut child: Option<Child> = None;
    let mut launch_error: Option<String> = None;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = async { if let Some(token) = &shutdown { token.cancelled().await } else { std::future::pending::<()>().await } } => break,
            _ = tick.tick() => {
                if let Some(process)=child.as_mut() && let Some(status)=process.try_wait()? {
                    launch_error=Some(format!("Launched service exited: {status}; check configuration and logs"));child=None;
                }
                match api(&client, &endpoint, token.as_deref(), "/v1/status", reqwest::Method::GET).await {
                    Ok(v) => { update_rates(&v, &mut previous, &mut rates); update_mcp_rate(&v, &mut previous_mcp, &mut mcp_rate); snapshot = Some(v); error = None;launch_error=None; }
                    Err(e) => error = Some(launch_error.clone().unwrap_or_else(||format!("{e:#}"))),
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(40)) => {
                if event::poll(Duration::ZERO)? && let Event::Key(k) = event::read()? && k.kind != KeyEventKind::Release {
                    if k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL) {
                        confirm_stop = true;
                    } else {
                        match k.code {
                            KeyCode::Char('q') if confirm_stop => confirm_stop = false,
                            KeyCode::Char('q') => break,
                            KeyCode::Char('?') => help = !help,
                            KeyCode::Tab | KeyCode::Right => { tab = (tab + 1) % 6; confirm_stop = false; }
                            KeyCode::BackTab | KeyCode::Left => { tab = (tab + 5) % 6; confirm_stop = false; }
                            KeyCode::Char('r') => match api(&client, &endpoint, token.as_deref(), "/v1/status", reqwest::Method::GET).await {
                                Ok(v) => { update_rates(&v, &mut previous, &mut rates); update_mcp_rate(&v, &mut previous_mcp, &mut mcp_rate); snapshot = Some(v); error = None; }
                                Err(e) => error = Some(format!("{e:#}")),
                            },
                            KeyCode::Char('c') => match api(&client, &endpoint, token.as_deref(), "/v1/admin/cache-clear", reqwest::Method::POST).await {
                                Ok(_) => error = Some("Cache clear requested".into()),
                                Err(e) => error = Some(format!("{e:#}")),
                            },
                            KeyCode::Char('l') if snapshot.is_none() && child.is_none() => {
                                if !is_loopback_url(url) {
                                    error = Some("Service launch is available only for a loopback connection".into());
                                } else if let Some(path) = config {
                                    if endpoint == "http://127.0.0.1:7788" {
                                        match crate::config::Config::load(path) {
                                            Ok(cfg) => endpoint = local_url(&cfg.server.listen),
                                            Err(e) => { error = Some(format!("could not load launch config: {e:#}")); continue; }
                                        }
                                    }
                                    match std::env::current_exe() {
                                        Ok(exe) => match tokio::process::Command::new(exe).arg("serve").arg("--config").arg(path).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).spawn() {
                                            Ok(proc) => { child = Some(proc);launch_error=None; error = Some("Started service from config; waiting for status…".into()); }
                                            Err(e) => error = Some(format!("could not launch service: {e}")),
                                        },
                                        Err(e) => error = Some(format!("could not locate skerry executable: {e}")),
                                    }
                                } else { error = Some("Pass --config <path> to launch a service from the dashboard".into()); }
                            }
                            KeyCode::Char('s') => confirm_stop = true,
                            KeyCode::Enter if confirm_stop => {
                                match api(&client, &endpoint, token.as_deref(), "/v1/admin/shutdown", reqwest::Method::POST).await {
                                    Ok(_) => {
                                        if let Some(mut proc) = child.take() { let _ = tokio::time::timeout(Duration::from_secs(35), proc.wait()).await; }
                                        break;
                                    }
                                    Err(e) => error = Some(format!("{e:#}")),
                                }
                                confirm_stop = false;
                            }
                            KeyCode::Esc => confirm_stop = false,
                            _ => {}
                        }
                    }
                }
            }
        }
        terminal.draw(|f| {
            draw(
                f,
                UiState {
                    tab,
                    snapshot: snapshot.as_ref(),
                    rates,
                    mcp_rate,
                    error: error.as_deref(),
                    help,
                    confirm: confirm_stop,
                    can_launch: config.is_some(),
                    managed: child.is_some(),
                },
            )
        })?;
    }
    Ok(())
}

async fn api(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
    path: &str,
    method: reqwest::Method,
) -> Result<Value> {
    let mut req = client.request(method, format!("{}{}", url.trim_end_matches('/'), path));
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let res = req.send().await.context("connecting to service")?;
    let status = res.status();
    let body = res.text().await?;
    anyhow::ensure!(status.is_success(), "service returned {status}: {body}");
    Ok(serde_json::from_str(&body).unwrap_or_else(|_| serde_json::json!({"message":body})))
}
struct UiState<'a> {
    tab: usize,
    snapshot: Option<&'a Value>,
    rates: Option<(f64, f64, f64, f64)>,
    mcp_rate: Option<f64>,
    error: Option<&'a str>,
    help: bool,
    confirm: bool,
    can_launch: bool,
    managed: bool,
}
fn draw(f: &mut ratatui::Frame<'_>, state: UiState<'_>) {
    let UiState {
        tab,
        snapshot,
        rates,
        mcp_rate,
        error,
        help,
        confirm,
        can_launch,
        managed,
    } = state;
    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(2),
        ])
        .split(area);
    let titles = ["Overview", "Storage", "I/O", "Cache", "Logs", "MCP"]
        .iter()
        .map(|t| Line::from(Span::styled(*t, Style::default().fg(Color::Cyan))))
        .collect::<Vec<_>>();
    f.render_widget(
        Tabs::new(titles)
            .select(tab)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("SkerryKV Dashboard"),
            )
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        chunks[0],
    );
    let body = if help {
        format!(
            "? help   Tab/←/→ switch tabs (Overview, Storage, I/O, Cache, Logs, MCP)   r refresh   c clear cache   s stop service   q detach\n{}\nStop requires Enter confirmation. A dashboard launched service is tracked; q detaches and leaves it running.",
            if can_launch {
                "l launch service from config when offline"
            } else {
                "Pass --config <path> to enable service launch"
            }
        )
    } else if confirm {
        format!(
            "Stop the service? Press Enter to confirm, Esc or q to cancel.{}",
            if managed {
                " This dashboard launched the process."
            } else {
                ""
            }
        )
    } else {
        render_tab(tab, snapshot, rates, mcp_rate)
    };
    f.render_widget(
        Paragraph::new(body)
            .block(Block::default().borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        chunks[1],
    );
    let footer = error.unwrap_or("? help  Tab switch  r refresh  c clear cache  s stop  q detach");
    f.render_widget(
        Paragraph::new(footer).style(Style::default().fg(if error.is_some() {
            Color::Yellow
        } else {
            Color::DarkGray
        })),
        chunks[2],
    );
}
fn render_tab(
    tab: usize,
    v: Option<&Value>,
    rates: Option<(f64, f64, f64, f64)>,
    mcp_rate: Option<f64>,
) -> String {
    let Some(v) = v else {
        return "Waiting for service status…".into();
    };
    let storage = &v["storage"];
    let io = &v["io"];
    let cache = &v["cache"];
    let field = |value: &Value, key: &str| {
        if value[key].is_null() {
            "—".to_owned()
        } else {
            value[key]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value[key].to_string())
        }
    };
    match tab {
        0 => format!(
            "Health: {}\nVersion: {}\nUptime: {} s\nListen: {}\nPolicy: {}\nRead workers: {}\nConfigured budget: {} MiB\nObjects: {}\nPayload bytes: {}\nPhysical bytes: {}",
            field(v, "health"),
            field(v, "version"),
            field(v, "uptime_seconds"),
            field(v, "listen"),
            field(v, "policy"),
            field(v, "read_workers"),
            field(v, "configured_budget_mib"),
            field(storage, "objects"),
            field(storage, "payload_bytes"),
            field(storage, "physical_bytes")
        ),
        1 => format!(
            "Segments: {}\nActive segment: {}\nActive bytes: {}\nIndex bytes: {}\nOpen segment handles: {}\nActive utilization: {}\nWrite queue bytes/capacity: {}/{}\nSession deduplication bytes saved: {}",
            field(storage, "segments"),
            field(storage, "active_segment"),
            field(storage, "active_bytes"),
            field(storage, "index_bytes"),
            field(storage, "open_files"),
            ratio(
                storage["active_bytes"].as_u64(),
                v["segment_capacity_bytes"].as_u64()
            ),
            field(v, "write_queue_bytes"),
            field(v, "write_queue_capacity_bytes"),
            field(io, "deduplicated_bytes")
        ),
        2 => format!(
            "Reads: {}   Writes: {}\nRead bytes: {}   Write bytes: {}\nCurrent rate: {} reads/s, {} writes/s\nByte rate: {} read B/s, {} write B/s\nDeduplicated writes: {}   Coalesced reads: {}\nRead/write queue: {}/{}   Inflight reads: {}\nLatency p50/p95/p99: {}/{}/{} ms",
            field(io, "reads"),
            field(io, "writes"),
            field(io, "read_bytes"),
            field(io, "write_bytes"),
            rate(rates, 0),
            rate(rates, 1),
            rate(rates, 2),
            rate(rates, 3),
            field(io, "deduplicated"),
            field(io, "read_coalesced"),
            field(io, "read_queue"),
            field(io, "write_queue"),
            field(io, "inflight_reads"),
            field(io, "latency_p50_ms"),
            field(io, "latency_p95_ms"),
            field(io, "latency_p99_ms")
        ),
        3 => format!(
            "Entries: {}\nBytes: {}\nRetained bytes: {}\nHits: {}\nMisses: {}\nAdmissions: {}\nEvictions: {}\nHit rate: {}\nBytes served from verified cache: {}",
            field(cache, "entries"),
            field(cache, "bytes"),
            field(cache, "retained_bytes"),
            field(cache, "hits"),
            field(cache, "misses"),
            field(cache, "admissions"),
            field(cache, "evictions"),
            ratio(
                cache["hits"].as_u64(),
                cache["hits"]
                    .as_u64()
                    .zip(cache["misses"].as_u64())
                    .map(|(h, m)| h + m)
            ),
            field(cache, "served_bytes")
        ),
        4 => {
            let lines = |key: &str| {
                v[key]
                    .as_array()
                    .map(|items| {
                        items
                            .iter()
                            .rev()
                            .take(16)
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default()
            };
            format!(
                "Recent errors (newest first):\n{}\n\nRecent events (newest first):\n{}",
                lines("errors"),
                lines("events")
            )
        }
        _ => {
            let mcp = &v["mcp"];
            format!(
                "Enabled: {}\nRead only: {}\nActive MCP HTTP exchanges: {}\nTool calls: {} ({}/s)\nFailed tool calls: {}\nAuthorization failures: {}\nAverage tool latency: {} ms\nInflight requests: {}\nVerification jobs: {}",
                field(mcp, "enabled"),
                field(mcp, "read_only"),
                field(mcp, "active_connections"),
                field(mcp, "tool_calls_total"),
                mcp_rate
                    .map(|r| format!("{r:.1}"))
                    .unwrap_or_else(|| "—".into()),
                field(mcp, "failed_tool_calls"),
                field(mcp, "authorization_failures"),
                field(mcp, "average_tool_latency_ms"),
                field(mcp, "inflight_requests"),
                field(mcp, "verification_jobs")
            )
        }
    }
}

fn ratio(numerator: Option<u64>, denominator: Option<u64>) -> String {
    match numerator.zip(denominator).filter(|(_, d)| *d > 0) {
        Some((n, d)) => format!("{:.1}%", n as f64 / d as f64 * 100.),
        None => "—".into(),
    }
}

fn update_rates(
    v: &Value,
    previous: &mut Option<(Instant, u64, u64, u64, u64)>,
    rates: &mut Option<(f64, f64, f64, f64)>,
) {
    let io = &v["io"];
    let current = (
        Instant::now(),
        io["reads"].as_u64().unwrap_or(0),
        io["writes"].as_u64().unwrap_or(0),
        io["read_bytes"].as_u64().unwrap_or(0),
        io["write_bytes"].as_u64().unwrap_or(0),
    );
    if let Some(prev) = previous {
        let seconds = current.0.duration_since(prev.0).as_secs_f64();
        if seconds > 0.0 {
            *rates = Some((
                (current.1.saturating_sub(prev.1)) as f64 / seconds,
                (current.2.saturating_sub(prev.2)) as f64 / seconds,
                (current.3.saturating_sub(prev.3)) as f64 / seconds,
                (current.4.saturating_sub(prev.4)) as f64 / seconds,
            ));
        }
    }
    *previous = Some(current);
}
fn update_mcp_rate(v: &Value, previous: &mut Option<(Instant, u64)>, rate: &mut Option<f64>) {
    let current = (
        Instant::now(),
        v["mcp"]["tool_calls_total"].as_u64().unwrap_or(0),
    );
    if let Some(prev) = previous {
        let seconds = current.0.duration_since(prev.0).as_secs_f64();
        if seconds > 0.0 {
            *rate = Some(current.1.saturating_sub(prev.1) as f64 / seconds);
        }
    }
    *previous = Some(current);
}
fn rate(rates: Option<(f64, f64, f64, f64)>, index: usize) -> String {
    rates
        .map(|r| format!("{:.1}", [r.0, r.1, r.2, r.3][index]))
        .unwrap_or_else(|| "—".into())
}
fn is_loopback_url(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    parsed.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    })
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
