//! Versioned HTTP API and bounded streaming batch transport.
use std::{
    convert::Infallible,
    future::IntoFuture,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use bytes::{Buf, BytesMut};
use futures_util::{
    FutureExt, StreamExt,
    stream::{FuturesOrdered, FuturesUnordered},
};
use serde::Deserialize;
use subtle::ConstantTimeEq;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::coordinator::{Coordinator, Priority, ResourceExhausted};

pub const BATCH_MEDIA_TYPE: &str = "application/vnd.skerry.batch-v1";
pub const FRAME_HEADER_LEN: usize = 56;
const REQUEST_PUT: u8 = 1;
const REQUEST_GET: u8 = 2;
const RESULT_PUT: u8 = 129;
const RESULT_GET: u8 = 130;
const TERMINAL_ERROR: u8 = 255;
const IO_TIMEOUT: Duration = Duration::from_secs(30);

type ApiResult = std::result::Result<Response, ApiError>;

#[derive(Default)]
struct AdmissionStats {
    accepted: std::sync::atomic::AtomicU64,
    timed_out: std::sync::atomic::AtomicU64,
    wait_us: std::sync::atomic::AtomicU64,
    upload_accepted: std::sync::atomic::AtomicU64,
    upload_timeouts: std::sync::atomic::AtomicU64,
    upload_wait_us: std::sync::atomic::AtomicU64,
}

// Routing hints never cache existence or payloads. The fixed 4096-entry
// bound adds modest memory overhead and avoids repeated metadata lookups.
type HintShard = (
    std::collections::HashSet<[u8; 32]>,
    std::collections::VecDeque<[u8; 32]>,
);
struct SmallRoutingHints {
    shards: Vec<parking_lot::Mutex<HintShard>>,
}
impl SmallRoutingHints {
    fn new() -> Self {
        Self {
            shards: (0..16)
                .map(|_| {
                    parking_lot::Mutex::new((
                        std::collections::HashSet::new(),
                        std::collections::VecDeque::new(),
                    ))
                })
                .collect(),
        }
    }
    fn shard(&self, hash: &[u8; 32]) -> usize {
        usize::from(hash[0]) % self.shards.len()
    }
    fn contains(&self, hash: &[u8; 32]) -> bool {
        self.shards[self.shard(hash)].lock().0.contains(hash)
    }
    fn insert(&self, hash: [u8; 32]) {
        let mut shard = self.shards[self.shard(&hash)].lock();
        if shard.0.contains(&hash) {
            return;
        }
        if shard.1.len() == 256
            && let Some(old) = shard.1.pop_front()
        {
            shard.0.remove(&old);
        }
        shard.0.insert(hash);
        shard.1.push_back(hash);
    }
}
fn buffered_object(config: &crate::config::Config, length: u64) -> bool {
    length <= config.max_object_bytes() as u64
        && length <= (config.streaming.threshold_mib as u64) * 1024 * 1024
}

#[derive(Clone)]
struct App {
    store: Arc<Coordinator>,
    shutdown: CancellationToken,
    requests: Arc<Semaphore>,
    admin_requests: Arc<Semaphore>,
    admission_stats: Arc<AdmissionStats>,
    snapshot_error: Arc<std::sync::atomic::AtomicBool>,
    snapshot:
        Arc<parking_lot::Mutex<Option<(crate::coordinator::Status, std::time::Instant, u64)>>>,
    upload_bytes: Arc<Semaphore>,
    stream_uploads: Arc<Semaphore>,
    stream_reads: Arc<Semaphore>,
    metadata_slots: Arc<Semaphore>,
    small_hints: Arc<SmallRoutingHints>,
    token: Option<Arc<str>>,
    mcp_credentials: crate::mcp::Credentials,
}

#[derive(Debug)]
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if self.0 == StatusCode::SERVICE_UNAVAILABLE {
            return (self.0, [(header::RETRY_AFTER, "1")], Json(serde_json::json!({"error": self.1, "code": "resource_exhausted", "retryable": true}))).into_response();
        }
        (self.0, Json(serde_json::json!({"error": self.1}))).into_response()
    }
}

fn internal(error: anyhow::Error) -> ApiError {
    if error.downcast_ref::<crate::engine::StorageFull>().is_some() {
        return ApiError(
            StatusCode::INSUFFICIENT_STORAGE,
            "storage free-space reserve exhausted".into(),
        );
    }
    if error.downcast_ref::<ResourceExhausted>().is_some() {
        return ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "resource capacity exhausted; retry later".into(),
        );
    }
    tracing::error!(error = %error, "HTTP operation failed");
    ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        "storage operation failed".into(),
    )
}

fn hash_from_text(text: &str) -> std::result::Result<[u8; 32], ApiError> {
    let mut hash = [0; 32];
    if text.len() != 64 || hex::decode_to_slice(text, &mut hash).is_err() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "expected a 64-character BLAKE3 hex digest".into(),
        ));
    }
    Ok(hash)
}

/// Builds a router with the same admission and authentication rules as `serve`.
pub fn router(store: Arc<Coordinator>, shutdown: CancellationToken) -> Result<Router> {
    router_with_mcp_credentials(store, shutdown, crate::mcp::Credentials::from_env())
}

#[cfg(test)]
fn router_with_token(
    store: Arc<Coordinator>,
    shutdown: CancellationToken,
    token: Option<String>,
) -> Result<Router> {
    let mut credentials = crate::mcp::Credentials::from_env();
    credentials.service = token;
    router_with_mcp_credentials(store, shutdown, credentials)
}

pub fn router_with_mcp_credentials(
    store: Arc<Coordinator>,
    shutdown: CancellationToken,
    credentials: crate::mcp::Credentials,
) -> Result<Router> {
    let token = credentials.service.clone();
    let mcp = crate::mcp_http::router(store.clone(), shutdown.clone(), credentials.clone())?;
    let addr: SocketAddr = store
        .config
        .server
        .listen
        .parse()
        .context("invalid listen address")?;
    let token = token.filter(|s| !s.is_empty());
    if !addr.ip().is_loopback() && token.is_none() {
        return Err(anyhow!("a non-loopback listener requires SKERRY_TOKEN"));
    }
    let upload_budget = store
        .config
        .writes
        .max_pending_mib
        .checked_mul(1024 * 1024)
        .context("upload memory budget overflow")?;
    let initial_status = store.initial_monitoring_status()?;
    let initial_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let app = App {
        requests: Arc::new(Semaphore::new(store.config.resources.max_requests)),
        admin_requests: Arc::new(Semaphore::new(store.config.resources.admin_max_requests)),
        admission_stats: Arc::new(AdmissionStats::default()),
        snapshot_error: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        snapshot: Arc::new(parking_lot::Mutex::new(Some((
            initial_status,
            std::time::Instant::now(),
            initial_timestamp,
        )))),
        upload_bytes: Arc::new(Semaphore::new(upload_budget)),
        stream_uploads: Arc::new(Semaphore::new(store.config.streaming.max_uploads)),
        stream_reads: Arc::new(Semaphore::new(store.config.streaming.max_reads)),
        metadata_slots: Arc::new(Semaphore::new(store.config.reads.max_concurrency)),
        small_hints: Arc::new(SmallRoutingHints::new()),
        token: token.map(Arc::from),
        mcp_credentials: credentials,
        store,
        shutdown,
    };
    let snapshot = app.snapshot.clone();
    let snapshot_error = app.snapshot_error.clone();
    let weak_store = Arc::downgrade(&app.store);
    let stop = app.store.monitor_stop();
    app.store.register_monitor(async move {
        loop {
            tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
            let Some(store) = weak_store.upgrade() else {
                break;
            };
            match store.monitoring_status().await {
                Ok(status) => {
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    *snapshot.lock() = Some((status, std::time::Instant::now(), timestamp));
                    snapshot_error.store(false, std::sync::atomic::Ordering::Relaxed);
                }
                Err(error) => {
                    snapshot_error.store(true, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(%error, "status refresh failed");
                }
            }
            drop(store);
        }
    });
    Ok(Router::new()
        .route("/v1/health", get(health))
        .route("/v1/ready", get(ready))
        .route("/v1/status", get(status))
        .route("/v1/indexes", get(indexes))
        .route("/v1/metrics", get(metrics))
        .route("/v1/objects", put(put_object))
        .route(
            "/v1/objects/{hash}",
            get(get_object).head(head_object).delete(delete_object),
        )
        .route("/v1/objects/batch-get", post(batch_get))
        .route("/v1/objects/batch-put", post(batch_put))
        .route("/v1/cache/stats", get(cache_stats))
        .route("/v1/admin/cache-clear", post(cache_clear))
        .route("/v1/admin/gc", post(collect_garbage))
        .route("/v1/admin/checkpoint", post(checkpoint_journal))
        .route("/v1/admin/shutdown", post(shutdown_handler))
        .with_state(app.clone())
        .merge(mcp)
        .layer(middleware::from_fn_with_state(app, admission)))
}

fn request_deadline(config: &crate::config::Config, req: &Request) -> Duration {
    let streaming_upload = config.streaming.enabled
        && req.method() == axum::http::Method::PUT
        && req.uri().path() == "/v1/objects"
        && req
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .is_none_or(|n| {
                n > (config.streaming.threshold_mib as u64) * 1024 * 1024
                    || n > config.max_object_bytes() as u64
            });
    let object_get = config.streaming.enabled
        && req.method() == axum::http::Method::GET
        && req.uri().path().starts_with("/v1/objects/");
    Duration::from_secs(if streaming_upload || object_get {
        config.streaming.transfer_timeout_seconds
    } else {
        config.server.request_timeout_seconds
    })
}

async fn admission(State(app): State<App>, req: Request, next: Next) -> Response {
    let path = req.uri().path();
    if path != "/v1/health"
        && path != "/v1/ready"
        && path != app.store.config.mcp.http_path
        && path != "/v1/mcp/status"
    {
        let supplied = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        let matches = |token: &Option<String>| {
            token
                .as_ref()
                .is_some_and(|t| bool::from(supplied.as_bytes().ct_eq(t.as_bytes())))
        };
        let scoped = &app.mcp_credentials;
        let has_credentials = app.token.is_some()
            || scoped.read.is_some()
            || scoped.write.is_some()
            || scoped.maintenance.is_some()
            || scoped.gateway.is_some();
        let admin = app
            .token
            .as_ref()
            .is_some_and(|t| bool::from(supplied.as_bytes().ct_eq(t.as_bytes())))
            || (!has_credentials && supplied.is_empty());
        // Deletion always requires an explicitly configured service credential.
        if ((req.method() == axum::http::Method::DELETE && path.starts_with("/v1/objects/"))
            || (req.method() == axum::http::Method::POST
                && matches!(path, "/v1/admin/gc" | "/v1/admin/checkpoint")))
            && (app.token.is_none() || !admin)
        {
            return ApiError(
                if has_credentials {
                    StatusCode::FORBIDDEN
                } else {
                    StatusCode::UNAUTHORIZED
                },
                "service administrator credential required".into(),
            )
            .into_response();
        }
        if !admin {
            let read =
                matches(&scoped.read) || matches(&scoped.write) || matches(&scoped.maintenance);
            let write = matches(&scoped.write);
            let read_route = ((req.method() == axum::http::Method::GET
                || req.method() == axum::http::Method::HEAD)
                && path.starts_with("/v1/objects/"))
                || (req.method() == axum::http::Method::POST && path == "/v1/objects/batch-get")
                || (req.method() == axum::http::Method::GET && path == "/v1/cache/stats");
            let write_route = (req.method() == axum::http::Method::PUT && path == "/v1/objects")
                || (req.method() == axum::http::Method::POST && path == "/v1/objects/batch-put");
            if !read {
                return ApiError(StatusCode::UNAUTHORIZED, "bearer token required".into())
                    .into_response();
            }
            if !(read_route || write && write_route) {
                return ApiError(
                    StatusCode::FORBIDDEN,
                    "operation is not authorized for this token".into(),
                )
                .into_response();
            }
        }
    }
    if app.shutdown.is_cancelled() {
        return ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "service is draining".into(),
        )
        .into_response();
    }
    let capacity = if matches!(
        path,
        "/v1/health" | "/v1/ready" | "/v1/status" | "/v1/metrics" | "/v1/cache/stats"
    ) {
        app.admin_requests.clone()
    } else {
        app.requests.clone()
    };
    let started = std::time::Instant::now();
    let permit = match tokio::time::timeout(
        Duration::from_millis(app.store.config.resources.admission_timeout_ms),
        capacity.acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        _ => {
            app.admission_stats
                .timed_out
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            app.admission_stats.wait_us.fetch_add(
                started.elapsed().as_micros() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            return ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "request capacity exhausted; retry later".into(),
            )
            .into_response();
        }
    };
    app.admission_stats
        .accepted
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    app.admission_stats.wait_us.fetch_add(
        started.elapsed().as_micros() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
    let timeout = request_deadline(&app.store.config, &req);
    let response = match tokio::time::timeout(timeout, next.run(req)).await {
        Ok(response) => response,
        Err(_) => ApiError(
            StatusCode::REQUEST_TIMEOUT,
            "request deadline exceeded".into(),
        )
        .into_response(),
    };
    let (parts, body) = response.into_parts();
    // Keep admission capacity charged until response data is sent or cancelled.
    let stream = async_stream::stream! {
        let _permit = permit;
        let mut source = body.into_data_stream();
        while let Some(chunk) = source.next().await { yield chunk; }
    };
    Response::from_parts(parts, Body::from_stream(stream))
}

async fn health() -> ApiResult {
    Ok(
        Json(serde_json::json!({"health":"ok","version":env!("CARGO_PKG_VERSION")}))
            .into_response(),
    )
}
async fn ready(State(app): State<App>) -> ApiResult {
    let ready = app.store.is_ready() && !app.shutdown.is_cancelled();
    Ok((
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(serde_json::json!({"ready":ready})),
    )
        .into_response())
}

async fn status(State(app): State<App>) -> ApiResult {
    let guard = app.snapshot.lock();
    let (status, updated, timestamp) = guard.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "status snapshot unavailable".into(),
        )
    })?;
    let mut live = status.clone();
    app.store.refresh_live_status(&mut live);
    let mut value = serde_json::to_value(live).map_err(|e| internal(e.into()))?;
    let age = updated.elapsed().as_secs_f64();
    value["stats_timestamp"] = (*timestamp).into();
    value["stats_age_seconds"] = age.into();
    value["stats_stale"] = (age > 3.0
        || app
            .snapshot_error
            .load(std::sync::atomic::Ordering::Relaxed))
    .into();
    value["stats_error"] = if app
        .snapshot_error
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        serde_json::json!("storage statistics refresh failed")
    } else {
        serde_json::Value::Null
    };
    value["admission"] = serde_json::json!({"accepted": app.admission_stats.accepted.load(std::sync::atomic::Ordering::Relaxed), "timeouts": app.admission_stats.timed_out.load(std::sync::atomic::Ordering::Relaxed), "wait_seconds": app.admission_stats.wait_us.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1_000_000.0, "inflight": app.store.config.resources.max_requests - app.requests.available_permits()});
    Ok(Json(value).into_response())
}

#[derive(Deserialize)]
struct IndexQuery {
    after: Option<String>,
    limit: Option<usize>,
}
async fn indexes(State(app): State<App>, Query(query): Query<IndexQuery>) -> ApiResult {
    let limit = query.limit.unwrap_or(100);
    if !(1..=1000).contains(&limit) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "limit must be between 1 and 1000".into(),
        ));
    }
    if query.after.as_deref().is_some_and(|name| {
        name.strip_prefix("generation-")
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .is_none()
    }) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "invalid index cursor".into(),
        ));
    }
    let permit = app.store.try_request_permit().map_err(internal)?;
    let store = app.store.storage();
    let page = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        store.indexes(query.after.as_deref(), limit)
    })
    .await
    .map_err(|e| internal(e.into()))?
    .map_err(internal)?;
    Ok(Json(page).into_response())
}

async fn metrics(State(app): State<App>) -> ApiResult {
    let status = app.snapshot.lock().as_ref().map(|s| s.0.clone());
    let mut text = app.store.metrics();
    use std::sync::atomic::Ordering;
    text.push_str(&format!("skerry_admission_accepted_total {}\nskerry_admission_timeouts_total {}\nskerry_admission_wait_seconds_total {}\nskerry_admission_inflight {}\n", app.admission_stats.accepted.load(Ordering::Relaxed), app.admission_stats.timed_out.load(Ordering::Relaxed), app.admission_stats.wait_us.load(Ordering::Relaxed) as f64 / 1_000_000.0, app.store.config.resources.max_requests - app.requests.available_permits()));
    text.push_str(&format!("skerry_upload_admission_accepted_total {}\nskerry_upload_admission_timeouts_total {}\nskerry_upload_admission_wait_seconds_total {}\nskerry_upload_inflight_bytes {}\n", app.admission_stats.upload_accepted.load(Ordering::Relaxed), app.admission_stats.upload_timeouts.load(Ordering::Relaxed), app.admission_stats.upload_wait_us.load(Ordering::Relaxed) as f64 / 1_000_000.0, app.store.config.writes.max_pending_mib * 1024 * 1024 - app.upload_bytes.available_permits()));
    let Some(status) = status else {
        return Ok((
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            text,
        )
            .into_response());
    };
    for (name, value) in [
        ("objects", status.storage.objects as f64),
        ("payload_bytes", status.storage.payload_bytes as f64),
        ("segment_file_bytes", status.storage.physical_bytes as f64),
        ("segments", status.storage.segments as f64),
        ("index_bytes", status.storage.index_bytes as f64),
        ("open_segment_files", status.storage.open_files as f64),
        ("write_queue_bytes", status.write_queue_bytes as f64),
        (
            "write_queue_capacity_bytes",
            status.write_queue_capacity_bytes as f64,
        ),
        (
            "configured_budget_bytes",
            status.configured_budget_mib as f64 * 1024. * 1024.,
        ),
        ("healthy", if status.health == "running" { 1. } else { 0. }),
    ] {
        text.push_str(&format!(
            "# TYPE skerry_{name} gauge\nskerry_{name} {value}\n"
        ));
    }
    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        text,
    )
        .into_response())
}

async fn cache_stats(State(app): State<App>) -> ApiResult {
    Ok(Json(app.store.live_cache_status()).into_response())
}

async fn cache_clear(State(app): State<App>) -> Response {
    app.store.cache_clear();
    Json(serde_json::json!({"cleared":true})).into_response()
}

async fn shutdown_handler(State(app): State<App>) -> Response {
    app.shutdown.cancel();
    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"state":"draining"})),
    )
        .into_response()
}

async fn upload_permit(
    app: &App,
    bytes: usize,
) -> std::result::Result<tokio::sync::OwnedSemaphorePermit, ApiError> {
    let started = std::time::Instant::now();
    let permit = tokio::time::timeout(
        Duration::from_millis(app.store.config.resources.admission_timeout_ms),
        app.upload_bytes
            .clone()
            .acquire_many_owned(bytes.max(1) as u32),
    )
    .await
    .ok()
    .and_then(Result::ok);
    use std::sync::atomic::Ordering;
    app.admission_stats
        .upload_wait_us
        .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
    match permit {
        Some(permit) => {
            app.admission_stats
                .upload_accepted
                .fetch_add(1, Ordering::Relaxed);
            Ok(permit)
        }
        None => {
            app.admission_stats
                .upload_timeouts
                .fetch_add(1, Ordering::Relaxed);
            Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "upload capacity exhausted; retry later".into(),
            ))
        }
    }
}

fn check_binary(headers: &HeaderMap, batch: bool) -> std::result::Result<(), ApiError> {
    let expected = if batch {
        BATCH_MEDIA_TYPE
    } else {
        "application/octet-stream"
    };
    let received = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .unwrap_or("")
        .trim();
    if received != expected {
        return Err(ApiError(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("Content-Type must be {expected}"),
        ));
    }
    Ok(())
}

async fn put_object(State(app): State<App>, headers: HeaderMap, body: Body) -> ApiResult {
    check_binary(&headers, false)?;
    let max = if app.store.config.streaming.enabled {
        app.store.config.streaming.max_object_mib * 1024 * 1024
    } else {
        app.store.config.max_object_bytes()
    };
    if headers.get_all(header::CONTENT_LENGTH).iter().count() > 1 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "invalid Content-Length".into(),
        ));
    }
    let declared = match headers.get(header::CONTENT_LENGTH) {
        Some(value) => {
            let length = value
                .to_str()
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or_else(|| {
                    ApiError(StatusCode::BAD_REQUEST, "invalid Content-Length".into())
                })?;
            if length > max as u64 {
                return Err(ApiError(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "object exceeds configured limit".into(),
                ));
            }
            Some(length as usize)
        }
        None => None,
    };
    if app.store.config.streaming.enabled
        && declared.is_none_or(|n| {
            n > app.store.config.streaming.threshold_mib * 1024 * 1024
                || n > app.store.config.max_object_bytes()
        })
    {
        return stream_put(&app, body, declared).await;
    }
    if declared.is_some_and(|n| n > app.store.config.max_object_bytes()) {
        return Err(ApiError(
            StatusCode::PAYLOAD_TOO_LARGE,
            "object exceeds configured limit".into(),
        ));
    }
    let limit = declared.unwrap_or(max);
    let mut reservations = Vec::new();
    if let Some(length) = declared {
        reservations.push(upload_permit(&app, length).await?);
    }
    let mut source = body.into_data_stream();
    // Allocate only the reserved amount; geometric growth can exceed the ingress budget.
    let mut data = BytesMut::with_capacity(declared.unwrap_or(0));
    loop {
        match tokio::time::timeout(IO_TIMEOUT, source.next()).await {
            Err(_) => {
                return Err(ApiError(
                    StatusCode::REQUEST_TIMEOUT,
                    "upload idle timeout".into(),
                ));
            }
            Ok(None) => break,
            Ok(Some(Err(_))) => {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    "invalid request body".into(),
                ));
            }
            Ok(Some(Ok(chunk))) => {
                if chunk.len() > limit.saturating_sub(data.len()) {
                    return Err(if declared.is_some() {
                        ApiError(
                            StatusCode::BAD_REQUEST,
                            "body exceeds Content-Length".into(),
                        )
                    } else {
                        ApiError(
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "object exceeds configured limit".into(),
                        )
                    });
                }
                if declared.is_none() && !chunk.is_empty() {
                    reservations.push(upload_permit(&app, chunk.len()).await?);
                }
                data.extend_from_slice(&chunk);
            }
        }
    }
    if declared.is_some_and(|length| data.len() != length) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "body does not match Content-Length".into(),
        ));
    }
    let result = app.store.put(data.freeze()).await.map_err(internal)?;
    let hash = hex::encode(result.hash);
    let mut response = (
        StatusCode::OK,
        Json(serde_json::json!({"hash":hash,"duplicate":result.duplicate})),
    )
        .into_response();
    response.headers_mut().insert(
        "x-skerry-hash",
        HeaderValue::from_str(&hash).expect("hex is a header value"),
    );
    Ok(response)
}

async fn stream_put(app: &App, body: Body, declared: Option<usize>) -> ApiResult {
    let timeout = Duration::from_millis(app.store.config.resources.admission_timeout_ms);
    let slot = tokio::time::timeout(timeout, app.stream_uploads.clone().acquire_owned())
        .await
        .ok()
        .and_then(Result::ok)
        .ok_or_else(|| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "stream upload capacity exhausted".into(),
            )
        })?;
    let store = app.store.storage();
    let maximum = (app.store.config.streaming.max_object_mib as u64) * 1024 * 1024;
    let (mut upload, mut slot) = tokio::task::spawn_blocking(move || {
        Ok::<_, anyhow::Error>((
            store.begin_upload(maximum, declared.map(|v| v as u64))?,
            slot,
        ))
    })
    .await
    .map_err(|e| internal(e.into()))?
    .map_err(internal)?;
    let mut input = body.into_data_stream();
    let mut total = 0usize;
    let chunk_size = (app.store.config.streaming.chunk_kib * 1024)
        .min(app.store.config.writes.max_pending_mib * 1024 * 1024);
    while let Some(chunk) = tokio::time::timeout(IO_TIMEOUT, input.next())
        .await
        .map_err(|_| ApiError(StatusCode::REQUEST_TIMEOUT, "upload idle timeout".into()))?
    {
        let chunk =
            chunk.map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid request body".into()))?;
        total = total.checked_add(chunk.len()).ok_or_else(|| {
            ApiError(
                StatusCode::PAYLOAD_TOO_LARGE,
                "object exceeds configured limit".into(),
            )
        })?;
        if total as u64 > maximum || declared.is_some_and(|n| total > n) {
            return Err(ApiError(
                StatusCode::PAYLOAD_TOO_LARGE,
                "object exceeds configured limit".into(),
            ));
        }
        for part in chunk.chunks(chunk_size) {
            let permit = upload_permit(app, part.len()).await?;
            let bytes = Bytes::copy_from_slice(part);
            (upload, slot) = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                upload.write_chunk(&bytes)?;
                Ok::<_, anyhow::Error>((upload, slot))
            })
            .await
            .map_err(|e| internal(e.into()))?
            .map_err(internal)?;
        }
    }
    if declared.is_some_and(|n| n != total) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "body does not match Content-Length".into(),
        ));
    }
    let result = app
        .store
        .finish_stream_upload(upload, slot, total)
        .await
        .map_err(internal)?;
    let hash = hex::encode(result.hash);
    let mut response = (
        StatusCode::OK,
        Json(serde_json::json!({"hash":hash,"duplicate":result.duplicate})),
    )
        .into_response();
    response.headers_mut().insert(
        "x-skerry-hash",
        HeaderValue::from_str(&hash).expect("hex header"),
    );
    Ok(response)
}

#[derive(Deserialize, Default)]
struct ReadOptions {
    priority: Option<String>,
}
fn parse_priority(value: Option<&str>) -> std::result::Result<Priority, ApiError> {
    match value.unwrap_or("normal") {
        "interactive" => Ok(Priority::Interactive),
        "normal" => Ok(Priority::Normal),
        "bulk" => Ok(Priority::Bulk),
        _ => Err(ApiError(
            StatusCode::BAD_REQUEST,
            "invalid read priority".into(),
        )),
    }
}
#[derive(Deserialize, Default)]
struct GcOptions {
    #[serde(default)]
    force: bool,
}
fn require_v2(app: &App) -> std::result::Result<(), ApiError> {
    if !app.store.storage().deletion_enabled() {
        return Err(ApiError(StatusCode::CONFLICT,
            "v1 store requires offline upgrade: stop the service and run skerry upgrade --path <store>".into()));
    }
    Ok(())
}
async fn collect_garbage(State(app): State<App>, Query(options): Query<GcOptions>) -> ApiResult {
    require_v2(&app)?;
    let result = app
        .store
        .collect_garbage(options.force)
        .await
        .map_err(internal)?;
    Ok(Json(result).into_response())
}

async fn checkpoint_journal(State(app): State<App>, Query(options): Query<GcOptions>) -> ApiResult {
    require_v2(&app)?;
    Ok(Json(
        app.store
            .checkpoint_journal(options.force)
            .await
            .map_err(internal)?,
    )
    .into_response())
}

async fn delete_object(State(app): State<App>, Path(hash): Path<String>) -> ApiResult {
    let hash = hash_from_text(&hash)?;
    require_v2(&app)?;
    app.store.delete(hash).await.map_err(internal)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

struct StreamChunk {
    bytes: Vec<u8>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl AsRef<[u8]> for StreamChunk {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
struct StreamReadCounter(Arc<Coordinator>);
impl Drop for StreamReadCounter {
    fn drop(&mut self) {
        self.0.stream_read_finished();
    }
}
struct CancelStream(Arc<std::sync::atomic::AtomicBool>);
impl Drop for CancelStream {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

async fn stream_get(app: &App, hash: [u8; 32]) -> ApiResult {
    let timeout = Duration::from_millis(app.store.config.resources.admission_timeout_ms);
    let slot = tokio::time::timeout(timeout, app.stream_reads.clone().acquire_owned())
        .await
        .ok()
        .and_then(Result::ok)
        .ok_or_else(|| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "stream read capacity exhausted".into(),
            )
        })?;
    let gate = app.store.stream_read_gate().await.map_err(internal)?;
    let request = app.store.stream_request_permit().await.map_err(internal)?;
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guard = CancelStream(cancel.clone());
    let store = app.store.storage();
    let (reader, slot, request, gate) = tokio::task::spawn_blocking(move || {
        Ok::<_, anyhow::Error>((
            store.prepare_stream_read(&hash, cancel)?,
            slot,
            request,
            gate,
        ))
    })
    .await
    .map_err(|e| internal(e.into()))?
    .map_err(internal)?;
    let mut reader =
        reader.ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "object not found".into()))?;
    let length = reader.length();
    let coordinator = app.store.clone();
    coordinator.stream_read_started();
    let counter = StreamReadCounter(coordinator.clone());
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(1);
    let stop = coordinator.monitor_stop();
    let idle = Duration::from_secs(app.store.config.streaming.idle_timeout_seconds);
    app.store.register_monitor(async move {
        let _counter = counter;
        let mut slot = slot;
        let mut request = request;
        let mut gate = gate;
        loop {
            let permit = tokio::select! {
                _ = stop.cancelled() => break,
                _ = tx.closed() => break,
                permit = coordinator.read_memory_permit(reader.chunk_size()) => match permit {
                    Ok(permit) => permit,
                    Err(error) => { let _ = tx.try_send(Err(std::io::Error::other(error))); break; }
                }
            };
            let worker = tokio::task::spawn_blocking(move || {
                let next = reader.next_chunk(); (next, reader, slot, request, gate, permit)
            });
            let (next, returned_reader, returned_slot, returned_request, returned_gate, permit) = match worker.await {
                Ok(value) => value,
                Err(error) => { let _ = tx.try_send(Err(std::io::Error::other(error))); break; }
            };
            reader = returned_reader; slot = returned_slot; request = returned_request; gate = returned_gate;
            let bytes = match next {
                Ok(Some(bytes)) => bytes,
                Ok(None) => break,
                Err(error) => { let _ = tx.try_send(Err(std::io::Error::other(error))); break; }
            };
            coordinator.stream_read_bytes(bytes.len());
            let chunk = Bytes::from_owner(StreamChunk { bytes, _permit: permit });
            tokio::select! {
                _ = stop.cancelled() => break,
                result = tokio::time::timeout(idle, tx.send(Ok(chunk))) => if !matches!(result, Ok(Ok(()))) { break; },
            }
        }
    });
    let stream = futures_util::stream::unfold((rx, guard), |(mut rx, guard)| async move {
        rx.recv().await.map(|item| (item, (rx, guard)))
    });
    let mut response = Body::from_stream(stream).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string()).expect("length header"),
    );
    response.headers_mut().insert(
        "x-skerry-hash",
        HeaderValue::from_str(&hex::encode(hash)).expect("hex header"),
    );
    Ok(response)
}

async fn get_object(
    State(app): State<App>,
    Path(hash): Path<String>,
    Query(options): Query<ReadOptions>,
) -> ApiResult {
    let hash = hash_from_text(&hash)?;
    let priority = parse_priority(options.priority.as_deref())?;
    if app.store.config.streaming.enabled
        && !app.store.has_cached_payload(&hash)
        && !app.small_hints.contains(&hash)
    {
        // Metadata routing has its own bounded capacity. It must not reject a
        // cacheable GET merely because data coordinator permits are in use.
        let permit = tokio::time::timeout(
            Duration::from_millis(app.store.config.resources.admission_timeout_ms),
            app.metadata_slots.clone().acquire_owned(),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .ok_or_else(|| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "stream read capacity exhausted".into(),
            )
        })?;
        let storage = app.store.storage();
        let info = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            storage.object_info(hash)
        })
        .await
        .map_err(|e| internal(e.into()))?
        .map_err(internal)?;
        if let Some(length) = info.payload_length {
            if !buffered_object(&app.store.config, length) {
                return stream_get(&app, hash).await;
            }
            if info.exists {
                app.small_hints.insert(hash);
            }
        }
    }
    match app
        .store
        .get_priority(hash, priority)
        .await
        .map_err(internal)?
    {
        Some(payload) => {
            let mut response = (
                [(header::CONTENT_TYPE, "application/octet-stream")],
                payload,
            )
                .into_response();
            response.headers_mut().insert(
                "x-skerry-hash",
                HeaderValue::from_str(&hex::encode(hash)).expect("hex header"),
            );
            Ok(response)
        }
        None => Err(ApiError(StatusCode::NOT_FOUND, "object not found".into())),
    }
}

async fn head_object(State(app): State<App>, Path(hash): Path<String>) -> ApiResult {
    let hash = hash_from_text(&hash)?;
    let info = app.store.object_info(hash).await.map_err(internal)?;
    if !info.exists {
        return Err(ApiError(StatusCode::NOT_FOUND, "object not found".into()));
    }
    let mut response = StatusCode::OK.into_response();
    response.headers_mut().insert(
        "x-skerry-hash",
        HeaderValue::from_str(&hex::encode(hash)).expect("hex header"),
    );
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    if let Some(length) = info.payload_length {
        response.headers_mut().insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&length.to_string()).expect("length header"),
        );
    }
    Ok(response)
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub kind: u8,
    pub status: u8,
    pub ordinal: u64,
    pub hash: [u8; 32],
    pub payload: Bytes,
}

/// Encodes a complete frame. Used by clients and golden-vector tests.
pub fn encode_frame(frame: &Frame) -> Bytes {
    let mut bytes = frame_header(
        frame.kind,
        frame.status,
        frame.ordinal,
        frame.hash,
        frame.payload.len(),
    );
    bytes.extend_from_slice(&frame.payload);
    bytes.into()
}

fn frame_header(kind: u8, status: u8, ordinal: u64, hash: [u8; 32], length: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + FRAME_HEADER_LEN);
    bytes.extend_from_slice(&((FRAME_HEADER_LEN + length) as u32).to_le_bytes());
    bytes.extend_from_slice(b"SKB1");
    bytes.extend_from_slice(&[kind, status, 0, 0]);
    bytes.extend_from_slice(&ordinal.to_le_bytes());
    bytes.extend_from_slice(&hash);
    bytes.extend_from_slice(&(length as u64).to_le_bytes());
    bytes
}

struct Decoder {
    input: futures_util::stream::BoxStream<'static, std::result::Result<Bytes, axum::Error>>,
    chunk: Bytes,
}

impl Decoder {
    fn new(body: Body) -> Self {
        Self {
            input: body.into_data_stream().boxed(),
            chunk: Bytes::new(),
        }
    }

    async fn exact(&mut self, length: usize, allow_eof: bool) -> Result<Option<Bytes>> {
        let mut result = BytesMut::with_capacity(length);
        while result.len() < length {
            if self.chunk.is_empty() {
                match tokio::time::timeout(IO_TIMEOUT, self.input.next())
                    .await
                    .context("batch idle timeout")?
                {
                    Some(Ok(chunk)) => {
                        self.chunk = chunk;
                        continue;
                    }
                    Some(Err(error)) => return Err(error.into()),
                    None if allow_eof && result.is_empty() => return Ok(None),
                    None => return Err(anyhow!("truncated batch frame")),
                }
            }
            let count = (length - result.len()).min(self.chunk.len());
            result.extend_from_slice(&self.chunk[..count]);
            self.chunk.advance(count);
        }
        Ok(Some(result.freeze()))
    }

    async fn header(&mut self, kind: u8, maximum: usize) -> Result<Option<(Frame, usize)>> {
        let Some(prefix) = self.exact(4, true).await? else {
            return Ok(None);
        };
        let frame_len = u32::from_le_bytes(prefix[..].try_into().expect("four bytes")) as usize;
        if !(FRAME_HEADER_LEN..=FRAME_HEADER_LEN + maximum).contains(&frame_len) {
            return Err(anyhow!("invalid batch frame length"));
        }
        let header = self
            .exact(FRAME_HEADER_LEN, false)
            .await?
            .expect("required header");
        if &header[..4] != b"SKB1" || header[4] != kind || header[5..8] != [0, 0, 0] {
            return Err(anyhow!("invalid batch frame header"));
        }
        let ordinal = u64::from_le_bytes(header[8..16].try_into()?);
        if ordinal == u64::MAX {
            return Err(anyhow!("reserved ordinal"));
        }
        let hash: [u8; 32] = header[16..48].try_into()?;
        let payload_len = u64::from_le_bytes(header[48..56].try_into()?);
        if payload_len != (frame_len - FRAME_HEADER_LEN) as u64 {
            return Err(anyhow!("inconsistent batch lengths"));
        }
        if kind == REQUEST_GET && payload_len != 0 || kind == REQUEST_PUT && hash != [0; 32] {
            return Err(anyhow!("invalid batch operation fields"));
        }
        Ok(Some((
            Frame {
                kind,
                status: 0,
                ordinal,
                hash,
                payload: Bytes::new(),
            },
            payload_len as usize,
        )))
    }

    async fn next(&mut self, kind: u8, maximum: usize) -> Result<Option<Frame>> {
        let Some((mut frame, length)) = self.header(kind, maximum).await? else {
            return Ok(None);
        };
        if length != 0 {
            frame.payload = self.exact(length, false).await?.expect("required payload");
        }
        Ok(Some(frame))
    }
}

fn batch_response(
    stream: impl futures_util::Stream<Item = std::result::Result<Bytes, Infallible>> + Send + 'static,
) -> Response {
    (
        [(header::CONTENT_TYPE, BATCH_MEDIA_TYPE)],
        Body::from_stream(stream),
    )
        .into_response()
}

async fn batch_put(State(app): State<App>, headers: HeaderMap, body: Body) -> ApiResult {
    check_binary(&headers, true)?;
    let maximum = app.store.config.max_object_bytes();
    let permit = upload_permit(&app, 1).await?;
    let window = app.store.config.resources.max_inflight_per_batch;
    let max_items = app.store.config.resources.max_batch_items;
    let store = app.store.clone();
    let stream = batch_put_stream(
        body,
        BatchWriteLimits {
            maximum,
            window,
            max_items,
            admission_timeout: Duration::from_millis(
                app.store.config.resources.admission_timeout_ms,
            ),
        },
        app.upload_bytes.clone(),
        permit,
        move |payload| {
            let store = store.clone();
            async move { store.put(payload).await }.boxed()
        },
    );
    Ok(batch_response(stream))
}

struct BatchWriteLimits {
    maximum: usize,
    window: usize,
    max_items: usize,
    admission_timeout: Duration,
}

struct ActiveBatchWrite {
    active: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}

fn batch_source_stopped(ended: bool, write_failed: &std::sync::atomic::AtomicBool) -> bool {
    ended || write_failed.load(Ordering::Acquire)
}
impl Drop for ActiveBatchWrite {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        self.changed.notify_one();
    }
}

fn batch_put_stream(
    body: Body,
    limits: BatchWriteLimits,
    upload_bytes: Arc<Semaphore>,
    permit: tokio::sync::OwnedSemaphorePermit,
    write: impl Fn(
        Bytes,
    )
        -> futures_util::future::BoxFuture<'static, anyhow::Result<crate::engine::PutResult>>
    + Send
    + 'static,
) -> impl futures_util::Stream<Item = std::result::Result<Bytes, Infallible>> + Send + 'static {
    async_stream::stream! {
        let BatchWriteLimits { maximum, window, max_items, admission_timeout } = limits;
        drop(permit);
        let mut decoder = Decoder::new(body);
        let source = async_stream::stream! {
            loop {
                match decoder.header(REQUEST_PUT, maximum).await {
                    Ok(Some((mut frame, length))) => {
                        let reservation = match tokio::time::timeout(admission_timeout, upload_bytes.clone().acquire_many_owned(length.max(1) as u32)).await {
                            Ok(Ok(p)) => p, _ => { yield Err(anyhow::Error::from(ResourceExhausted)); break; }
                        };
                        if length != 0 {
                            match decoder.exact(length, false).await {
                                Ok(Some(payload)) => frame.payload = payload,
                                Ok(None) => { yield Err(anyhow!("truncated payload")); break; }
                                Err(e) => { yield Err(e); break; }
                            }
                        }
                        yield Ok((frame, reservation));
                    }
                    Ok(None) => break,
                    Err(error) => { yield Err(error); break; }
                }
            }
        };
        futures_util::pin_mut!(source);
        let mut pending: FuturesOrdered<futures_util::future::BoxFuture<'static, (u64, anyhow::Result<crate::engine::PutResult>)>> = FuturesOrdered::new();
        let active_writes = Arc::new(AtomicUsize::new(0));
        let active_changed = Arc::new(Notify::new());
        let write_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        // Ordered output can retain completed later ordinals behind a slow
        // earlier write. Bound those results independently from active I/O so
        // their completed futures do not consume the full active window.
        let result_limit = max_items.min(window.max(1).saturating_mul(4)).max(1);
        let mut count = 0;
        let mut ended = false;
        let mut terminal_status = None;
        loop {
            if ended && pending.is_empty() { break; }
            tokio::select! {
                _ = active_changed.notified() => {}
                result = pending.next(), if !pending.is_empty() => {
                    if let Some((ordinal, result)) = result {
                        match result {
                            Ok(result) => yield Ok(Bytes::from(frame_header(RESULT_PUT, 0, ordinal, result.hash, 0))),
                            Err(error) => {
                                tracing::error!(%error, "batch write failed");
                                let status = if error.downcast_ref::<ResourceExhausted>().is_some() || error.downcast_ref::<crate::engine::StorageFull>().is_some() { 5 } else { 6 };
                                yield Ok(Bytes::from(frame_header(RESULT_PUT, status, ordinal, [0;32], 0)));
                                terminal_status = None;
                                ended = true;
                                // A suspended decoder may retain a partial payload.
                                // Keep its reservation until the source is dropped.
                            }
                        }
                    }
                }
                next = source.next(), if !batch_source_stopped(ended, &write_failed)
                    && active_writes.load(Ordering::Acquire) < window.max(1)
                    && pending.len() < result_limit => {
                    match next {
                        None => { ended = true; }
                        Some(Err(error)) => {
                            tracing::warn!(%error, "invalid batch upload");
                            ended = true;
                            terminal_status = Some(if error.downcast_ref::<ResourceExhausted>().is_some() { 5 } else { 3 });
                        }
                        Some(Ok((frame, permit))) => {
                            // The select guard is evaluated before pending
                            // futures are polled. Recheck after source.next()
                            // returns because that poll may observe an
                            // out-of-order failure and set write_failed.
                            if batch_source_stopped(ended, &write_failed) {
                                ended = true;
                            } else {
                                count += 1;
                                if count > max_items {
                                    ended = true;
                                    terminal_status = Some(5);
                                } else {
                                    let write = write(frame.payload);
                                    active_writes.fetch_add(1, Ordering::AcqRel);
                                    let active = active_writes.clone();
                                    let changed = active_changed.clone();
                                    let write_failed = write_failed.clone();
                                    let guard = ActiveBatchWrite { active, changed };
                                    pending.push_back(async move {
                                        // Capture the guard outside the async
                                        // body so dropping an unpolled future
                                        // releases the active slot as well.
                                        let _active = guard;
                                        let _permit = permit;
                                        let result = write.await;
                                        if result.is_err() {
                                            write_failed.store(true, Ordering::Release);
                                        }
                                        (frame.ordinal, result)
                                    }.boxed());
                                }
                            }
                        }
                    }
                }
            }
        }
        if let Some(status) = terminal_status {
            yield Ok(Bytes::from(frame_header(TERMINAL_ERROR, status, u64::MAX, [0;32], 0)));
        }
    }
}

#[derive(Deserialize)]
struct BatchOptions {
    #[serde(default = "default_ordered")]
    ordered: bool,
    priority: Option<String>,
}
fn default_ordered() -> bool {
    true
}

async fn read_frame(app: App, frame: Frame, priority: Priority) -> Frame {
    match app.store.get_priority(frame.hash, priority).await {
        Ok(Some(payload)) => Frame {
            kind: RESULT_GET,
            status: 0,
            payload,
            ..frame
        },
        Ok(None) => Frame {
            kind: RESULT_GET,
            status: 1,
            ..frame
        },
        Err(error) => {
            tracing::error!(%error, "batch read failed");
            let status = if error.downcast_ref::<ResourceExhausted>().is_some() {
                5
            } else {
                2
            };
            Frame {
                kind: RESULT_GET,
                status,
                ..frame
            }
        }
    }
}

async fn batch_get(
    State(app): State<App>,
    Query(options): Query<BatchOptions>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult {
    check_binary(&headers, true)?;
    let priority = parse_priority(options.priority.as_deref())?;
    let stream = async_stream::stream! {
        let mut decoder = Decoder::new(body);
        // The generator retains an in-progress decoder read across select!
        // cancellation, so a slow sender cannot discard partially read frames.
        let source = async_stream::stream! {
            loop {
                match decoder.next(REQUEST_GET, 0).await {
                    Ok(Some(frame)) => yield Ok(frame),
                    Ok(None) => break,
                    Err(error) => { yield Err(error); break; }
                }
            }
        };
        futures_util::pin_mut!(source);
        let mut pending: FuturesUnordered<futures_util::future::BoxFuture<'static, Frame>> = FuturesUnordered::new();
        let window = if options.ordered { 1 } else { app.store.config.reads.max_concurrency.clamp(1, 32) };
        let mut count = 0;
        let mut ended = false;
        let mut terminal_status = None;
        loop {
            if ended && pending.is_empty() { break; }
            tokio::select! {
                // Poll completed reads even while the next input frame is pending.
                result = pending.next(), if !pending.is_empty() => {
                    if let Some(frame) = result {
                        yield Ok(Bytes::from(frame_header(frame.kind, frame.status, frame.ordinal, frame.hash, frame.payload.len())));
                        if !frame.payload.is_empty() { yield Ok(frame.payload); }
                    }
                }
                next = source.next(), if !ended && pending.len() < window => {
                    match next {
                        None => ended = true,
                        Some(Ok(frame)) => {
                            count += 1;
                            if count > app.store.config.resources.max_batch_items {
                                ended = true;
                                terminal_status = Some(5);
                            } else { pending.push(read_frame(app.clone(), frame, priority).boxed()); }
                        }
                        Some(Err(error)) => {
                            tracing::warn!(%error, "invalid batch retrieval");
                            ended = true;
                            terminal_status = Some(3);
                        }
                    }
                }
            }
        }
        if let Some(status) = terminal_status {
            yield Ok(Bytes::from(frame_header(TERMINAL_ERROR, status, u64::MAX, [0;32], 0)));
        }
    };
    Ok(batch_response(stream))
}

/// Socket wrapper that wakes pending connection I/O on a forced drain deadline.
/// Dropping only Axum's serve future would leave its spawned connection tasks alive.
struct DrainIo {
    stream: tokio::net::TcpStream,
    _connection_permit: tokio::sync::OwnedSemaphorePermit,
    cancelled: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
}
impl DrainIo {
    fn is_cancelled(&mut self, cx: &mut std::task::Context<'_>) -> bool {
        self.cancelled.as_mut().poll(cx).is_ready()
    }
    fn aborted() -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "HTTP drain deadline reached",
        )
    }
}
impl tokio::io::AsyncRead for DrainIo {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.is_cancelled(cx) {
            return std::task::Poll::Ready(Err(Self::aborted()));
        }
        std::pin::Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl tokio::io::AsyncWrite for DrainIo {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.is_cancelled(cx) {
            return std::task::Poll::Ready(Err(Self::aborted()));
        }
        std::pin::Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.is_cancelled(cx) {
            return std::task::Poll::Ready(Err(Self::aborted()));
        }
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.is_cancelled(cx) {
            return std::task::Poll::Ready(Err(Self::aborted()));
        }
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
struct DrainListener {
    listener: tokio::net::TcpListener,
    force: CancellationToken,
    connections: Arc<Semaphore>,
}
impl axum::serve::Listener for DrainListener {
    type Io = DrainIo;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            // Reserve before accept: slow headers and idle keep-alive sockets
            // consume capacity even before routing/request admission runs.
            let permit = self
                .connections
                .clone()
                .acquire_owned()
                .await
                .expect("private connection semaphore is never closed");
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    return (
                        DrainIo {
                            stream,
                            _connection_permit: permit,
                            cancelled: Box::pin(self.force.clone().cancelled_owned()),
                        },
                        addr,
                    );
                }
                Err(error) => {
                    tracing::error!(%error, "accept connection failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

/// Runs the foreground service. New admission stops at cancellation; accepted
/// requests get a bounded drain interval, then the durable writer is drained.
pub async fn serve(store: Arc<Coordinator>, shutdown: CancellationToken) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(&store.config.server.listen)
        .await
        .context("cannot bind HTTP listener")?;
    serve_on_listener(store, shutdown, listener).await
}

/// Starts on an already-bound socket, useful for embedding and race-free tests.
pub async fn serve_on_listener(
    store: Arc<Coordinator>,
    shutdown: CancellationToken,
    listener: tokio::net::TcpListener,
) -> Result<()> {
    let app = router(store.clone(), shutdown.clone())?;
    tracing::info!(listen = %listener.local_addr()?, "SkerryKV listening");
    let force = CancellationToken::new();
    let listener = DrainListener {
        listener,
        force: force.clone(),
        connections: Arc::new(Semaphore::new(store.config.resources.max_requests * 2)),
    };
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    tokio::pin!(server);
    let result = tokio::select! {
        result = &mut server => result.context("HTTP server failed"),
        _ = shutdown.cancelled() => {
            match tokio::time::timeout(Duration::from_secs(store.config.server.shutdown_timeout_seconds), &mut server).await {
                Ok(result) => result.context("HTTP server failed"),
                Err(_) => {
                    force.cancel();
                    // Force-cancel wakes read/write operations in every accepted
                    // socket. Join connection teardown before final persistence.
                    let _ = tokio::time::timeout(Duration::from_secs(2), &mut server).await;
                    Err(anyhow!("HTTP drain timeout"))
                }
            }
        }
    };
    let persisted = store.shutdown().await;
    result?;
    persisted
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn storage_full_maps_to_insufficient_storage() {
        let response = internal(crate::engine::StorageFull.into()).into_response();
        assert_eq!(response.status(), StatusCode::INSUFFICIENT_STORAGE);
    }

    #[test]
    fn streaming_upload_uses_transfer_deadline() {
        let config = crate::config::Config::default();
        let large = axum::http::Request::put("/v1/objects")
            .header(header::CONTENT_LENGTH, 9 * 1024 * 1024)
            .body(Body::empty())
            .unwrap();
        let unknown = axum::http::Request::put("/v1/objects")
            .body(Body::empty())
            .unwrap();
        let small = axum::http::Request::put("/v1/objects")
            .header(header::CONTENT_LENGTH, 3)
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            request_deadline(&config, &large),
            Duration::from_secs(config.streaming.transfer_timeout_seconds)
        );
        assert_eq!(
            request_deadline(&config, &unknown),
            Duration::from_secs(config.streaming.transfer_timeout_seconds)
        );
        let get = axum::http::Request::get("/v1/objects/abc")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            request_deadline(&config, &get),
            Duration::from_secs(config.streaming.transfer_timeout_seconds)
        );
        assert_eq!(
            request_deadline(&config, &small),
            Duration::from_secs(config.server.request_timeout_seconds)
        );
    }

    #[tokio::test]
    async fn accepted_connection_capacity_covers_idle_sockets_and_releases() {
        use axum::serve::Listener;
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let connections = Arc::new(Semaphore::new(2));
        let mut listener = DrainListener {
            listener: tcp,
            force: CancellationToken::new(),
            connections: connections.clone(),
        };
        let _client_one = tokio::net::TcpStream::connect(address).await.unwrap();
        let (first, _) = listener.accept().await;
        let _client_two = tokio::net::TcpStream::connect(address).await.unwrap();
        let (_second, _) = listener.accept().await;
        assert_eq!(connections.available_permits(), 0);
        let _client_three = tokio::net::TcpStream::connect(address).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
        drop(first);
        let (_third, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap();
        assert_eq!(connections.available_permits(), 0);
    }
    #[tokio::test]
    async fn configured_token_authenticates_control_and_ignores_query_tokens() {
        use tower::ServiceExt;
        let directory = tempfile::tempdir().unwrap();
        let mut config = crate::config::Config::default();
        config.storage.root = directory.path().to_path_buf();
        let store = Coordinator::open(config).await.unwrap();
        let app = router_with_token(
            store.clone(),
            CancellationToken::new(),
            Some("test-secret".into()),
        )
        .unwrap();
        for (path, token, expected) in [
            ("/v1/status", None, StatusCode::UNAUTHORIZED),
            ("/v1/status", Some("Bearer wrong"), StatusCode::UNAUTHORIZED),
            (
                "/v1/status?token=test-secret",
                None,
                StatusCode::UNAUTHORIZED,
            ),
            ("/v1/status", Some("Bearer test-secret"), StatusCode::OK),
            ("/v1/health", None, StatusCode::OK),
        ] {
            let mut request = axum::http::Request::get(path);
            if let Some(token) = token {
                request = request.header(header::AUTHORIZATION, token);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
        }
        store.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn deletion_requires_service_credential_and_is_idempotent() {
        use tower::ServiceExt;
        let directory = tempfile::tempdir().unwrap();
        let mut config = crate::config::Config::default();
        config.storage.root = directory.path().to_path_buf();
        let store = Coordinator::open(config).await.unwrap();
        let credentials = crate::mcp::Credentials {
            service: Some("admin-secret".into()),
            read: Some("read-secret".into()),
            write: Some("write-secret".into()),
            maintenance: Some("maintenance-secret".into()),
            gateway: None,
        };
        let app = router_with_mcp_credentials(store.clone(), CancellationToken::new(), credentials)
            .unwrap();
        let inserted = store.put(Bytes::from_static(b"to delete")).await.unwrap();
        let hash = hex::encode(inserted.hash);
        for (credential, expected) in [
            ("read-secret", StatusCode::FORBIDDEN),
            ("write-secret", StatusCode::FORBIDDEN),
            ("maintenance-secret", StatusCode::FORBIDDEN),
            ("admin-secret", StatusCode::NO_CONTENT),
            ("admin-secret", StatusCode::NO_CONTENT),
        ] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::delete(format!("/v1/objects/{hash}"))
                        .header(header::AUTHORIZATION, format!("Bearer {credential}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
        }
        assert!(!store.exists(inserted.hash).await.unwrap());
        for credential in ["read-secret", "write-secret", "maintenance-secret"] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::post("/v1/admin/gc")
                        .header(header::AUTHORIZATION, format!("Bearer {credential}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
        let response = app
            .oneshot(
                axum::http::Request::delete("/v1/objects/not-a-hash")
                    .header(header::AUTHORIZATION, "Bearer admin-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        store.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn single_upload_reserves_declared_size_and_releases_on_cancel() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = crate::config::Config::default();
        config.storage.root = directory.path().to_path_buf();
        let store = Coordinator::open(config).await.unwrap();
        let capacity = Arc::new(Semaphore::new(8));
        let app = App {
            store: store.clone(),
            shutdown: CancellationToken::new(),
            requests: Arc::new(Semaphore::new(1)),
            admin_requests: Arc::new(Semaphore::new(16)),
            admission_stats: Arc::new(AdmissionStats::default()),
            snapshot_error: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            snapshot: Arc::new(parking_lot::Mutex::new(None)),
            upload_bytes: capacity.clone(),
            stream_uploads: Arc::new(Semaphore::new(2)),
            stream_reads: Arc::new(Semaphore::new(2)),
            metadata_slots: Arc::new(Semaphore::new(2)),
            small_hints: Arc::new(SmallRoutingHints::new()),
            token: None,
            mcp_credentials: crate::mcp::Credentials::from_env(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("3"));
        let task = tokio::spawn(put_object(
            State(app.clone()),
            headers.clone(),
            Body::from_stream(futures_util::stream::pending::<Result<Bytes, Infallible>>()),
        ));
        tokio::task::yield_now().await;
        assert_eq!(capacity.available_permits(), 5);
        task.abort();
        let _ = task.await;
        assert_eq!(capacity.available_permits(), 8);
        for data in ["ab", "abcd"] {
            let error = put_object(State(app.clone()), headers.clone(), Body::from(data))
                .await
                .unwrap_err();
            assert_eq!(error.0, StatusCode::BAD_REQUEST);
            assert_eq!(capacity.available_permits(), 8);
        }
        assert_eq!(
            put_object(State(app), headers, Body::from("abc"))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn batch_put_groups_writes_and_orders_results_before_terminal_error() {
        use tower::ServiceExt;
        let directory = tempfile::tempdir().unwrap();
        let mut config = crate::config::Config::default();
        config.storage.root = directory.path().to_path_buf();
        config.resources.max_object_mib = 1;
        config.writes.batch_max_records = 8;
        config.writes.max_pending_mib = 2;
        config.writes.batch_max_bytes_mib = 2;
        config.writes.batch_max_delay_ms = 1000;
        let store = Coordinator::open(config).await.unwrap();
        let app = router_with_token(store.clone(), CancellationToken::new(), None).unwrap();
        let mut input = Vec::new();
        for ordinal in 0..8 {
            input.extend_from_slice(&encode_frame(&Frame {
                kind: REQUEST_PUT,
                status: 0,
                ordinal,
                hash: [0; 32],
                payload: Bytes::from(vec![ordinal as u8]),
            }));
        }
        input.extend_from_slice(&[1, 2]); // truncated next frame
        let response = app
            .oneshot(
                axum::http::Request::post("/v1/objects/batch-put")
                    .header(header::CONTENT_TYPE, BATCH_MEDIA_TYPE)
                    .body(Body::from(input))
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = tokio::time::timeout(
            Duration::from_millis(750),
            axum::body::to_bytes(response.into_body(), usize::MAX),
        )
        .await
        .expect("eight small writes should fill one writer batch")
        .unwrap();
        assert_eq!(bytes.len(), 9 * (4 + FRAME_HEADER_LEN));
        for (i, frame) in bytes
            .as_chunks::<{ 4 + FRAME_HEADER_LEN }>()
            .0
            .iter()
            .enumerate()
        {
            if i < 8 {
                assert_eq!(frame[8], RESULT_PUT);
                assert_eq!(frame[9], 0);
                assert_eq!(
                    u64::from_le_bytes(frame[12..20].try_into().unwrap()),
                    i as u64
                );
            } else {
                assert_eq!(frame[8], TERMINAL_ERROR);
                assert_eq!(frame[9], 3);
            }
        }
        store.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn monitoring_remains_available_during_data_saturation() {
        use tower::ServiceExt;
        let directory = tempfile::tempdir().unwrap();
        let mut config = crate::config::Config::default();
        config.storage.root = directory.path().to_path_buf();
        config.resources.max_requests = 1;
        config.resources.admission_timeout_ms = 30;
        let store = Coordinator::open(config).await.unwrap();
        let app = router_with_token(store.clone(), CancellationToken::new(), None).unwrap();
        let pending = app.clone();
        let task = tokio::spawn(async move {
            pending
                .oneshot(
                    axum::http::Request::put("/v1/objects")
                        .header(header::CONTENT_TYPE, "application/octet-stream")
                        .header(header::CONTENT_LENGTH, "1")
                        .body(Body::from_stream(futures_util::stream::pending::<
                            Result<Bytes, Infallible>,
                        >()))
                        .unwrap(),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let status = app
            .clone()
            .oneshot(
                axum::http::Request::get("/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let rejected = app
            .oneshot(
                axum::http::Request::get(
                    "/v1/objects/0000000000000000000000000000000000000000000000000000000000000000",
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(rejected.headers().contains_key(header::RETRY_AFTER));
        task.abort();
        store.shutdown().await.unwrap();
    }

    #[test]
    fn small_routing_hints_are_bounded_and_threshold_exact() {
        let mut config = crate::config::Config::default();
        config.streaming.threshold_mib = 1;
        assert!(buffered_object(&config, 1024 * 1024));
        assert!(!buffered_object(&config, 1024 * 1024 + 1));
        config.streaming.threshold_mib = 100;
        assert!(buffered_object(&config, config.max_object_bytes() as u64));
        assert!(!buffered_object(
            &config,
            config.max_object_bytes() as u64 + 1
        ));
        let hints = SmallRoutingHints::new();
        for i in 0..8192u32 {
            let mut hash = [0u8; 32];
            hash[..4].copy_from_slice(&i.to_le_bytes());
            hints.insert(hash);
        }
        let total: usize = hints.shards.iter().map(|shard| shard.lock().0.len()).sum();
        assert!(total <= 4096);
        assert!(hints.shards.iter().all(|shard| shard.lock().0.len() <= 256));
    }

    #[tokio::test]
    async fn small_routing_hint_does_not_preserve_deleted_object() {
        use tower::ServiceExt;
        let directory = tempfile::tempdir().unwrap();
        let mut config = crate::config::Config::default();
        config.storage.root = directory.path().to_path_buf();
        let store = Coordinator::open(config).await.unwrap();
        let hash = store
            .put(Bytes::from_static(b"small hinted object"))
            .await
            .unwrap()
            .hash;
        let app = router_with_token(store.clone(), CancellationToken::new(), None).unwrap();
        let path = format!("/v1/objects/{}", hex::encode(hash));
        let response = app
            .clone()
            .oneshot(axum::http::Request::get(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);
        store.delete(hash).await.unwrap();
        let response = app
            .oneshot(axum::http::Request::get(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cached_get_waits_for_coordinator_capacity_while_status_remains_available() {
        use tower::ServiceExt;
        let directory = tempfile::tempdir().unwrap();
        let mut config = crate::config::Config::default();
        config.storage.root = directory.path().to_path_buf();
        config.resources.max_requests = 1;
        let store = Coordinator::open(config).await.unwrap();
        let hash = store
            .put(Bytes::from_static(b"cached payload"))
            .await
            .unwrap()
            .hash;
        store.get(hash).await.unwrap();
        store.get(hash).await.unwrap();
        assert!(store.has_cached_payload(&hash));
        let app = router_with_token(store.clone(), CancellationToken::new(), None).unwrap();
        let held = store.try_request_permit().unwrap();
        let pending = app.clone();
        let request = tokio::spawn(async move {
            pending
                .oneshot(
                    axum::http::Request::get(format!("/v1/objects/{}", hex::encode(hash)))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!request.is_finished());
        let status = app
            .oneshot(
                axum::http::Request::get("/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        drop(held);
        let response = tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn streamed_http_round_trip_and_head() {
        use tower::ServiceExt;
        let directory = tempfile::tempdir().unwrap();
        let mut config = crate::config::Config::default();
        config.storage.root = directory.path().to_path_buf();
        config.streaming.threshold_mib = 1;
        config.streaming.min_free_mib = 0;
        let store = Coordinator::open(config).await.unwrap();
        let app = router_with_token(store.clone(), CancellationToken::new(), None).unwrap();
        let data = vec![42u8; 1024 * 1024 + 1];
        let hash = blake3::hash(&data).to_hex().to_string();
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::put("/v1/objects")
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .header(header::CONTENT_LENGTH, data.len())
                    .body(Body::from(data.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let path = format!("/v1/objects/{hash}");
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::head(&path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.headers()[header::CONTENT_LENGTH],
            data.len().to_string()
        );
        let response = app
            .oneshot(axum::http::Request::get(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), data.len() + 1)
                .await
                .unwrap()
                .as_ref(),
            data
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unread_stream_releases_resources_after_idle_timeout() {
        use tower::ServiceExt;
        let directory = tempfile::tempdir().unwrap();
        let mut config = crate::config::Config::default();
        config.storage.root = directory.path().to_path_buf();
        config.streaming.threshold_mib = 1;
        config.streaming.idle_timeout_seconds = 1;
        config.streaming.min_free_mib = 0;
        let store = Coordinator::open(config).await.unwrap();
        let app = router_with_token(store.clone(), CancellationToken::new(), None).unwrap();
        let data = vec![23u8; 3 * 1024 * 1024];
        let hash = blake3::hash(&data).to_hex().to_string();
        let put = app
            .clone()
            .oneshot(
                axum::http::Request::put("/v1/objects")
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .header(header::CONTENT_LENGTH, data.len())
                    .body(Body::from(data))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK);
        let response = app
            .oneshot(
                axum::http::Request::get(format!("/v1/objects/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if store.status().await.unwrap().io.inflight_reads == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("producer must release an unconsumed stream");
        drop(response);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn batch_storage_full_is_retryable_result() {
        let frame = Frame {
            kind: REQUEST_PUT,
            status: 0,
            ordinal: 7,
            hash: [0; 32],
            payload: Bytes::from_static(b"x"),
        };
        let budget = Arc::new(Semaphore::new(4));
        let permit = budget.clone().acquire_owned().await.unwrap();
        let limits = BatchWriteLimits {
            maximum: 4,
            window: 1,
            max_items: 10,
            admission_timeout: Duration::from_secs(1),
        };
        let stream = batch_put_stream(
            Body::from(encode_frame(&frame)),
            limits,
            budget,
            permit,
            |_| async { Err(crate::engine::StorageFull.into()) }.boxed(),
        );
        let output = axum::body::to_bytes(Body::from_stream(stream), 1024)
            .await
            .unwrap();
        assert_eq!(output[8], RESULT_PUT);
        assert_eq!(output[9], 5);
    }

    #[test]
    fn batch_source_rechecks_failure_after_select_guard_was_evaluated() {
        let write_failed = std::sync::atomic::AtomicBool::new(false);
        // The branch guard can be enabled at select construction time, then
        // pending.next may observe a failure before source.next yields a frame.
        let select_guard_was_enabled = !batch_source_stopped(false, &write_failed);
        assert!(select_guard_was_enabled);
        write_failed.store(true, Ordering::Release);
        assert!(batch_source_stopped(false, &write_failed));
    }

    #[test]
    fn active_batch_guard_releases_slot_when_queued_future_is_never_polled() {
        let active = Arc::new(AtomicUsize::new(1));
        let changed = Arc::new(Notify::new());
        let guard = ActiveBatchWrite {
            active: active.clone(),
            changed,
        };
        let queued = async move {
            let _active = guard;
            futures_util::future::pending::<()>().await;
        };
        drop(queued);
        assert_eq!(active.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn batch_memory_timeout_reports_retryable_terminal_status() {
        let frame = Frame {
            kind: REQUEST_PUT,
            status: 0,
            ordinal: 7,
            hash: [0; 32],
            payload: Bytes::from_static(b"abc"),
        };
        let budget = Arc::new(Semaphore::new(4));
        let initial = budget.clone().acquire_owned().await.unwrap();
        let held = budget.clone().acquire_many_owned(3).await.unwrap();
        let stream = batch_put_stream(
            Body::from(encode_frame(&frame)),
            BatchWriteLimits {
                maximum: 64,
                window: 2,
                max_items: 10,
                admission_timeout: Duration::from_millis(20),
            },
            budget,
            initial,
            |_| async { panic!("write must not run") }.boxed(),
        );
        let output = axum::body::to_bytes(Body::from_stream(stream), 1024)
            .await
            .unwrap();
        assert_eq!(output[8], TERMINAL_ERROR);
        assert_eq!(output[9], 5);
        drop(held);
    }

    #[tokio::test]
    async fn batch_small_payload_uses_actual_capacity() {
        let frame = Frame {
            kind: REQUEST_PUT,
            status: 0,
            ordinal: 7,
            hash: [0; 32],
            payload: Bytes::from_static(b"abc"),
        };
        let budget = Arc::new(Semaphore::new(4));
        let permit = budget.clone().acquire_owned().await.unwrap();
        let stream = batch_put_stream(
            Body::from(encode_frame(&frame)),
            BatchWriteLimits {
                maximum: 64,
                window: 2,
                max_items: 10,
                admission_timeout: Duration::from_secs(1),
            },
            budget.clone(),
            permit,
            |_| {
                async {
                    Ok(crate::engine::PutResult {
                        hash: [1; 32],
                        duplicate: false,
                    })
                }
                .boxed()
            },
        );
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            axum::body::to_bytes(Body::from_stream(stream), 1024),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!result.is_empty());
        assert_eq!(budget.available_permits(), 4);
    }

    #[tokio::test]
    async fn batch_refills_active_window_behind_blocked_earlier_ordinal_and_keeps_wire_order() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mut input = Vec::new();
        for ordinal in 0..3 {
            input.extend_from_slice(&encode_frame(&Frame {
                kind: REQUEST_PUT,
                status: 0,
                ordinal,
                hash: [0; 32],
                payload: Bytes::from(vec![ordinal as u8]),
            }));
        }
        let budget = Arc::new(Semaphore::new(16));
        let permit = budget.clone().acquire_owned().await.unwrap();
        let started = Arc::new(AtomicUsize::new(0));
        let started_notify = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let stream = batch_put_stream(
            Body::from(input),
            BatchWriteLimits {
                maximum: 4,
                window: 2,
                max_items: 10,
                admission_timeout: Duration::from_secs(1),
            },
            budget,
            permit,
            {
                let started = started.clone();
                let started_notify = started_notify.clone();
                let release_first = release_first.clone();
                move |payload| {
                    let started = started.clone();
                    let started_notify = started_notify.clone();
                    let release_first = release_first.clone();
                    async move {
                        let value = payload[0];
                        started.fetch_add(1, Ordering::SeqCst);
                        started_notify.notify_one();
                        if value == 0 {
                            release_first.notified().await;
                        }
                        Ok(crate::engine::PutResult {
                            hash: [value; 32],
                            duplicate: false,
                        })
                    }
                    .boxed()
                }
            },
        );
        let collector = tokio::spawn(async move {
            axum::body::to_bytes(Body::from_stream(stream), usize::MAX)
                .await
                .unwrap()
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while started.load(Ordering::SeqCst) < 3 {
                started_notify.notified().await;
            }
        })
        .await
        .expect("the third write must start while ordinal zero is still blocked");
        assert_eq!(started.load(Ordering::SeqCst), 3);
        release_first.notify_one();
        let bytes = tokio::time::timeout(Duration::from_secs(2), collector)
            .await
            .expect("ordered response should drain")
            .unwrap();
        let frames = bytes.as_chunks::<{ 4 + FRAME_HEADER_LEN }>().0;
        assert_eq!(frames.len(), 3);
        for (index, frame) in frames.iter().enumerate() {
            assert_eq!(frame[8], RESULT_PUT);
            assert_eq!(
                u64::from_le_bytes(frame[12..20].try_into().unwrap()),
                index as u64
            );
        }
    }

    #[tokio::test]
    async fn batch_bounds_completed_out_of_order_result_buffer() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mut input = Vec::new();
        for ordinal in 0..12 {
            input.extend_from_slice(&encode_frame(&Frame {
                kind: REQUEST_PUT,
                status: 0,
                ordinal,
                hash: [0; 32],
                payload: Bytes::from(vec![ordinal as u8]),
            }));
        }
        let budget = Arc::new(Semaphore::new(64));
        let permit = budget.clone().acquire_owned().await.unwrap();
        let started = Arc::new(AtomicUsize::new(0));
        let started_notify = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let stream = batch_put_stream(
            Body::from(input),
            BatchWriteLimits {
                maximum: 4,
                window: 2,
                max_items: 12,
                admission_timeout: Duration::from_secs(1),
            },
            budget,
            permit,
            {
                let started = started.clone();
                let started_notify = started_notify.clone();
                let release_first = release_first.clone();
                move |payload| {
                    let started = started.clone();
                    let started_notify = started_notify.clone();
                    let release_first = release_first.clone();
                    async move {
                        let value = payload[0];
                        started.fetch_add(1, Ordering::SeqCst);
                        started_notify.notify_one();
                        if value == 0 {
                            release_first.notified().await;
                        }
                        Ok(crate::engine::PutResult {
                            hash: [value; 32],
                            duplicate: false,
                        })
                    }
                    .boxed()
                }
            },
        );
        let collector = tokio::spawn(async move {
            axum::body::to_bytes(Body::from_stream(stream), usize::MAX)
                .await
                .unwrap()
        });

        // window=2 gives a bounded ordered-result capacity of 8. With the
        // first future blocked, only seven subsequent writes may be retained.
        tokio::time::timeout(Duration::from_secs(2), async {
            while started.load(Ordering::SeqCst) < 8 {
                started_notify.notified().await;
            }
        })
        .await
        .expect("bounded result buffer should fill");
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(started.load(Ordering::SeqCst), 8);
        release_first.notify_one();
        let bytes = tokio::time::timeout(Duration::from_secs(2), collector)
            .await
            .expect("all remaining writes should resume after the head completes")
            .unwrap();
        assert_eq!(started.load(Ordering::SeqCst), 12);
        let frames = bytes.as_chunks::<{ 4 + FRAME_HEADER_LEN }>().0;
        assert_eq!(frames.len(), 12);
        for (index, frame) in frames.iter().enumerate() {
            assert_eq!(
                u64::from_le_bytes(frame[12..20].try_into().unwrap()),
                index as u64
            );
        }
    }

    #[tokio::test]
    async fn batch_out_of_order_failure_stops_refill_and_drains_in_wire_order() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mut input = Vec::new();
        for ordinal in 0..4 {
            input.extend_from_slice(&encode_frame(&Frame {
                kind: REQUEST_PUT,
                status: 0,
                ordinal,
                hash: [0; 32],
                payload: Bytes::from(vec![ordinal as u8]),
            }));
        }
        let budget = Arc::new(Semaphore::new(16));
        let permit = budget.clone().acquire_owned().await.unwrap();
        let started = Arc::new(AtomicUsize::new(0));
        let started_notify = Arc::new(Notify::new());
        let failed_second = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let stream = batch_put_stream(
            Body::from(input),
            BatchWriteLimits {
                maximum: 4,
                window: 2,
                max_items: 10,
                admission_timeout: Duration::from_secs(1),
            },
            budget,
            permit,
            {
                let started = started.clone();
                let started_notify = started_notify.clone();
                let failed_second = failed_second.clone();
                let release_first = release_first.clone();
                move |payload| {
                    let started = started.clone();
                    let started_notify = started_notify.clone();
                    let failed_second = failed_second.clone();
                    let release_first = release_first.clone();
                    async move {
                        let value = payload[0];
                        started.fetch_add(1, Ordering::SeqCst);
                        started_notify.notify_one();
                        if value == 0 {
                            release_first.notified().await;
                        }
                        if value == 1 {
                            failed_second.notify_one();
                            Err(anyhow!("injected ordinal one failure"))
                        } else {
                            Ok(crate::engine::PutResult {
                                hash: [value; 32],
                                duplicate: false,
                            })
                        }
                    }
                    .boxed()
                }
            },
        );
        let collector = tokio::spawn(async move {
            axum::body::to_bytes(Body::from_stream(stream), usize::MAX)
                .await
                .unwrap()
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while started.load(Ordering::SeqCst) < 2 {
                started_notify.notified().await;
            }
            failed_second.notified().await;
        })
        .await
        .expect("ordinal one must complete with an error");
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(started.load(Ordering::SeqCst), 2);
        release_first.notify_one();
        let bytes = tokio::time::timeout(Duration::from_secs(2), collector)
            .await
            .expect("failed batch should drain")
            .unwrap();
        assert_eq!(started.load(Ordering::SeqCst), 2);
        let frames = bytes.as_chunks::<{ 4 + FRAME_HEADER_LEN }>().0;
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0][9], 0);
        assert_eq!(frames[1][9], 6);
        for (index, frame) in frames.iter().enumerate() {
            assert_eq!(
                u64::from_le_bytes(frame[12..20].try_into().unwrap()),
                index as u64
            );
        }
    }

    #[tokio::test]
    async fn batch_write_failure_drains_already_dispatched_results() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let mut input = Vec::new();
        for ordinal in 0..4 {
            input.extend_from_slice(&encode_frame(&Frame {
                kind: REQUEST_PUT,
                status: 0,
                ordinal,
                hash: [0; 32],
                payload: Bytes::from(vec![ordinal as u8]),
            }));
        }
        let budget = Arc::new(Semaphore::new(16));
        let permit = budget.clone().acquire_many_owned(4).await.unwrap();
        let dispatched = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let stream = batch_put_stream(
            Body::from(input),
            BatchWriteLimits {
                maximum: 4,
                window: 3,
                max_items: 10,
                admission_timeout: Duration::from_secs(1),
            },
            budget.clone(),
            permit,
            {
                let dispatched = dispatched.clone();
                move |payload| {
                    dispatched.fetch_add(1, Ordering::SeqCst);
                    let barrier = barrier.clone();
                    async move {
                        barrier.wait().await;
                        if payload[0] == 0 {
                            Err(anyhow!("injected write failure"))
                        } else {
                            Ok(crate::engine::PutResult {
                                hash: [payload[0]; 32],
                                duplicate: false,
                            })
                        }
                    }
                    .boxed()
                }
            },
        );
        let bytes = tokio::time::timeout(
            Duration::from_secs(2),
            axum::body::to_bytes(Body::from_stream(stream), usize::MAX),
        )
        .await
        .expect("queued writes should resolve")
        .unwrap();
        assert_eq!(dispatched.load(Ordering::SeqCst), 3);
        assert_eq!(budget.available_permits(), 16);
        assert_eq!(bytes.len(), 3 * (4 + FRAME_HEADER_LEN));
        for (i, frame) in bytes
            .as_chunks::<{ 4 + FRAME_HEADER_LEN }>()
            .0
            .iter()
            .enumerate()
        {
            assert_eq!(frame[8], RESULT_PUT);
            assert_eq!(frame[9], if i == 0 { 6 } else { 0 });
            assert_eq!(
                u64::from_le_bytes(frame[12..20].try_into().unwrap()),
                i as u64
            );
        }
    }
    #[tokio::test]
    async fn protocol_golden_and_fragmented_decode() {
        let frame = Frame {
            kind: REQUEST_PUT,
            status: 0,
            ordinal: 7,
            hash: [0; 32],
            payload: Bytes::from_static(b"abc"),
        };
        let encoded = encode_frame(&frame);
        assert_eq!(
            &encoded[..20],
            &[
                59, 0, 0, 0, b'S', b'K', b'B', b'1', 1, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0
            ]
        );
        assert_eq!(&encoded[52..60], &[3, 0, 0, 0, 0, 0, 0, 0]);
        let chunks: Vec<_> = encoded
            .iter()
            .map(|b| Ok::<_, Infallible>(Bytes::copy_from_slice(&[*b])))
            .collect();
        let mut decoder = Decoder::new(Body::from_stream(futures_util::stream::iter(chunks)));
        let decoded = decoder.next(REQUEST_PUT, 100).await.unwrap().unwrap();
        assert_eq!(decoded.payload, "abc");
        assert_eq!(decoded.ordinal, 7);
        assert!(decoder.next(REQUEST_PUT, 100).await.unwrap().is_none());
    }
    #[tokio::test]
    async fn protocol_rejects_truncation_and_length_mismatch() {
        let frame = Frame {
            kind: REQUEST_PUT,
            status: 0,
            ordinal: 0,
            hash: [0; 32],
            payload: Bytes::from_static(b"abc"),
        };
        let encoded = encode_frame(&frame);
        let mut decoder = Decoder::new(Body::from(encoded.slice(..encoded.len() - 1)));
        assert!(decoder.next(REQUEST_PUT, 100).await.is_err());
        let mut bad = encoded.to_vec();
        bad[52] = 4;
        let mut decoder = Decoder::new(Body::from(bad));
        assert!(decoder.next(REQUEST_PUT, 100).await.is_err());
    }
}

/// Validates the listener and credentials before the socket is opened.
pub fn validate_bootstrap(
    config: &crate::config::Config,
    credentials: &crate::mcp::Credentials,
) -> Result<()> {
    credentials.validate()?;
    let addr: SocketAddr = config
        .server
        .listen
        .parse()
        .context("invalid listen address")?;
    if !addr.ip().is_loopback() && credentials.service.is_none() {
        return Err(anyhow!("a non-loopback listener requires SKERRY_TOKEN"));
    }
    if let Some(address) = &config.server.admin_listen {
        let admin: SocketAddr = address.parse().context("invalid admin listen address")?;
        if !admin.ip().is_loopback() && credentials.service.is_none() {
            return Err(anyhow!(
                "a non-loopback admin listener requires SKERRY_TOKEN"
            ));
        }
    }
    Ok(())
}

type ReadyService = Arc<parking_lot::RwLock<Option<(Router, Arc<Coordinator>)>>>;
#[derive(Clone)]
struct Bootstrap {
    config: crate::config::Config,
    credentials: crate::mcp::Credentials,
    shutdown: CancellationToken,
    progress: Arc<crate::progress::Progress>,
    ready: ReadyService,
    error: Arc<parking_lot::RwLock<Option<String>>>,
    requests: Arc<Semaphore>,
    admin_requests: Arc<Semaphore>,
}

fn public_startup(app: &Bootstrap) -> serde_json::Value {
    let mut snapshot = app.progress.snapshot();
    if !snapshot.failures.is_empty() {
        snapshot.failures = vec!["storage initialization failed; inspect server logs".into()];
    }
    serde_json::to_value(snapshot).unwrap_or_default()
}

async fn bootstrap_admission(State(app): State<Bootstrap>, req: Request, next: Next) -> Response {
    let capacity = if matches!(
        req.uri().path(),
        "/v1/health" | "/v1/ready" | "/v1/status" | "/v1/metrics" | "/v1/cache/stats"
    ) {
        app.admin_requests.clone()
    } else {
        app.requests.clone()
    };
    let permit = match tokio::time::timeout(
        Duration::from_millis(app.config.resources.admission_timeout_ms),
        capacity.acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        _ => {
            return ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "request capacity exhausted; retry later".into(),
            )
            .into_response();
        }
    };
    let timeout = request_deadline(&app.config, &req);
    let response = match tokio::time::timeout(timeout, next.run(req)).await {
        Ok(response) => response,
        Err(_) => ApiError(
            StatusCode::REQUEST_TIMEOUT,
            "request deadline exceeded".into(),
        )
        .into_response(),
    };
    let (parts, body) = response.into_parts();
    let stream = async_stream::stream! {
        let _permit = permit;
        let mut source = body.into_data_stream();
        while let Some(chunk) = source.next().await { yield chunk; }
    };
    Response::from_parts(parts, Body::from_stream(stream))
}

async fn bootstrap_dispatch(State(app): State<Bootstrap>, req: Request) -> Response {
    use tower::ServiceExt;
    let path = req.uri().path();
    if path == "/v1/health" {
        return Json(serde_json::json!({"health":"ok","version":env!("CARGO_PKG_VERSION")}))
            .into_response();
    }
    if path == "/v1/ready" {
        let coordinator = app.ready.read().as_ref().map(|(_, c)| c.clone());
        let ready = if let Some(coordinator) = coordinator {
            coordinator.is_ready()
        } else {
            false
        };
        return (
            if ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            Json(serde_json::json!({"ready":ready})),
        )
            .into_response();
    }
    let credentials = &app.credentials;
    let supplied = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    let all = [
        &credentials.service,
        &credentials.read,
        &credentials.write,
        &credentials.maintenance,
        &credentials.gateway,
    ];
    let has = all.iter().any(|v| v.is_some());
    let admin = credentials
        .service
        .as_ref()
        .is_some_and(|v| bool::from(supplied.as_bytes().ct_eq(v.as_bytes())))
        || (!has && supplied.is_empty());
    let scoped = [
        &credentials.read,
        &credentials.write,
        &credentials.maintenance,
    ]
    .iter()
    .any(|v| {
        v.as_ref()
            .is_some_and(|v| bool::from(supplied.as_bytes().ct_eq(v.as_bytes())))
    });
    if !admin && !scoped && path != app.config.mcp.http_path && path != "/v1/mcp/status" {
        return ApiError(StatusCode::UNAUTHORIZED, "bearer token required".into()).into_response();
    }
    if path == "/v1/indexes" && !admin {
        return ApiError(
            StatusCode::FORBIDDEN,
            "operation is not authorized for this token".into(),
        )
        .into_response();
    }
    if path == "/v1/status" || path == "/v1/admin/shutdown" {
        if !admin {
            return ApiError(
                StatusCode::FORBIDDEN,
                "operation is not authorized for this token".into(),
            )
            .into_response();
        }
        if path == "/v1/admin/shutdown" && req.method() == axum::http::Method::POST {
            app.shutdown.cancel();
            return Json(serde_json::json!({"status":"shutting down"})).into_response();
        }
        if path == "/v1/status" && req.method() == axum::http::Method::GET {
            let router = app.ready.read().as_ref().map(|(router, _)| router.clone());
            let mut value = if let Some(router) = router {
                let response = router
                    .oneshot(req)
                    .await
                    .unwrap_or_else(|never| match never {});
                match axum::body::to_bytes(response.into_body(), 1024 * 1024).await {
                    Ok(bytes) => {
                        serde_json::from_slice(&bytes).unwrap_or_else(|_| serde_json::json!({}))
                    }
                    Err(_) => serde_json::json!({}),
                }
            } else {
                serde_json::json!({})
            };
            let error = app.error.read().clone();
            value["lifecycle"] = serde_json::json!(if error.is_some() {
                "failed"
            } else if app.ready.read().is_some() {
                "ready"
            } else {
                "starting"
            });
            value["startup"] = public_startup(&app);
            if let Some(error) = error {
                value["startup_error"] = serde_json::json!(error);
            }
            return Json(value).into_response();
        }
    }
    let router = app.ready.read().as_ref().map(|(r, _)| r.clone());
    if let Some(router) = router {
        return router
            .oneshot(req)
            .await
            .unwrap_or_else(|never| match never {});
    }
    if !admin {
        return ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage is not ready".into(),
        )
        .into_response();
    }
    let failed = app.error.read().is_some();
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "error": "storage is not ready",
            "lifecycle": if failed { "failed" } else { "starting" },
            "startup": public_startup(&app),
        })),
    )
        .into_response()
}

async fn admin_dispatch(State(app): State<Bootstrap>, req: Request) -> Response {
    if !matches!(
        req.uri().path(),
        "/v1/health"
            | "/v1/ready"
            | "/v1/status"
            | "/v1/metrics"
            | "/v1/cache/stats"
            | "/v1/mcp/status"
            | "/v1/admin/shutdown"
    ) {
        return StatusCode::NOT_FOUND.into_response();
    }
    bootstrap_dispatch(State(app), req).await
}

/// Serve immediately while the coordinator opens in the background.
pub async fn serve_bootstrap(
    config: crate::config::Config,
    credentials: crate::mcp::Credentials,
    progress: Arc<crate::progress::Progress>,
    shutdown: CancellationToken,
    listener: tokio::net::TcpListener,
) -> Result<()> {
    validate_bootstrap(&config, &credentials)?;
    let app = Bootstrap {
        config: config.clone(),
        credentials: credentials.clone(),
        shutdown: shutdown.clone(),
        progress: progress.clone(),
        ready: Arc::new(parking_lot::RwLock::new(None)),
        error: Arc::new(parking_lot::RwLock::new(None)),
        requests: Arc::new(Semaphore::new(config.resources.max_requests)),
        admin_requests: Arc::new(Semaphore::new(config.resources.admin_max_requests)),
    };
    let admin_listener = if let Some(address) = &config.server.admin_listen {
        Some(
            tokio::net::TcpListener::bind(address)
                .await
                .context("cannot bind admin listener")?,
        )
    } else {
        None
    };
    let startup_app = app.clone();
    let startup = tokio::spawn(async move {
        match Coordinator::open_with_progress(config, progress.clone()).await {
            Ok(store) => {
                if startup_app.shutdown.is_cancelled() {
                    let _ = store.shutdown().await;
                    return;
                }
                match router_with_mcp_credentials(
                    store.clone(),
                    startup_app.shutdown.clone(),
                    credentials,
                ) {
                    Ok(router) => {
                        let installed = {
                            let mut ready = startup_app.ready.write();
                            if startup_app.shutdown.is_cancelled() {
                                false
                            } else {
                                *ready = Some((router, store.clone()));
                                true
                            }
                        };
                        if installed {
                            progress.finish();
                        } else {
                            let _ = store.shutdown().await;
                        }
                    }
                    Err(_) => {
                        *startup_app.error.write() = Some("API initialization failed".into());
                        progress.fail("API initialization failed");
                        let _ = store.shutdown().await;
                    }
                }
            }
            Err(error) => {
                tracing::error!(error = %error, "storage startup failed");
                *startup_app.error.write() =
                    Some("storage initialization failed; inspect server logs".into());
                progress.fail("storage initialization failed; inspect server logs");
            }
        }
    });
    let force = CancellationToken::new();
    let listener = DrainListener {
        listener,
        force: force.clone(),
        connections: Arc::new(Semaphore::new(app.config.resources.max_requests * 2)),
    };
    let router = Router::new()
        .fallback(bootstrap_dispatch)
        .with_state(app.clone())
        .layer(middleware::from_fn_with_state(
            app.clone(),
            bootstrap_admission,
        ));
    let admin_server = admin_listener.map(|listener| {
        let admin_router = Router::new()
            .fallback(admin_dispatch)
            .with_state(app.clone())
            .layer(middleware::from_fn_with_state(
                app.clone(),
                bootstrap_admission,
            ));
        let listener = DrainListener {
            listener,
            force: force.clone(),
            connections: Arc::new(Semaphore::new(app.config.resources.admin_max_requests * 2)),
        };
        let stop = shutdown.clone();
        tokio::spawn(async move {
            axum::serve(listener, admin_router)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await
        })
    });
    let server = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    tokio::pin!(server);
    let result = tokio::select! { result = &mut server => result.context("HTTP server failed"), _ = shutdown.cancelled() => {
        app.progress.cancel();
        match tokio::time::timeout(Duration::from_secs(app.config.server.shutdown_timeout_seconds), &mut server).await {
            Ok(result) => result.context("HTTP server failed"), Err(_) => { force.cancel(); Err(anyhow!("HTTP drain timeout")) }
        }
    }};
    shutdown.cancel();
    if let Some(admin_server) = admin_server {
        let _ = tokio::time::timeout(
            Duration::from_secs(app.config.server.shutdown_timeout_seconds),
            admin_server,
        )
        .await;
    }
    app.progress.cancel();
    let coordinator = app.ready.write().take().map(|(_, c)| c);
    if let Some(coordinator) = coordinator {
        coordinator.shutdown().await?;
    }
    // Recovery checks cancellation between bounded units of work.
    if tokio::time::timeout(
        Duration::from_secs(app.config.server.shutdown_timeout_seconds),
        startup,
    )
    .await
    .is_err()
    {
        tracing::warn!("storage startup did not stop before the shutdown deadline");
    }
    result
}

#[cfg(test)]
mod bootstrap_tests {
    use super::*;
    use tower::ServiceExt;

    #[tokio::test]
    async fn starting_api_reports_liveness_readiness_and_requires_admin() {
        let shutdown = CancellationToken::new();
        let progress = Arc::new(crate::progress::Progress::new());
        let app = Bootstrap {
            config: crate::config::Config::default(),
            credentials: crate::mcp::Credentials {
                service: Some("secret".into()),
                read: Some("reader".into()),
                ..Default::default()
            },
            shutdown: shutdown.clone(),
            progress,
            ready: Arc::new(parking_lot::RwLock::new(None)),
            error: Arc::new(parking_lot::RwLock::new(None)),
            requests: Arc::new(Semaphore::new(8)),
            admin_requests: Arc::new(Semaphore::new(16)),
        };
        let router = Router::new()
            .fallback(bootstrap_dispatch)
            .with_state(app.clone());
        for (path, token, expected) in [
            ("/v1/health", None, StatusCode::OK),
            ("/v1/ready", None, StatusCode::SERVICE_UNAVAILABLE),
            ("/v1/status", None, StatusCode::UNAUTHORIZED),
            ("/v1/status", Some("secret"), StatusCode::OK),
            (
                "/v1/indexes",
                Some("secret"),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            let mut request = Request::builder().uri(path);
            if let Some(token) = token {
                request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
            }
            let response = router
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{path}");
        }
        for (path, token, expected) in [
            ("/v1/mcp/status", None, StatusCode::SERVICE_UNAVAILABLE),
            ("/v1/indexes", Some("reader"), StatusCode::FORBIDDEN),
            (
                "/v1/objects/abc",
                Some("reader"),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            let mut request = Request::builder().uri(path);
            if let Some(token) = token {
                request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
            }
            let response = router
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            let body = axum::body::to_bytes(response.into_body(), 8192)
                .await
                .unwrap();
            assert!(!String::from_utf8_lossy(&body).contains("startup"));
        }
        *app.error.write() = Some("storage initialization failed".into());
        app.progress.fail("failed to read /private/store/path");
        let request = Request::builder()
            .uri("/v1/status")
            .header(header::AUTHORIZATION, "Bearer secret")
            .body(Body::empty())
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["lifecycle"], "failed");
        assert!(!String::from_utf8_lossy(&bytes).contains("/private/store/path"));
        let request = Request::builder()
            .method("POST")
            .uri("/v1/admin/shutdown")
            .header(header::AUTHORIZATION, "Bearer secret")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router.oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );
        assert!(shutdown.is_cancelled());
    }
    #[tokio::test]
    async fn index_listing_requires_admin_and_supports_pagination() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::config::Config::default();
        config.storage.root = dir.path().to_path_buf();
        let coordinator = Coordinator::open(config).await.unwrap();
        let fake = format!("generation-{}", uuid::Uuid::new_v4());
        std::fs::create_dir(dir.path().join("index").join(fake)).unwrap();
        let credentials = crate::mcp::Credentials {
            service: Some("admin".into()),
            read: Some("reader".into()),
            ..Default::default()
        };
        let router =
            router_with_mcp_credentials(coordinator.clone(), CancellationToken::new(), credentials)
                .unwrap();
        for (token, expected) in [
            (None, StatusCode::UNAUTHORIZED),
            (Some("reader"), StatusCode::FORBIDDEN),
            (Some("admin"), StatusCode::OK),
        ] {
            let mut request = Request::builder().uri("/v1/indexes?limit=1");
            if let Some(token) = token {
                request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
            }
            let response = router
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            if expected == StatusCode::OK {
                let body = axum::body::to_bytes(response.into_body(), 16384)
                    .await
                    .unwrap();
                let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let cursor = page["next_cursor"].as_str().unwrap();
                let request = Request::builder()
                    .uri(format!("/v1/indexes?limit=1&after={cursor}"))
                    .header(header::AUTHORIZATION, "Bearer admin")
                    .body(Body::empty())
                    .unwrap();
                let response = router.clone().oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                let body = axum::body::to_bytes(response.into_body(), 16384)
                    .await
                    .unwrap();
                let next: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(next["indexes"].as_array().unwrap().len(), 1);
                assert!(next["next_cursor"].is_null());
            }
        }
        coordinator.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn failed_storage_start_keeps_api_available_and_stoppable() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("not-a-directory");
        std::fs::write(&root, b"file").unwrap();
        let mut config = crate::config::Config::default();
        config.storage.root = root;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        config.server.listen = listener.local_addr().unwrap().to_string();
        let url = format!("http://{}", config.server.listen);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(serve_bootstrap(
            config,
            Default::default(),
            Arc::new(crate::progress::Progress::new()),
            cancel,
            listener,
        ));
        let client = reqwest::Client::new();
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(response) = client.get(format!("{url}/v1/status")).send().await {
                    let status: serde_json::Value = response.json().await.unwrap();
                    if status["lifecycle"] == "failed" {
                        break status;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(status["lifecycle"], "failed");
        assert_eq!(
            client
                .get(format!("{url}/v1/health"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client
                .get(format!("{url}/v1/ready"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            client
                .post(format!("{url}/v1/admin/shutdown"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
