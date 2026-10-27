//! Versioned HTTP API and bounded streaming batch transport.
use std::{convert::Infallible, future::IntoFuture, net::SocketAddr, sync::Arc, time::Duration};

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
use tokio::sync::Semaphore;
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

#[derive(Clone)]
struct App {
    store: Arc<Coordinator>,
    shutdown: CancellationToken,
    requests: Arc<Semaphore>,
    upload_bytes: Arc<Semaphore>,
    token: Option<Arc<str>>,
    mcp_credentials: crate::mcp::Credentials,
}

#[derive(Debug)]
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({"error": self.1}))).into_response()
    }
}

fn internal(error: anyhow::Error) -> ApiError {
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
    let app = App {
        requests: Arc::new(Semaphore::new(store.config.resources.max_requests)),
        upload_bytes: Arc::new(Semaphore::new(upload_budget)),
        token: token.map(Arc::from),
        mcp_credentials: credentials,
        store,
        shutdown,
    };
    Ok(Router::new()
        .route("/v1/health", get(health))
        .route("/v1/status", get(status))
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
        .route("/v1/admin/shutdown", post(shutdown_handler))
        .with_state(app.clone())
        .merge(mcp)
        .layer(middleware::from_fn_with_state(app, admission)))
}

async fn admission(State(app): State<App>, req: Request, next: Next) -> Response {
    let path = req.uri().path();
    if path != "/v1/health" && path != app.store.config.mcp.http_path && path != "/v1/mcp/status" {
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
            || (req.method() == axum::http::Method::POST && path == "/v1/admin/gc"))
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
    let permit = match app.requests.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "request capacity exhausted; retry later".into(),
            )
            .into_response();
        }
    };
    let timeout = Duration::from_secs(app.store.config.server.request_timeout_seconds);
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

async fn health(State(app): State<App>) -> ApiResult {
    let status = app.store.status().await.map_err(internal)?;
    let ok = status.health == "ok" || status.health == "running" || status.health == "healthy";
    Ok((
        if ok {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(serde_json::json!({"health":status.health,"version":status.version})),
    )
        .into_response())
}

async fn status(State(app): State<App>) -> ApiResult {
    Ok(Json(app.store.status().await.map_err(internal)?).into_response())
}

async fn metrics(State(app): State<App>) -> ApiResult {
    let status = app.store.status().await.map_err(internal)?;
    let mut text = app.store.metrics();
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
    Ok(Json(app.store.status().await.map_err(internal)?.cache).into_response())
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
    app.upload_bytes
        .clone()
        .try_acquire_many_owned(bytes as u32)
        .map_err(|_| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "upload capacity exhausted; retry later".into(),
            )
        })
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
    let max = app.store.config.max_object_bytes();
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
    let limit = declared.unwrap_or(max);
    let _permit = upload_permit(&app, limit).await?;
    let mut source = body.into_data_stream();
    // Allocate only the reserved amount; geometric growth can exceed the ingress budget.
    let mut data = BytesMut::with_capacity(limit);
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

async fn delete_object(State(app): State<App>, Path(hash): Path<String>) -> ApiResult {
    let hash = hash_from_text(&hash)?;
    require_v2(&app)?;
    app.store.delete(hash).await.map_err(internal)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn get_object(
    State(app): State<App>,
    Path(hash): Path<String>,
    Query(options): Query<ReadOptions>,
) -> ApiResult {
    let hash = hash_from_text(&hash)?;
    let priority = parse_priority(options.priority.as_deref())?;
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
    if !app.store.exists(hash).await.map_err(internal)? {
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

    async fn next(&mut self, kind: u8, maximum: usize) -> Result<Option<Frame>> {
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
        let payload = if payload_len == 0 {
            Bytes::new()
        } else {
            self.exact(payload_len as usize, false)
                .await?
                .expect("required payload")
        };
        Ok(Some(Frame {
            kind,
            status: 0,
            ordinal,
            hash,
            payload,
        }))
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
    let permit = upload_permit(&app, maximum).await?;
    let window = app.store.config.writes.batch_max_records.clamp(1, 64);
    let max_items = app.store.config.resources.max_batch_items;
    let store = app.store.clone();
    let stream = batch_put_stream(
        body,
        maximum,
        window,
        max_items,
        app.upload_bytes.clone(),
        permit,
        move |payload| {
            let store = store.clone();
            async move { store.put(payload).await }.boxed()
        },
    );
    Ok(batch_response(stream))
}

fn batch_put_stream(
    body: Body,
    maximum: usize,
    window: usize,
    max_items: usize,
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
        // The decoder reserves a maximum object before reading; each completed frame
        // keeps only its payload reservation while the write is outstanding.
        // FuturesOrdered polls all writes concurrently while preserving response order.
        let mut slot = Some(permit);
        let mut decoder = Decoder::new(body);
        let source = async_stream::stream! {
            loop {
                match decoder.next(REQUEST_PUT, maximum).await {
                    Ok(Some(frame)) => yield Ok(frame),
                    Ok(None) => break,
                    Err(error) => { yield Err(error); break; }
                }
            }
        };
        futures_util::pin_mut!(source);
        let mut pending: FuturesOrdered<futures_util::future::BoxFuture<'static, (u64, anyhow::Result<crate::engine::PutResult>)>> = FuturesOrdered::new();
        let mut count = 0;
        let mut ended = false;
        let mut terminal_status = None;
        loop {
            if ended && pending.is_empty() { break; }
            tokio::select! {
                result = pending.next(), if !pending.is_empty() => {
                    if let Some((ordinal, result)) = result {
                        match result {
                            Ok(result) => yield Ok(Bytes::from(frame_header(RESULT_PUT, 0, ordinal, result.hash, 0))),
                            Err(error) => {
                                tracing::error!(%error, "batch write failed");
                                let status = if error.downcast_ref::<ResourceExhausted>().is_some() { 5 } else { 6 };
                                yield Ok(Bytes::from(frame_header(RESULT_PUT, status, ordinal, [0;32], 0)));
                                terminal_status = None;
                                ended = true;
                                // A suspended decoder may retain a partial payload.
                                // Keep its reservation until the source is dropped.
                            }
                        }
                    }
                }
                permit = upload_bytes.clone().acquire_many_owned(maximum as u32), if !ended && slot.is_none() && pending.len() < window => {
                    slot = Some(permit.expect("private upload semaphore is never closed"));
                }
                next = source.next(), if !ended && slot.is_some() && pending.len() < window => {
                    match next {
                        None => { ended = true; slot = None; }
                        Some(Err(error)) => {
                            tracing::warn!(%error, "invalid batch upload");
                            ended = true;
                            slot = None;
                            terminal_status = Some(3);
                        }
                        Some(Ok(frame)) => {
                            count += 1;
                            if count > max_items {
                                ended = true;
                                slot = None;
                                terminal_status = Some(5);
                            } else {
                                let mut reservation = slot.take().expect("decoder slot reserved");
                                let permit = reservation.split(frame.payload.len().max(1))
                                    .expect("frame fits the decoder reservation");
                                drop(reservation);
                                let write = write(frame.payload);
                                pending.push_back(async move {
                                    let _permit = permit;
                                    let result = write.await;
                                    (frame.ordinal, result)
                                }.boxed());
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
            upload_bytes: capacity.clone(),
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
        for (i, frame) in bytes.chunks_exact(4 + FRAME_HEADER_LEN).enumerate() {
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
        let stream = batch_put_stream(Body::from(input), 4, 3, 10, budget.clone(), permit, {
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
        });
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
        for (i, frame) in bytes.chunks_exact(4 + FRAME_HEADER_LEN).enumerate() {
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
