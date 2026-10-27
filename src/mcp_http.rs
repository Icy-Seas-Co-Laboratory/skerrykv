//! Streamable HTTP security and resource envelope around the official SDK.
use crate::{
    coordinator::Coordinator,
    mcp::{Credentials, Handler, Principal, Runtime},
};
use anyhow::{Result, ensure};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::StreamExt;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::never::NeverSessionManager,
};
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct Guard {
    store: Arc<Coordinator>,
    credentials: Credentials,
    shutdown: CancellationToken,
}
struct Activity {
    runtime: Arc<Runtime>,
}
impl Drop for Activity {
    fn drop(&mut self) {
        self.runtime.active.fetch_sub(1, Ordering::Relaxed);
        self.runtime.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}
fn error(status: StatusCode, message: &str) -> Response {
    (status, axum::Json(serde_json::json!({"error":message}))).into_response()
}
fn bearer(headers: &HeaderMap) -> &str {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}
fn equal(supplied: &str, expected: &Option<String>) -> bool {
    expected
        .as_ref()
        .is_some_and(|expected| bool::from(supplied.as_bytes().ct_eq(expected.as_bytes())))
}
impl Guard {
    fn principal(&self, headers: &HeaderMap) -> Option<Principal> {
        let token = bearer(headers);
        let key = |text: &str| *blake3::hash(text.as_bytes()).as_bytes();
        if self.store.config.mcp.auth_gateway {
            if !equal(token, &self.credentials.gateway) {
                return None;
            }
            let subject = headers.get("x-skerry-mcp-subject")?.to_str().ok()?;
            if subject.is_empty()
                || subject.len() > 128
                || !subject.bytes().all(|c| c.is_ascii_graphic())
            {
                return None;
            }
            let scopes = headers.get("x-skerry-mcp-scopes")?.to_str().ok()?;
            if scopes.len() > 128 {
                return None;
            }
            let mut p = Principal {
                key: key(subject),
                read: false,
                write: false,
                maintenance: false,
            };
            for scope in scopes.split_whitespace() {
                match scope {
                    "read" => p.read = true,
                    "write" => p.write = true,
                    "maintenance" => p.maintenance = true,
                    _ => return None,
                }
            }
            // Read is a required baseline; no implicit privilege escalations.
            return p.read.then_some(p);
        }
        let (read, write, maintenance) = if equal(token, &self.credentials.write) {
            (true, true, false)
        } else if equal(token, &self.credentials.maintenance) {
            (true, false, true)
        } else if equal(token, &self.credentials.read)
            || equal(token, &self.credentials.service)
            || (token.is_empty()
                && self.credentials.service.is_none()
                && self.credentials.read.is_none()
                && self.credentials.write.is_none()
                && self.credentials.maintenance.is_none())
        {
            (true, false, false)
        } else {
            return None;
        };
        Some(Principal {
            key: key(if token.is_empty() {
                "anonymous-loopback"
            } else {
                token
            }),
            read,
            write,
            maintenance,
        })
    }
}
/// Returns only MCP routes; the caller wraps them in the existing global HTTP admission layer.
pub fn router(
    store: Arc<Coordinator>,
    shutdown: CancellationToken,
    credentials: Credentials,
) -> Result<Router> {
    credentials.validate()?;
    let config = &store.config.mcp;
    let addr: SocketAddr = store.config.server.listen.parse()?;
    if config.enabled && !addr.ip().is_loopback() {
        ensure!(
            config.auth_gateway,
            "non-loopback MCP requires a compatible authentication gateway; disable MCP or configure mcp.auth_gateway and external_url"
        );
    }
    if config.enabled && config.auth_gateway {
        ensure!(
            credentials.gateway.is_some(),
            "MCP gateway requires SKERRY_MCP_GATEWAY_TOKEN"
        );
    }
    let state = Guard {
        store: store.clone(),
        credentials,
        shutdown: shutdown.clone(),
    };
    let mut router = Router::new().route("/v1/mcp/status", get(mcp_status));
    if config.enabled {
        let mut sdk = StreamableHttpServerConfig::default();
        sdk.legacy_session_mode = false;
        sdk.json_response = true;
        sdk.cancellation_token = shutdown;
        sdk.max_request_body_bytes = config.max_request_kib * 1024;
        // Our guard strictly validates exact allowed origins, including an empty deny list.
        // SDK host validation adds a DNS-rebinding check even for clients without Origin.
        if let Some(external) = &config.external_url {
            let url = reqwest::Url::parse(external)?;
            if let Some(host) = url.host_str() {
                sdk.allowed_hosts.push(host.to_owned());
            }
        }
        let factory_store = store.clone();
        let service = StreamableHttpService::new(
            move || {
                Ok(Handler {
                    store: factory_store.clone(),
                })
            },
            Arc::new(NeverSessionManager::default()),
            sdk,
        );
        router = router.route_service(&config.http_path, service);
    }
    Ok(router
        .layer(middleware::from_fn_with_state(state.clone(), guard))
        .with_state(state))
}
async fn mcp_status(State(state): State<Guard>) -> axum::Json<crate::mcp::McpStatus> {
    axum::Json(state.store.mcp.status())
}
async fn guard(State(state): State<Guard>, mut request: Request, next: Next) -> Response {
    let runtime = state.store.mcp.clone();
    if state.shutdown.is_cancelled() {
        return error(StatusCode::SERVICE_UNAVAILABLE, "service is draining");
    }
    if [
        "authorization",
        "x-skerry-mcp-subject",
        "x-skerry-mcp-scopes",
    ]
    .iter()
    .any(|key| request.headers().get_all(*key).iter().count() > 1)
    {
        return error(StatusCode::BAD_REQUEST, "duplicate authentication headers");
    }
    if request.headers().get_all("origin").iter().count() > 1 {
        return error(StatusCode::FORBIDDEN, "invalid Origin");
    }
    if let Some(origin) = request.headers().get("origin") {
        let valid = origin
            .to_str()
            .ok()
            .and_then(|v| reqwest::Url::parse(v).ok())
            .filter(|u| {
                u.username().is_empty()
                    && u.password().is_none()
                    && u.path() == "/"
                    && u.query().is_none()
                    && u.fragment().is_none()
            })
            .is_some_and(|origin| {
                state
                    .store
                    .config
                    .mcp
                    .allowed_origins
                    .iter()
                    .filter_map(|v| reqwest::Url::parse(v).ok())
                    .any(|allowed| origin.origin() == allowed.origin())
            });
        if !valid {
            return error(StatusCode::FORBIDDEN, "Origin is not allowed");
        }
    }
    let principal = match state.principal(request.headers()) {
        Some(p) => p,
        None => {
            runtime.auth_failures.fetch_add(1, Ordering::Relaxed);
            return error(StatusCode::UNAUTHORIZED, "MCP authentication required");
        }
    };
    if !runtime.allow_rate(principal.key) {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "MCP request rate exceeded; retry later",
        );
    }
    let concurrency = match runtime.concurrent.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "MCP request capacity exhausted; retry later",
            );
        }
    };
    let capacity = state.store.config.mcp.max_encoded_response_kib * 1024;
    let response_permit = match runtime
        .responses
        .clone()
        .try_acquire_many_owned(capacity as u32)
    {
        Ok(p) => p,
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "MCP response capacity exhausted; retry later",
            );
        }
    };
    runtime.active.fetch_add(1, Ordering::Relaxed);
    runtime.inflight.fetch_add(1, Ordering::Relaxed);
    let activity = Activity { runtime };
    request.extensions_mut().insert(principal);
    let timeout = Duration::from_secs(state.store.config.mcp.tool_timeout_seconds + 1);
    let response = match tokio::time::timeout(timeout, next.run(request)).await {
        Ok(r) => r,
        Err(_) => {
            return error(
                StatusCode::REQUEST_TIMEOUT,
                "MCP request execution deadline exceeded",
            );
        }
    };
    let (parts, body) = response.into_parts();
    // SDK responses are unary JSON in stateless mode. Bound serialized bytes before
    // exposing a result and retain its reservation through network ownership.
    let mut chunks = body.into_data_stream();
    let mut data = Vec::with_capacity(capacity);
    let collect = async {
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|_| ())?;
            if chunk.len() > capacity.saturating_sub(data.len()) {
                return Err(());
            }
            data.extend_from_slice(&chunk);
        }
        Ok::<_, ()>(())
    };
    if !matches!(tokio::time::timeout(timeout, collect).await, Ok(Ok(()))) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "MCP encoded response limit or execution deadline exceeded",
        );
    }
    struct ResponseBuffer {
        data: Vec<u8>,
        _permit: tokio::sync::OwnedSemaphorePermit,
        _concurrency: tokio::sync::OwnedSemaphorePermit,
        _activity: Activity,
    }
    impl AsRef<[u8]> for ResponseBuffer {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }
    let bytes = Bytes::from_owner(ResponseBuffer {
        data,
        _permit: response_permit,
        _concurrency: concurrency,
        _activity: activity,
    });
    Response::from_parts(parts, Body::from(bytes))
}
