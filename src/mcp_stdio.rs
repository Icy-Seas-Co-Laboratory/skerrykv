//! Stdio MCP client bridge. This process is deliberately a pure client: all
//! storage operations are sent to a running SkerryKV service over HTTP.

use anyhow::{Context, Result, bail};
use rmcp::{
    ServerHandler, ServiceExt,
    model::*,
    service::{RequestContext, RoleClient, RoleServer, RunningService},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use std::{
    borrow::Cow,
    collections::HashMap,
    io,
    sync::{Arc, Mutex as StdMutex},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_RESPONSE_BYTES: usize = 512 * 1024;
const MAX_STDIO_LINE_BYTES: usize = 1024 * 1024;
const JSONRPC_ENVELOPE_RESERVE: usize = 4 * 1024;

/// Connect stdio MCP to the running HTTP MCP endpoint. No local database is
/// opened and no coordinator or store lock is acquired here.
pub async fn run(connect: &str, path: &str) -> Result<()> {
    let endpoint = validate_endpoint(connect, path)?;
    let token = std::env::var("SKERRY_MCP_TOKEN")
        .ok()
        .or_else(|| std::env::var("SKERRY_TOKEN").ok());
    let mut config = StreamableHttpClientTransportConfig::with_uri(endpoint.as_str())
        .max_concurrent_requests(16)
        .max_sse_event_size(MAX_RESPONSE_BYTES);
    // The SDK reqwest client rejects cross-origin redirects and only follows
    // same-origin 307/308 redirects, preserving auth/session boundaries.
    if let Some(token) = token {
        config = config.auth_header(token);
    }
    let transport = StreamableHttpClientTransport::from_config(config);
    let remote = ()
        .serve(transport)
        .await
        .map_err(|_| anyhow::anyhow!("could not establish MCP connection to SkerryKV service"))?;
    let proxy = Proxy {
        remote: Arc::new(remote),
        inflight: Arc::new(Semaphore::new(16)),
    };
    let (stdin, stdout) = rmcp::transport::io::stdio();
    let transport = BoundedStdioTransport::new(stdin, stdout);
    let server = proxy
        .serve(transport)
        .await
        .context("starting stdio MCP bridge")?;
    server.waiting().await.context("stdio MCP bridge failed")?;
    Ok(())
}

fn validate_endpoint(connect: &str, path: &str) -> Result<reqwest::Url> {
    let mut base = reqwest::Url::parse(connect).context("invalid --connect URL")?;
    let authority = base
        .as_str()
        .split_once("://")
        .map(|(_, rest)| rest.split('/').next().unwrap_or_default())
        .unwrap_or_default();
    if !matches!(base.scheme(), "http" | "https")
        || authority.contains('@')
        || !base.username().is_empty()
        || base.password().is_some()
        || base.query().is_some()
        || base.fragment().is_some()
    {
        bail!("--connect must be an HTTP(S) URL without credentials, query, or fragment");
    }
    let loopback = base.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if base.scheme() == "http" && !loopback {
        bail!("plain HTTP MCP connections are allowed only to loopback hosts; use HTTPS");
    }
    if path.is_empty()
        || !path.starts_with('/')
        || path.contains('?')
        || path.contains('#')
        || path.contains("..")
    {
        bail!("--http-path must be an absolute path without query, fragment, or parent segments");
    }
    base.set_path(&format!("{}{}", base.path().trim_end_matches('/'), path));
    Ok(base)
}

struct Proxy {
    remote: Arc<RunningService<RoleClient, ()>>,
    inflight: Arc<Semaphore>,
}
impl Proxy {
    fn err(e: impl std::fmt::Display) -> rmcp::ErrorData {
        // Do not include URLs, credentials, filesystem paths, or backend detail.
        tracing::warn!("MCP upstream request failed");
        let _ = e;
        rmcp::ErrorData::internal_error("SkerryKV MCP upstream request failed", None)
    }
    fn bounded<T: serde::Serialize>(value: T) -> Result<T, rmcp::ErrorData> {
        let size = serde_json::to_vec(&value).map_err(Self::err)?.len();
        if size > MAX_RESPONSE_BYTES.saturating_sub(JSONRPC_ENVELOPE_RESERVE) {
            return Err(rmcp::ErrorData::internal_error(
                "MCP response exceeds configured size limit",
                None,
            ));
        }
        Ok(value)
    }
    fn permit(&self) -> Result<OwnedSemaphorePermit, rmcp::ErrorData> {
        self.inflight.clone().try_acquire_owned().map_err(|_| {
            rmcp::ErrorData::internal_error("MCP bridge is at capacity; retry shortly", None)
        })
    }
    fn server_info(&self) -> ServerConfig {
        let peer = self.remote.peer_info();
        let Some(info) = peer else {
            return ServerConfig::default();
        };
        let server_info = info
            .server_info
            .clone()
            .unwrap_or_else(Implementation::from_build_env);
        let mut capabilities = info.capabilities.clone();
        capabilities.prompts = None;
        capabilities.logging = None;
        capabilities.completions = None;
        capabilities.extensions = None;
        capabilities.experimental = None;
        if let Some(resources) = &mut capabilities.resources {
            resources.subscribe = None;
            resources.list_changed = None;
        }
        if let Some(tools) = &mut capabilities.tools {
            tools.list_changed = None;
        }
        let mut config = ServerConfig::new(capabilities)
            .with_protocol_version(info.protocol_version.clone())
            .with_server_info(server_info);
        config.instructions = info.instructions.clone();
        config
    }
}

impl ServerHandler for Proxy {
    fn get_info(&self) -> ServerConfig {
        self.server_info()
    }
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(ProtocolVersion::KNOWN_VERSIONS)
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        let _permit = self.permit()?;
        Self::bounded(
            self.remote
                .peer()
                .list_tools(request)
                .await
                .map_err(Self::err)?,
        )
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, rmcp::ErrorData> {
        let _permit = self.permit()?;
        match self
            .remote
            .peer()
            .call_tool_once(request)
            .await
            .map_err(Self::err)?
        {
            CallToolResponse::Complete(value) => {
                Ok(CallToolResponse::Complete(Self::bounded(value)?))
            }
            CallToolResponse::InputRequired(value) => {
                Ok(CallToolResponse::InputRequired(Self::bounded(value)?))
            }
            CallToolResponse::Task(value) => Ok(CallToolResponse::Task(Self::bounded(value)?)),
            _ => Err(rmcp::ErrorData::internal_error(
                "Unsupported MCP tool response",
                None,
            )),
        }
    }
    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, rmcp::ErrorData> {
        let _permit = self.permit()?;
        Self::bounded(
            self.remote
                .peer()
                .list_resources(request)
                .await
                .map_err(Self::err)?,
        )
    }
    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, rmcp::ErrorData> {
        let _permit = self.permit()?;
        Self::bounded(
            self.remote
                .peer()
                .list_resource_templates(request)
                .await
                .map_err(Self::err)?,
        )
    }
    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, rmcp::ErrorData> {
        let _permit = self.permit()?;
        match self
            .remote
            .peer()
            .read_resource_once(request)
            .await
            .map_err(Self::err)?
        {
            ReadResourceResponse::Complete(value) => {
                Ok(ReadResourceResponse::Complete(Self::bounded(value)?))
            }
            ReadResourceResponse::InputRequired(value) => {
                Ok(ReadResourceResponse::InputRequired(Self::bounded(value)?))
            }
            _ => Err(rmcp::ErrorData::internal_error(
                "Unsupported MCP resource response",
                None,
            )),
        }
    }
}

/// Bounds requests accepted by rmcp even when stdout is not being drained. The
/// permit is attached to the JSON-RPC id and survives handler completion until
/// the corresponding response has actually been written.
struct BoundedStdioTransport<R: tokio::io::AsyncRead, W: tokio::io::AsyncWrite> {
    inner: rmcp::transport::async_rw::AsyncRwTransport<RoleServer, R, W>,
    slots: Arc<Semaphore>,
    outstanding: Arc<StdMutex<HashMap<rmcp::model::RequestId, OwnedSemaphorePermit>>>,
    pending_request: Option<rmcp::service::RxJsonRpcMessage<RoleServer>>,
}

impl<R, W> BoundedStdioTransport<R, W>
where
    R: tokio::io::AsyncRead + Send + Unpin,
    W: tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    fn new(read: R, write: W) -> Self {
        Self {
            inner: rmcp::transport::async_rw::AsyncRwTransport::new_server(read, write)
                .with_max_line_length(MAX_STDIO_LINE_BYTES),
            slots: Arc::new(Semaphore::new(16)),
            outstanding: Arc::new(StdMutex::new(HashMap::new())),
            pending_request: None,
        }
    }

    fn register_request(&self, id: rmcp::model::RequestId, permit: OwnedSemaphorePermit) -> bool {
        let mut outstanding = self.outstanding.lock().unwrap_or_else(|e| e.into_inner());
        if outstanding.contains_key(&id) {
            return false;
        }
        outstanding.insert(id, permit);
        true
    }
}

struct ResponsePermitGuard {
    outstanding: Arc<StdMutex<HashMap<rmcp::model::RequestId, OwnedSemaphorePermit>>>,
    id: rmcp::model::RequestId,
}
impl Drop for ResponsePermitGuard {
    fn drop(&mut self) {
        self.outstanding
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

impl<R, W> rmcp::transport::Transport<RoleServer> for BoundedStdioTransport<R, W>
where
    R: tokio::io::AsyncRead + Send + Unpin + 'static,
    W: tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    type Error = io::Error;

    fn send(
        &mut self,
        item: rmcp::service::TxJsonRpcMessage<RoleServer>,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + 'static {
        let response_id = match &item {
            rmcp::model::JsonRpcMessage::Response(response) => Some(response.id.clone()),
            rmcp::model::JsonRpcMessage::Error(error) => error.id.clone(),
            _ => None,
        };
        let encoded_size = serde_json::to_vec(&item)
            .map(|bytes| bytes.len().saturating_add(1))
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "MCP response encoding failed")
            });
        let outstanding = self.outstanding.clone();
        let send = rmcp::transport::Transport::send(&mut self.inner, item);
        async move {
            let _permit_guard = response_id.map(|id| ResponsePermitGuard { outstanding, id });
            if encoded_size? > MAX_RESPONSE_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "MCP complete response exceeds encoded limit",
                ));
            }
            send.await
        }
    }

    async fn receive(&mut self) -> Option<rmcp::service::RxJsonRpcMessage<RoleServer>> {
        loop {
            if self.pending_request.is_some() {
                let acquire = self.slots.clone().acquire_owned();
                tokio::pin!(acquire);
                tokio::select! {
                    permit = &mut acquire => {
                        let permit = permit.ok()?;
                        let message = self.pending_request.take()?;
                        let id = match &message {
                            rmcp::model::JsonRpcMessage::Request(request) => request.id.clone(),
                            _ => unreachable!("only request messages are deferred"),
                        };
                        if self.register_request(id.clone(), permit) {
                            return Some(message);
                        }
                        let error = rmcp::model::ErrorData::invalid_request("Duplicate request id", None);
                        if self.inner.send(rmcp::service::TxJsonRpcMessage::<RoleServer>::error(error, Some(id))).await.is_err() {
                            return None;
                        }
                    }
                    incoming = self.inner.receive() => {
                        let message = incoming?;
                        match &message {
                            rmcp::model::JsonRpcMessage::Request(request) => {
                                let error = rmcp::model::ErrorData::internal_error("MCP stdio bridge is at capacity; retry shortly", None);
                                if self.inner.send(rmcp::service::TxJsonRpcMessage::<RoleServer>::error(error, Some(request.id.clone()))).await.is_err() {
                                    return None;
                                }
                            }
                            // Notifications (especially cancellation) remain deliverable at capacity.
                            _ => return Some(message),
                        }
                    }
                }
                continue;
            }

            let message = self.inner.receive().await?;
            match &message {
                rmcp::model::JsonRpcMessage::Request(request) => {
                    if let Ok(permit) = self.slots.clone().try_acquire_owned() {
                        let id = request.id.clone();
                        if self.register_request(id.clone(), permit) {
                            return Some(message);
                        }
                        let error =
                            rmcp::model::ErrorData::invalid_request("Duplicate request id", None);
                        if self
                            .inner
                            .send(rmcp::service::TxJsonRpcMessage::<RoleServer>::error(
                                error,
                                Some(id),
                            ))
                            .await
                            .is_err()
                        {
                            return None;
                        }
                    } else {
                        // Keep only one deferred request while continuing to read notifications.
                        self.pending_request = Some(message);
                    }
                }
                _ => return Some(message),
            }
        }
    }

    fn close(&mut self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        self.inner.close()
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    use rmcp::transport::Transport;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn stdio_transport_keeps_response_slots_until_write_and_passes_notifications_at_capacity()
    {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let (read, write) = tokio::io::split(server);
        let mut transport = BoundedStdioTransport::new(read, write);

        for id in 1..=16 {
            client
                .write_all(
                    format!(
                        "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"method\":\"ping\",\"params\":{{}}}}\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
        for _ in 0..16 {
            assert!(matches!(
                transport.receive().await,
                Some(rmcp::model::JsonRpcMessage::Request(_))
            ));
        }
        assert_eq!(transport.outstanding.lock().unwrap().len(), 16);

        client
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":17,\"method\":\"ping\",\"params\":{}}\n")
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), transport.receive())
                .await
                .is_err()
        );
        assert!(
            transport.pending_request.is_some(),
            "one deferred frame is the hard receive buffer"
        );

        client
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
        assert!(matches!(
            transport.receive().await,
            Some(rmcp::model::JsonRpcMessage::Notification(_))
        ));

        let id = rmcp::model::RequestId::Number(1);
        let response = rmcp::service::TxJsonRpcMessage::<RoleServer>::error(
            rmcp::ErrorData::internal_error("test", None),
            Some(id),
        );
        let mut response_send = Box::pin(transport.send(response));
        let mut pending_receive = Box::pin(transport.receive());
        // Let receive enter its wait while no additional input is available.
        assert!(tokio::time::timeout(std::time::Duration::from_millis(200), async {
            tokio::select! {
                message = &mut pending_receive => panic!("deferred request returned before a slot opened: {message:?}"),
                _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => (),
            }
        }).await.is_ok());
        let (pending, sent) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(&mut pending_receive, &mut response_send)
        })
        .await
        .unwrap();
        sent.unwrap();
        assert!(matches!(
            pending,
            Some(rmcp::model::JsonRpcMessage::Request(_))
        ));
        drop(pending_receive);
        drop(response_send);
        assert_eq!(transport.outstanding.lock().unwrap().len(), 16);
    }
    #[tokio::test]
    async fn stdio_encoded_limit_covers_the_entire_jsonrpc_envelope() {
        let (mut client, server) = tokio::io::duplex(1024);
        let (read, write) = tokio::io::split(server);
        let mut transport = BoundedStdioTransport::new(read, write);
        let message = rmcp::service::TxJsonRpcMessage::<RoleServer>::error(
            rmcp::ErrorData::internal_error("x".repeat(MAX_RESPONSE_BYTES), None),
            Some(rmcp::model::RequestId::Number(1)),
        );
        let error = transport.send(message).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let mut byte = [0; 1];
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                tokio::io::AsyncReadExt::read(&mut client, &mut byte)
            )
            .await
            .is_err(),
            "oversized responses must not be partially emitted"
        );
    }
}
