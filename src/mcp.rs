//! MCP tools are a thin, permission-checked adapter over the existing coordinator.
use crate::{
    config::{Config, Mcp},
    coordinator::{Coordinator, ResourceExhausted},
    inspection::VerificationReport,
};
use anyhow::{Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use parking_lot::Mutex;
use rmcp::{ErrorData, RoleServer, ServerHandler, model::*, service::RequestContext};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use uuid::Uuid;

/// Secrets are environment-only and intentionally do not implement Debug/Serialize.
#[derive(Default, Clone)]
pub struct Credentials {
    pub service: Option<String>,
    pub read: Option<String>,
    pub write: Option<String>,
    pub maintenance: Option<String>,
    pub gateway: Option<String>,
}
impl Credentials {
    pub fn from_env() -> Self {
        let env = |key| std::env::var(key).ok().filter(|s| !s.is_empty());
        Self {
            service: env("SKERRY_TOKEN"),
            read: env("SKERRY_MCP_READ_TOKEN"),
            write: env("SKERRY_MCP_WRITE_TOKEN"),
            maintenance: env("SKERRY_MCP_MAINTENANCE_TOKEN"),
            gateway: env("SKERRY_MCP_GATEWAY_TOKEN"),
        }
    }
    pub fn validate(&self) -> Result<()> {
        let tokens = [
            &self.service,
            &self.read,
            &self.write,
            &self.maintenance,
            &self.gateway,
        ];
        for (i, a) in tokens.iter().enumerate() {
            if let Some(a) = a {
                ensure!(
                    !a.is_empty() && a.len() <= 4096,
                    "MCP credentials must be nonempty and at most 4096 bytes"
                );
                for b in tokens.iter().skip(i + 1).filter_map(|t| t.as_ref()) {
                    ensure!(
                        a != b,
                        "MCP credentials for different scopes must be distinct"
                    );
                }
            }
        }
        Ok(())
    }
}
#[derive(Clone)]
pub(crate) struct Principal {
    pub key: [u8; 32],
    pub read: bool,
    pub write: bool,
    pub maintenance: bool,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct McpStatus {
    pub enabled: bool,
    pub read_only: bool,
    pub active_connections: usize,
    pub tool_calls_total: u64,
    pub failed_tool_calls: u64,
    pub authorization_failures: u64,
    pub average_tool_latency_ms: f64,
    pub inflight_requests: usize,
    pub verification_jobs: usize,
    pub rate_rejections: u64,
    pub retained_response_bytes: usize,
}
struct Rate {
    since: Instant,
    count: u32,
}
#[derive(Serialize, Clone)]
pub struct VerificationStatus {
    pub job_id: String,
    pub segment_id: String,
    pub state: String,
    pub verified_bytes: u64,
    pub report: Option<VerificationReport>,
    pub error: Option<String>,
}
struct Job {
    id: Uuid,
    segment: Uuid,
    owner: [u8; 32],
    created: Instant,
    progress: Arc<AtomicU64>,
    cancel: Arc<AtomicBool>,
    result: Mutex<Option<std::result::Result<VerificationReport, String>>>,
}
pub struct Runtime {
    pub(crate) config: Mcp,
    pub(crate) concurrent: Arc<Semaphore>,
    pub(crate) responses: Arc<Semaphore>,
    pub(crate) active: AtomicUsize,
    pub(crate) inflight: AtomicUsize,
    pub(crate) auth_failures: AtomicU64,
    pub(crate) rate_rejections: AtomicU64,
    stopping: AtomicBool,
    calls: AtomicU64,
    failures: AtomicU64,
    latency_us: AtomicU64,
    rates: Mutex<HashMap<[u8; 32], Rate>>,
    jobs: Mutex<HashMap<Uuid, Arc<Job>>>,
    job_slots: Arc<Semaphore>,
    job_handles: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
impl Runtime {
    pub fn new(config: &Config) -> Arc<Self> {
        Arc::new(Self {
            config: config.mcp.clone(),
            concurrent: Arc::new(Semaphore::new(config.mcp.max_concurrent_requests)),
            responses: Arc::new(Semaphore::new(
                config.mcp.max_response_memory_mib * 1024 * 1024,
            )),
            active: AtomicUsize::new(0),
            inflight: AtomicUsize::new(0),
            auth_failures: AtomicU64::new(0),
            rate_rejections: AtomicU64::new(0),
            stopping: AtomicBool::new(false),
            calls: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            latency_us: AtomicU64::new(0),
            rates: Mutex::new(HashMap::new()),
            jobs: Mutex::new(HashMap::new()),
            job_slots: Arc::new(Semaphore::new(config.mcp.max_verification_jobs)),
            job_handles: tokio::sync::Mutex::new(vec![]),
        })
    }
    pub fn status(&self) -> McpStatus {
        let calls = self.calls.load(Ordering::Relaxed);
        McpStatus {
            enabled: self.config.enabled,
            read_only: self.config.read_only,
            active_connections: self.active.load(Ordering::Relaxed),
            inflight_requests: self.inflight.load(Ordering::Relaxed),
            tool_calls_total: calls,
            failed_tool_calls: self.failures.load(Ordering::Relaxed),
            authorization_failures: self.auth_failures.load(Ordering::Relaxed),
            average_tool_latency_ms: if calls == 0 {
                0.
            } else {
                self.latency_us.load(Ordering::Relaxed) as f64 / calls as f64 / 1000.
            },
            verification_jobs: self.config.max_verification_jobs
                - self.job_slots.available_permits(),
            rate_rejections: self.rate_rejections.load(Ordering::Relaxed),
            retained_response_bytes: self.config.max_response_memory_mib * 1024 * 1024
                - self.responses.available_permits(),
        }
    }
    pub(crate) fn allow_rate(&self, key: [u8; 32]) -> bool {
        let mut rates = self.rates.lock();
        rates.retain(|_, r| r.since.elapsed() < Duration::from_secs(60));
        if !rates.contains_key(&key) && rates.len() >= 1024 {
            self.rate_rejections.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let rate = rates.entry(key).or_insert(Rate {
            since: Instant::now(),
            count: 0,
        });
        if rate.count >= self.config.requests_per_minute {
            self.rate_rejections.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        rate.count += 1;
        true
    }
    pub(crate) fn record(&self, start: Instant, failed: bool) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.latency_us
            .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
        if failed {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub async fn cancel_jobs(&self) {
        self.stopping.store(true, Ordering::Release);
        let mut handles = self.job_handles.lock().await;
        for job in self.jobs.lock().values() {
            job.cancel.store(true, Ordering::Release);
        }
        for handle in handles.iter_mut() {
            let _ = handle.await;
        }
        handles.clear();
    }
    async fn verify(
        self: &Arc<Self>,
        store: Arc<Coordinator>,
        principal: &Principal,
        id: Uuid,
    ) -> std::result::Result<Value, ErrorData> {
        let info = store
            .segment_info(id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| invalid("segment not found"))?;
        let mut handles = self.job_handles.lock().await;
        if self.stopping.load(Ordering::Acquire) {
            return Err(busy());
        }
        let slot = self
            .job_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let request = store.try_request_permit().map_err(storage_error)?;
        let memory = store
            .try_read_memory_permit(64 * 1024)
            .map_err(storage_error)?;
        let job = Arc::new(Job {
            id: Uuid::new_v4(),
            segment: id,
            owner: principal.key,
            created: Instant::now(),
            progress: Arc::new(AtomicU64::new(0)),
            cancel: Arc::new(AtomicBool::new(false)),
            result: Mutex::new(None),
        });
        {
            let mut jobs = self.jobs.lock();
            if jobs.len() >= self.config.max_retained_jobs {
                let old = jobs
                    .iter()
                    .filter(|(_, j)| j.result.lock().is_some())
                    .min_by_key(|(_, j)| j.created)
                    .map(|(id, _)| *id);
                if let Some(old) = old {
                    jobs.remove(&old);
                } else {
                    return Err(busy());
                }
            }
            jobs.insert(job.id, job.clone());
        }
        let result =
            json!({"job_id":job.id.to_string(),"segment_id":info.segment_id,"state":"running"});
        let timeout = Duration::from_secs(self.config.verification_timeout_seconds);
        let db = store.storage();
        let retained_job = job.clone();
        let handle = tokio::spawn(async move {
            let (_slot, _request, _memory, _store) = (slot, request, memory, store);
            let progress = job.progress.clone();
            let cancel = job.cancel.clone();
            let verified = tokio::task::spawn_blocking(move || {
                db.verify_segment_bounded(id, progress, cancel, timeout)
            })
            .await;
            let verified = match verified {
                Ok(Ok(report)) => Ok(report),
                _ => Err(if retained_job.cancel.load(Ordering::Acquire) {
                    "verification cancelled".into()
                } else {
                    "verification failed or timed out; inspect server logs".into()
                }),
            };
            *retained_job.result.lock() = Some(verified);
        });
        handles.retain(|h| !h.is_finished());
        handles.push(handle);
        Ok(result)
    }
    fn job_status(&self, id: Uuid, principal: &Principal) -> std::result::Result<Value, ErrorData> {
        let jobs = self.jobs.lock();
        let job = jobs
            .get(&id)
            .filter(|j| j.owner == principal.key)
            .ok_or_else(|| invalid("verification job not found"))?;
        let result = job.result.lock();
        let (state, report, error) = match &*result {
            None => ("running", None, None),
            Some(Ok(report)) => ("completed", Some(report.clone()), None),
            Some(Err(error)) => (
                if job.cancel.load(Ordering::Acquire) {
                    "cancelled"
                } else {
                    "failed"
                },
                None,
                Some(error.clone()),
            ),
        };
        Ok(json!(VerificationStatus {
            job_id: job.id.to_string(),
            segment_id: job.segment.to_string(),
            state: state.into(),
            verified_bytes: job.progress.load(Ordering::Relaxed),
            report,
            error
        }))
    }
}
fn invalid(message: &str) -> ErrorData {
    ErrorData::invalid_params(message.to_owned(), None)
}
fn denied() -> ErrorData {
    ErrorData::new(ErrorCode(-32001), "operation is not authorized", None)
}
fn busy() -> ErrorData {
    ErrorData::new(
        ErrorCode(-32002),
        "resource capacity exhausted; retry later",
        None,
    )
}
fn storage_error(error: anyhow::Error) -> ErrorData {
    if error.downcast_ref::<ResourceExhausted>().is_some() {
        busy()
    } else {
        tracing::error!("MCP storage operation failed");
        ErrorData::internal_error("storage operation failed; inspect server logs", None)
    }
}
fn hash(text: &str) -> std::result::Result<[u8; 32], ErrorData> {
    let mut hash = [0; 32];
    if text.len() != 64 || hex::decode_to_slice(text, &mut hash).is_err() {
        return Err(invalid("expected a 64-character BLAKE3 hash"));
    }
    Ok(hash)
}
fn segment(text: &str) -> std::result::Result<Uuid, ErrorData> {
    let id = Uuid::parse_str(text).map_err(|_| invalid("invalid UUIDv4 identifier"))?;
    if id.get_version_num() != 4 {
        return Err(invalid("expected UUIDv4"));
    }
    Ok(id)
}
fn principal(context: &RequestContext<RoleServer>) -> std::result::Result<Principal, ErrorData> {
    context
        .extensions
        .get::<axum::http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<Principal>())
        .cloned()
        .ok_or_else(denied)
}
fn json_result(value: Value) -> std::result::Result<CallToolResponse, ErrorData> {
    let text = serde_json::to_string(&value)
        .map_err(|_| ErrorData::internal_error("response encoding failed", None))?;
    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
    result.structured_content = Some(value);
    Ok(result.into())
}

#[derive(Clone)]
pub(crate) struct Handler {
    pub store: Arc<Coordinator>,
}
impl Handler {
    async fn execute(
        &self,
        name: &str,
        args: Value,
        p: &Principal,
    ) -> std::result::Result<Value, ErrorData> {
        let object = args
            .as_object()
            .ok_or_else(|| invalid("tool arguments must be an object"))?;
        let allowed: &[&str] = match name {
            "store_status" => &[],
            "list_segments" => &["cursor", "limit"],
            "segment_info" | "verify_segment" => &["segment_id"],
            "object_info" | "get_object" => &["hash"],
            "put_object" => &["data_base64"],
            "verification_status" => &["job_id"],
            _ => return Err(invalid("unknown tool")),
        };
        if object.keys().any(|k| !allowed.contains(&k.as_str())) {
            return Err(invalid("unexpected tool argument"));
        }
        let text = |key: &str| {
            object
                .get(key)
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("missing or invalid string argument"))
        };
        let config = &self.store.config.mcp;
        match name {
            "store_status" => {
                let status = self.store.status().await.map_err(storage_error)?;
                let mut value =
                    serde_json::to_value(status).map_err(|_| invalid("status unavailable"))?;
                if let Some(o) = value.as_object_mut() {
                    o.remove("errors");
                    o.remove("events");
                }
                Ok(value)
            }
            "list_segments" => {
                let limit = match object.get("limit") {
                    None => config.max_list_page_size,
                    Some(value) => usize::try_from(
                        value
                            .as_u64()
                            .ok_or_else(|| invalid("limit must be a positive integer"))?,
                    )
                    .map_err(|_| invalid("invalid limit"))?,
                };
                if limit == 0 || limit > config.max_list_page_size {
                    return Err(invalid("listing limit outside configured bounds"));
                }
                let cursor = object
                    .get("cursor")
                    .filter(|v| !v.is_null())
                    .map(|v| v.as_str().ok_or_else(|| invalid("invalid cursor")))
                    .transpose()?;
                let page = self
                    .store
                    .list_segments(cursor.map(str::to_owned), limit)
                    .await
                    .map_err(|_| invalid("invalid cursor or listing unavailable"))?;
                Ok(json!(page))
            }
            "segment_info" => {
                let id = segment(text("segment_id")?)?;
                let info = self.store.segment_info(id).await.map_err(storage_error)?;
                let known=self.store.mcp.jobs.lock().values().filter(|j|j.segment==id).max_by_key(|j|j.created).map(|j| {
                    let result=j.result.lock();
                    let state=match &*result {None=>"running",Some(Ok(_))=>"completed",Some(Err(_))=>"failed"};
                    json!({"state":state,"verified_bytes":j.progress.load(Ordering::Relaxed),"report":result.as_ref().and_then(|r|r.as_ref().ok()),"error":result.as_ref().and_then(|r|r.as_ref().err())})
                });
                Ok(json!({"segment":info,"known_verification":known}))
            }
            "object_info" => Ok(json!(
                self.store
                    .object_info(hash(text("hash")?)?)
                    .await
                    .map_err(storage_error)?
            )),
            "get_object" => self.object(hash(text("hash")?)?).await,
            "put_object" => {
                if config.read_only || !p.write {
                    self.store.mcp.auth_failures.fetch_add(1, Ordering::Relaxed);
                    return Err(denied());
                }
                let encoded = text("data_base64")?;
                let limit = config.max_put_object_kib * 1024;
                if encoded.len() > limit.div_ceil(3) * 4 {
                    return Err(invalid("encoded payload exceeds MCP PUT limit"));
                }
                let data = STANDARD
                    .decode(encoded)
                    .map_err(|_| invalid("data_base64 must be canonical padded base64"))?;
                if data.len() > limit {
                    return Err(invalid("payload exceeds MCP PUT limit"));
                }
                let length = data.len();
                let result = self.store.put(data.into()).await.map_err(storage_error)?;
                Ok(
                    json!({"hash":hex::encode(result.hash),"payload_length":length,"duplicate":result.duplicate}),
                )
            }
            "verify_segment" => {
                if !config.allow_verification || !p.maintenance {
                    self.store.mcp.auth_failures.fetch_add(1, Ordering::Relaxed);
                    return Err(denied());
                }
                self.store
                    .mcp
                    .verify(self.store.clone(), p, segment(text("segment_id")?)?)
                    .await
            }
            "verification_status" => self.store.mcp.job_status(segment(text("job_id")?)?, p),
            _ => Err(invalid("unknown tool")),
        }
    }
    async fn object(&self, hash: [u8; 32]) -> std::result::Result<Value, ErrorData> {
        let info = self.store.object_info(hash).await.map_err(storage_error)?;
        if !info.exists {
            return Ok(json!({"hash":hex::encode(hash),"exists":false,"inline":false}));
        }
        if info.payload_length.unwrap_or(0)
            > self.store.config.mcp.max_inline_object_kib as u64 * 1024
        {
            return Ok(
                json!({"hash":info.hash,"exists":true,"inline":false,"payload_length":info.payload_length,"http_path":format!("/v1/objects/{}",info.hash),"message":"Object exceeds MCP inline limit; use the authenticated native HTTP API. No payload was truncated."}),
            );
        }
        let bytes = self
            .store
            .get(hash)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| invalid("object disappeared"))?;
        Ok(
            json!({"hash":hex::encode(hash),"exists":true,"inline":true,"payload_length":bytes.len(),"data_base64":STANDARD.encode(&bytes)}),
        )
    }
}
impl ServerHandler for Handler {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().enable_resources().build()).with_server_info(Implementation::new("skerrykv",env!("CARGO_PKG_VERSION"))).with_instructions("Immutable BLAKE3-addressed binary store. Tools enforce read/write/maintenance scopes. Hash order is arbitrary, not chronological. Large objects use authenticated native HTTP.")
    }
    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, ErrorData> {
        let p = principal(&context)?;
        if !p.read {
            return Err(denied());
        }
        Ok(ListToolsResult {
            tools: tools(&self.store.config.mcp),
            ..Default::default()
        })
    }
    fn get_tool(&self, name: &str) -> Option<Tool> {
        tools(&self.store.config.mcp)
            .into_iter()
            .find(|t| t.name == name)
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, ErrorData> {
        let start = Instant::now();
        let result=async{let p=principal(&context)?;if !p.read{return Err(denied());}let args=Value::Object(request.arguments.unwrap_or_default());tokio::time::timeout(Duration::from_secs(self.store.config.mcp.tool_timeout_seconds),self.execute(&request.name,args,&p)).await.map_err(|_|ErrorData::new(ErrorCode(-32003),"tool execution deadline exceeded; an interrupted PUT may already be committed, retry safely by content",None))?.and_then(json_result)}.await;
        self.store.mcp.record(start, result.is_err());
        result
    }
    async fn list_resources(
        &self,
        _: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListResourcesResult, ErrorData> {
        let _ = principal(&context)?;
        Ok(ListResourcesResult::default())
    }
    async fn list_resource_templates(
        &self,
        _: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListResourceTemplatesResult, ErrorData> {
        let _ = principal(&context)?;
        serde_json::from_value(json!({"resourceTemplates":[{"uriTemplate":"skerry://objects/{hash}","name":"Immutable object by BLAKE3","description":"Verified binary object, within configured inline size. Larger objects require authenticated native HTTP.","mimeType":"application/octet-stream"}]})).map_err(|_|invalid("resource template unavailable"))
    }
    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<ReadResourceResponse, ErrorData> {
        let p = principal(&context)?;
        if !p.read {
            return Err(denied());
        }
        let text = request
            .uri
            .strip_prefix("skerry://objects/")
            .ok_or_else(|| invalid("unsupported resource URI"))?;
        let value = tokio::time::timeout(
            Duration::from_secs(self.store.config.mcp.tool_timeout_seconds),
            self.object(hash(text)?),
        )
        .await
        .map_err(|_| busy())??;
        if value["exists"] != true {
            return Err(invalid("object not found"));
        }
        if value["inline"] != true {
            return Err(invalid(
                "object exceeds MCP inline limit; use authenticated /v1/objects/{hash}; no payload was truncated",
            ));
        }
        let blob = value["data_base64"]
            .as_str()
            .ok_or_else(|| invalid("payload unavailable"))?;
        let result:ReadResourceResult=serde_json::from_value(json!({"contents":[{"uri":request.uri,"mimeType":"application/octet-stream","blob":blob}]})).map_err(|_|invalid("resource encoding failed"))?;
        Ok(result.into())
    }
}
fn tools(config: &Mcp) -> Vec<Tool> {
    let hash = json!({"type":"string","pattern":"^[0-9a-fA-F]{64}$","description":"Full BLAKE3 content digest in hexadecimal"});
    let id = json!({"type":"string","format":"uuid"});
    [
        ("store_status","Inspect measured service health, capacity, storage, I/O, queues and cache. No sensitive paths or log contents.",json!({}),vec![],true,true),
        ("list_segments","List a bounded page in arbitrary UUID/index order; cursor is opaque, not chronological. No paths are exposed.",json!({"cursor":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":config.max_list_page_size}}),vec![],true,true),
        ("segment_info","Inspect segment state, size, utilization, seal identity and known verification job metadata.",json!({"segment_id":id}),vec!["segment_id"],true,true),
        ("object_info","Validate a BLAKE3 digest and inspect existence, length and non-sensitive segment location.",json!({"hash":hash}),vec!["hash"],true,true),
        ("get_object","Return a complete verified small object as base64; oversized objects return metadata and native HTTP instructions, never truncated bytes.",json!({"hash":hash}),vec!["hash"],true,true),
        ("put_object","Durably store canonical base64 binary bytes through the shared writer. Requires write scope and read_only=false. Content retries deduplicate.",json!({"data_base64":{"type":"string","contentEncoding":"base64","maxLength":(config.max_put_object_kib*1024).div_ceil(3)*4}}),vec!["data_base64"],false,true),
        ("verify_segment","Start a bounded, cancellable integrity scan; requires maintenance scope and allow_verification=true. Does not repair or mutate stored bytes. Returns an ephemeral job id.",json!({"segment_id":id}),vec!["segment_id"],false,false),
        ("verification_status","Read progress/results for your verification job. Completed history is bounded and may expire.",json!({"job_id":id}),vec!["job_id"],true,true),
    ].into_iter().map(|(name,description,properties,required,read_only,idempotent)|serde_json::from_value(json!({"name":name,"description":description,"inputSchema":{"type":"object","properties":properties,"required":required,"additionalProperties":false},"annotations":{"readOnlyHint":read_only,"destructiveHint":false,"idempotentHint":idempotent,"openWorldHint":false}})).expect("static valid tool schema")).collect()
}
