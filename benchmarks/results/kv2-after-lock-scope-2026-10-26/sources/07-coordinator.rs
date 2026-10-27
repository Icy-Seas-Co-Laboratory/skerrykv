//! Bounded asynchronous coordination. All storage/index calls execute on blocking workers.
use crate::{
    config::Config,
    engine::{PutResult, StorageStatus, Store},
};
use anyhow::{Result, anyhow, ensure};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

#[derive(Debug, thiserror::Error)]
#[error("resource capacity exhausted; retry later")]
pub struct ResourceExhausted;

fn shared_error(error: String) -> anyhow::Error {
    if error.contains("capacity exhausted") || error.contains("queue exhausted") {
        ResourceExhausted.into()
    } else {
        anyhow::Error::msg(error)
    }
}

type Hash = [u8; 32];
type SharedResult = std::result::Result<Option<Bytes>, String>;
type Waiters = Arc<Mutex<HashMap<Hash, Vec<oneshot::Sender<SharedResult>>>>>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Priority {
    Interactive,
    #[default]
    Normal,
    Bulk,
}
impl Priority {
    fn rank(self, age: Duration) -> u8 {
        if age >= Duration::from_millis(100) {
            0
        } else {
            match self {
                Self::Interactive => 0,
                Self::Normal => 1,
                Self::Bulk => 2,
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub version: String,
    pub uptime_seconds: f64,
    pub health: String,
    pub listen: String,
    pub storage: StorageStatus,
    pub io: IoStatus,
    pub cache: CacheStatus,
    pub errors: Vec<String>,
    pub policy: String,
    pub read_workers: usize,
    pub configured_budget_mib: usize,
    pub segment_capacity_bytes: u64,
    pub write_queue_bytes: usize,
    pub write_queue_capacity_bytes: usize,
    pub events: Vec<String>,
    pub mcp: crate::mcp::McpStatus,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IoStatus {
    pub reads: u64,
    pub writes: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub deduplicated: u64,
    pub deduplicated_bytes: u64,
    pub read_coalesced: u64,
    pub read_queue: usize,
    pub write_queue: usize,
    pub inflight_reads: usize,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub index_lookup_ms: f64,
    pub queue_wait_ms: f64,
    pub disk_read_ms: f64,
    pub storage_write_ms: f64,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStatus {
    pub entries: usize,
    pub bytes: usize,
    pub retained_bytes: usize,
    pub hits: u64,
    pub served_bytes: u64,
    pub misses: u64,
    pub admissions: u64,
    pub evictions: u64,
}
#[derive(Default)]
struct Counters {
    reads: AtomicU64,
    writes: AtomicU64,
    read_bytes: AtomicU64,
    write_bytes: AtomicU64,
    deduplicated: AtomicU64,
    deduplicated_bytes: AtomicU64,
    read_coalesced: AtomicU64,
    read_queue: AtomicUsize,
    write_queue: AtomicUsize,
    inflight_reads: AtomicUsize,
    latencies: Mutex<VecDeque<f64>>,
    errors: Mutex<VecDeque<String>>,
    events: Mutex<VecDeque<String>>,
    index_lookup_us: AtomicU64,
    queue_wait_us: AtomicU64,
    disk_read_us: AtomicU64,
    storage_write_us: AtomicU64,
    faulted: AtomicBool,
}
impl Counters {
    fn latency(&self, since: Instant) {
        let mut l = self.latencies.lock();
        if l.len() >= 4096 {
            l.pop_front();
        }
        l.push_back(since.elapsed().as_secs_f64() * 1000.);
    }
    fn error(&self, error: String) {
        tracing::error!(error = %error, "storage operation failed");
        let mut e = self.errors.lock();
        if e.len() >= 32 {
            e.pop_front();
        }
        e.push_back(error);
    }
    fn event(&self, event: String) {
        tracing::info!(event=%event);
        let mut events = self.events.lock();
        if events.len() >= 64 {
            events.pop_front();
        }
        events.push_back(event);
    }
    fn snapshot(&self) -> IoStatus {
        let mut samples: Vec<_> = self.latencies.lock().iter().copied().collect();
        samples.sort_by(f64::total_cmp);
        let percentile = |p: usize| -> f64 {
            if samples.is_empty() {
                0.
            } else {
                samples[((samples.len() - 1) * p) / 100]
            }
        };
        IoStatus {
            reads: self.reads.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            deduplicated: self.deduplicated.load(Ordering::Relaxed),
            deduplicated_bytes: self.deduplicated_bytes.load(Ordering::Relaxed),
            read_coalesced: self.read_coalesced.load(Ordering::Relaxed),
            read_queue: self.read_queue.load(Ordering::Relaxed),
            write_queue: self.write_queue.load(Ordering::Relaxed),
            inflight_reads: self.inflight_reads.load(Ordering::Relaxed),
            latency_p50_ms: percentile(50),
            latency_p95_ms: percentile(95),
            latency_p99_ms: percentile(99),
            index_lookup_ms: self.index_lookup_us.load(Ordering::Relaxed) as f64 / 1000.,
            queue_wait_ms: self.queue_wait_us.load(Ordering::Relaxed) as f64 / 1000.,
            disk_read_ms: self.disk_read_us.load(Ordering::Relaxed) as f64 / 1000.,
            storage_write_ms: self.storage_write_us.load(Ordering::Relaxed) as f64 / 1000.,
        }
    }
}

/// A buffer keeps its allocation budget until the last network/coalesced reference drops.
struct BudgetBuffer {
    data: Vec<u8>,
    _permit: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for BudgetBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
struct Entry {
    bytes: Bytes,
    tick: u64,
    protected: bool,
}
struct PayloadCache {
    entries: HashMap<Hash, Entry>,
    probation: BTreeMap<u64, Hash>,
    protected: BTreeMap<u64, Hash>,
    seen: HashMap<Hash, (Instant, u64)>,
    seen_order: BTreeMap<u64, Hash>,
    tick: u64,
    bytes: usize,
    protected_bytes: usize,
    limit: usize,
    admission_limit: usize,
    window: Duration,
    budget: Arc<Semaphore>,
    stats: CacheStatus,
}
impl PayloadCache {
    fn new(c: &Config) -> Self {
        Self {
            entries: HashMap::new(),
            probation: BTreeMap::new(),
            protected: BTreeMap::new(),
            seen: HashMap::new(),
            seen_order: BTreeMap::new(),
            tick: 0,
            bytes: 0,
            protected_bytes: 0,
            limit: c.cache.payload_mib * 1024 * 1024,
            admission_limit: c.cache.admission_entries,
            window: Duration::from_secs(c.cache.admission_window_seconds),
            budget: Arc::new(Semaphore::new(c.cache.payload_mib * 1024 * 1024)),
            stats: CacheStatus::default(),
        }
    }
    fn get(&mut self, hash: &Hash) -> Option<Bytes> {
        self.tick += 1;
        if let Some(e) = self.entries.get_mut(hash) {
            if e.protected {
                self.protected.remove(&e.tick);
            } else {
                self.probation.remove(&e.tick);
                self.protected_bytes += e.bytes.len();
            }
            e.tick = self.tick;
            e.protected = true;
            self.protected.insert(e.tick, *hash);
            let result = e.bytes.clone();
            self.stats.hits += 1;
            self.stats.served_bytes += result.len() as u64;
            self.balance();
            Some(result)
        } else {
            self.stats.misses += 1;
            None
        }
    }
    fn balance(&mut self) {
        while self.protected_bytes > self.limit * 4 / 5 {
            let Some((tick, hash)) = self.protected.pop_first() else {
                break;
            };
            if let Some(e) = self.entries.get_mut(&hash) {
                e.protected = false;
                self.protected_bytes -= e.bytes.len();
                self.probation.insert(tick, hash);
            }
        }
    }
    fn evict(&mut self) -> bool {
        let victim = self
            .probation
            .pop_first()
            .or_else(|| self.protected.pop_first());
        if let Some((_, hash)) = victim {
            if let Some(e) = self.entries.remove(&hash) {
                self.bytes -= e.bytes.len();
                if e.protected {
                    self.protected_bytes -= e.bytes.len();
                }
                self.stats.evictions += 1;
            }
            true
        } else {
            false
        }
    }
    fn observe(&mut self, hash: Hash, data: &Bytes) {
        if self.limit == 0 || data.len() > self.limit / 4 || self.entries.contains_key(&hash) {
            return;
        }
        self.tick += 1;
        let second = self
            .seen
            .remove(&hash)
            .map(|(when, tick)| {
                self.seen_order.remove(&tick);
                when.elapsed() <= self.window
            })
            .unwrap_or(false);
        if !second {
            self.seen.insert(hash, (Instant::now(), self.tick));
            self.seen_order.insert(self.tick, hash);
            while self.seen.len() > self.admission_limit {
                if let Some((_, h)) = self.seen_order.pop_first() {
                    self.seen.remove(&h);
                }
            }
            return;
        }
        while self.bytes + data.len() > self.limit || self.entries.len() >= self.admission_limit {
            if !self.evict() {
                return;
            }
        }
        let permit = loop {
            match self
                .budget
                .clone()
                .try_acquire_many_owned(data.len().max(1) as u32)
            {
                Ok(p) => break p,
                Err(_) => {
                    if !self.evict() {
                        return;
                    }
                }
            }
        };
        let bytes = Bytes::from_owner(BudgetBuffer {
            data: data.to_vec(),
            _permit: permit,
        });
        self.bytes += bytes.len();
        self.probation.insert(self.tick, hash);
        self.entries.insert(
            hash,
            Entry {
                bytes,
                tick: self.tick,
                protected: false,
            },
        );
        self.stats.admissions += 1;
    }
    fn clear(&mut self) {
        self.entries.clear();
        self.probation.clear();
        self.protected.clear();
        self.seen.clear();
        self.seen_order.clear();
        self.bytes = 0;
        self.protected_bytes = 0;
    }
    fn maintain(&mut self) {
        while let Some((tick, hash)) = self.seen_order.first_key_value().map(|(k, v)| (*k, *v)) {
            if self
                .seen
                .get(&hash)
                .is_some_and(|(when, _)| when.elapsed() < self.window)
            {
                break;
            }
            self.seen_order.remove(&tick);
            self.seen.remove(&hash);
        }
    }
    fn status(&self) -> CacheStatus {
        CacheStatus {
            entries: self.entries.len(),
            bytes: self.bytes,
            retained_bytes: self.limit - self.budget.available_permits(),
            ..self.stats.clone()
        }
    }
}

struct WriteRequest {
    bytes: Bytes,
    response: oneshot::Sender<std::result::Result<PutResult, String>>,
    _permit: OwnedSemaphorePermit,
    submitted: Instant,
}
enum WriteMessage {
    Put(WriteRequest),
    Shutdown(oneshot::Sender<std::result::Result<(), String>>),
}
struct ReadRequest {
    hash: Hash,
    priority: Priority,
    submitted: Instant,
}

enum ShutdownState {
    Running,
    Waiting(oneshot::Receiver<std::result::Result<(), String>>),
    Done(std::result::Result<(), String>),
}

pub struct Coordinator {
    pub mcp: Arc<crate::mcp::Runtime>,
    read_budget: Arc<Semaphore>,
    pub config: Config,
    store: Arc<Store>,
    write_tx: mpsc::Sender<WriteMessage>,
    read_tx: mpsc::Sender<ReadRequest>,
    write_budget: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    waiters: Waiters,
    cache: Arc<Mutex<PayloadCache>>,
    counters: Arc<Counters>,
    started: Instant,
    accepting: AtomicBool,
    admission: tokio::sync::Mutex<()>,
    shutdown_result: tokio::sync::Mutex<ShutdownState>,
    read_stop: tokio_util::sync::CancellationToken,
    read_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    write_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Coordinator {
    pub async fn open(config: Config) -> Result<Arc<Self>> {
        config.validate()?;
        let c = config.clone();
        let store = Arc::new(tokio::task::spawn_blocking(move || Store::open(&c)).await??);
        let (write_tx, write_rx) = mpsc::channel(config.writes.batch_max_records * 2);
        let (read_tx, read_rx) = mpsc::channel(config.reads.queue_items);
        let counters = Arc::new(Counters::default());
        counters.event("store opened; durable write coordinator ready".into());
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        let cache = Arc::new(Mutex::new(PayloadCache::new(&config)));
        let write_handle = tokio::spawn(write_loop(
            store.clone(),
            write_rx,
            counters.clone(),
            config.clone(),
        ));
        let read_stop = tokio_util::sync::CancellationToken::new();
        let read_budget = Arc::new(Semaphore::new(
            config.resources.max_inflight_read_mib * 1024 * 1024,
        ));
        let read_handle = tokio::spawn(read_loop(
            store.clone(),
            read_rx,
            waiters.clone(),
            cache.clone(),
            counters.clone(),
            config.clone(),
            ReadControl {
                stop: read_stop.clone(),
                budget: read_budget.clone(),
            },
        ));
        let weak = Arc::downgrade(&cache);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                let Some(cache) = weak.upgrade() else { break };
                cache.lock().maintain();
            }
        });
        Ok(Arc::new(Self {
            mcp: crate::mcp::Runtime::new(&config),
            read_budget,
            write_budget: Arc::new(Semaphore::new(config.writes.max_pending_mib * 1024 * 1024)),
            requests: Arc::new(Semaphore::new(config.resources.max_requests)),
            config,
            store,
            write_tx,
            read_tx,
            waiters,
            cache,
            counters,
            started: Instant::now(),
            accepting: AtomicBool::new(true),
            admission: tokio::sync::Mutex::new(()),
            shutdown_result: tokio::sync::Mutex::new(ShutdownState::Running),
            read_stop,
            read_handle: tokio::sync::Mutex::new(Some(read_handle)),
            write_handle: tokio::sync::Mutex::new(Some(write_handle)),
        }))
    }
    pub(crate) fn storage(&self) -> Arc<Store> {
        self.store.clone()
    }
    pub(crate) fn try_request_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| ResourceExhausted.into())
    }
    pub(crate) fn try_read_memory_permit(&self, bytes: usize) -> Result<OwnedSemaphorePermit> {
        self.read_budget
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| ResourceExhausted.into())
    }
    async fn blocking_metadata<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Arc<Store>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self.try_request_permit()?;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(store)
        })
        .await?
    }
    pub async fn list_segments(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<crate::inspection::SegmentPage> {
        self.blocking_metadata(move |store| store.list_segments(cursor.as_deref(), limit))
            .await
    }
    pub async fn segment_info(
        &self,
        id: uuid::Uuid,
    ) -> Result<Option<crate::inspection::SegmentInfo>> {
        self.blocking_metadata(move |store| store.segment_info(id))
            .await
    }
    pub async fn object_info(&self, hash: [u8; 32]) -> Result<crate::inspection::ObjectInfo> {
        self.blocking_metadata(move |store| store.object_info(hash))
            .await
    }
    pub async fn put(&self, bytes: Bytes) -> Result<PutResult> {
        let start = Instant::now();
        ensure!(
            bytes.len() <= self.config.max_object_bytes(),
            "object too large"
        );
        let request = self.requests.clone().acquire_owned().await?;
        let permit = self
            .write_budget
            .clone()
            .acquire_many_owned(bytes.len().max(1) as u32)
            .await?;
        let store = self.store.clone();
        let (duplicate, bytes, _request, permit) = tokio::task::spawn_blocking(move || {
            // Hashing cannot be aborted: retain both byte and request capacity
            // until this worker exits even if its caller disconnects.
            let result = store.committed_duplicate(&bytes);
            (result, bytes, request, permit)
        })
        .await?;
        if let Some(result) = duplicate? {
            let _guard = self.admission.lock().await;
            ensure!(
                self.accepting.load(Ordering::Acquire),
                "service shutting down"
            );
            ensure!(
                !self.counters.faulted.load(Ordering::Acquire),
                "writer faulted; restart and recover before writing"
            );
            self.counters.writes.fetch_add(1, Ordering::Relaxed);
            self.counters
                .write_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            self.counters.deduplicated.fetch_add(1, Ordering::Relaxed);
            self.counters
                .deduplicated_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            self.counters.latency(start);
            return Ok(result);
        }
        let response = {
            let _guard = self.admission.lock().await;
            ensure!(
                self.accepting.load(Ordering::Acquire),
                "service shutting down"
            );
            ensure!(
                !self.counters.faulted.load(Ordering::Acquire),
                "writer faulted; restart and recover before writing"
            );
            let (tx, rx) = oneshot::channel();
            let slot = self
                .write_tx
                .reserve()
                .await
                .map_err(|_| anyhow!("writer stopped"))?;
            self.counters.write_queue.fetch_add(1, Ordering::Relaxed);
            slot.send(WriteMessage::Put(WriteRequest {
                bytes,
                response: tx,
                _permit: permit,
                submitted: start,
            }));
            rx
        };
        let result = response
            .await
            .map_err(|_| anyhow!("writer stopped before completion"))?
            .map_err(anyhow::Error::msg);
        self.counters.latency(start);
        result
    }
    pub async fn get(&self, hash: Hash) -> Result<Option<Bytes>> {
        self.get_priority(hash, Priority::Normal).await
    }
    pub async fn get_priority(&self, hash: Hash, priority: Priority) -> Result<Option<Bytes>> {
        let start = Instant::now();
        let _request = self.requests.clone().acquire_owned().await?;
        let admission = self.admission.lock().await;
        ensure!(
            self.accepting.load(Ordering::Acquire),
            "service shutting down"
        );
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        if let Some(bytes) = self.cache.lock().get(&hash) {
            self.counters
                .read_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            self.counters.latency(start);
            return Ok(Some(bytes));
        }
        let rx = {
            let mut waiters = self.waiters.lock();
            let (tx, rx) = oneshot::channel();
            if let Some(existing) = waiters.get_mut(&hash) {
                existing.retain(|sender| !sender.is_closed());
                if existing.len() >= self.config.resources.max_requests {
                    return Err(ResourceExhausted.into());
                }
                existing.push(tx);
                self.counters.read_coalesced.fetch_add(1, Ordering::Relaxed);
            } else {
                waiters.insert(hash, vec![tx]);
                self.counters.read_queue.fetch_add(1, Ordering::Relaxed);
                if self
                    .read_tx
                    .try_send(ReadRequest {
                        hash,
                        priority,
                        submitted: start,
                    })
                    .is_err()
                {
                    waiters.remove(&hash);
                    self.counters.read_queue.fetch_sub(1, Ordering::Relaxed);
                    return Err(ResourceExhausted.into());
                }
            }
            rx
        };
        drop(admission);
        let result = rx
            .await
            .map_err(|_| anyhow!("reader stopped"))?
            .map_err(shared_error)?;
        if let Some(bytes) = &result {
            self.counters
                .read_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        self.counters.latency(start);
        Ok(result)
    }
    pub async fn exists(&self, hash: Hash) -> Result<bool> {
        self.blocking_metadata(move |store| store.exists(&hash))
            .await
    }
    pub async fn status(&self) -> Result<Status> {
        let storage = self.blocking_metadata(|store| store.status()).await?;
        Ok(Status {
            mcp: self.mcp.status(),
            version: env!("CARGO_PKG_VERSION").into(),
            uptime_seconds: self.started.elapsed().as_secs_f64(),
            health: if self.counters.faulted.load(Ordering::Acquire) {
                "write-faulted"
            } else if !self.accepting.load(Ordering::Acquire) {
                "stopping"
            } else {
                "running"
            }
            .into(),
            listen: self.config.server.listen.clone(),
            storage,
            io: self.counters.snapshot(),
            cache: self.cache.lock().status(),
            errors: self.counters.errors.lock().iter().cloned().collect(),
            policy: self.config.reads.policy.clone(),
            read_workers: self.config.reads.max_concurrency,
            segment_capacity_bytes: self.config.segments.max_size_bytes,
            write_queue_bytes: self.config.writes.max_pending_mib * 1024 * 1024
                - self.write_budget.available_permits(),
            write_queue_capacity_bytes: self.config.writes.max_pending_mib * 1024 * 1024,
            events: self.counters.events.lock().iter().cloned().collect(),
            configured_budget_mib: self.config.mcp.max_response_memory_mib
                + self.config.mcp.max_concurrent_requests * self.config.mcp.max_request_kib / 1024
                + 2 * self.config.writes.max_pending_mib
                + self.config.resources.max_inflight_read_mib
                + self.config.cache.payload_mib
                + self.config.cache.index_mib,
        })
    }
    pub fn cache_clear(&self) {
        self.cache.lock().clear();
        self.counters.event("payload cache cleared".into());
    }
    pub async fn shutdown(&self) -> Result<()> {
        // Keep the receiver and join handles in state across caller cancellation.
        let result = {
            let mut state = self.shutdown_result.lock().await;
            if matches!(*state, ShutdownState::Running) {
                let _guard = self.admission.lock().await;
                self.accepting.store(false, Ordering::Release);
                self.counters
                    .event("shutdown requested; draining accepted operations".into());
                let slot = self
                    .write_tx
                    .reserve()
                    .await
                    .map_err(|_| anyhow!("writer stopped"))?;
                let (tx, rx) = oneshot::channel();
                *state = ShutdownState::Waiting(rx);
                slot.send(WriteMessage::Shutdown(tx));
            }
            if let ShutdownState::Waiting(receiver) = &mut *state {
                let result = receiver
                    .await
                    .unwrap_or_else(|_| Err("writer stopped during shutdown".into()));
                *state = ShutdownState::Done(result);
            }
            match &*state {
                ShutdownState::Done(result) => result.clone(),
                _ => unreachable!(),
            }
        };
        self.mcp.cancel_jobs().await;
        self.read_stop.cancel();
        join_worker(&self.write_handle).await?;
        join_worker(&self.read_handle).await?;
        result.map_err(anyhow::Error::msg)
    }
    pub fn metrics(&self) -> String {
        let io = self.counters.snapshot();
        let cache = self.cache.lock().status();
        let mcp = self.mcp.status();
        let mut out = String::new();
        for (name, value) in [
            ("mcp_enabled", f64::from(mcp.enabled)),
            ("mcp_read_only", f64::from(mcp.read_only)),
            ("mcp_active_exchanges", mcp.active_connections as f64),
            ("mcp_calls_total", mcp.tool_calls_total as f64),
            ("mcp_failed_calls_total", mcp.failed_tool_calls as f64),
            (
                "mcp_authorization_failures_total",
                mcp.authorization_failures as f64,
            ),
            ("mcp_rate_rejections_total", mcp.rate_rejections as f64),
            ("mcp_average_tool_latency_ms", mcp.average_tool_latency_ms),
            (
                "mcp_retained_response_bytes",
                mcp.retained_response_bytes as f64,
            ),
            ("mcp_verification_jobs", mcp.verification_jobs as f64),
            ("reads_total", io.reads as f64),
            ("writes_total", io.writes as f64),
            ("read_bytes_total", io.read_bytes as f64),
            ("write_bytes_total", io.write_bytes as f64),
            ("deduplicated_total", io.deduplicated as f64),
            ("deduplicated_bytes_total", io.deduplicated_bytes as f64),
            ("coalesced_reads_total", io.read_coalesced as f64),
            ("cache_hits_total", cache.hits as f64),
            ("cache_served_bytes_total", cache.served_bytes as f64),
            ("cache_misses_total", cache.misses as f64),
            ("cache_evictions_total", cache.evictions as f64),
            ("cache_resident_bytes", cache.bytes as f64),
            ("cache_retained_bytes", cache.retained_bytes as f64),
            ("read_queue", io.read_queue as f64),
            ("write_queue", io.write_queue as f64),
            ("inflight_reads", io.inflight_reads as f64),
            ("latency_p50_ms", io.latency_p50_ms),
            ("latency_p95_ms", io.latency_p95_ms),
            ("latency_p99_ms", io.latency_p99_ms),
            ("queue_wait_seconds_total", io.queue_wait_ms / 1000.),
            ("disk_read_seconds_total", io.disk_read_ms / 1000.),
            ("index_lookup_seconds_total", io.index_lookup_ms / 1000.),
            ("storage_write_seconds_total", io.storage_write_ms / 1000.),
        ] {
            let kind = if name.ends_with("_total") {
                "counter"
            } else {
                "gauge"
            };
            out.push_str(&format!(
                "# TYPE skerry_{name} {kind}\nskerry_{name} {value}\n"
            ));
        }
        out
    }
}

async fn join_worker(
    handle: &tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
) -> Result<()> {
    let mut handle = handle.lock().await;
    if let Some(task) = handle.as_mut() {
        task.await?;
    }
    handle.take();
    Ok(())
}

async fn write_loop(
    store: Arc<Store>,
    mut rx: mpsc::Receiver<WriteMessage>,
    counters: Arc<Counters>,
    config: Config,
) {
    let mut pending_shutdown = None;
    while let Some(message) = rx.recv().await {
        let first = match message {
            WriteMessage::Put(r) => r,
            WriteMessage::Shutdown(done) => {
                pending_shutdown = Some(done);
                break;
            }
        };
        let mut batch = vec![first];
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(config.writes.batch_max_delay_ms);
        while batch.len() < config.writes.batch_max_records {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(WriteMessage::Put(r))) => batch.push(r),
                Ok(Some(WriteMessage::Shutdown(done))) => {
                    pending_shutdown = Some(done);
                    break;
                }
                _ => break,
            }
        }
        counters
            .write_queue
            .fetch_sub(batch.len(), Ordering::Relaxed);
        for r in &batch {
            counters
                .queue_wait_us
                .fetch_add(r.submitted.elapsed().as_micros() as u64, Ordering::Relaxed);
        }
        let payloads: Vec<_> = batch.iter().map(|r| r.bytes.clone()).collect();
        let db = store.clone();
        let start = Instant::now();
        let result = if counters.faulted.load(Ordering::Acquire) {
            Err(anyhow!("writer faulted"))
        } else {
            tokio::task::spawn_blocking(move || db.put_many(&payloads))
                .await
                .map_err(anyhow::Error::from)
                .and_then(|x| x)
        };
        counters
            .storage_write_us
            .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
        match result {
            Ok(results) => {
                counters.event(format!(
                    "durably completed {} write requests",
                    results.len()
                ));
                for (r, result) in batch.into_iter().zip(results) {
                    counters.writes.fetch_add(1, Ordering::Relaxed);
                    counters
                        .write_bytes
                        .fetch_add(r.bytes.len() as u64, Ordering::Relaxed);
                    if result.duplicate {
                        counters.deduplicated.fetch_add(1, Ordering::Relaxed);
                        counters
                            .deduplicated_bytes
                            .fetch_add(r.bytes.len() as u64, Ordering::Relaxed);
                    }
                    let _ = r.response.send(Ok(result));
                }
            }
            Err(e) => {
                let error = format!("{e:#}");
                counters.faulted.store(true, Ordering::Release);
                counters.error(error.clone());
                for r in batch {
                    let _ = r.response.send(Err(error.clone()));
                }
            }
        }
        if pending_shutdown.is_some() {
            break;
        }
    }
    let result = if counters.faulted.load(Ordering::Acquire) {
        Err(anyhow!(
            "writer faulted; shutdown cannot certify final persistence"
        ))
    } else {
        tokio::task::spawn_blocking(move || {
            store.seal()?;
            store.sync()
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(|x| x)
    };
    if let Some(done) = pending_shutdown {
        let _ = done.send(result.map_err(|e| format!("{e:#}")));
    } else if let Err(e) = result {
        counters.error(format!("{e:#}"));
    }
}

struct ReadControl {
    stop: tokio_util::sync::CancellationToken,
    budget: Arc<Semaphore>,
}

async fn read_loop(
    store: Arc<Store>,
    mut rx: mpsc::Receiver<ReadRequest>,
    waiters: Waiters,
    cache: Arc<Mutex<PayloadCache>>,
    counters: Arc<Counters>,
    config: Config,
    control: ReadControl,
) {
    let ReadControl { stop, budget } = control;
    let workers = Arc::new(Semaphore::new(config.reads.max_concurrency));

    loop {
        let first = tokio::select! { biased; item=rx.recv()=>match item {Some(item)=>item,None=>break}, _=stop.cancelled()=>break };
        let mut batch = vec![first];
        if config.reads.policy == "hdd" {
            tokio::time::sleep(Duration::from_millis(config.reads.hdd_window_ms)).await;
        }
        while batch.len() < 512 {
            match rx.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }
        // Resolve metadata in one blocking job. Sort HDD reads only within aged priority classes.
        let db = store.clone();
        let hdd = config.reads.policy == "hdd";
        let lookup_counters = counters.clone();
        let resolved = tokio::task::spawn_blocking(move || {
            let mut items: Vec<_> = batch
                .into_iter()
                .map(|r| {
                    let start = Instant::now();
                    let location = db.location(&r.hash).map_err(|e| format!("{e:#}"));
                    lookup_counters
                        .index_lookup_us
                        .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
                    (r, location)
                })
                .collect();
            let scheduled_at = Instant::now();
            items.sort_by_key(|(r, l)| {
                let physical = if hdd {
                    l.as_ref()
                        .ok()
                        .and_then(|x| x.as_ref())
                        .map(|l| (l.segment, l.offset))
                        .unwrap_or(([0; 16], 0))
                } else {
                    ([0; 16], 0)
                };
                (
                    r.priority
                        .rank(scheduled_at.saturating_duration_since(r.submitted)),
                    physical,
                )
            });
            items
        })
        .await;
        let items = match resolved {
            Ok(v) => v,
            Err(e) => {
                counters.error(format!("read scheduler failed: {e}"));
                let waiting = std::mem::take(&mut *waiters.lock());
                for (_, senders) in waiting {
                    for tx in senders {
                        let _ = tx.send(Err("read scheduler failed".into()));
                    }
                }
                continue;
            }
        };
        for (request, location) in items {
            counters.read_queue.fetch_sub(1, Ordering::Relaxed);
            let length = match &location {
                Ok(Some(l)) => l.length,
                _ => 0,
            };
            if length > config.max_object_bytes() as u64 {
                complete(
                    &waiters,
                    request.hash,
                    Err("stored object exceeds configured maximum".into()),
                );
                continue;
            }
            let permit = match budget.clone().try_acquire_many_owned(length.max(1) as u32) {
                Ok(p) => p,
                Err(_) => {
                    complete(
                        &waiters,
                        request.hash,
                        Err("read buffer capacity exhausted; retry later".into()),
                    );
                    continue;
                }
            };
            let worker = match workers.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break,
            };
            counters.queue_wait_us.fetch_add(
                request.submitted.elapsed().as_micros() as u64,
                Ordering::Relaxed,
            );
            counters.inflight_reads.fetch_add(1, Ordering::Relaxed);
            let db = store.clone();
            let waiters = waiters.clone();
            let cache = cache.clone();
            let counters = counters.clone();
            tokio::spawn(async move {
                let _worker = worker;
                let hash = request.hash;
                let start = Instant::now();
                let result = match location {
                    Err(e) => Err(e),
                    Ok(None) => Ok(None),
                    Ok(Some(_)) => tokio::task::spawn_blocking(move || db.get(&hash))
                        .await
                        .map_err(|e| e.to_string())
                        .and_then(|r| r.map_err(|e| format!("{e:#}"))),
                };
                counters
                    .disk_read_us
                    .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
                let result = result.map(|data| {
                    data.map(|data| {
                        Bytes::from_owner(BudgetBuffer {
                            data,
                            _permit: permit,
                        })
                    })
                });
                if let Ok(Some(bytes)) = &result {
                    cache.lock().observe(hash, bytes);
                }
                if let Err(e) = &result {
                    counters.error(e.clone());
                }
                counters.inflight_reads.fetch_sub(1, Ordering::Relaxed);
                complete(&waiters, hash, result);
            });
        }
    }
    // Every accepted read finishes before dispatcher shutdown releases store handles.
    let _drained = workers
        .acquire_many(config.reads.max_concurrency as u32)
        .await;
}
fn complete(waiters: &Waiters, hash: Hash, result: SharedResult) {
    if let Some(senders) = waiters.lock().remove(&hash) {
        for tx in senders {
            let _ = tx.send(result.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_cache_second_hit_and_retained_buffers() {
        let mut config = Config::default();
        config.cache.payload_mib = 1;
        config.cache.admission_entries = 2;
        let mut cache = PayloadCache::new(&config);
        let data = Bytes::from(vec![1; 100]);
        let hash = [1; 32];
        cache.observe(hash, &data);
        assert_eq!(cache.status().entries, 0);
        cache.observe(hash, &data);
        assert_eq!(cache.status().entries, 1);
        let held = cache.get(&hash).unwrap();
        cache.clear();
        assert_eq!(cache.status().bytes, 0);
        assert_eq!(cache.status().retained_bytes, 100);
        drop(held);
        assert_eq!(cache.status().retained_bytes, 0);
        for h in 0..100 {
            cache.observe([h; 32], &data);
        }
        assert!(cache.seen.len() <= 2);
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    #[test]
    fn cancelled_duplicate_probe_retains_byte_and_request_capacity() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let root = tempfile::tempdir().unwrap();
            let mut config = Config::default();
            config.storage.root = root.path().into();
            let c = Coordinator::open(config).await.unwrap();
            let capacity = c.write_budget.available_permits();
            let (started_tx, started_rx) = oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.await.unwrap();
            let put = {
                let c = c.clone();
                tokio::spawn(async move { c.put(Bytes::from_static(b"probe")).await })
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                while c.write_budget.available_permits() == capacity {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            put.abort();
            let _ = put.await;
            assert_eq!(c.write_budget.available_permits(), capacity - 5);
            assert_eq!(
                c.requests.available_permits(),
                c.config.resources.max_requests - 1
            );
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            let all = tokio::time::timeout(
                Duration::from_secs(5),
                c.write_budget.clone().acquire_many_owned(capacity as u32),
            )
            .await
            .unwrap()
            .unwrap();
            drop(all);
            assert_eq!(
                c.requests.available_permits(),
                c.config.resources.max_requests
            );
            assert_eq!(c.counters.snapshot().writes, 0);
            c.shutdown().await.unwrap();
        });
    }
    #[tokio::test]
    async fn durable_duplicates_bypass_window_but_obey_admission_and_accounting() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        config.writes.batch_max_delay_ms = 1000;
        let bytes = Bytes::from_static(b"already committed");
        {
            let store = Store::open(&config).unwrap();
            store.put_many(std::slice::from_ref(&bytes)).unwrap();
        }
        let c = Coordinator::open(config).await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(500), c.put(bytes.clone()))
            .await
            .expect("duplicate must bypass the one-second accumulation window")
            .unwrap();
        assert!(result.duplicate);
        let io = c.counters.snapshot();
        assert_eq!((io.writes, io.deduplicated, io.write_queue), (1, 1, 0));
        assert_eq!(io.deduplicated_bytes, bytes.len() as u64);
        c.counters.faulted.store(true, Ordering::Release);
        assert!(c.put(bytes.clone()).await.is_err());
        c.counters.faulted.store(false, Ordering::Release);
        c.shutdown().await.unwrap();
        assert!(c.put(bytes).await.is_err());
    }
    #[tokio::test]
    async fn abandoned_coalesced_waiters_are_pruned() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        config.cache.payload_mib = 0;
        config.reads.policy = "hdd".into();
        config.reads.hdd_window_ms = 1000;
        let c = Coordinator::open(config).await.unwrap();
        let hash = c
            .put(Bytes::from_static(b"cancelled waiter"))
            .await
            .unwrap()
            .hash;
        for _ in 0..32 {
            let _ = tokio::time::timeout(Duration::from_millis(1), c.get(hash)).await;
        }
        assert!(
            c.waiters
                .lock()
                .get(&hash)
                .is_some_and(|waiters| waiters.len() <= 1)
        );
        c.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn cancelled_metadata_call_retains_capacity_until_blocking_worker_exits() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        config.resources.max_requests = 1;
        let c = Coordinator::open(config).await.unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let call = {
            let c = c.clone();
            tokio::spawn(async move {
                c.blocking_metadata(move |_| {
                    let _ = started_tx.send(());
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .await
            })
        };
        started_rx.await.unwrap();
        call.abort();
        let _ = call.await;
        assert!(
            c.try_request_permit().is_err(),
            "cancelled waiter must not release running worker capacity"
        );
        release_tx.send(()).unwrap();
        let released =
            tokio::time::timeout(Duration::from_secs(5), c.requests.clone().acquire_owned())
                .await
                .unwrap()
                .unwrap();
        drop(released);
        assert!(c.status().await.is_ok());
        c.shutdown().await.unwrap();
    }
}
