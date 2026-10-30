//! Bounded asynchronous coordination. All storage/index calls execute on blocking workers.
mod cache;
mod checkpoints;
mod writers;
pub use writers::{WriteLaneStatus, WriterStatus};

use crate::{
    config::Config,
    engine::{DeleteResult, PreparedPut, PutResult, StorageStatus, Store},
};
use anyhow::{Result, anyhow, ensure};
use bytes::Bytes;
use cache::PayloadCache;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
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
type ReadKey = (Hash, u64);
type Waiters = Arc<Mutex<HashMap<ReadKey, Vec<oneshot::Sender<SharedResult>>>>>;

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
    #[serde(default)]
    pub storage_io: crate::engine::WriteTimingSnapshot,
    pub io: IoStatus,
    #[serde(default)]
    pub writers: WriterStatus,
    pub cache: CacheStatus,
    #[serde(default)]
    pub gc: GcStatus,
    #[serde(default)]
    pub checkpoint: CheckpointStatus,
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
pub struct CheckpointStatus {
    pub enabled: bool,
    pub running: bool,
    pub completed_runs: u64,
    pub last_result: Option<serde_json::Value>,
    pub last_error: Option<String>,
    pub progress: Option<crate::progress::ProgressSnapshot>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GcStatus {
    pub enabled: bool,
    pub running: bool,
    pub completed_runs: u64,
    pub last_result: Option<serde_json::Value>,
    pub last_error: Option<String>,
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
    #[serde(default)]
    pub write_batches: u64,
    #[serde(default)]
    pub write_batch_records: u64,
    #[serde(default)]
    pub write_batch_bytes: u64,
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
    #[serde(default)]
    pub metadata_bytes: usize,
    #[serde(default)]
    pub metadata_budget_bytes: usize,
    #[serde(default)]
    pub payload_budget_bytes: usize,
    #[serde(default)]
    pub skipped_large: u64,
    #[serde(default)]
    pub skipped_metadata: u64,
    #[serde(default)]
    pub skipped_payload: u64,
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
    write_batches: AtomicU64,
    write_batch_records: AtomicU64,
    write_batch_bytes: AtomicU64,
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
            write_batches: self.write_batches.load(Ordering::Relaxed),
            write_batch_records: self.write_batch_records.load(Ordering::Relaxed),
            write_batch_bytes: self.write_batch_bytes.load(Ordering::Relaxed),
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

#[derive(Clone, Debug)]
enum WriteFailure {
    StorageFull,
    Other(String),
}
impl From<WriteFailure> for anyhow::Error {
    fn from(value: WriteFailure) -> Self {
        match value {
            WriteFailure::StorageFull => crate::engine::StorageFull.into(),
            WriteFailure::Other(message) => anyhow::Error::msg(message),
        }
    }
}
struct WriteRequest {
    payload: PreparedPut,
    response: oneshot::Sender<std::result::Result<PutResult, WriteFailure>>,
    _permit: OwnedSemaphorePermit,
    _request: OwnedSemaphorePermit,
    ticket: writers::WriteTicket,
}
enum WriteMessage {
    Put(WriteRequest),
    Delete {
        hash: Hash,
        response: oneshot::Sender<std::result::Result<DeleteResult, String>>,
        _request: OwnedSemaphorePermit,
        ticket: writers::WriteTicket,
    },
    Stream {
        upload: crate::engine::PreparedStreamUpload,
        length: usize,
        response: oneshot::Sender<std::result::Result<PutResult, WriteFailure>>,
        _slot: OwnedSemaphorePermit,
        _gate: OwnedSemaphorePermit,
        _request: OwnedSemaphorePermit,
        ticket: writers::WriteTicket,
    },
    Shutdown(oneshot::Sender<std::result::Result<(), String>>),
}
struct ReadRequest {
    hash: Hash,
    epoch: u64,
    priority: Priority,
    submitted: Instant,
}

enum ShutdownState {
    Running,
    Waiting(oneshot::Receiver<std::result::Result<(), String>>),
    Done(std::result::Result<(), String>),
}

type MonitorHandle = Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>;
#[cfg(test)]
type PutProbeHook = (oneshot::Sender<()>, oneshot::Receiver<()>);
pub struct Coordinator {
    pub mcp: Arc<crate::mcp::Runtime>,
    #[cfg(test)]
    put_probe_hook: Mutex<Option<PutProbeHook>>,
    #[cfg(test)]
    stream_prepare_hook: Mutex<Option<PutProbeHook>>,
    read_budget: Arc<Semaphore>,
    pub config: Config,
    store: Arc<Store>,
    write_tx: mpsc::Sender<WriteMessage>,
    read_tx: mpsc::Sender<ReadRequest>,
    write_budget: Arc<Semaphore>,
    writer_state: Arc<writers::WriterState>,
    requests: Arc<Semaphore>,
    stream_finalize_gate: Arc<Semaphore>,
    stream_read_gate: Arc<Semaphore>,
    waiters: Waiters,
    cache: Arc<PayloadCache>,
    read_epoch: Arc<AtomicU64>,
    gc_status: Arc<Mutex<GcStatus>>,
    gc_gate: Arc<Semaphore>,
    maintenance_stop: tokio_util::sync::CancellationToken,
    monitor_handles: Mutex<Vec<MonitorHandle>>,
    gc_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    checkpoint_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    checkpoint_state: Arc<Mutex<checkpoints::State>>,
    counters: Arc<Counters>,
    started: Instant,
    accepting: AtomicBool,
    admission: Arc<tokio::sync::Mutex<()>>,
    shutdown_result: tokio::sync::Mutex<ShutdownState>,
    read_stop: tokio_util::sync::CancellationToken,
    read_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    write_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Coordinator {
    /// Constant-time readiness; does not enumerate files or index entries.
    pub fn is_ready(&self) -> bool {
        self.accepting.load(Ordering::Acquire) && !self.write_faulted()
    }
    fn write_faulted(&self) -> bool {
        self.counters.faulted.load(Ordering::Acquire) || self.store.publication_faulted()
    }
    pub async fn open(config: Config) -> Result<Arc<Self>> {
        Self::open_with_progress(config, Arc::new(crate::progress::Progress::new())).await
    }
    pub async fn open_with_progress(
        config: Config,
        progress: Arc<crate::progress::Progress>,
    ) -> Result<Arc<Self>> {
        config.validate()?;
        let c = config.clone();
        let store = Arc::new(
            tokio::task::spawn_blocking(move || Store::open_with_progress(&c, &progress)).await??,
        );
        let (write_tx, write_rx) = mpsc::channel(config.writes.batch_max_records * 2);
        let (read_tx, read_rx) = mpsc::channel(config.reads.queue_items);
        let counters = Arc::new(Counters::default());
        counters.event("store opened; durable write coordinator ready".into());
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        let cache = Arc::new(PayloadCache::new(&config));
        let read_epoch = Arc::new(AtomicU64::new(0));
        let writer_state = Arc::new(writers::WriterState::new(
            config.writes.writer_lanes,
            counters.clone(),
        ));
        let write_handle = tokio::spawn(writers::write_loop(
            store.clone(),
            write_rx,
            counters.clone(),
            config.clone(),
            cache.clone(),
            read_epoch.clone(),
            writer_state.clone(),
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
                epoch: read_epoch.clone(),
            },
        ));
        let weak = Arc::downgrade(&cache);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                let Some(cache) = weak.upgrade() else { break };
                cache.maintain();
            }
        });
        let gc_status = Arc::new(Mutex::new(GcStatus {
            enabled: config.gc.enabled,
            ..GcStatus::default()
        }));
        let maintenance_stop = tokio_util::sync::CancellationToken::new();
        let coordinator = Arc::new(Self {
            mcp: crate::mcp::Runtime::new(&config),
            #[cfg(test)]
            put_probe_hook: Mutex::new(None),
            #[cfg(test)]
            stream_prepare_hook: Mutex::new(None),
            read_budget,
            write_budget: Arc::new(Semaphore::new(config.writes.max_pending_mib * 1024 * 1024)),
            requests: Arc::new(Semaphore::new(config.resources.max_requests)),
            stream_finalize_gate: Arc::new(Semaphore::new(config.streaming.max_uploads)),
            stream_read_gate: Arc::new(Semaphore::new(config.streaming.max_reads)),
            config,
            store,
            write_tx,
            writer_state,
            read_tx,
            waiters,
            cache,
            read_epoch,
            gc_status,
            gc_gate: Arc::new(Semaphore::new(1)),
            maintenance_stop: maintenance_stop.clone(),
            monitor_handles: Mutex::new(Vec::new()),
            gc_handle: tokio::sync::Mutex::new(None),
            checkpoint_handle: tokio::sync::Mutex::new(None),
            checkpoint_state: Arc::new(Mutex::new(checkpoints::State::default())),
            counters,
            started: Instant::now(),
            accepting: AtomicBool::new(true),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            shutdown_result: tokio::sync::Mutex::new(ShutdownState::Running),
            read_stop,
            read_handle: tokio::sync::Mutex::new(Some(read_handle)),
            write_handle: tokio::sync::Mutex::new(Some(write_handle)),
        });
        if coordinator.config.gc.enabled {
            let weak = Arc::downgrade(&coordinator);
            let interval_seconds = coordinator.config.gc.interval_seconds;
            let handle = tokio::spawn(async move {
                let start = tokio::time::Instant::now() + Duration::from_secs(interval_seconds);
                let mut interval =
                    tokio::time::interval_at(start, Duration::from_secs(interval_seconds));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        biased;
                        _ = maintenance_stop.cancelled() => break,
                        _ = interval.tick() => {
                            let Some(c) = weak.upgrade() else { break };
                            if let Err(error) = c.collect_garbage(false).await {
                                c.counters.error(format!("scheduled GC: {error:#}"));
                            }
                        }
                    }
                }
            });
            *coordinator.gc_handle.lock().await = Some(handle);
        }
        *coordinator.checkpoint_handle.lock().await = checkpoints::spawn(&coordinator);
        Ok(coordinator)
    }
    pub(crate) fn has_cached_payload(&self, hash: &[u8; 32]) -> bool {
        self.cache.contains(hash)
    }
    pub(crate) fn monitor_stop(&self) -> tokio_util::sync::CancellationToken {
        self.maintenance_stop.clone()
    }
    pub(crate) fn register_monitor(
        &self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        let mut handles = self.monitor_handles.lock();
        if self.maintenance_stop.is_cancelled() {
            return;
        }
        // Reap completed producers as new work arrives; retaining every joined
        // stream until shutdown would make the tracking list grow without bound.
        handles.retain(|handle| {
            handle.try_lock().map_or(true, |guard| {
                guard.as_ref().is_some_and(|task| !task.is_finished())
            })
        });
        handles.push(Arc::new(tokio::sync::Mutex::new(Some(tokio::spawn(task)))));
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
    pub(crate) async fn stream_read_gate(&self) -> Result<OwnedSemaphorePermit> {
        let permit = tokio::time::timeout(
            Duration::from_millis(self.config.resources.admission_timeout_ms),
            self.stream_read_gate.clone().acquire_owned(),
        )
        .await
        .map_err(|_| ResourceExhausted)??;
        {
            let _guard = self.admission.lock().await;
            ensure!(self.is_ready(), "service is not accepting reads");
        }
        Ok(permit)
    }
    pub(crate) async fn stream_request_permit(&self) -> Result<OwnedSemaphorePermit> {
        Ok(tokio::time::timeout(
            Duration::from_millis(self.config.resources.admission_timeout_ms),
            self.requests.clone().acquire_owned(),
        )
        .await
        .map_err(|_| ResourceExhausted)??)
    }
    pub(crate) fn stream_read_started(&self) {
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        self.counters.inflight_reads.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn stream_read_finished(&self) {
        self.counters.inflight_reads.fetch_sub(1, Ordering::Relaxed);
    }
    pub(crate) fn stream_read_bytes(&self, bytes: usize) {
        self.counters
            .read_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub(crate) async fn read_memory_permit(&self, bytes: usize) -> Result<OwnedSemaphorePermit> {
        Ok(tokio::time::timeout(
            Duration::from_millis(self.config.resources.admission_timeout_ms),
            self.read_budget
                .clone()
                .acquire_many_owned(bytes.max(1) as u32),
        )
        .await
        .map_err(|_| ResourceExhausted)??)
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
    pub(crate) async fn finish_stream_upload(
        &self,
        upload: crate::engine::StreamUpload,
        slot: OwnedSemaphorePermit,
        length: usize,
    ) -> Result<PutResult> {
        let started = Instant::now();
        let gate = self.stream_finalize_gate.clone().acquire_owned().await?;
        let request = tokio::time::timeout(
            Duration::from_millis(self.config.resources.admission_timeout_ms),
            self.requests.clone().acquire_owned(),
        )
        .await
        .map_err(|_| ResourceExhausted)??;
        {
            let _guard = self.admission.lock().await;
            ensure!(self.is_ready(), "service is not accepting writes");
        }
        // Bulk checksum/footer/sync work must not occupy an append lane or the
        // global admission lock. Its blocking worker owns every transfer permit.
        let (upload, gate, slot, request) = tokio::task::spawn_blocking(move || {
            let prepared = upload.prepare_finish();
            prepared.map(|upload| (upload, gate, slot, request))
        })
        .await??;
        ensure!(
            upload.len() == length as u64,
            "stream completion length mismatch"
        );
        #[cfg(test)]
        {
            let hook = self.stream_prepare_hook.lock().take();
            if let Some((ready, release)) = hook {
                let _ = ready.send(());
                let _ = release.await;
            }
        }
        let response = {
            let _guard = self.admission.lock().await;
            ensure!(self.is_ready(), "service is not accepting writes");
            let channel = self
                .write_tx
                .reserve()
                .await
                .map_err(|_| anyhow!("writer stopped"))?;
            let (response, wait) = oneshot::channel();
            // A prepared file consumes no queued payload RAM, but retains a
            // global request slot and its transfer/finalization permits.
            let ticket = self.writer_state.ticket(upload.hash(), 0, started);
            channel.send(WriteMessage::Stream {
                upload,
                length,
                response,
                _slot: slot,
                _gate: gate,
                _request: request,
                ticket,
            });
            wait
        };
        response
            .await
            .map_err(|_| anyhow!("writer stopped before stream completion"))?
            .map_err(anyhow::Error::from)
    }
    pub async fn put(&self, bytes: Bytes) -> Result<PutResult> {
        let start = Instant::now();
        ensure!(
            bytes.len() <= self.config.max_object_bytes(),
            "object too large"
        );
        let request = tokio::time::timeout(
            Duration::from_millis(self.config.resources.admission_timeout_ms),
            self.requests.clone().acquire_owned(),
        )
        .await
        .map_err(|_| ResourceExhausted)??;
        let permit = tokio::time::timeout(
            Duration::from_millis(self.config.resources.admission_timeout_ms),
            self.write_budget
                .clone()
                .acquire_many_owned(bytes.len().max(1) as u32),
        )
        .await
        .map_err(|_| ResourceExhausted)??;
        let store = self.store.clone();
        let probe_epoch = self.read_epoch.load(Ordering::Acquire);
        let (payload, duplicate, request, permit) = tokio::task::spawn_blocking(move || {
            // A cancelled caller cannot release the bytes/request while hashing.
            let payload = store.prepare_put(bytes);
            // The healthy publication gate certifies this mapping. Admission
            // below still checks shard work and the deletion epoch before ack.
            let duplicate = payload.as_ref().map_or_else(
                |_| Ok(None),
                |payload| store.committed_prepared_duplicate(payload),
            );
            (payload, duplicate, request, permit)
        })
        .await?;
        let payload = payload?;
        let duplicate = duplicate?;
        #[cfg(test)]
        {
            let hook = self.put_probe_hook.lock().take();
            if let Some((ready, release)) = hook {
                let _ = ready.send(());
                let _ = release.await;
            }
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
            if let Some(result) = duplicate
                && self.writer_state.pending_deletes(&payload.hash()) == 0
                && self.read_epoch.load(Ordering::Acquire) == probe_epoch
            {
                ensure!(
                    !self.store.publication_faulted(),
                    "writer faulted; restart and recover before writing"
                );
                self.counters.writes.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .write_bytes
                    .fetch_add(payload.len() as u64, Ordering::Relaxed);
                self.counters.deduplicated.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .deduplicated_bytes
                    .fetch_add(payload.len() as u64, Ordering::Relaxed);
                self.counters.latency(start);
                return Ok(result);
            }
            let (tx, rx) = oneshot::channel();
            let slot = self
                .write_tx
                .reserve()
                .await
                .map_err(|_| anyhow!("writer stopped"))?;
            let ticket = self
                .writer_state
                .ticket(payload.hash(), payload.len(), start);
            slot.send(WriteMessage::Put(WriteRequest {
                payload,
                response: tx,
                _permit: permit,
                _request: request,
                ticket,
            }));
            rx
        };
        let result = response
            .await
            .map_err(|_| anyhow!("writer stopped before completion"))?
            .map_err(anyhow::Error::from);
        self.counters.latency(start);
        result
    }
    pub async fn delete(&self, hash: Hash) -> Result<DeleteResult> {
        let started = Instant::now();
        let request = self.requests.clone().acquire_owned().await?;
        let receiver = {
            let _admission = self.admission.lock().await;
            ensure!(
                self.accepting.load(Ordering::Acquire),
                "service shutting down"
            );
            ensure!(
                !self.counters.faulted.load(Ordering::Acquire),
                "writer faulted"
            );
            let slot = self
                .write_tx
                .reserve()
                .await
                .map_err(|_| anyhow!("writer stopped"))?;
            let (tx, rx) = oneshot::channel();
            let ticket = self.writer_state.delete_ticket(hash, started);
            slot.send(WriteMessage::Delete {
                hash,
                response: tx,
                _request: request,
                ticket,
            });
            rx
        };
        let result = receiver
            .await
            .map_err(|_| anyhow!("writer stopped before deletion completed"))?
            .map_err(anyhow::Error::msg);
        self.counters.latency(started);
        result
    }
    pub async fn collect_garbage(&self, force: bool) -> Result<serde_json::Value> {
        let request = self.requests.clone().acquire_owned().await?;
        let handle = {
            let _admission = self.admission.lock().await;
            ensure!(
                self.accepting.load(Ordering::Acquire),
                "service shutting down"
            );
            ensure!(
                !self.counters.faulted.load(Ordering::Acquire),
                "writer faulted"
            );
            let gate = self
                .gc_gate
                .clone()
                .try_acquire_owned()
                .map_err(|_| ResourceExhausted)?;
            let store = self.store.clone();
            let status = self.gc_status.clone();
            let counters = self.counters.clone();
            status.lock().running = true;
            tokio::task::spawn_blocking(move || {
                struct Run {
                    status: Arc<Mutex<GcStatus>>,
                    _request: OwnedSemaphorePermit,
                    _gate: OwnedSemaphorePermit,
                }
                impl Drop for Run {
                    fn drop(&mut self) {
                        self.status.lock().running = false;
                    }
                }
                let _run = Run {
                    status: status.clone(),
                    _request: request,
                    _gate: gate,
                };
                let result = store.collect_garbage(force);
                let mut state = status.lock();
                state.completed_runs += 1;
                match &result {
                    Ok(value) => {
                        state.last_result = Some(value.clone());
                        state.last_error = None;
                        counters.event("garbage collection pass completed".into());
                    }
                    Err(error) => {
                        state.last_error = Some(format!("{error:#}"));
                        if store.write_faulted() {
                            counters.faulted.store(true, Ordering::Release);
                        }
                    }
                }
                result
            })
        };
        handle.await?
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
        let (cached, epoch) = {
            (
                self.cache.get(&hash),
                self.read_epoch.load(Ordering::Acquire),
            )
        };
        if let Some(bytes) = cached {
            self.counters
                .read_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            self.counters.latency(start);
            return Ok(Some(bytes));
        }
        let key = (hash, epoch);
        let rx = {
            let mut waiters = self.waiters.lock();
            let (tx, rx) = oneshot::channel();
            if let Some(existing) = waiters.get_mut(&key) {
                existing.retain(|sender| !sender.is_closed());
                if existing.len() >= self.config.resources.max_requests {
                    return Err(ResourceExhausted.into());
                }
                existing.push(tx);
                self.counters.read_coalesced.fetch_add(1, Ordering::Relaxed);
            } else {
                waiters.insert(key, vec![tx]);
                self.counters.read_queue.fetch_add(1, Ordering::Relaxed);
                if self
                    .read_tx
                    .try_send(ReadRequest {
                        hash,
                        epoch,
                        priority,
                        submitted: start,
                    })
                    .is_err()
                {
                    waiters.remove(&key);
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
        Ok(self.status_from_storage(storage))
    }
    pub(crate) fn initial_monitoring_status(&self) -> Result<Status> {
        Ok(self.status_from_storage(self.store.status()?))
    }
    pub(crate) async fn monitoring_status(&self) -> Result<Status> {
        let store = self.store.clone();
        let storage = tokio::task::spawn_blocking(move || store.status()).await??;
        Ok(self.status_from_storage(storage))
    }
    fn status_from_storage(&self, storage: StorageStatus) -> Status {
        Status {
            mcp: self.mcp.status(),
            version: env!("CARGO_PKG_VERSION").into(),
            uptime_seconds: self.started.elapsed().as_secs_f64(),
            health: if self.write_faulted() {
                "write-faulted"
            } else if !self.accepting.load(Ordering::Acquire) {
                "stopping"
            } else {
                "running"
            }
            .into(),
            listen: self.config.server.listen.clone(),
            storage,
            storage_io: self.store.write_timings(),
            io: self.counters.snapshot(),
            writers: self.writer_status(),
            cache: self.cache.status(),
            gc: self.gc_status.lock().clone(),
            checkpoint: self
                .checkpoint_state
                .lock()
                .status(self.config.checkpoint.enabled),
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
                + self.config.cache.index_mib
                + self.config.cache.metadata_mib
                + if self.config.streaming.checksum_cache_mib > 0 {
                    self.config.streaming.checksum_cache_mib
                } else {
                    let chunk = self.config.streaming.chunk_kib * 1024;
                    let bytes =
                        (self.config.streaming.max_object_mib * 1024 * 1024).div_ceil(chunk) * 32
                            + 512;
                    bytes
                        .saturating_mul(self.config.streaming.max_reads)
                        .div_ceil(1024 * 1024)
                },
        }
    }
    pub fn writer_status(&self) -> WriterStatus {
        let mut status = self.writer_state.status();
        for (lane, timings) in status.lanes.iter_mut().zip(self.store.write_lane_timings()) {
            lane.append_seconds = timings.append_us as f64 / 1_000_000.;
            lane.data_sync_seconds = timings.data_sync_us as f64 / 1_000_000.;
            lane.commit_wait_seconds = timings.commit_wait_us as f64 / 1_000_000.;
        }
        status
    }
    pub(crate) fn live_cache_status(&self) -> CacheStatus {
        self.cache.status()
    }
    pub(crate) fn refresh_live_status(&self, status: &mut Status) {
        status.uptime_seconds = self.started.elapsed().as_secs_f64();
        status.io = self.counters.snapshot();
        status.writers = self.writer_status();
        status.storage_io = self.store.write_timings();
        status.checkpoint = self
            .checkpoint_state
            .lock()
            .status(self.config.checkpoint.enabled);
        status.cache = self.cache.status();
        status.health = if self.write_faulted() {
            "write-faulted"
        } else if !self.accepting.load(Ordering::Acquire) {
            "stopping"
        } else {
            "running"
        }
        .into();
        status.write_queue_bytes = self.config.writes.max_pending_mib * 1024 * 1024
            - self.write_budget.available_permits();
    }
    pub fn cache_clear(&self) {
        self.cache.clear();
        self.counters.event("payload cache cleared".into());
    }
    pub async fn shutdown(&self) -> Result<()> {
        // Keep the receiver and join handles in state across caller cancellation.
        let result = {
            let mut state = self.shutdown_result.lock().await;
            if matches!(*state, ShutdownState::Running) {
                {
                    let _guard = self.admission.lock().await;
                    self.accepting.store(false, Ordering::Release);
                    self.maintenance_stop.cancel();
                    checkpoints::cancel(self);
                }
                let _streams_drained = self
                    .stream_finalize_gate
                    .clone()
                    .acquire_many_owned(self.config.streaming.max_uploads as u32)
                    .await?;
                let _reads_drained = self
                    .stream_read_gate
                    .clone()
                    .acquire_many_owned(self.config.streaming.max_reads as u32)
                    .await?;
                // Non-abortable compaction retains this gate until its last
                // blocking operation finishes, including after caller cancellation.
                let _gc_drained = self.gc_gate.clone().acquire_owned().await?;
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
        join_worker(&self.gc_handle).await?;
        join_worker(&self.checkpoint_handle).await?;
        let monitors = self.monitor_handles.lock().clone();
        for monitor in monitors {
            join_worker(&monitor).await?;
        }
        self.read_stop.cancel();
        join_worker(&self.write_handle).await?;
        join_worker(&self.read_handle).await?;
        result.map_err(anyhow::Error::msg)
    }
    pub fn metrics(&self) -> String {
        let io = self.counters.snapshot();
        let cache = self.cache.status();
        let storage_io = self.store.write_timings();
        let mcp = self.mcp.status();
        let mut out = String::new();
        out.push_str(&format!(
            "# TYPE skerry_ready gauge\nskerry_ready {}\n# TYPE skerry_writer_faulted gauge\nskerry_writer_faulted {}\n",
            u8::from(self.is_ready()), u8::from(self.write_faulted()),
        ));
        let writers = self.writer_status();
        out.push_str(&format!(
            "# TYPE skerry_writer_lanes gauge\nskerry_writer_lanes {}\n",
            writers.configured_lanes
        ));
        for lane in writers.lanes {
            for (name, value, kind) in [
                ("queued_items", lane.queued_items as f64, "gauge"),
                ("queued_bytes", lane.queued_bytes as f64, "gauge"),
                ("inflight_items", lane.inflight_items as f64, "gauge"),
                ("inflight_bytes", lane.inflight_bytes as f64, "gauge"),
                ("active_shards", lane.active_shards as f64, "gauge"),
                ("oldest_wait_seconds", lane.oldest_wait_seconds, "gauge"),
                ("batches_total", lane.batches as f64, "counter"),
                (
                    "completed_items_total",
                    lane.completed_items as f64,
                    "counter",
                ),
                (
                    "queue_wait_seconds_total",
                    lane.queue_wait_seconds,
                    "counter",
                ),
                ("service_seconds_total", lane.service_seconds, "counter"),
                ("append_seconds_total", lane.append_seconds, "counter"),
                ("data_sync_seconds_total", lane.data_sync_seconds, "counter"),
                (
                    "commit_wait_seconds_total",
                    lane.commit_wait_seconds,
                    "counter",
                ),
            ] {
                if lane.lane == 0 {
                    out.push_str(&format!("# TYPE skerry_writer_lane_{name} {kind}\n"));
                }
                out.push_str(&format!(
                    "skerry_writer_lane_{name}{{lane=\"{}\"}} {value}\n",
                    lane.lane
                ));
            }
        }
        for (name, value) in [
            ("data_sync_us", storage_io.data_sync_us),
            ("data_sync_calls", storage_io.data_sync_calls),
            ("journal_sync_us", storage_io.journal_sync_us),
            ("journal_sync_calls", storage_io.journal_sync_calls),
            ("witness_sync_us", storage_io.witness_sync_us),
            ("witness_sync_calls", storage_io.witness_sync_calls),
            ("index_apply_us", storage_io.index_apply_us),
            ("index_persist_us", storage_io.index_persist_us),
        ] {
            out.push_str(&format!(
                "# TYPE skerry_{name}_total counter\nskerry_{name}_total {value}\n"
            ));
        }
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
            ("write_batches_total", io.write_batches as f64),
            ("write_batch_records_total", io.write_batch_records as f64),
            ("write_batch_bytes_total", io.write_batch_bytes as f64),
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

fn complete_write_error(counters: &Counters, batch: Vec<WriteRequest>, error: anyhow::Error) {
    let failure = if error.downcast_ref::<crate::engine::StorageFull>().is_some() {
        WriteFailure::StorageFull
    } else {
        counters.faulted.store(true, Ordering::Release);
        WriteFailure::Other(format!("{error:#}"))
    };
    counters.error(format!("{error:#}"));
    for request in batch {
        let _ = request.response.send(Err(failure.clone()));
    }
}

struct ReadControl {
    stop: tokio_util::sync::CancellationToken,
    budget: Arc<Semaphore>,
    epoch: Arc<AtomicU64>,
}

async fn read_loop(
    store: Arc<Store>,
    mut rx: mpsc::Receiver<ReadRequest>,
    waiters: Waiters,
    cache: Arc<PayloadCache>,
    counters: Arc<Counters>,
    config: Config,
    control: ReadControl,
) {
    let ReadControl {
        stop,
        budget,
        epoch,
    } = control;
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
                    (request.hash, request.epoch),
                    Err("stored object exceeds configured maximum".into()),
                );
                continue;
            }
            let permit = match budget.clone().try_acquire_many_owned(length.max(1) as u32) {
                Ok(p) => p,
                Err(_) => {
                    complete(
                        &waiters,
                        (request.hash, request.epoch),
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
            let epoch = epoch.clone();
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
                if let Ok(Some(bytes)) = &result
                    && epoch.load(Ordering::Acquire) == request.epoch
                {
                    cache.observe(hash, bytes);
                }
                if let Err(e) = &result {
                    counters.error(e.clone());
                }
                counters.inflight_reads.fetch_sub(1, Ordering::Relaxed);
                complete(&waiters, (hash, request.epoch), result);
            });
        }
    }
    // Every accepted read finishes before dispatcher shutdown releases store handles.
    let _drained = workers
        .acquire_many(config.reads.max_concurrency as u32)
        .await;
}
fn complete(waiters: &Waiters, key: ReadKey, result: SharedResult) {
    if let Some(senders) = waiters.lock().remove(&key) {
        for tx in senders {
            let _ = tx.send(result.clone());
        }
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    #[tokio::test]
    async fn cancelling_prepared_stream_before_admission_cleans_file_and_permits() {
        fn segment_files(root: &std::path::Path) -> std::collections::HashSet<std::path::PathBuf> {
            let mut files = std::collections::HashSet::new();
            let mut pending = vec![root.to_path_buf()];
            while let Some(path) = pending.pop() {
                for entry in std::fs::read_dir(path).unwrap() {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        pending.push(path);
                    } else if path.extension().is_some_and(|ext| ext == "seg") {
                        files.insert(path);
                    }
                }
            }
            files
        }
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        config.writes.writer_lanes = 4;
        let store = Coordinator::open(config.clone()).await.unwrap();
        let old_bytes = Bytes::from_static(b"acknowledged data survives cancelled prepared stream");
        let old_hash = store.put(old_bytes.clone()).await.unwrap().hash;
        tokio::time::timeout(Duration::from_secs(10), async {
            while store.requests.available_permits() != config.resources.max_requests {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let before = segment_files(&root.path().join("segments"));
        let bytes = b"prepared but never admitted for publication";
        let mut upload = store
            .store
            .begin_upload(1024 * 1024, Some(bytes.len() as u64))
            .unwrap();
        upload.write_chunk(bytes).unwrap();
        let slots = Arc::new(Semaphore::new(1));
        let slot = slots.clone().acquire_owned().await.unwrap();
        let (ready, prepared) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        *store.stream_prepare_hook.lock() = Some((ready, wait));
        let finish = {
            let store = store.clone();
            tokio::spawn(async move { store.finish_stream_upload(upload, slot, bytes.len()).await })
        };
        tokio::time::timeout(Duration::from_secs(10), prepared)
            .await
            .unwrap()
            .unwrap();
        let after_prepare = segment_files(&root.path().join("segments"));
        assert_eq!(after_prepare.difference(&before).count(), 1);
        assert_eq!(
            store.requests.available_permits(),
            config.resources.max_requests - 1
        );
        assert_eq!(
            store.stream_finalize_gate.available_permits(),
            config.streaming.max_uploads - 1
        );
        assert_eq!(slots.available_permits(), 0);
        finish.abort();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), finish)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        let _ = release.send(());
        assert_eq!(segment_files(&root.path().join("segments")), before);
        assert_eq!(
            store.requests.available_permits(),
            config.resources.max_requests
        );
        assert_eq!(
            store.stream_finalize_gate.available_permits(),
            config.streaming.max_uploads
        );
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(store.store.get(&old_hash).unwrap().unwrap(), old_bytes);
        assert_eq!(store.store.status().unwrap().objects, 1);
        store.shutdown().await.unwrap();
        drop(store);
        let reopened = Store::open(&config).unwrap();
        assert_eq!(reopened.get(&old_hash).unwrap().unwrap(), old_bytes);
        assert_eq!(reopened.status().unwrap().objects, 1);
    }

    #[tokio::test]
    async fn duplicate_probe_cannot_acknowledge_across_completed_deletion() {
        for lanes in [1, 2, 4, 8] {
            let root = tempfile::tempdir().unwrap();
            let mut config = Config::default();
            config.storage.root = root.path().into();
            config.writes.writer_lanes = lanes;
            let store = Coordinator::open(config).await.unwrap();
            let bytes = Bytes::from_static(b"duplicate probe deletion fence");
            let hash = store.put(bytes.clone()).await.unwrap().hash;
            let (ready, paused) = oneshot::channel();
            let (release, wait) = oneshot::channel();
            *store.put_probe_hook.lock() = Some((ready, wait));
            let put = {
                let store = store.clone();
                tokio::spawn(async move { store.put(bytes).await })
            };
            paused.await.unwrap();
            assert!(store.delete(hash).await.unwrap().deleted);
            assert!(store.store.get(&hash).unwrap().is_none());
            release.send(()).unwrap();
            let result = put.await.unwrap().unwrap();
            assert!(!result.duplicate);
            assert_eq!(
                store.store.get(&hash).unwrap().unwrap(),
                b"duplicate probe deletion fence"
            );
            store.shutdown().await.unwrap();
        }
    }
    #[test]
    fn duplicate_probe_bypasses_queued_put_but_waits_for_delete() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            for lanes in [1, 2, 4, 8] {
                for delete in [false, true] {
                    let root = tempfile::tempdir().unwrap();
                    let mut config = Config::default();
                    config.storage.root = root.path().into();
                    config.writes.writer_lanes = lanes;
                    let store = Coordinator::open(config).await.unwrap();
                    let bytes =
                        Bytes::from_static(b"same-shard mutation guards duplicate fast ack");
                    let hash = store.put(bytes.clone()).await.unwrap().hash;
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while store.writer_state.pending(&hash) != 0 {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    let epoch = store.read_epoch.load(Ordering::Acquire);
                    let (ready, paused) = oneshot::channel();
                    let (release_probe, wait_probe) = oneshot::channel();
                    *store.put_probe_hook.lock() = Some((ready, wait_probe));
                    let put = {
                        let store = store.clone();
                        let bytes = bytes.clone();
                        tokio::spawn(async move { store.put(bytes).await })
                    };
                    tokio::time::timeout(Duration::from_secs(5), paused)
                        .await
                        .unwrap()
                        .unwrap();
                    // Occupy the only blocking worker after the duplicate probe:
                    // the earlier queued mutation cannot publish or advance epoch.
                    let (started, wait_started) = oneshot::channel();
                    let (release_io, wait_io) = std::sync::mpsc::channel();
                    let blocker = tokio::task::spawn_blocking(move || {
                        started.send(()).unwrap();
                        wait_io.recv().unwrap();
                    });
                    wait_started.await.unwrap();
                    let request = store.requests.clone().acquire_owned().await.unwrap();
                    let message = if delete {
                        let (response, _wait) = oneshot::channel();
                        WriteMessage::Delete {
                            hash,
                            response,
                            _request: request,
                            ticket: store.writer_state.delete_ticket(hash, Instant::now()),
                        }
                    } else {
                        let payload = store.store.prepare_put(bytes.clone()).unwrap();
                        let permit = store
                            .write_budget
                            .clone()
                            .acquire_many_owned(payload.len() as u32)
                            .await
                            .unwrap();
                        let (response, _wait) = oneshot::channel();
                        WriteMessage::Put(WriteRequest {
                            payload,
                            response,
                            _permit: permit,
                            _request: request,
                            ticket: store.writer_state.ticket(hash, bytes.len(), Instant::now()),
                        })
                    };
                    {
                        let _admission = store.admission.lock().await;
                        store.write_tx.send(message).await.unwrap();
                    }
                    assert_eq!(store.read_epoch.load(Ordering::Acquire), epoch);
                    release_probe.send(()).unwrap();
                    let result = if delete {
                        tokio::time::timeout(Duration::from_secs(5), async {
                            while store.writer_state.pending(&hash) != 2 {
                                assert!(
                                    !put.is_finished(),
                                    "fast duplicate must not bypass pending deletion"
                                );
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .unwrap();
                        release_io.send(()).unwrap();
                        blocker.await.unwrap();
                        tokio::time::timeout(Duration::from_secs(5), put)
                            .await
                            .unwrap()
                            .unwrap()
                            .unwrap()
                    } else {
                        let result = tokio::time::timeout(Duration::from_millis(500), put)
                            .await
                            .expect("committed duplicate need not wait for a queued PUT")
                            .unwrap()
                            .unwrap();
                        assert_eq!(store.writer_state.pending(&hash), 1);
                        release_io.send(()).unwrap();
                        blocker.await.unwrap();
                        tokio::time::timeout(Duration::from_secs(5), async {
                            while store.writer_state.pending(&hash) != 0 {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .unwrap();
                        result
                    };
                    assert_eq!(result.duplicate, !delete);
                    assert_eq!(store.store.get(&hash).unwrap().unwrap(), bytes);
                    assert_eq!(
                        store.read_epoch.load(Ordering::Acquire),
                        epoch + u64::from(delete)
                    );
                    store.shutdown().await.unwrap();
                }
            }
        });
    }

    #[tokio::test]
    async fn duplicate_probe_rechecks_publication_fault_before_fast_ack() {
        for lanes in [1, 2, 4, 8] {
            let root = tempfile::tempdir().unwrap();
            let mut config = Config::default();
            config.storage.root = root.path().into();
            config.writes.writer_lanes = lanes;
            let store = Coordinator::open(config).await.unwrap();
            let bytes =
                Bytes::from_static(b"published duplicate must not bypass a new publication fault");
            let hash = store.put(bytes.clone()).await.unwrap().hash;
            tokio::time::timeout(Duration::from_secs(5), async {
                while store.writer_state.pending(&hash) != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let epoch = store.read_epoch.load(Ordering::Acquire);
            let (ready, paused) = oneshot::channel();
            let (release, wait) = oneshot::channel();
            *store.put_probe_hook.lock() = Some((ready, wait));
            let put = {
                let store = store.clone();
                tokio::spawn(async move { store.put(bytes).await })
            };
            tokio::time::timeout(Duration::from_secs(5), paused)
                .await
                .unwrap()
                .unwrap();
            let mut cached_status = store.status().await.unwrap();
            assert_eq!(cached_status.health, "running");
            // Simulate failure after probing but before the coordinator has
            // copied the engine's global publication fault into its counters.
            store.store.set_publication_fault_for_test(true);
            assert!(!store.is_ready());
            assert_eq!(store.status().await.unwrap().health, "write-faulted");
            store.refresh_live_status(&mut cached_status);
            assert_eq!(cached_status.health, "write-faulted");
            let metrics = store.metrics();
            assert!(metrics.contains("skerry_ready 0\n"));
            assert!(metrics.contains("skerry_writer_faulted 1\n"));
            assert!(!store.counters.faulted.load(Ordering::Acquire));
            assert_eq!(store.read_epoch.load(Ordering::Acquire), epoch);
            assert_eq!(store.writer_state.pending_deletes(&hash), 0);
            release.send(()).unwrap();
            assert!(put.await.unwrap().is_err());
            assert_eq!(store.counters.writes.load(Ordering::Acquire), 1);
            assert_eq!(store.writer_state.pending(&hash), 0);
            store.store.set_publication_fault_for_test(false);
            store.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn storage_full_fanout_preserves_type_and_writer_remains_healthy() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        let store = Coordinator::open(config).await.unwrap();
        let (tx, rx) = oneshot::channel();
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        let request = WriteRequest {
            payload: store
                .store
                .prepare_put(Bytes::from_static(b"rejected"))
                .unwrap(),
            response: tx,
            _permit: permit,
            _request: Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap(),
            ticket: store.writer_state.ticket(
                *blake3::hash(b"rejected").as_bytes(),
                8,
                Instant::now(),
            ),
        };
        complete_write_error(
            &store.counters,
            vec![request],
            crate::engine::StorageFull.into(),
        );
        let error: anyhow::Error = rx.await.unwrap().err().unwrap().into();
        assert!(error.downcast_ref::<crate::engine::StorageFull>().is_some());
        assert!(store.is_ready());
        store
            .put(Bytes::from_static(b"subsequent write"))
            .await
            .unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn completed_monitors_are_reaped_on_registration() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        let store = Coordinator::open(config).await.unwrap();
        for _ in 0..64 {
            let (done, wait) = tokio::sync::oneshot::channel();
            store.register_monitor(async move {
                let _ = done.send(());
            });
            wait.await.unwrap();
            tokio::task::yield_now().await;
        }
        assert!(store.monitor_handles.lock().len() <= 2);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_joins_monitor_even_if_first_shutdown_is_cancelled() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        let store = Coordinator::open(config).await.unwrap();
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let (started, ready) = tokio::sync::oneshot::channel::<()>();
        store.register_monitor(async move {
            let _ = started.send(());
            let _ = wait.await;
        });
        ready.await.unwrap();
        let first = {
            let store = store.clone();
            tokio::spawn(async move { store.shutdown().await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!first.is_finished());
        first.abort();
        let second = {
            let store = store.clone();
            tokio::spawn(async move { store.shutdown().await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!second.is_finished());
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn stream_finalize_gate_does_not_block_small_writes_and_shutdown_drains() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        let store = Coordinator::open(config).await.unwrap();
        let held = store
            .stream_finalize_gate
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(2),
            store.put(Bytes::from_static(b"small")),
        )
        .await
        .unwrap()
        .unwrap();
        let shutting = {
            let store = store.clone();
            tokio::spawn(async move { store.shutdown().await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!shutting.is_finished());
        drop(held);
        tokio::time::timeout(Duration::from_secs(2), shutting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn real_stream_finalize_and_shutdown_race() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        config.streaming.min_free_mib = 0;
        let store = Coordinator::open(config).await.unwrap();
        let storage = store.storage();
        let mut upload = storage
            .begin_upload(8 * 1024 * 1024, Some(4 * 1024 * 1024))
            .unwrap();
        let bytes = vec![71; 1024 * 1024];
        for _ in 0..4 {
            upload.write_chunk(&bytes).unwrap();
        }
        let slot = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        let writing = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .finish_stream_upload(upload, slot, 4 * 1024 * 1024)
                    .await
            })
        };
        tokio::task::yield_now().await;
        tokio::time::timeout(
            Duration::from_secs(3),
            store.put(Bytes::from_static(b"concurrent small write")),
        )
        .await
        .unwrap()
        .unwrap();
        let finishing = {
            let store = store.clone();
            tokio::spawn(async move { store.shutdown().await })
        };
        writing.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(5), finishing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn scheduled_collection_reclaims_and_shutdown_releases_store() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        config.gc.enabled = true;
        config.gc.interval_seconds = 1;
        config.gc.delete_grace_seconds = 0;
        config.gc.cooldown_seconds = 0;
        config.gc.min_free_mib = 0;
        {
            let store = Store::open(&config).unwrap();
            let results = store
                .put_many(&[Bytes::from(vec![8; 65536]), Bytes::new()])
                .unwrap();
            store.seal().unwrap();
            store.delete(&results[0].hash).unwrap();
        }
        let c = Coordinator::open(config.clone()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if c.gc_status.lock().completed_runs > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(c.gc_status.lock().last_error.is_none());
        assert_eq!(
            c.gc_status.lock().last_result.as_ref().unwrap()["retired_segments"],
            1
        );
        c.shutdown().await.unwrap();
        drop(c);
        let store = Store::open(&config).unwrap();
        assert_eq!(store.status().unwrap().objects, 1);
        assert!(store.exists(blake3::hash(b"").as_bytes()).unwrap());
    }
    #[test]
    fn cancelled_gc_retains_admission_and_shutdown_waits_for_worker() {
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
            let (started_tx, started_rx) = oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.await.unwrap();
            let gc = {
                let c = c.clone();
                tokio::spawn(async move { c.collect_garbage(true).await })
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                while c.gc_gate.available_permits() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            gc.abort();
            let _ = gc.await;
            assert!(c.gc_status.lock().running);
            assert_eq!(
                c.requests.available_permits(),
                c.config.resources.max_requests - 1
            );
            let shutdown = {
                let c = c.clone();
                tokio::spawn(async move { c.shutdown().await })
            };
            tokio::task::yield_now().await;
            assert!(!shutdown.is_finished());
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), shutdown)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(!c.gc_status.lock().running);
            assert_eq!(c.gc_gate.available_permits(), 1);
            assert_eq!(
                c.requests.available_permits(),
                c.config.resources.max_requests
            );
        });
    }
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
        for lanes in [1, 2, 4, 8] {
            let root = tempfile::tempdir().unwrap();
            let mut config = Config::default();
            config.storage.root = root.path().into();
            config.writes.writer_lanes = lanes;
            config.writes.batch_max_delay_ms = 1000;
            config.resources.admission_timeout_ms = 20;
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
            let request_capacity = c.config.resources.max_requests;
            let requests = c
                .requests
                .clone()
                .acquire_many_owned(request_capacity as u32)
                .await
                .unwrap();
            let error = c.put(bytes.clone()).await.err().unwrap();
            assert!(error.downcast_ref::<ResourceExhausted>().is_some());
            drop(requests);
            let byte_capacity = c.config.writes.max_pending_mib * 1024 * 1024;
            let byte_permit = c
                .write_budget
                .clone()
                .acquire_many_owned(byte_capacity as u32)
                .await
                .unwrap();
            let error = c.put(bytes.clone()).await.err().unwrap();
            assert!(error.downcast_ref::<ResourceExhausted>().is_some());
            drop(byte_permit);
            assert_eq!(c.requests.available_permits(), request_capacity);
            c.counters.faulted.store(true, Ordering::Release);
            assert!(c.put(bytes.clone()).await.is_err());
            c.counters.faulted.store(false, Ordering::Release);
            c.shutdown().await.unwrap();
            assert!(c.put(bytes).await.is_err());
        }
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
                .get(&(hash, c.read_epoch.load(Ordering::Acquire)))
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
