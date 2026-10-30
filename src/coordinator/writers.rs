//! Hash-shard FIFO scheduling over a fixed set of append lanes.
use super::*;
use std::collections::BTreeMap;

const SHARDS: usize = 256;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WriterStatus {
    pub configured_lanes: usize,
    pub virtual_shards: usize,
    pub lanes: Vec<WriteLaneStatus>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WriteLaneStatus {
    pub lane: usize,
    pub queued_items: usize,
    pub queued_bytes: usize,
    pub inflight_items: usize,
    pub inflight_bytes: usize,
    pub active_shards: usize,
    pub oldest_wait_seconds: f64,
    pub batches: u64,
    pub completed_items: u64,
    pub queue_wait_seconds: f64,
    /// Full blocking engine service time, including append, sync, and publication.
    pub service_seconds: f64,
    pub append_seconds: f64,
    pub data_sync_seconds: f64,
    pub commit_wait_seconds: f64,
}
#[derive(Default)]
struct LaneState {
    queued_items: usize,
    queued_bytes: usize,
    inflight_items: usize,
    inflight_bytes: usize,
    waiting: BTreeMap<Instant, usize>,
    batches: u64,
    completed_items: u64,
    queue_wait_us: u64,
    service_us: u64,
}
pub(super) struct WriterState {
    lanes: Vec<Mutex<LaneState>>,
    pending: Vec<AtomicUsize>,
    pending_deletes: Vec<AtomicUsize>,
    counters: Arc<Counters>,
}
impl WriterState {
    pub(super) fn new(lanes: usize, counters: Arc<Counters>) -> Self {
        Self {
            lanes: (0..lanes)
                .map(|_| Mutex::new(LaneState::default()))
                .collect(),
            pending: (0..SHARDS).map(|_| AtomicUsize::new(0)).collect(),
            pending_deletes: (0..SHARDS).map(|_| AtomicUsize::new(0)).collect(),
            counters,
        }
    }
    #[cfg(test)]
    pub(super) fn pending(&self, hash: &Hash) -> usize {
        self.pending[usize::from(hash[0])].load(Ordering::Acquire)
    }
    pub(super) fn pending_deletes(&self, hash: &Hash) -> usize {
        self.pending_deletes[usize::from(hash[0])].load(Ordering::Acquire)
    }
    pub(super) fn delete_ticket(self: &Arc<Self>, hash: Hash, submitted: Instant) -> WriteTicket {
        self.make_ticket(hash, 0, submitted, true)
    }
    pub(super) fn ticket(
        self: &Arc<Self>,
        hash: Hash,
        bytes: usize,
        submitted: Instant,
    ) -> WriteTicket {
        self.make_ticket(hash, bytes, submitted, false)
    }
    fn make_ticket(
        self: &Arc<Self>,
        hash: Hash,
        bytes: usize,
        submitted: Instant,
        deletion: bool,
    ) -> WriteTicket {
        let enqueued = Instant::now();
        let shard = usize::from(hash[0]);
        let lane = shard % self.lanes.len();
        if deletion {
            self.pending_deletes[shard].fetch_add(1, Ordering::AcqRel);
        }
        self.pending[shard].fetch_add(1, Ordering::AcqRel);
        self.counters.write_queue.fetch_add(1, Ordering::Relaxed);
        {
            let mut state = self.lanes[lane].lock();
            state.queued_items += 1;
            state.queued_bytes += bytes;
            *state.waiting.entry(enqueued).or_default() += 1;
        }
        WriteTicket {
            state: self.clone(),
            shard,
            bytes,
            submitted,
            enqueued,
            started: false,
            deletion,
        }
    }
    pub(super) fn status(&self) -> WriterStatus {
        let lanes = self
            .lanes
            .iter()
            .enumerate()
            .map(|(lane, state)| {
                let state = state.lock();
                WriteLaneStatus {
                    lane,
                    queued_items: state.queued_items,
                    queued_bytes: state.queued_bytes,
                    inflight_items: state.inflight_items,
                    inflight_bytes: state.inflight_bytes,
                    active_shards: self
                        .pending
                        .iter()
                        .enumerate()
                        .filter(|(shard, count)| {
                            shard % self.lanes.len() == lane && count.load(Ordering::Relaxed) != 0
                        })
                        .count(),
                    oldest_wait_seconds: state
                        .waiting
                        .first_key_value()
                        .map_or(0., |(time, _)| time.elapsed().as_secs_f64()),
                    batches: state.batches,
                    completed_items: state.completed_items,
                    queue_wait_seconds: state.queue_wait_us as f64 / 1_000_000.,
                    service_seconds: state.service_us as f64 / 1_000_000.,
                    append_seconds: 0.,
                    data_sync_seconds: 0.,
                    commit_wait_seconds: 0.,
                }
            })
            .collect();
        WriterStatus {
            configured_lanes: self.lanes.len(),
            virtual_shards: SHARDS,
            lanes,
        }
    }
    fn batch_finished(&self, lane: usize, elapsed: Duration) {
        let mut state = self.lanes[lane].lock();
        state.batches += 1;
        state.service_us = state.service_us.saturating_add(elapsed.as_micros() as u64);
    }
}

/// The queued operation owns its admission accounting even if its caller vanishes.
pub(super) struct WriteTicket {
    state: Arc<WriterState>,
    shard: usize,
    bytes: usize,
    submitted: Instant,
    enqueued: Instant,
    started: bool,
    deletion: bool,
}
impl WriteTicket {
    fn start(&mut self) {
        if self.started {
            return;
        }
        let lane = self.shard % self.state.lanes.len();
        let mut state = self.state.lanes[lane].lock();
        remove_waiter(&mut state.waiting, self.enqueued);
        state.queued_items -= 1;
        state.queued_bytes -= self.bytes;
        state.inflight_items += 1;
        state.inflight_bytes += self.bytes;
        let waited = self.submitted.elapsed().as_micros() as u64;
        state.queue_wait_us = state.queue_wait_us.saturating_add(waited);
        self.state
            .counters
            .write_queue
            .fetch_sub(1, Ordering::Relaxed);
        self.state
            .counters
            .queue_wait_us
            .fetch_add(waited, Ordering::Relaxed);
        self.started = true;
    }
}
impl Drop for WriteTicket {
    fn drop(&mut self) {
        let lane = self.shard % self.state.lanes.len();
        let mut state = self.state.lanes[lane].lock();
        if self.started {
            state.inflight_items -= 1;
            state.inflight_bytes -= self.bytes;
        } else {
            remove_waiter(&mut state.waiting, self.enqueued);
            state.queued_items -= 1;
            state.queued_bytes -= self.bytes;
            self.state
                .counters
                .write_queue
                .fetch_sub(1, Ordering::Relaxed);
        }
        state.completed_items += 1;
        self.state.pending[self.shard].fetch_sub(1, Ordering::AcqRel);
        if self.deletion {
            self.state.pending_deletes[self.shard].fetch_sub(1, Ordering::AcqRel);
        }
    }
}
fn remove_waiter(waiting: &mut BTreeMap<Instant, usize>, time: Instant) {
    if let Some(count) = waiting.get_mut(&time) {
        *count -= 1;
        if *count == 0 {
            waiting.remove(&time);
        }
    }
}
fn ticket(message: &WriteMessage) -> &WriteTicket {
    match message {
        WriteMessage::Put(request) => &request.ticket,
        WriteMessage::Delete { ticket, .. } | WriteMessage::Stream { ticket, .. } => ticket,
        WriteMessage::Shutdown(_) => unreachable!("shutdown is not a shard operation"),
    }
}

struct ShardQueues {
    shards: Vec<VecDeque<WriteMessage>>,
    ready: Vec<VecDeque<usize>>,
}
enum LaneBatch {
    Puts(Vec<WriteRequest>),
    Control(Box<WriteMessage>),
}
impl ShardQueues {
    fn new(lanes: usize) -> Self {
        Self {
            shards: (0..SHARDS).map(|_| VecDeque::new()).collect(),
            ready: (0..lanes).map(|_| VecDeque::new()).collect(),
        }
    }
    fn push(&mut self, message: WriteMessage) {
        let shard = ticket(&message).shard;
        if self.shards[shard].is_empty() {
            let lane = shard % self.ready.len();
            self.ready[lane].push_back(shard);
        }
        self.shards[shard].push_back(message);
    }
    fn immediate(&self, lane: usize, config: &Config) -> bool {
        let Some(shard) = self.ready[lane].front().copied() else {
            return false;
        };
        let first = self.shards[shard].front().expect("ready shard has work");
        if matches!(
            first,
            WriteMessage::Delete { .. } | WriteMessage::Stream { .. }
        ) {
            return true;
        }
        let state = ticket(first).state.lanes[lane].lock();
        state.queued_items >= config.writes.batch_max_records
            || state.queued_bytes >= config.writes.batch_max_bytes_mib * 1024 * 1024
    }
    fn take(&mut self, lane: usize, config: &Config) -> Option<LaneBatch> {
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        while let Some(shard) = self.ready[lane].front().copied() {
            let message = self.shards[shard].front().expect("ready shard has work");
            if matches!(
                message,
                WriteMessage::Delete { .. } | WriteMessage::Stream { .. }
            ) {
                if !batch.is_empty() {
                    break;
                }
                self.ready[lane].pop_front();
                let message = self.shards[shard].pop_front().unwrap();
                if !self.shards[shard].is_empty() {
                    self.ready[lane].push_back(shard);
                }
                return Some(LaneBatch::Control(Box::new(message)));
            }
            let length = ticket(message).bytes;
            if !batch.is_empty()
                && (batch.len() >= config.writes.batch_max_records
                    || bytes.saturating_add(length)
                        > config.writes.batch_max_bytes_mib * 1024 * 1024)
            {
                break;
            }
            self.ready[lane].pop_front();
            let WriteMessage::Put(request) = self.shards[shard].pop_front().unwrap() else {
                unreachable!()
            };
            bytes += length;
            batch.push(request);
            if !self.shards[shard].is_empty() {
                self.ready[lane].push_back(shard);
            }
            if batch.len() >= config.writes.batch_max_records
                || bytes >= config.writes.batch_max_bytes_mib * 1024 * 1024
            {
                break;
            }
        }
        if batch.is_empty() {
            None
        } else {
            Some(LaneBatch::Puts(batch))
        }
    }
    fn is_empty(&self) -> bool {
        self.ready.iter().all(VecDeque::is_empty)
    }
}

/// Each idle lane gets a fresh bounded refill window. Queue age measures
/// waiting, but must not eliminate coalescing after the preceding I/O completes.
#[derive(Clone, Copy, Default)]
struct GatherWindow {
    deadline: Option<tokio::time::Instant>,
}
impl GatherWindow {
    fn ready(
        &mut self,
        has_work: bool,
        immediate: bool,
        stopping: bool,
        now: tokio::time::Instant,
        delay: Duration,
    ) -> bool {
        if !has_work {
            self.deadline = None;
            return false;
        }
        let deadline = *self.deadline.get_or_insert(now + delay);
        immediate || stopping || deadline <= now
    }
    fn reset(&mut self) {
        self.deadline = None;
    }
}

pub(super) async fn write_loop(
    store: Arc<Store>,
    mut rx: mpsc::Receiver<WriteMessage>,
    counters: Arc<Counters>,
    config: Config,
    cache: Arc<PayloadCache>,
    epoch: Arc<AtomicU64>,
    state: Arc<WriterState>,
) {
    let mut queues = ShardQueues::new(config.writes.writer_lanes);
    let mut busy = vec![false; config.writes.writer_lanes];
    let mut gather = vec![GatherWindow::default(); config.writes.writer_lanes];
    let mut jobs = tokio::task::JoinSet::new();
    let mut stopping = false;
    let mut shutdown = None;
    loop {
        // Drain only a bounded number at a time so completion and status keep progressing.
        for _ in 0..SHARDS {
            if stopping {
                break;
            }
            match rx.try_recv() {
                Ok(WriteMessage::Shutdown(done)) => {
                    stopping = true;
                    shutdown = Some(done);
                }
                Ok(message) => queues.push(message),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    stopping = true;
                    break;
                }
            }
        }
        let now = tokio::time::Instant::now();
        for (lane, active) in busy.iter_mut().enumerate() {
            if *active {
                continue;
            }
            if gather[lane].ready(
                !queues.ready[lane].is_empty(),
                queues.immediate(lane, &config),
                stopping,
                now,
                Duration::from_millis(config.writes.batch_max_delay_ms),
            ) && let Some(batch) = queues.take(lane, &config)
            {
                gather[lane].reset();
                *active = true;
                jobs.spawn(run_batch(
                    lane,
                    batch,
                    store.clone(),
                    counters.clone(),
                    cache.clone(),
                    epoch.clone(),
                    state.clone(),
                ));
            }
        }
        if stopping && queues.is_empty() && jobs.is_empty() {
            break;
        }
        let next = gather
            .iter()
            .filter_map(|window| window.deadline)
            .min()
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(3600));
        tokio::select! {
            biased;
            result = jobs.join_next(), if !jobs.is_empty() => {
                match result {
                    Some(Ok(lane)) => busy[lane] = false,
                    Some(Err(error)) => {
                        counters.faulted.store(true, Ordering::Release);
                        counters.error(format!("write lane task failed: {error}"));
                        // A panic is not expected. Drain remaining jobs before final sync;
                        // stop accepting channel work and fail queued operations on drop.
                        stopping = true;
                        queues = ShardQueues::new(config.writes.writer_lanes);
                        rx.close();
                    }
                    None => {}
                }
            },
            message = rx.recv(), if !stopping => match message {
                Some(WriteMessage::Shutdown(done)) => { stopping = true; shutdown = Some(done); },
                Some(message) => queues.push(message),
                None => stopping = true,
            },
            _ = tokio::time::sleep_until(next) => {}
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
        .and_then(|result| result)
    };
    if let Some(done) = shutdown {
        let _ = done.send(result.map_err(|error| format!("{error:#}")));
    } else if let Err(error) = result {
        counters.error(format!("{error:#}"));
    }
}

async fn run_batch(
    lane: usize,
    batch: LaneBatch,
    store: Arc<Store>,
    counters: Arc<Counters>,
    cache: Arc<PayloadCache>,
    epoch: Arc<AtomicU64>,
    state: Arc<WriterState>,
) -> usize {
    match batch {
        LaneBatch::Puts(mut batch) => {
            for request in &mut batch {
                request.ticket.start();
            }
            counters.write_batches.fetch_add(1, Ordering::Relaxed);
            counters
                .write_batch_records
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            counters.write_batch_bytes.fetch_add(
                batch
                    .iter()
                    .map(|request| request.payload.len() as u64)
                    .sum::<u64>(),
                Ordering::Relaxed,
            );
            let db = store.clone();
            let io_counters = counters.clone();
            let start = Instant::now();
            // The blocking task owns permits and byte buffers through its last I/O.
            let completed = tokio::task::spawn_blocking(move || {
                let result = if io_counters.faulted.load(Ordering::Acquire) {
                    Err(anyhow!("writer faulted"))
                } else {
                    let payloads: Vec<_> = batch
                        .iter()
                        .map(|request| request.payload.clone())
                        .collect();
                    db.put_prepared_many(lane, &payloads)
                };
                (batch, result)
            })
            .await;
            counters
                .storage_write_us
                .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
            state.batch_finished(lane, start.elapsed());
            match completed {
                Ok((batch, Ok(results))) if batch.len() == results.len() => {
                    counters.event(format!(
                        "lane {lane} durably completed {} write requests",
                        results.len()
                    ));
                    for (request, result) in batch.into_iter().zip(results) {
                        counters.writes.fetch_add(1, Ordering::Relaxed);
                        counters
                            .write_bytes
                            .fetch_add(request.payload.len() as u64, Ordering::Relaxed);
                        if result.duplicate {
                            counters.deduplicated.fetch_add(1, Ordering::Relaxed);
                            counters
                                .deduplicated_bytes
                                .fetch_add(request.payload.len() as u64, Ordering::Relaxed);
                        }
                        let _ = request.response.send(Ok(result));
                    }
                }
                Ok((batch, Ok(_))) => complete_write_error(
                    &counters,
                    batch,
                    anyhow!("engine write result count mismatch"),
                ),
                Ok((batch, Err(error))) => complete_write_error(&counters, batch, error),
                Err(error) => {
                    counters.faulted.store(true, Ordering::Release);
                    counters.error(format!("blocking write failed: {error}"));
                }
            }
        }
        LaneBatch::Control(message) => {
            run_control(lane, *message, store, counters, cache, epoch, state).await;
        }
    }
    lane
}

async fn run_control(
    lane: usize,
    message: WriteMessage,
    store: Arc<Store>,
    counters: Arc<Counters>,
    cache: Arc<PayloadCache>,
    epoch: Arc<AtomicU64>,
    state: Arc<WriterState>,
) {
    match message {
        WriteMessage::Delete {
            hash,
            response,
            _request,
            mut ticket,
        } => {
            ticket.start();
            let io_counters = counters.clone();
            let start = Instant::now();
            let completed = tokio::task::spawn_blocking(move || {
                let result = if io_counters.faulted.load(Ordering::Acquire) {
                    Err(anyhow!("writer faulted"))
                } else {
                    store.delete(&hash)
                };
                let faulted = store.write_faulted();
                (result, faulted, response, _request, ticket)
            })
            .await;
            state.batch_finished(lane, start.elapsed());
            match completed {
                Ok((result, faulted, response, _request, _ticket)) => {
                    if faulted {
                        counters.faulted.store(true, Ordering::Release);
                    }
                    match result {
                        Ok(result) => {
                            epoch.fetch_add(1, Ordering::AcqRel);
                            cache.invalidate(&hash);
                            counters.event(format!(
                                "durable delete {} (existed={})",
                                hex::encode(hash),
                                result.deleted
                            ));
                            let _ = response.send(Ok(result));
                        }
                        Err(error) => {
                            counters.error(format!("{error:#}"));
                            let _ = response.send(Err(format!("{error:#}")));
                        }
                    }
                }
                Err(error) => {
                    counters.faulted.store(true, Ordering::Release);
                    counters.error(format!("blocking delete failed: {error}"));
                }
            }
        }
        WriteMessage::Stream {
            upload,
            length,
            response,
            _slot,
            _gate,
            _request,
            mut ticket,
        } => {
            ticket.start();
            let db = store.clone();
            let io_counters = counters.clone();
            let start = Instant::now();
            let completed = tokio::task::spawn_blocking(move || {
                let result = if io_counters.faulted.load(Ordering::Acquire) {
                    Err(anyhow!("writer faulted"))
                } else {
                    upload.finish()
                };
                let faulted = db.write_faulted();
                (result, faulted, response, _slot, _gate, _request, ticket)
            })
            .await;
            state.batch_finished(lane, start.elapsed());
            match completed {
                Ok((result, faulted, response, _slot, _gate, _request, _ticket)) => {
                    if faulted {
                        counters.faulted.store(true, Ordering::Release);
                    }
                    match result {
                        Ok(result) => {
                            counters.writes.fetch_add(1, Ordering::Relaxed);
                            counters
                                .write_bytes
                                .fetch_add(length as u64, Ordering::Relaxed);
                            if result.duplicate {
                                counters.deduplicated.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .deduplicated_bytes
                                    .fetch_add(length as u64, Ordering::Relaxed);
                            }
                            let _ = response.send(Ok(result));
                        }
                        Err(error) => {
                            let failure =
                                if error.downcast_ref::<crate::engine::StorageFull>().is_some() {
                                    WriteFailure::StorageFull
                                } else {
                                    WriteFailure::Other(format!("{error:#}"))
                                };
                            counters.error(format!("{error:#}"));
                            let _ = response.send(Err(failure));
                        }
                    }
                }
                Err(error) => {
                    counters.faulted.store(true, Ordering::Release);
                    counters.error(format!("blocking stream publication failed: {error}"));
                }
            }
        }
        _ => unreachable!("control batch contains only stream publication or deletion"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes_in_shard(shard: u8, salt: u32) -> Bytes {
        for index in 0..100_000u32 {
            let bytes = Bytes::from(format!("shard-{shard}-salt-{salt}-item-{index}"));
            if blake3::hash(&bytes).as_bytes()[0] == shard {
                return bytes;
            }
        }
        panic!("failed to find test payload shard");
    }
    async fn put_request(store: &Arc<Coordinator>, bytes: Bytes) -> WriteMessage {
        let payload = store.store.prepare_put(bytes).unwrap();
        let _permit = store
            .write_budget
            .clone()
            .acquire_many_owned(payload.len().max(1) as u32)
            .await
            .unwrap();
        let _request = store.requests.clone().acquire_owned().await.unwrap();
        let ticket = store
            .writer_state
            .ticket(payload.hash(), payload.len(), Instant::now());
        let (response, _receiver) = oneshot::channel();
        WriteMessage::Put(WriteRequest {
            payload,
            response,
            _permit,
            _request,
            ticket,
        })
    }
    async fn delete_request(store: &Arc<Coordinator>, hash: Hash) -> WriteMessage {
        let _request = store.requests.clone().acquire_owned().await.unwrap();
        let ticket = store.writer_state.delete_ticket(hash, Instant::now());
        let (response, _receiver) = oneshot::channel();
        WriteMessage::Delete {
            hash,
            response,
            _request,
            ticket,
        }
    }
    fn hashes(batch: LaneBatch) -> Vec<Hash> {
        let LaneBatch::Puts(requests) = batch else {
            panic!("expected PUT batch")
        };
        requests
            .iter()
            .map(|request| request.payload.hash())
            .collect()
    }
    async fn fixture(lanes: usize, records: usize) -> (tempfile::TempDir, Arc<Coordinator>) {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        config.writes.writer_lanes = lanes;
        config.writes.batch_max_records = records;
        let store = Coordinator::open(config).await.unwrap();
        (directory, store)
    }

    #[test]
    fn each_idle_batch_gets_one_fresh_bounded_gather_window() {
        let start = tokio::time::Instant::now();
        let delay = Duration::from_millis(1);
        let mut window = GatherWindow::default();
        assert!(!window.ready(true, false, false, start, delay));
        assert!(!window.ready(true, false, false, start + delay / 2, delay));
        // Arrivals during the window must not extend its deadline.
        assert!(window.ready(true, false, false, start + delay, delay));
        window.reset();
        // Work retained during an earlier I/O receives a fresh refill window.
        let next = start + Duration::from_secs(1);
        assert!(!window.ready(true, false, false, next, delay));
        assert!(window.ready(true, false, false, next + delay, delay));
        window.reset();
        assert!(window.ready(true, true, false, next, delay));
        window.reset();
        assert!(window.ready(true, false, true, next, delay));
        assert!(!window.ready(false, false, false, next, delay));
        assert!(window.deadline.is_none());
    }

    #[tokio::test]
    async fn round_robin_serves_other_shards_before_hot_shard_second_item() {
        let (_directory, store) = fixture(2, 3).await;
        let a = bytes_in_shard(0, 1);
        let b = bytes_in_shard(0, 2);
        let c = bytes_in_shard(0, 3);
        let d = bytes_in_shard(2, 4);
        let e = bytes_in_shard(4, 5);
        let mut queues = ShardQueues::new(2);
        for bytes in [&a, &b, &c, &d, &e] {
            queues.push(put_request(&store, bytes.clone()).await);
        }
        let first = hashes(queues.take(0, &store.config).unwrap());
        assert_eq!(
            first,
            [&a, &d, &e].map(|bytes| *blake3::hash(bytes).as_bytes())
        );
        let second = hashes(queues.take(0, &store.config).unwrap());
        assert_eq!(
            second,
            [&b, &c].map(|bytes| *blake3::hash(bytes).as_bytes())
        );
        assert!(queues.is_empty());
        assert_eq!(store.writer_status().lanes[0].queued_items, 0);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn delete_is_a_fifo_fence_between_same_shard_put_batches() {
        let (_directory, store) = fixture(2, 8).await;
        let bytes = bytes_in_shard(0, 9);
        let hash = *blake3::hash(&bytes).as_bytes();
        let mut queues = ShardQueues::new(2);
        queues.push(put_request(&store, bytes.clone()).await);
        queues.push(delete_request(&store, hash).await);
        queues.push(put_request(&store, bytes).await);
        assert_eq!(hashes(queues.take(0, &store.config).unwrap()), vec![hash]);
        let LaneBatch::Control(control) = queues.take(0, &store.config).unwrap() else {
            panic!("expected delete fence")
        };
        assert!(matches!(*control, WriteMessage::Delete { hash: actual, .. } if actual == hash));
        drop(control);
        assert_eq!(hashes(queues.take(0, &store.config).unwrap()), vec![hash]);
        assert!(queues.is_empty());
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn streamed_publication_recreates_after_queued_put_and_delete() {
        let (_directory, store) = fixture(2, 8).await;
        let bytes = bytes_in_shard(3, 42);
        let hash = *blake3::hash(&bytes).as_bytes();
        let mut upload = store
            .store
            .begin_upload(1024 * 1024, Some(bytes.len() as u64))
            .unwrap();
        upload.write_chunk(&bytes).unwrap();
        let upload = upload.prepare_finish().unwrap();
        let _gate = store
            .stream_finalize_gate
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let _slot = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        let _request = store.requests.clone().acquire_owned().await.unwrap();
        let ticket = store.writer_state.ticket(hash, 0, Instant::now());
        let (response, completed) = oneshot::channel();
        let stream = WriteMessage::Stream {
            upload,
            length: bytes.len(),
            response,
            _slot,
            _gate,
            _request,
            ticket,
        };
        let put = put_request(&store, bytes.clone()).await;
        let delete = delete_request(&store, hash).await;
        {
            let _admission = store.admission.lock().await;
            store.write_tx.send(put).await.unwrap();
            store.write_tx.send(delete).await.unwrap();
            store.write_tx.send(stream).await.unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(5), completed)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!result.duplicate);
        assert_eq!(store.store.get(&hash).unwrap().unwrap(), bytes);
        assert_eq!(store.read_epoch.load(Ordering::Acquire), 1);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn discarded_caller_does_not_release_queued_admission() {
        let (_directory, store) = fixture(4, 8).await;
        let capacity = store.requests.available_permits();
        let bytes_capacity = store.write_budget.available_permits();
        let bytes = bytes_in_shard(7, 1);
        let length = bytes.len();
        let mut queues = ShardQueues::new(4);
        // The helper drops its response receiver, as a disconnected caller does.
        queues.push(put_request(&store, bytes).await);
        assert_eq!(store.requests.available_permits(), capacity - 1);
        assert_eq!(
            store.write_budget.available_permits(),
            bytes_capacity - length
        );
        assert_eq!(store.writer_status().lanes[3].queued_items, 1);
        drop(queues);
        assert_eq!(store.requests.available_permits(), capacity);
        assert_eq!(store.write_budget.available_permits(), bytes_capacity);
        assert_eq!(store.writer_status().lanes[3].queued_items, 0);
        store.shutdown().await.unwrap();
    }

    #[test]
    fn cancelled_accepted_put_retains_request_through_blocking_io() {
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
            config.resources.max_requests = 1;
            config.writes.batch_max_delay_ms = 1000;
            let store = Coordinator::open(config).await.unwrap();
            let bytes = Bytes::from_static(b"cancelled accepted request still commits");
            let hash = *blake3::hash(&bytes).as_bytes();
            let put = {
                let store = store.clone();
                tokio::spawn(async move { store.put(bytes).await })
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                while store.counters.write_queue.load(Ordering::Acquire) != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let (started, wait_started) = oneshot::channel();
            let (release, wait_release) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = started.send(());
                wait_release.recv().unwrap();
            });
            wait_started.await.unwrap();
            put.abort();
            let _ = put.await;
            tokio::time::timeout(Duration::from_secs(5), async {
                while store.writer_status().lanes[0].inflight_items != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(store.requests.available_permits(), 0);
            release.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while store.requests.available_permits() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                store.store.get(&hash).unwrap().unwrap(),
                b"cancelled accepted request still commits"
            );
            store.shutdown().await.unwrap();
        });
    }

    #[tokio::test]
    async fn full_record_and_byte_batches_dispatch_and_delete_completes() {
        for byte_limit in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut config = Config::default();
            config.storage.root = root.path().into();
            config.writes.batch_max_delay_ms = 1000;
            let bytes = if byte_limit {
                config.writes.batch_max_bytes_mib = 1;
                Bytes::from(vec![19; 1024 * 1024])
            } else {
                config.writes.batch_max_records = 1;
                Bytes::from_static(b"full one-record batch")
            };
            let store = Coordinator::open(config).await.unwrap();
            let result = tokio::time::timeout(Duration::from_secs(2), store.put(bytes))
                .await
                .expect("a full batch must dispatch without waiting on a moving deadline")
                .unwrap();
            let deleted = tokio::time::timeout(Duration::from_secs(2), store.delete(result.hash))
                .await
                .expect("delete must dispatch with a stable immediate deadline")
                .unwrap();
            assert!(deleted.deleted);
            store.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn lane_metrics_have_one_type_declaration_and_global_budget() {
        let (_directory, store) = fixture(4, 8).await;
        assert_eq!(
            store.requests.available_permits(),
            store.config.resources.max_requests
        );
        assert_eq!(
            store.write_budget.available_permits(),
            store.config.writes.max_pending_mib * 1024 * 1024
        );
        let metrics = store.metrics();
        assert_eq!(
            metrics
                .matches("# TYPE skerry_writer_lane_queued_items gauge")
                .count(),
            1
        );
        for lane in 0..4 {
            assert!(metrics.contains(&format!(
                "skerry_writer_lane_queued_items{{lane=\"{lane}\"}} 0"
            )));
        }
        store.shutdown().await.unwrap();
    }
}
