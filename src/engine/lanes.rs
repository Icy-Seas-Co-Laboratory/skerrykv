//! Parallel append lanes with one durable publication sequencer.
use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

const LANE_SLOTS: usize = 8;
#[cfg(test)]
pub(super) struct LanePause {
    lane: usize,
    arrived: std::sync::mpsc::Sender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
#[cfg(test)]
struct PublicationPause {
    arrived: std::sync::mpsc::Sender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    fail: bool,
}

#[derive(Clone)]
pub(crate) struct PreparedPut {
    bytes: Bytes,
    hash: [u8; 32],
}
impl PreparedPut {
    pub(crate) fn hash(&self) -> [u8; 32] {
        self.hash
    }
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }
}

pub(super) fn new_lanes() -> Vec<Mutex<Writer>> {
    (0..LANE_SLOTS)
        .map(|_| Mutex::new(Writer::default()))
        .collect()
}
pub(super) fn segment_lane(id: Uuid) -> usize {
    usize::from(id.as_bytes()[0]) % LANE_SLOTS
}

#[derive(Default)]
pub(super) struct LaneTimings {
    pub(super) append_us: AtomicU64,
    pub(super) data_sync_us: AtomicU64,
    pub(super) commit_wait_us: AtomicU64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WriteLaneTimingSnapshot {
    pub lane: usize,
    pub append_us: u64,
    pub data_sync_us: u64,
    pub commit_wait_us: u64,
}
pub(super) fn new_timings() -> Vec<LaneTimings> {
    (0..LANE_SLOTS).map(|_| LaneTimings::default()).collect()
}

struct Pending {
    index: usize,
    hash: [u8; 32],
    loc: Location,
}
struct Batch {
    meta: Meta,
    pending: Vec<Pending>,
    results: Vec<PutResult>,
    bytes: usize,
}
struct WorkState {
    lead: bool,
    result: Option<std::result::Result<Vec<PutResult>, String>>,
}
struct Work {
    batch: Batch,
    state: Mutex<WorkState>,
    ready: Condvar,
}
#[derive(Default)]
pub(super) struct CommitQueue {
    active: bool,
    pending: VecDeque<Arc<Work>>,
    #[cfg(test)]
    panic_before_publish: bool,
    #[cfg(test)]
    publication_pause: Option<Arc<PublicationPause>>,
    #[cfg(test)]
    gather_started: Option<std::sync::mpsc::Sender<()>>,
}

impl Store {
    pub(crate) fn prepare_put(&self, bytes: Bytes) -> Result<PreparedPut> {
        ensure!(
            bytes.len() <= self.config.max_object_bytes(),
            "object exceeds configured maximum"
        );
        ensure!(
            (bytes.len() as u64)
                .checked_add(RECORD_HEADER + SEGMENT_HEADER + SEGMENT_FOOTER)
                .is_some_and(|n| n <= self.config.segments.max_size_bytes),
            "object does not fit segment"
        );
        let hash = *blake3::hash(&bytes).as_bytes();
        Ok(PreparedPut { bytes, hash })
    }
    pub fn write_lane_timings(&self) -> Vec<WriteLaneTimingSnapshot> {
        self.lane_timings
            .iter()
            .enumerate()
            .take(self.config.writes.writer_lanes)
            .map(|(lane, s)| WriteLaneTimingSnapshot {
                lane,
                append_us: s.append_us.load(Ordering::Relaxed),
                data_sync_us: s.data_sync_us.load(Ordering::Relaxed),
                commit_wait_us: s.commit_wait_us.load(Ordering::Relaxed),
            })
            .collect()
    }
    pub(super) fn put_partitioned(&self, payloads: &[Bytes]) -> Result<Vec<PutResult>> {
        ensure!(
            payloads.len() <= self.config.writes.batch_max_records,
            "write batch exceeds configured record limit"
        );
        let prepared: Vec<_> = payloads
            .iter()
            .cloned()
            .map(|b| self.prepare_put(b))
            .collect::<Result<_>>()?;
        let mut unique = HashSet::new();
        let mut known = HashSet::new();
        let mut required = 0u64;
        {
            let global = self.writer.lock();
            ensure!(
                !global.faulted && !self.publication_fault.load(Ordering::Acquire),
                "write coordinator faulted"
            );
            for p in &prepared {
                if unique.insert(p.hash) {
                    if self.objects.contains_key(p.hash)? {
                        known.insert(p.hash);
                    } else {
                        required = required
                            .checked_add(p.bytes.len() as u64 + RECORD_HEADER + 1024)
                            .context("write batch space estimate overflow")?;
                    }
                }
            }
        }
        if required > 0 {
            self.ensure_write_space(required)?;
        }
        let mut output = vec![None; prepared.len()];
        for lane in 0..self.config.writes.writer_lanes {
            let positions: Vec<_> = prepared
                .iter()
                .enumerate()
                .filter(|(_, p)| usize::from(p.hash[0]) % self.config.writes.writer_lanes == lane)
                .map(|(i, _)| i)
                .collect();
            if positions.is_empty() {
                continue;
            }
            let entries: Vec<_> = positions.iter().map(|&i| prepared[i].clone()).collect();
            for (i, result) in
                positions
                    .into_iter()
                    .zip(self.put_prepared_inner(lane, &entries, Some(&known))?)
            {
                output[i] = Some(result);
            }
        }
        output
            .into_iter()
            .map(|p| p.context("missing lane result"))
            .collect()
    }
    pub(crate) fn put_prepared_many(
        &self,
        lane: usize,
        payloads: &[PreparedPut],
    ) -> Result<Vec<PutResult>> {
        self.put_prepared_inner(lane, payloads, None)
    }
    fn put_prepared_inner(
        &self,
        lane: usize,
        payloads: &[PreparedPut],
        prechecked: Option<&HashSet<[u8; 32]>>,
    ) -> Result<Vec<PutResult>> {
        ensure!(
            lane < self.config.writes.writer_lanes,
            "invalid append lane"
        );
        ensure!(
            payloads.len() <= self.config.writes.batch_max_records,
            "write batch exceeds configured record limit"
        );
        if payloads.is_empty() {
            return Ok(vec![]);
        }
        ensure!(
            payloads
                .iter()
                .all(|p| usize::from(p.hash[0]) % self.config.writes.writer_lanes == lane),
            "write routed to incorrect hash lane"
        );
        if !self.multi_writer {
            let bytes: Vec<_> = payloads.iter().map(|p| p.bytes.clone()).collect();
            let hashes: Vec<_> = payloads.iter().map(|p| p.hash).collect();
            return self.put_legacy_hashed(&bytes, &hashes);
        }
        let _barrier = self.lane_barrier.read();
        let mut writer = self.append_lanes[lane].lock();
        ensure!(
            !writer.faulted && !self.publication_fault.load(Ordering::Acquire),
            "write coordinator faulted"
        );
        // Detect all clean capacity errors before the first append. Rejection
        // never poisons the lane or the shared publication path.
        let known = if let Some(known) = prechecked {
            known.clone()
        } else {
            // This is a tentative classification, not a duplicate acknowledgment.
            // Index entries can be visible while a publication is persisting.
            // A later successful shared commit, or the all-duplicate fence below,
            // must certify the observation before returning success.
            let mut known = HashSet::new();
            for p in payloads {
                if self.objects.contains_key(p.hash)? {
                    known.insert(p.hash);
                }
            }
            known
        };
        let mut unique = HashSet::new();
        let mut required = 0u64;
        for p in payloads {
            if !known.contains(&p.hash) && unique.insert(p.hash) {
                required = required
                    .checked_add(p.bytes.len() as u64 + RECORD_HEADER + 1024)
                    .context("write batch space estimate overflow")?;
            }
        }
        if prechecked.is_none() && required > 0 {
            self.ensure_write_space(required)?;
        }
        let run = catch_unwind(AssertUnwindSafe(|| {
            self.append_prepared(lane, &mut writer, payloads, &known)
        }));
        match run {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => {
                writer.faulted = true;
                self.publication_fault.store(true, Ordering::Release);
                self.commit_changed.notify_all();
                self.writer.lock().faulted = true;
                Err(error)
            }
            Err(_) => {
                writer.faulted = true;
                self.publication_fault.store(true, Ordering::Release);
                self.commit_changed.notify_all();
                self.writer.lock().faulted = true;
                bail!("append lane panicked; restart and recover before writing")
            }
        }
    }
    fn append_prepared(
        &self,
        lane: usize,
        writer: &mut Writer,
        payloads: &[PreparedPut],
        known: &HashSet<[u8; 32]>,
    ) -> Result<Vec<PutResult>> {
        let mut results = Vec::with_capacity(payloads.len());
        let mut pending = Vec::new();
        let mut seen = HashSet::new();
        let mut batch_bytes = 0usize;
        let mut uncertified_duplicates = false;
        for p in payloads {
            if known.contains(&p.hash) || !seen.insert(p.hash) {
                uncertified_duplicates = true;
                results.push(PutResult {
                    hash: p.hash,
                    duplicate: true,
                });
                continue;
            }
            if writer.active.as_ref().is_some_and(|a| {
                a.meta.end + RECORD_HEADER + p.bytes.len() as u64 + SEGMENT_FOOTER
                    > self.config.segments.max_size_bytes
            }) {
                let published = !pending.is_empty();
                self.flush_lane(lane, writer, &mut pending, &mut results, batch_bytes)?;
                if published {
                    uncertified_duplicates = false;
                }
                batch_bytes = 0;
                self.seal_lane(writer)?;
            }
            if writer.active.is_none() {
                self.create_lane(lane, writer)?;
            }
            let a = writer.active.as_mut().unwrap();
            let started = Instant::now();
            let needed = a.meta.end + RECORD_HEADER + p.bytes.len() as u64;
            if self.config.segments.growth_factor == 2 && needed > a.meta.physical {
                let mut capacity = a.meta.physical.max(SEGMENT_HEADER);
                while capacity < needed {
                    capacity = capacity
                        .saturating_mul(2)
                        .min(self.config.segments.max_size_bytes - SEGMENT_FOOTER);
                }
                a.file.set_len(capacity)?;
                a.meta.physical = capacity;
            }
            a.file.seek(SeekFrom::Start(a.meta.end))?;
            a.file.write_all(&record_header(&p.bytes, &p.hash))?;
            failpoint("lane_after_record_header")?;
            let offset = a.meta.end + RECORD_HEADER;
            a.file.write_all(&p.bytes)?;
            self.lane_timings[lane]
                .append_us
                .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
            failpoint("lane_after_payload")?;
            a.meta.end = offset + p.bytes.len() as u64;
            a.meta.physical = a.meta.physical.max(a.meta.end);
            a.meta.count += 1;
            pending.push(Pending {
                index: results.len(),
                hash: p.hash,
                loc: Location {
                    segment: *a.meta.id.as_bytes(),
                    offset,
                    length: p.bytes.len() as u64,
                },
            });
            results.push(PutResult {
                hash: p.hash,
                duplicate: false,
            });
            batch_bytes += p.bytes.len();
        }
        let published = !pending.is_empty();
        self.flush_lane(lane, writer, &mut pending, &mut results, batch_bytes)?;
        if published {
            uncertified_duplicates = false;
        }
        if uncertified_duplicates {
            #[cfg(test)]
            self.pause_lane(lane);
            // A successful publication serialized after the tentative lookup
            // certifies every earlier visible mapping. If this batch appended
            // nothing (or ended in duplicates after a rollover publication),
            // take that same healthy fence explicitly. A failed persistence
            // sets the atomic fault guard before releasing this mutex.
            let global = self.writer.lock();
            ensure!(
                !global.faulted && !self.publication_fault.load(Ordering::Acquire),
                "write coordinator faulted"
            );
        }
        Ok(results)
    }
    #[cfg(test)]
    fn pause_lane(&self, lane: usize) {
        let pause = {
            let mut hook = self.lane_pause.lock();
            if hook.as_ref().is_some_and(|p| p.lane == lane) {
                hook.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            let _ = pause.arrived.send(());
            let _ = pause.release.lock().recv();
        }
    }
    #[cfg(test)]
    pub(super) fn pause_publication(&self) -> Result<()> {
        let pause = self.commit_queue.lock().publication_pause.take();
        if let Some(pause) = pause {
            let _ = pause.arrived.send(());
            let _ = pause.release.lock().recv();
            ensure!(!pause.fail, "injected publication persistence failure");
        }
        Ok(())
    }
    fn flush_lane(
        &self,
        lane: usize,
        writer: &mut Writer,
        pending: &mut Vec<Pending>,
        results: &mut [PutResult],
        bytes: usize,
    ) -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let a = writer.active.as_mut().context("no active append lane")?;
        let started = Instant::now();
        let result = a.file.sync_all();
        let elapsed = started.elapsed().as_micros() as u64;
        self.lane_timings[lane]
            .data_sync_us
            .fetch_add(elapsed, Ordering::Relaxed);
        self.write_timings
            .data_sync_us
            .fetch_add(elapsed, Ordering::Relaxed);
        self.write_timings
            .data_sync_calls
            .fetch_add(1, Ordering::Relaxed);
        result?;
        #[cfg(test)]
        self.pause_lane(lane);
        failpoint("lane_after_data_sync")?;
        failpoint(&format!("lane_{lane}_after_data_sync"))?;
        let started = Instant::now();
        let batch = Batch {
            meta: a.meta.clone(),
            pending: std::mem::take(pending),
            results: results.to_vec(),
            bytes,
        };
        let committed = self.submit_lane_commit(batch)?;
        self.lane_timings[lane]
            .commit_wait_us
            .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        results.clone_from_slice(&committed);
        // DELETE can change live/garbage accounting while payload I/O runs;
        // the sequencer merges against the authoritative current metadata.
        a.meta = decode_meta(
            a.meta.id,
            &self
                .segments
                .get(a.meta.id.as_bytes())?
                .context("published lane metadata missing")?,
        )?;
        Ok(())
    }
    fn submit_lane_commit(&self, batch: Batch) -> Result<Vec<PutResult>> {
        let work = Arc::new(Work {
            batch,
            state: Mutex::new(WorkState {
                lead: false,
                result: None,
            }),
            ready: Condvar::new(),
        });
        {
            let mut queue = self.commit_queue.lock();
            queue.pending.push_back(work.clone());
            if !queue.active {
                queue.active = true;
                work.state.lock().lead = true;
            }
            self.commit_changed.notify_one();
        }
        loop {
            let mut state = work.state.lock();
            if let Some(result) = state.result.take() {
                return result.map_err(anyhow::Error::msg);
            }
            if state.lead {
                state.lead = false;
                drop(state);
                self.commit_round();
            } else {
                work.ready.wait(&mut state);
            }
        }
    }
    fn gather_lane_commits(&self) {
        // This is separate from the coordinator's per-lane gather window.
        // A lane finishing the previous commit still needs that window and a
        // data sync before its next group can join the shared publication.
        if self.config.writes.writer_lanes <= 1 || self.config.writes.commit_max_delay_ms == 0 {
            return;
        }
        let deadline =
            Instant::now() + Duration::from_millis(self.config.writes.commit_max_delay_ms);
        let mut queue = self.commit_queue.lock();
        loop {
            let (records, bytes) =
                queue
                    .pending
                    .iter()
                    .fold((0usize, 0usize), |(records, bytes), work| {
                        (
                            records.saturating_add(work.batch.pending.len()),
                            bytes.saturating_add(work.batch.bytes),
                        )
                    });
            // One unpublished group per lane makes pending.len an exact
            // ready-lane count. Never hold the publication mutex here.
            if self.publication_fault.load(Ordering::Acquire)
                || queue.pending.len() >= self.config.writes.writer_lanes
                || records >= self.config.writes.batch_max_records
                || bytes >= self.config.writes.batch_max_bytes_mib * 1024 * 1024
                || Instant::now() >= deadline
            {
                break;
            }
            #[cfg(test)]
            if let Some(started) = queue.gather_started.take() {
                let _ = started.send(());
            }
            self.commit_changed.wait_until(&mut queue, deadline);
        }
    }
    fn commit_round(&self) {
        // Keep selected work outside the unwind boundary: even a panic while
        // selecting a group must leave every caller with a completion result.
        let mut group = Vec::new();
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            self.gather_lane_commits();
            // Do not freeze a partial group before waiting for publication.
            // Other lanes can complete data synchronization during that wait.
            let mut writer = self.writer.lock();
            let mut failure = maintenance::PublicationFailureGuard::new(&self.publication_fault);
            {
                let mut q = self.commit_queue.lock();
                let mut records = 0usize;
                let mut bytes = 0usize;
                while let Some(next) = q.pending.front() {
                    if !group.is_empty()
                        && (records + next.batch.pending.len()
                            > self.config.writes.batch_max_records
                            || bytes.saturating_add(next.batch.bytes)
                                > self.config.writes.batch_max_bytes_mib * 1024 * 1024)
                    {
                        break;
                    }
                    let next = q.pending.pop_front().unwrap();
                    records += next.batch.pending.len();
                    bytes = bytes.saturating_add(next.batch.bytes);
                    group.push(next);
                }
            }
            let result = self.publish_lane_group(&mut writer, &group);
            if result.is_err() {
                writer.faulted = true;
            } else {
                failure.complete();
            }
            result
        }));
        let result = match outcome {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("publication sequencer panicked")),
        };
        if result.is_err() {
            self.publication_fault.store(true, Ordering::Release);
            self.commit_changed.notify_all();
            self.writer.lock().faulted = true;
            // Publication is globally faulted. Fail all already queued work,
            // including a leader not yet selected if selection itself failed.
            group.extend(self.commit_queue.lock().pending.drain(..));
        }
        match result {
            Ok(results) => {
                for (work, result) in group.iter().zip(results) {
                    work.state.lock().result = Some(Ok(result));
                    work.ready.notify_one();
                }
            }
            Err(error) => {
                for work in &group {
                    work.state.lock().result = Some(Err(format!("{error:#}")));
                    work.ready.notify_one();
                }
            }
        }
        // Hand leadership to another waiting caller, so a busy lane cannot
        // retain the first caller's append mutex indefinitely.
        let mut q = self.commit_queue.lock();
        if let Some(next) = q.pending.front() {
            next.state.lock().lead = true;
            next.ready.notify_one();
        } else {
            q.active = false;
        }
    }
    fn publish_lane_group(
        &self,
        writer: &mut Writer,
        group: &[Arc<Work>],
    ) -> Result<Vec<Vec<PutResult>>> {
        ensure!(
            !writer.faulted && !self.publication_fault.load(Ordering::Acquire),
            "write coordinator faulted"
        );
        let mut failure = maintenance::PublicationFailureGuard::new(&self.publication_fault);
        let _visible = self.visibility.write();
        let result = (|| {
            let mut changes = Vec::new();
            let mut outputs = Vec::new();
            let mut added = 0u64;
            for work in group {
                let batch = &work.batch;
                let mut results = batch.results.clone();
                let mut meta = batch.meta.clone();
                let old = self
                    .segments
                    .get(meta.id.as_bytes())?
                    .context("append segment missing")?;
                let old = decode_meta(meta.id, &old)?;
                meta.live = old.live;
                meta.garbage_since = old.garbage_since;
                meta.compacted = old.compacted;
                for item in &batch.pending {
                    if self.objects.contains_key(item.hash)? {
                        results[item.index].duplicate = true;
                        meta.garbage_since = now();
                    } else {
                        changes.push(Mutation::Put {
                            hash: item.hash,
                            location: encode_loc(item.loc),
                        });
                        meta.live += item.loc.length;
                        added += 1;
                    }
                }
                changes.push(Mutation::segment(&meta));
                outputs.push(results);
            }
            #[cfg(test)]
            if std::mem::take(&mut self.commit_queue.lock().panic_before_publish) {
                panic!("injected publication panic");
            }
            failpoint("lane_group_before_publish")?;
            self.publish(&changes)?;
            self.object_count.fetch_add(added, Ordering::Relaxed);
            failpoint("lane_group_after_publish")?;
            failpoint("after_index_sync")?;
            Ok(outputs)
        })();
        if result.is_err() {
            writer.faulted = true;
        } else {
            failure.complete();
        }
        result
    }
    fn create_lane(&self, lane: usize, writer: &mut Writer) -> Result<()> {
        let id = loop {
            let id = Uuid::new_v4();
            if segment_lane(id) == lane {
                break id;
            }
        };
        let created = now();
        let meta = Meta {
            id,
            state: 0,
            path: relative(id),
            physical: SEGMENT_HEADER,
            end: SEGMENT_HEADER,
            count: 0,
            live: 0,
            created,
            sealed: 0,
            digest: None,
            garbage_since: 0,
            compacted: 0,
        };
        let full = segment_path(&self.root, &meta);
        let parent = full.parent().unwrap();
        fs::create_dir_all(parent)?;
        sync_dir(&self.root.join("segments"))?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&full)?;
        file.write_all(&header(id, created))?;
        file.sync_all()?;
        sync_dir(parent)?;
        let mut global = self.writer.lock();
        ensure!(
            !global.faulted && !self.publication_fault.load(Ordering::Acquire),
            "write coordinator faulted"
        );
        let _visible = self.visibility.write();
        let result = self.publish(&[Mutation::segment(&meta)]);
        if result.is_err() {
            global.faulted = true;
        }
        result?;
        writer.active = Some(Active { file, meta });
        Ok(())
    }
    fn seal_lane(&self, writer: &mut Writer) -> Result<()> {
        let Some(mut a) = writer.active.take() else {
            return Ok(());
        };
        a.file.set_len(a.meta.end)?;
        a.file.sync_all()?;
        failpoint("seal_before_footer")?;
        let digest = digest_prefix(&mut a.file, a.meta.end)?;
        a.meta.sealed = now();
        a.file.seek(SeekFrom::Start(a.meta.end))?;
        a.file.write_all(&footer(&a.meta, &digest))?;
        failpoint("seal_after_footer")?;
        a.file.sync_all()?;
        failpoint("seal_after_sync")?;
        drop(a.file);
        a.meta.state = 1;
        a.meta.physical = a.meta.end + SEGMENT_FOOTER;
        a.meta.digest = Some(digest);
        let mut global = self.writer.lock();
        ensure!(
            !global.faulted && !self.publication_fault.load(Ordering::Acquire),
            "write coordinator faulted"
        );
        let _visible = self.visibility.write();
        let old = self
            .segments
            .get(a.meta.id.as_bytes())?
            .context("sealing segment missing")?;
        let old = decode_meta(a.meta.id, &old)?;
        a.meta.live = old.live;
        a.meta.garbage_since = old.garbage_since;
        a.meta.compacted = old.compacted;
        let result = self.publish(&[Mutation::segment(&a.meta)]);
        if result.is_err() {
            global.faulted = true;
        }
        result
    }
    pub(super) fn sync_lanes(&self, seal: bool) -> Result<()> {
        let _barrier = self.lane_barrier.write();
        ensure!(!self.write_faulted(), "write coordinator faulted");
        let run = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
            for lane in &self.append_lanes {
                let mut lane = lane.lock();
                if seal {
                    self.seal_lane(&mut lane)?;
                } else if let Some(active) = &lane.active {
                    active.file.sync_all()?;
                }
            }
            let mut global = self.writer.lock();
            ensure!(
                !global.faulted && !self.publication_fault.load(Ordering::Acquire),
                "write coordinator faulted"
            );
            let _visible = self.visibility.write();
            let mut failure = maintenance::PublicationFailureGuard::new(&self.publication_fault);
            let result = self.db.persist(PersistMode::SyncAll);
            if result.is_err() {
                global.faulted = true;
            } else {
                failure.complete();
            }
            Ok(result?)
        }));
        match run {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                self.writer.lock().faulted = true;
                Err(e)
            }
            Err(_) => {
                self.writer.lock().faulted = true;
                bail!("lane synchronization panicked")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn payload(lane: usize, sequence: u64) -> Bytes {
        for nonce in 0u64.. {
            let bytes = Bytes::from(format!("lane {lane} sequence {sequence} nonce {nonce}"));
            if usize::from(blake3::hash(&bytes).as_bytes()[0]) % 2 == lane {
                return bytes;
            }
        }
        unreachable!()
    }
    #[test]
    fn paused_data_sync_in_one_lane_does_not_block_other_lane_commit() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        config.writes.writer_lanes = 2;
        let store = Arc::new(Store::open(&config).unwrap());
        let initial = [payload(0, 0), payload(1, 0)];
        for bytes in &initial {
            store.put_many(std::slice::from_ref(bytes)).unwrap();
        }
        let (arrived_tx, arrived_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *store.lane_pause.lock() = Some(Arc::new(LanePause {
            lane: 0,
            arrived: arrived_tx,
            release: Mutex::new(release_rx),
        }));
        let a = payload(0, 1);
        let b = payload(1, 1);
        let first = {
            let store = store.clone();
            let bytes = a.clone();
            std::thread::spawn(move || store.put_many(&[bytes]))
        };
        arrived_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (complete_tx, complete_rx) = std::sync::mpsc::channel();
        let second = {
            let store = store.clone();
            let bytes = b.clone();
            std::thread::spawn(move || {
                let result = store.put_many(&[bytes]);
                complete_tx.send(result.is_ok()).unwrap();
                result
            })
        };
        let completed_while_paused = complete_rx.recv_timeout(Duration::from_secs(5));
        release_tx.send(()).unwrap();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        assert!(
            completed_while_paused.unwrap(),
            "independent lane must publish while first lane is paused"
        );
        assert_eq!(store.status().unwrap().objects, 4);
        store.seal().unwrap();
        store.sync().unwrap();
        drop(store);
        let reopened = Store::open(&config).unwrap();
        for bytes in initial.into_iter().chain([a, b]) {
            assert_eq!(
                reopened
                    .get(blake3::hash(&bytes).as_bytes())
                    .unwrap()
                    .unwrap(),
                bytes.as_ref()
            );
        }
    }
    #[test]
    fn lane_feature_is_sticky_across_lane_downshift_and_rebuild() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        config.writes.writer_lanes = 2;
        let bytes = [payload(0, 1), payload(1, 1)];
        {
            let store = Store::open(&config).unwrap();
            store.put_many(&bytes).unwrap();
        }
        assert_eq!(
            selected_activation(directory.path())
                .unwrap()
                .unwrap()
                .version,
            3
        );
        config.writes.writer_lanes = 1;
        {
            let store = Store::open(&config).unwrap();
            assert_eq!(store.status().unwrap().objects, 2);
            store.put_many(&[Bytes::from_static(b"downshift")]).unwrap();
        }
        Store::rebuild(&config, false).unwrap();
        assert_eq!(
            selected_activation(directory.path())
                .unwrap()
                .unwrap()
                .version,
            3
        );
        let store = Store::open(&config).unwrap();
        assert_eq!(store.status().unwrap().objects, 3);
        store.seal().unwrap();
    }
    #[test]
    fn publication_panic_faults_all_waiters_and_restart_preserves_acknowledged_data() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        config.writes.writer_lanes = 2;
        config.writes.batch_max_delay_ms = 10;
        let store = Arc::new(Store::open(&config).unwrap());
        let acknowledged = [payload(0, 0), payload(1, 0)];
        store.put_many(&acknowledged).unwrap();
        store.commit_queue.lock().panic_before_publish = true;
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let mut handles = vec![];
        for lane in 0..2 {
            let store = store.clone();
            let barrier = barrier.clone();
            let tx = finished_tx.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                tx.send(store.put_many(&[payload(lane, 1)]).is_err())
                    .unwrap();
            }));
        }
        barrier.wait();
        assert!(finished_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        assert!(finished_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        for handle in handles {
            handle.join().unwrap();
        }
        assert!(store.write_faulted());
        assert!(store.put_many(&[payload(0, 2)]).is_err());
        drop(store);
        let store = Store::open(&config).unwrap();
        for bytes in acknowledged {
            assert_eq!(
                store.get(blake3::hash(&bytes).as_bytes()).unwrap().unwrap(),
                bytes.as_ref()
            );
        }
        assert!(
            store
                .get(blake3::hash(&payload(0, 1)).as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get(blake3::hash(&payload(1, 1)).as_bytes())
                .unwrap()
                .is_none()
        );
        store.put_many(&[payload(0, 2)]).unwrap();
        store.seal().unwrap();
    }
    #[test]
    fn explicit_legacy_upgrade_can_enable_lane_feature() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        {
            let store = Store::open(&config).unwrap();
            store
                .put_many(&[Bytes::from_static(b"legacy durable payload")])
                .unwrap();
        }
        let generation = selected_generation(directory.path()).unwrap().unwrap();
        // Isolated legacy-format fixture: the record/index encodings are shared.
        for path in activation_files(directory.path()).unwrap() {
            fs::remove_file(path).unwrap();
        }
        fs::remove_file(directory.path().join("format-v2")).unwrap();
        activate(directory.path(), &generation).unwrap();
        config.writes.writer_lanes = 2;
        assert!(
            Store::open(&config).is_err(),
            "service cannot silently upgrade a legacy store"
        );
        let result = Store::upgrade(&config).unwrap();
        assert_eq!(result["version"], 3);
        let store = Store::open(&config).unwrap();
        assert_eq!(store.format_version(), 3);
        assert_eq!(
            store
                .get(blake3::hash(b"legacy durable payload").as_bytes())
                .unwrap()
                .unwrap(),
            b"legacy durable payload"
        );
        store.put_many(&[payload(1, 5)]).unwrap();
        store.seal().unwrap();
    }
    #[test]
    fn lane_data_sync_overlaps_another_lanes_held_publication_gate() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        config.writes.writer_lanes = 2;
        let store = Arc::new(Store::open(&config).unwrap());
        store.put_many(&[payload(0, 0), payload(1, 0)]).unwrap();
        let (published_tx, published_rx) = std::sync::mpsc::channel();
        let (release_pub_tx, release_pub_rx) = std::sync::mpsc::channel();
        store.commit_queue.lock().publication_pause = Some(Arc::new(PublicationPause {
            arrived: published_tx,
            release: Mutex::new(release_pub_rx),
            fail: false,
        }));
        let first = {
            let store = store.clone();
            std::thread::spawn(move || store.put_many(&[payload(0, 1)]))
        };
        published_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (synced_tx, synced_rx) = std::sync::mpsc::channel();
        let (release_lane_tx, release_lane_rx) = std::sync::mpsc::channel();
        *store.lane_pause.lock() = Some(Arc::new(LanePause {
            lane: 1,
            arrived: synced_tx,
            release: Mutex::new(release_lane_rx),
        }));
        let second = {
            let store = store.clone();
            let prepared = store.prepare_put(payload(1, 1)).unwrap();
            std::thread::spawn(move || store.put_prepared_many(1, &[prepared]))
        };
        let sync_while_publication_held = synced_rx.recv_timeout(Duration::from_secs(5));
        let _ = release_lane_tx.send(());
        release_pub_tx.send(()).unwrap();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        assert!(
            sync_while_publication_held.is_ok(),
            "data preparation must not wait for another lane's journal/index synchronization"
        );
        assert_eq!(store.status().unwrap().objects, 4);
    }
    #[test]
    fn optimistic_duplicate_waits_for_publication_and_rejects_failed_persistence() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        config.writes.writer_lanes = 2;
        let store = Arc::new(Store::open(&config).unwrap());
        let acknowledged = [payload(0, 0), payload(1, 0)];
        store.put_many(&acknowledged).unwrap();
        let bytes = payload(0, 4);
        let hash = *blake3::hash(&bytes).as_bytes();
        let mut upload = store
            .begin_upload(bytes.len() as u64, Some(bytes.len() as u64))
            .unwrap();
        upload.write_chunk(&bytes).unwrap();
        let prepared = upload.prepare_finish().unwrap();
        let (published_tx, published_rx) = std::sync::mpsc::channel();
        let (release_pub_tx, release_pub_rx) = std::sync::mpsc::channel();
        store.commit_queue.lock().publication_pause = Some(Arc::new(PublicationPause {
            arrived: published_tx,
            release: Mutex::new(release_pub_rx),
            fail: true,
        }));
        let publisher = std::thread::spawn(move || prepared.finish());
        published_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            store.objects.contains_key(hash).unwrap(),
            "test pauses after index visibility before final persistence"
        );
        let syncs = store.write_timings.data_sync_calls.load(Ordering::Relaxed);
        let (observed_tx, observed_rx) = std::sync::mpsc::channel();
        let (release_dup_tx, release_dup_rx) = std::sync::mpsc::channel();
        *store.lane_pause.lock() = Some(Arc::new(LanePause {
            lane: 0,
            arrived: observed_tx,
            release: Mutex::new(release_dup_rx),
        }));
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let duplicate = {
            let store = store.clone();
            let prepared = store.prepare_put(bytes).unwrap();
            std::thread::spawn(move || {
                let result = store.put_prepared_many(0, &[prepared]);
                done_tx.send(result.is_err()).unwrap();
                result
            })
        };
        let observed = observed_rx.recv_timeout(Duration::from_secs(5));
        let _ = release_dup_tx.send(());
        let premature_ack = done_rx.recv_timeout(Duration::from_millis(100));
        release_pub_tx.send(()).unwrap();
        assert!(publisher.join().unwrap().is_err());
        assert!(duplicate.join().unwrap().is_err());
        assert!(
            observed.is_ok(),
            "duplicate must tentatively observe the unpersisted entry"
        );
        assert!(
            premature_ack.is_err(),
            "an optimistic mapping cannot certify a duplicate before persistence completes"
        );
        assert_eq!(
            store.write_timings.data_sync_calls.load(Ordering::Relaxed),
            syncs,
            "duplicate classification must not append a second payload"
        );
        assert!(store.write_faulted());
        drop(store);
        let reopened = Store::open(&config).unwrap();
        for bytes in acknowledged {
            assert_eq!(
                reopened
                    .get(blake3::hash(&bytes).as_bytes())
                    .unwrap()
                    .unwrap(),
                bytes.as_ref()
            );
        }
        reopened.put_many(&[payload(0, 5)]).unwrap();
        reopened.seal().unwrap();
    }
    #[test]
    fn shared_gather_releases_publication_gate_and_combines_ready_lanes() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        config.writes.writer_lanes = 2;
        config.writes.commit_max_delay_ms = 1000;
        let store = Arc::new(Store::open(&config).unwrap());
        store.put_many(&[payload(0, 0), payload(1, 0)]).unwrap();
        let before = store
            .write_timings
            .journal_sync_calls
            .load(Ordering::Relaxed);
        // Finish the second lane's I/O before starting the gather window, so
        // disk latency and database teardown are not part of this assertion.
        let (synced_tx, synced_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *store.lane_pause.lock() = Some(Arc::new(LanePause {
            lane: 1,
            arrived: synced_tx,
            release: Mutex::new(release_rx),
        }));
        let second = {
            let store = store.clone();
            let p = store.prepare_put(payload(1, 1)).unwrap();
            std::thread::spawn(move || store.put_prepared_many(1, &[p]))
        };
        synced_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (gather_tx, gather_rx) = std::sync::mpsc::channel();
        store.commit_queue.lock().gather_started = Some(gather_tx);
        let first = {
            let store = store.clone();
            let p = store.prepare_put(payload(0, 1)).unwrap();
            std::thread::spawn(move || store.put_prepared_many(0, &[p]))
        };
        let gathering = gather_rx.recv_timeout(Duration::from_secs(5));
        let gate_available = store.writer.try_lock().is_some();
        let _ = release_tx.send(());
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        assert!(gathering.is_ok());
        assert!(
            gate_available,
            "coalescing cannot hold the publication mutex"
        );
        assert_eq!(
            store
                .write_timings
                .journal_sync_calls
                .load(Ordering::Relaxed)
                - before,
            1,
            "both ready lane groups must share one durable publication"
        );
    }
    #[test]
    fn zero_shared_delay_publishes_without_waiting_for_other_lanes() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        config.writes.writer_lanes = 2;
        config.writes.commit_max_delay_ms = 0;
        let store = Arc::new(Store::open(&config).unwrap());
        store.put_many(&[payload(0, 0), payload(1, 0)]).unwrap();
        let before = store
            .write_timings
            .journal_sync_calls
            .load(Ordering::Relaxed);
        let (synced_tx, synced_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *store.lane_pause.lock() = Some(Arc::new(LanePause {
            lane: 1,
            arrived: synced_tx,
            release: Mutex::new(release_rx),
        }));
        let second = {
            let store = store.clone();
            let p = store.prepare_put(payload(1, 1)).unwrap();
            std::thread::spawn(move || store.put_prepared_many(1, &[p]))
        };
        synced_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let first = {
            let store = store.clone();
            let p = store.prepare_put(payload(0, 1)).unwrap();
            std::thread::spawn(move || {
                let result = store.put_prepared_many(0, &[p]);
                let _ = done_tx.send(result.is_ok());
                result
            })
        };
        let completed = done_rx.recv_timeout(Duration::from_secs(5));
        let _ = release_tx.send(());
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        assert!(completed.unwrap());
        assert_eq!(
            store
                .write_timings
                .journal_sync_calls
                .load(Ordering::Relaxed)
                - before,
            2
        );
    }
    #[test]
    fn full_record_group_bypasses_shared_gather_wait() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        config.writes.writer_lanes = 2;
        config.writes.batch_max_records = 1;
        config.writes.commit_max_delay_ms = 1000;
        let store = Store::open(&config).unwrap();
        let (started, _) = std::sync::mpsc::channel();
        store.commit_queue.lock().gather_started = Some(started);
        let p = store.prepare_put(payload(0, 0)).unwrap();
        store.put_prepared_many(0, &[p]).unwrap();
        assert!(
            store.commit_queue.lock().gather_started.is_some(),
            "a full group should never enter the timed gather wait"
        );
    }

    #[test]
    fn downshifted_single_lane_ignores_shared_gather_delay() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        config.writes.writer_lanes = 2;
        {
            let store = Store::open(&config).unwrap();
            store.put_many(&[payload(0, 0), payload(1, 0)]).unwrap();
        }
        config.writes.writer_lanes = 1;
        config.writes.commit_max_delay_ms = 1000;
        let store = Store::open(&config).unwrap();
        assert_eq!(store.format_version(), 3);
        let (started, _) = std::sync::mpsc::channel();
        store.commit_queue.lock().gather_started = Some(started);
        let p = store.prepare_put(payload(0, 1)).unwrap();
        store.put_prepared_many(0, &[p]).unwrap();
        assert!(
            store.commit_queue.lock().gather_started.is_some(),
            "one lane never waits for an additional lane"
        );
    }
}
