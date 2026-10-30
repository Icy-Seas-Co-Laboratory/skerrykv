//! Sharded, bounded payload cache. Each shard owns fixed byte and metadata budgets.
use super::{BudgetBuffer, CacheStatus, Config, Hash};
use bytes::Bytes;
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const ENTRY_META: usize = 512;
const SEEN_META: usize = 256;
const EVICT_PER_OP: usize = 64;
struct Entry {
    bytes: Bytes,
    tick: u64,
    protected: bool,
}
struct Shard {
    entries: HashMap<Hash, Entry>,
    probation: BTreeMap<u64, Hash>,
    protected: BTreeMap<u64, Hash>,
    seen: HashMap<Hash, (Instant, u64)>,
    seen_order: BTreeMap<u64, Hash>,
    tick: u64,
    bytes: usize,
    protected_bytes: usize,
    byte_limit: usize,
    entry_limit: usize,
    metadata_limit: usize,
    max_object: usize,
    admission_limit: usize,
    window: Duration,
    budget: Arc<Semaphore>,
    stats: CacheStatus,
}
pub(super) struct PayloadCache {
    shards: Vec<Mutex<Shard>>,
    shard_mask: usize,
    payload_limit: usize,
    metadata_limit: usize,
}
impl PayloadCache {
    pub(super) fn new(c: &Config) -> Self {
        let n = c.cache.shards.clamp(1, 64).next_power_of_two().min(64);
        let total = c.cache.payload_mib.saturating_mul(1024 * 1024);
        let meta = c.cache.metadata_mib.saturating_mul(1024 * 1024);
        let shard = (0..n)
            .map(|i| {
                let byte_limit = total / n + usize::from(i < total % n);
                let metadata_limit = meta / n + usize::from(i < meta % n);
                Mutex::new(Shard {
                    entries: HashMap::new(),
                    probation: BTreeMap::new(),
                    protected: BTreeMap::new(),
                    seen: HashMap::new(),
                    seen_order: BTreeMap::new(),
                    tick: 0,
                    bytes: 0,
                    protected_bytes: 0,
                    byte_limit,
                    entry_limit: c.cache.max_entries / n + usize::from(i < c.cache.max_entries % n),
                    metadata_limit,
                    max_object: c.cache.max_object_mib.saturating_mul(1024 * 1024),
                    admission_limit: c.cache.admission_entries / n
                        + usize::from(i < c.cache.admission_entries % n),
                    window: Duration::from_secs(c.cache.admission_window_seconds),
                    budget: Arc::new(Semaphore::new(byte_limit)),
                    stats: CacheStatus::default(),
                })
            })
            .collect();
        Self {
            shards: shard,
            shard_mask: n - 1,
            payload_limit: total,
            metadata_limit: meta,
        }
    }
    fn shard(&self, h: &Hash) -> usize {
        let mut a = [0; 8];
        a.copy_from_slice(&h[..8]);
        (u64::from_le_bytes(a) as usize) & self.shard_mask
    }
    pub(super) fn contains(&self, h: &Hash) -> bool {
        self.shards[self.shard(h)].lock().entries.contains_key(h)
    }
    pub(super) fn get(&self, h: &Hash) -> Option<Bytes> {
        self.shards[self.shard(h)].lock().get(h)
    }
    pub(super) fn observe(&self, h: Hash, data: &Bytes) {
        self.shards[self.shard(&h)].lock().observe(h, data)
    }
    pub(super) fn invalidate(&self, h: &Hash) {
        self.shards[self.shard(h)].lock().invalidate(h)
    }
    pub(super) fn maintain(&self) {
        for s in &self.shards {
            s.lock().maintain(64)
        }
    }
    pub(super) fn clear(&self) {
        for s in &self.shards {
            s.lock().clear()
        }
    }
    pub(super) fn status(&self) -> CacheStatus {
        let mut o = CacheStatus::default();
        for s in &self.shards {
            let x = s.lock().status();
            o.entries += x.entries;
            o.bytes += x.bytes;
            o.retained_bytes += x.retained_bytes;
            o.hits += x.hits;
            o.served_bytes += x.served_bytes;
            o.misses += x.misses;
            o.admissions += x.admissions;
            o.evictions += x.evictions;
            o.metadata_bytes += x.metadata_bytes;
            o.skipped_large += x.skipped_large;
            o.skipped_metadata += x.skipped_metadata;
            o.skipped_payload += x.skipped_payload;
        }
        o.payload_budget_bytes = self.payload_limit;
        o.metadata_budget_bytes = self.metadata_limit;
        o
    }
}
impl Shard {
    fn get(&mut self, h: &Hash) -> Option<Bytes> {
        self.tick = self.tick.wrapping_add(1);
        if let Some(e) = self.entries.get_mut(h) {
            if e.protected {
                self.protected.remove(&e.tick);
            } else {
                self.probation.remove(&e.tick);
                self.protected_bytes += e.bytes.len();
            }
            e.tick = self.tick;
            e.protected = true;
            self.protected.insert(e.tick, *h);
            let b = e.bytes.clone();
            self.stats.hits += 1;
            self.stats.served_bytes += b.len() as u64;
            self.balance(64);
            Some(b)
        } else {
            self.stats.misses += 1;
            None
        }
    }
    fn balance(&mut self, n: usize) {
        for _ in 0..n {
            if self.protected_bytes <= self.byte_limit * 4 / 5 {
                break;
            }
            let Some((t, h)) = self.protected.pop_first() else {
                break;
            };
            if let Some(e) = self.entries.get_mut(&h) {
                e.protected = false;
                self.protected_bytes -= e.bytes.len();
                self.probation.insert(t, h);
            }
        }
    }
    fn evict(&mut self) -> bool {
        let v = self
            .probation
            .pop_first()
            .or_else(|| self.protected.pop_first());
        if let Some((_, h)) = v {
            if let Some(e) = self.entries.remove(&h) {
                self.bytes -= e.bytes.len();
                if e.protected {
                    self.protected_bytes -= e.bytes.len()
                }
                self.stats.evictions += 1;
            }
            true
        } else {
            false
        }
    }
    fn metadata_bytes(&self) -> usize {
        // Charge HashMap allocation capacity, not only live keys. The per slot
        // charge covers both LRUs (two BTreeMap entries per resident key); the
        // seen charge also covers the admission order tree.
        self.entries.capacity().saturating_mul(ENTRY_META)
            + self.seen.capacity().saturating_mul(SEEN_META)
            + self.entries.len().saturating_mul(128)
            + self.seen.len().saturating_mul(64)
    }
    fn reserve_seen(&mut self) -> bool {
        if self.seen.try_reserve(1).is_err() {
            return false;
        }
        if self.metadata_bytes().saturating_add(64) <= self.metadata_limit {
            return true;
        }
        self.seen.shrink_to_fit();
        false
    }
    fn reserve_entry(&mut self) -> bool {
        if self.entries.try_reserve(1).is_err() {
            return false;
        }
        if self.metadata_bytes().saturating_add(128) <= self.metadata_limit {
            return true;
        }
        self.entries.shrink_to_fit();
        false
    }
    fn observe(&mut self, h: Hash, data: &Bytes) {
        if self.byte_limit == 0
            || data.len() > self.max_object
            || data.len() > self.byte_limit
            || self.entries.contains_key(&h)
        {
            if data.len() > self.max_object {
                self.stats.skipped_large += 1
            }
            return;
        }
        self.tick = self.tick.wrapping_add(1);
        let second = self
            .seen
            .remove(&h)
            .map(|(t, k)| {
                self.seen_order.remove(&k);
                t.elapsed() <= self.window
            })
            .unwrap_or(false);
        if !second {
            if self.admission_limit == 0 {
                self.stats.skipped_metadata += 1;
                return;
            }
            // Keep the history rolling at its configured bound. Evicting one
            // oldest key per observation prevents a full tracker from
            // permanently disabling admission until the timeout elapses.
            if self.seen.len() >= self.admission_limit
                && let Some((_, oldest)) = self.seen_order.pop_first()
            {
                self.seen.remove(&oldest);
            }
            if !self.reserve_seen() {
                self.stats.skipped_metadata += 1;
                return;
            }
            self.seen.insert(h, (Instant::now(), self.tick));
            self.seen_order.insert(self.tick, h);
            return;
        }
        let mut n = 0;
        while (self.bytes.saturating_add(data.len()) > self.byte_limit
            || self.entries.len() >= self.entry_limit
            || self.metadata_bytes().saturating_add(ENTRY_META) > self.metadata_limit)
            && n < EVICT_PER_OP
        {
            if !self.evict() {
                break;
            }
            n += 1
        }
        if self.bytes.saturating_add(data.len()) > self.byte_limit
            || self.entries.len() >= self.entry_limit
            || self.metadata_bytes().saturating_add(ENTRY_META) > self.metadata_limit
        {
            self.stats.skipped_metadata += 1;
            return;
        }
        if data.len() > u32::MAX as usize {
            self.stats.skipped_payload += 1;
            return;
        }
        if !self.reserve_entry() {
            self.stats.skipped_metadata += 1;
            return;
        }
        let permit: OwnedSemaphorePermit = match self
            .budget
            .clone()
            .try_acquire_many_owned(data.len().max(1) as u32)
        {
            Ok(p) => p,
            Err(_) => {
                self.stats.skipped_payload += 1;
                return;
            }
        };
        let b = Bytes::from_owner(BudgetBuffer {
            data: data.to_vec(),
            _permit: permit,
        });
        self.bytes += b.len();
        self.probation.insert(self.tick, h);
        self.entries.insert(
            h,
            Entry {
                bytes: b,
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
        self.entries.shrink_to_fit();
        self.seen.shrink_to_fit();
        self.bytes = 0;
        self.protected_bytes = 0;
    }
    fn invalidate(&mut self, h: &Hash) {
        if let Some(e) = self.entries.remove(h) {
            self.bytes -= e.bytes.len();
            if e.protected {
                self.protected_bytes -= e.bytes.len();
                self.protected.remove(&e.tick);
            } else {
                self.probation.remove(&e.tick);
            }
        }
        if let Some((_, t)) = self.seen.remove(h) {
            self.seen_order.remove(&t);
        }
    }
    fn maintain(&mut self, n: usize) {
        for _ in 0..n {
            let Some((t, h)) = self.seen_order.first_key_value().map(|(k, v)| (*k, *v)) else {
                break;
            };
            if self
                .seen
                .get(&h)
                .is_some_and(|(when, _)| when.elapsed() < self.window)
            {
                break;
            }
            self.seen_order.remove(&t);
            self.seen.remove(&h);
        }
    }
    fn status(&self) -> CacheStatus {
        CacheStatus {
            entries: self.entries.len(),
            bytes: self.bytes,
            retained_bytes: self.byte_limit - self.budget.available_permits(),
            metadata_bytes: self.metadata_bytes(),
            ..self.stats.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_retention_and_entry_metadata_limits() {
        let mut c = Config::default();
        c.cache.payload_mib = 1;
        c.cache.shards = 1;
        c.cache.max_object_mib = 1;
        c.cache.max_entries = 2;
        c.cache.metadata_mib = 1;
        c.cache.admission_entries = 4;
        let cache = PayloadCache::new(&c);
        let data = Bytes::from(vec![7; 4096]);
        let h = [1; 32];
        cache.observe(h, &data);
        assert_eq!(cache.status().entries, 0);
        cache.observe(h, &data);
        assert_eq!(cache.status().entries, 1);
        let held = cache.get(&h).unwrap();
        cache.clear();
        assert_eq!(cache.status().retained_bytes, 4096);
        drop(held);
        assert_eq!(cache.status().retained_bytes, 0);
        for n in 2..16 {
            let h = [n; 32];
            cache.observe(h, &data);
            cache.observe(h, &data);
        }
        assert!(cache.status().entries <= 2);
        assert!(cache.status().metadata_bytes <= cache.status().metadata_budget_bytes);
    }
    #[test]
    fn history_rolls_when_tracker_is_full() {
        let mut c = Config::default();
        c.cache.shards = 1;
        c.cache.admission_entries = 2;
        let cache = PayloadCache::new(&c);
        let data = Bytes::from_static(b"same payload");
        cache.observe([1; 32], &data);
        cache.observe([2; 32], &data);
        cache.observe([3; 32], &data);
        cache.observe([3; 32], &data);
        assert_eq!(cache.status().entries, 1);
        assert!(cache.status().metadata_bytes <= cache.status().metadata_budget_bytes);
    }
    #[test]
    fn eviction_does_not_hide_allocated_metadata() {
        let mut c = Config::default();
        c.cache.shards = 1;
        c.cache.metadata_mib = 1;
        c.cache.max_entries = 8;
        c.cache.admission_entries = 2048;
        let cache = PayloadCache::new(&c);
        let data = Bytes::from_static(b"x");
        for i in 0..1500u16 {
            let mut hash = [0; 32];
            hash[..2].copy_from_slice(&i.to_le_bytes());
            cache.observe(hash, &data);
            cache.observe(hash, &data);
        }
        let status = cache.status();
        assert!(status.metadata_bytes <= status.metadata_budget_bytes);
        assert!(status.evictions > 0);
    }
    #[test]
    fn cache_skips_oversize_object() {
        let mut c = Config::default();
        c.cache.shards = 1;
        c.cache.max_object_mib = 1;
        let cache = PayloadCache::new(&c);
        let d = Bytes::from(vec![0; 1024 * 1024 + 1]);
        cache.observe([1; 32], &d);
        cache.observe([1; 32], &d);
        assert_eq!(cache.status().entries, 0);
        assert_eq!(cache.status().skipped_large, 2);
    }
}
