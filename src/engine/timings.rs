//! Cumulative commit-path timings, not physical-device telemetry.
use super::*;
#[derive(Default)]
pub(super) struct WriteTimings {
    pub data_sync_us: AtomicU64,
    pub data_sync_calls: AtomicU64,
    pub journal_sync_us: AtomicU64,
    pub journal_sync_calls: AtomicU64,
    pub witness_sync_us: AtomicU64,
    pub witness_sync_calls: AtomicU64,
    pub index_apply_us: AtomicU64,
    pub index_persist_us: AtomicU64,
    pub index_persist_calls: AtomicU64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WriteTimingSnapshot {
    pub data_sync_us: u64,
    pub data_sync_calls: u64,
    pub journal_sync_us: u64,
    pub journal_sync_calls: u64,
    pub witness_sync_us: u64,
    pub witness_sync_calls: u64,
    pub index_apply_us: u64,
    pub index_persist_us: u64,
    pub index_persist_calls: u64,
}
impl Store {
    pub fn write_timings(&self) -> WriteTimingSnapshot {
        let t = &self.write_timings;
        WriteTimingSnapshot {
            data_sync_us: t.data_sync_us.load(Ordering::Relaxed),
            data_sync_calls: t.data_sync_calls.load(Ordering::Relaxed),
            journal_sync_us: t.journal_sync_us.load(Ordering::Relaxed),
            journal_sync_calls: t.journal_sync_calls.load(Ordering::Relaxed),
            witness_sync_us: t.witness_sync_us.load(Ordering::Relaxed),
            witness_sync_calls: t.witness_sync_calls.load(Ordering::Relaxed),
            index_apply_us: t.index_apply_us.load(Ordering::Relaxed),
            index_persist_us: t.index_persist_us.load(Ordering::Relaxed),
            index_persist_calls: t.index_persist_calls.load(Ordering::Relaxed),
        }
    }
}
