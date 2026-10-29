//! Shared, bounded progress counters for recovery and explicit verification.
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressSnapshot {
    pub state: String,
    pub phase: String,
    pub bytes_scanned: u64,
    pub bytes_verified: u64,
    pub segments_verified: u64,
    pub bytes_total: Option<u64>,
    pub records_checked: u64,
    pub segments_processed: u64,
    pub segments_total: Option<u64>,
    pub journal_bytes: u64,
    pub transactions_processed: u64,
    #[serde(default)]
    pub recovery_mode: Option<String>,
    #[serde(default)]
    pub recovery_reason: Option<String>,
    #[serde(default)]
    pub index_keyspace: Option<String>,
    #[serde(default)]
    pub index_entries_cleared: u64,
    #[serde(default)]
    pub journal_bytes_total: Option<u64>,
    #[serde(default)]
    pub journal_bytes_remaining: Option<u64>,
    #[serde(default)]
    pub journal_bytes_per_second: f64,
    #[serde(default)]
    pub journal_eta_seconds: Option<f64>,
    pub index_entries_compared: u64,
    pub index_bytes_copied: u64,
    pub elapsed_seconds: f64,
    pub bytes_per_second: f64,
    pub failures: Vec<String>,
}
struct Detail {
    state: String,
    phase: String,
    bytes_total: Option<u64>,
    segments_total: Option<u64>,
    finished: Option<Instant>,
    failures: Vec<String>,
    recovery_mode: Option<String>,
    recovery_reason: Option<String>,
    index_keyspace: Option<String>,
    journal_total: Option<u64>,
    journal_started: Option<Instant>,
    journal_finished: Option<Instant>,
}
pub struct Progress {
    started: Instant,
    detail: Mutex<Detail>,
    bytes: AtomicU64,
    verified_bytes: AtomicU64,
    verified_segments: AtomicU64,
    records: AtomicU64,
    segments: AtomicU64,
    journal_bytes: AtomicU64,
    transactions: AtomicU64,
    index_entries: AtomicU64,
    index_bytes: AtomicU64,
    entries_cleared: AtomicU64,
    cancelled: AtomicBool,
}
impl Default for Progress {
    fn default() -> Self {
        Self::new()
    }
}
impl Progress {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            detail: Mutex::new(Detail {
                state: "pending".into(),
                phase: "pending".into(),
                bytes_total: None,
                segments_total: None,
                finished: None,
                failures: vec![],
                recovery_mode: None,
                recovery_reason: None,
                index_keyspace: None,
                journal_total: None,
                journal_started: None,
                journal_finished: None,
            }),
            bytes: AtomicU64::new(0),
            verified_bytes: AtomicU64::new(0),
            verified_segments: AtomicU64::new(0),
            records: AtomicU64::new(0),
            segments: AtomicU64::new(0),
            journal_bytes: AtomicU64::new(0),
            transactions: AtomicU64::new(0),
            index_entries: AtomicU64::new(0),
            index_bytes: AtomicU64::new(0),
            entries_cleared: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
        }
    }
    pub fn recovery(&self, mode: &str, reason: &str) {
        let mut detail = self.detail.lock();
        detail.recovery_mode = Some(mode.into());
        detail.recovery_reason = Some(reason.into());
    }
    pub fn clearing_keyspace(&self, keyspace: &str) {
        self.phase("clearing index");
        self.detail.lock().index_keyspace = Some(keyspace.into());
    }
    pub fn add_cleared_entries(&self, count: u64) {
        self.entries_cleared.fetch_add(count, Ordering::Relaxed);
    }
    pub fn begin_journal(&self, total: u64) {
        self.phase("replaying journal");
        let mut detail = self.detail.lock();
        detail.index_keyspace = None;
        detail.journal_total = Some(total);
        detail.journal_started = Some(Instant::now());
        detail.journal_finished = None;
    }
    pub fn finish_journal(&self) {
        let mut detail = self.detail.lock();
        detail.journal_total = Some(self.journal_bytes.load(Ordering::Relaxed));
        if detail.journal_started.is_some() && detail.journal_finished.is_none() {
            detail.journal_finished = Some(Instant::now());
        }
    }
    pub fn phase(&self, phase: &str) {
        let mut detail = self.detail.lock();
        if detail.finished.is_none() {
            detail.state = "running".into();
            detail.phase = phase.into();
        }
    }
    pub fn add_bytes(&self, n: u64) {
        self.bytes.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_verified_bytes(&self, n: u64) {
        self.verified_bytes.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_verified_segments(&self, n: u64) {
        self.verified_segments.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_records(&self, n: u64) {
        self.records.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_segments(&self, n: u64) {
        self.segments.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_journal_bytes(&self, n: u64) {
        self.journal_bytes.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_transactions(&self, n: u64) {
        self.transactions.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_index_entries(&self, n: u64) {
        self.index_entries.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_index_bytes(&self, n: u64) {
        self.index_bytes.fetch_add(n, Ordering::Relaxed);
    }
    pub fn clear_totals(&self) {
        let mut detail = self.detail.lock();
        detail.bytes_total = None;
        detail.segments_total = None;
    }
    pub fn set_bytes_total(&self, n: u64) {
        self.detail.lock().bytes_total = Some(n);
    }
    pub fn set_segments_total(&self, n: u64) {
        self.detail.lock().segments_total = Some(n);
    }
    pub fn finish(&self) {
        let mut detail = self.detail.lock();
        if detail.finished.is_none() {
            detail.state = if self.is_cancelled() {
                "cancelled"
            } else {
                "completed"
            }
            .into();
            detail.finished = Some(Instant::now());
        }
    }
    pub fn fail(&self, message: &str) {
        let mut detail = self.detail.lock();
        detail.state = if self.is_cancelled() {
            "cancelled"
        } else {
            "failed"
        }
        .into();
        detail.finished = Some(Instant::now());
        if detail.failures.len() < 16 {
            detail.failures.push(message.chars().take(256).collect());
        }
    }
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.finish();
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
    pub fn snapshot(&self) -> ProgressSnapshot {
        let detail = self.detail.lock();
        let elapsed = detail
            .finished
            .unwrap_or_else(Instant::now)
            .duration_since(self.started)
            .as_secs_f64();
        let bytes = self.bytes.load(Ordering::Relaxed);
        let journal_bytes = self.journal_bytes.load(Ordering::Relaxed);
        let journal_elapsed = detail.journal_started.map(|start| {
            detail
                .journal_finished
                .or(detail.finished)
                .unwrap_or_else(Instant::now)
                .saturating_duration_since(start)
                .as_secs_f64()
        });
        let journal_rate = journal_elapsed
            .filter(|elapsed| *elapsed > 0.0)
            .map_or(0.0, |elapsed| journal_bytes as f64 / elapsed);
        let journal_remaining = detail
            .journal_total
            .map(|total| total.saturating_sub(journal_bytes));
        ProgressSnapshot {
            state: detail.state.clone(),
            phase: detail.phase.clone(),
            bytes_scanned: bytes,
            bytes_verified: self.verified_bytes.load(Ordering::Relaxed),
            segments_verified: self.verified_segments.load(Ordering::Relaxed),
            bytes_total: detail.bytes_total,
            records_checked: self.records.load(Ordering::Relaxed),
            segments_processed: self.segments.load(Ordering::Relaxed),
            segments_total: detail.segments_total,
            journal_bytes: self.journal_bytes.load(Ordering::Relaxed),
            transactions_processed: self.transactions.load(Ordering::Relaxed),
            recovery_mode: detail.recovery_mode.clone(),
            recovery_reason: detail.recovery_reason.clone(),
            index_keyspace: detail.index_keyspace.clone(),
            index_entries_cleared: self.entries_cleared.load(Ordering::Relaxed),
            journal_bytes_total: detail.journal_total,
            journal_bytes_remaining: journal_remaining,
            journal_bytes_per_second: journal_rate,
            journal_eta_seconds: if journal_rate > 0.0 {
                journal_remaining.map(|remaining| remaining as f64 / journal_rate)
            } else {
                None
            },
            index_entries_compared: self.index_entries.load(Ordering::Relaxed),
            index_bytes_copied: self.index_bytes.load(Ordering::Relaxed),
            elapsed_seconds: elapsed,
            bytes_per_second: if elapsed > 0.0 {
                bytes as f64 / elapsed
            } else {
                0.0
            },
            failures: detail.failures.clone(),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_stages_report_clearing_and_journal_progress_separately() {
        let p = Progress::new();
        p.recovery("full", "missing_cursor");
        p.clearing_keyspace("objects");
        p.add_cleared_entries(1024);
        let clearing = p.snapshot();
        assert_eq!(clearing.phase, "clearing index");
        assert_eq!(clearing.index_keyspace.as_deref(), Some("objects"));
        assert_eq!(clearing.index_entries_cleared, 1024);
        assert_eq!(clearing.journal_bytes_total, None);
        p.begin_journal(1000);
        p.add_journal_bytes(400);
        p.add_transactions(2);
        let replay = p.snapshot();
        assert_eq!(replay.index_keyspace, None);
        assert_eq!(replay.journal_bytes_remaining, Some(600));
        assert!(replay.journal_bytes_per_second > 0.0);
        assert!(replay.journal_eta_seconds.unwrap() >= 0.0);
        p.finish_journal();
        assert_eq!(p.snapshot().journal_bytes_remaining, Some(0));
        let encoded = serde_json::to_value(p.snapshot()).unwrap();
        let mut old = encoded.as_object().unwrap().clone();
        for key in [
            "recovery_mode",
            "recovery_reason",
            "index_keyspace",
            "index_entries_cleared",
            "journal_bytes_total",
            "journal_bytes_remaining",
            "journal_bytes_per_second",
            "journal_eta_seconds",
        ] {
            old.remove(key);
        }
        let decoded: ProgressSnapshot =
            serde_json::from_value(serde_json::Value::Object(old)).unwrap();
        assert_eq!(decoded.index_entries_cleared, 0);
    }
    #[test]
    fn terminal_state_freezes_elapsed_and_preserves_counters() {
        let p = Progress::new();
        p.phase("segments");
        p.set_bytes_total(100);
        p.add_bytes(25);
        p.add_records(1);
        p.finish();
        let a = p.snapshot();
        let b = p.snapshot();
        assert_eq!(a.state, "completed");
        assert_eq!(a.bytes_scanned, 25);
        assert_eq!(a.records_checked, 1);
        assert_eq!(a.elapsed_seconds, b.elapsed_seconds);
    }
    #[test]
    fn cancellation_is_visible_and_failure_history_is_bounded() {
        let p = Progress::new();
        p.cancel();
        p.phase("ignored");
        assert!(p.is_cancelled());
        assert_eq!(p.snapshot().state, "cancelled");
        for _ in 0..32 {
            p.fail("failure");
        }
        assert_eq!(p.snapshot().failures.len(), 16);
    }
}
