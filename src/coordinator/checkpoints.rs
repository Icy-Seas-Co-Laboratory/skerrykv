use super::*;

#[derive(Default)]
pub(super) struct State {
    running: bool,
    completed: u64,
    last_result: Option<serde_json::Value>,
    last_error: Option<String>,
    progress: Option<Arc<crate::progress::Progress>>,
}
impl State {
    pub(super) fn status(&self, enabled: bool) -> CheckpointStatus {
        CheckpointStatus {
            enabled,
            running: self.running,
            completed_runs: self.completed,
            last_result: self.last_result.clone(),
            last_error: self.last_error.clone(),
            progress: self.progress.as_ref().map(|p| p.snapshot()),
        }
    }
}
pub(super) fn cancel(coordinator: &Coordinator) {
    if let Some(progress) = &coordinator.checkpoint_state.lock().progress {
        progress.cancel();
    }
}
pub(super) fn spawn(coordinator: &Arc<Coordinator>) -> Option<tokio::task::JoinHandle<()>> {
    if !coordinator.config.checkpoint.enabled {
        return None;
    }
    let weak = Arc::downgrade(coordinator);
    let stop = coordinator.maintenance_stop.clone();
    let seconds = coordinator.config.checkpoint.interval_seconds;
    Some(tokio::spawn(async move {
        let start = tokio::time::Instant::now() + Duration::from_secs(seconds);
        let mut interval = tokio::time::interval_at(start, Duration::from_secs(seconds));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _=stop.cancelled()=>break,
                _=interval.tick()=>{
                    let Some(coordinator)=weak.upgrade() else {break;};
                    let operation=coordinator.checkpoint_journal(false);
                    tokio::pin!(operation);
                    tokio::select! {
                        result=&mut operation=>{
                            if let Err(error)=result {coordinator.counters.error(format!("scheduled checkpoint: {error:#}"));}
                        },
                        _=stop.cancelled()=>{
                            cancel(&coordinator);
                            let _=operation.await;
                            break;
                        }
                    }
                }
            }
        }
    }))
}
impl Coordinator {
    pub async fn checkpoint_journal(&self, force: bool) -> Result<serde_json::Value> {
        let gate = self
            .gc_gate
            .clone()
            .try_acquire_owned()
            .map_err(|_| ResourceExhausted)?;
        let progress = Arc::new(crate::progress::Progress::new());
        {
            let _admission = self.admission.lock().await;
            ensure!(self.is_ready(), "service is not accepting maintenance");
            // Register cancellation before releasing admission: shutdown must
            // see every admitted checkpoint before waiting on the gate.
            let mut state = self.checkpoint_state.lock();
            state.running = true;
            state.last_error = None;
            state.progress = Some(progress.clone());
        }
        let store = self.store.clone();
        let state = self.checkpoint_state.clone();
        let counters = self.counters.clone();
        tokio::task::spawn_blocking(move || {
            let _gate = gate;
            let result = store.checkpoint_journal(force, Some(&progress));
            let mut state = state.lock();
            state.running = false;
            match &result {
                Ok(value) => {
                    if value["status"] == "checkpointed" {
                        state.completed += 1;
                    }
                    state.last_result = Some(value.clone());
                    progress.finish();
                }
                Err(error) => {
                    tracing::warn!(error=%error,"journal checkpoint failed");
                    state.last_error =
                        Some("checkpoint failed or cancelled; inspect service logs".into());
                    progress.fail("checkpoint failed or cancelled; inspect service logs");
                    if store.write_faulted() {
                        counters.faulted.store(true, Ordering::Release);
                    }
                }
            }
            result
        })
        .await?
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn scheduled_checkpoint_is_bounded_and_shutdown_releases_store() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = dir.path().join("store");
        config.checkpoint.interval_seconds = 1;
        config.checkpoint.min_journal_mib = 1;
        let coordinator = Coordinator::open(config.clone()).await.unwrap();
        let hash = coordinator
            .put(Bytes::from_static(b"scheduled namespace"))
            .await
            .unwrap()
            .hash;
        assert_eq!(
            coordinator.checkpoint_journal(true).await.unwrap()["status"],
            "checkpointed"
        );
        assert_eq!(
            coordinator
                .checkpoint_state
                .lock()
                .status(true)
                .completed_runs,
            1
        );
        coordinator.shutdown().await.unwrap();
        drop(coordinator);
        let store = Store::open(&config).unwrap();
        assert!(store.exists(&hash).unwrap());
    }
}
