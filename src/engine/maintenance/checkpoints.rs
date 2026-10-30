//! Snapshot plus bounded catch-up checkpoints without holding the writer during scanning.
use super::*;
use fjall::Readable;

struct Unselected {
    path: PathBuf,
    activated: bool,
}
impl Drop for Unselected {
    fn drop(&mut self) {
        if !self.activated {
            let _ = fs::remove_file(&self.path);
        }
    }
}
pub(super) fn cleanup_interrupted(root: &Path) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name
            .strip_prefix(".checkpoint-work-")
            .is_some_and(|suffix| Uuid::parse_str(suffix).is_ok())
            && entry.file_type()?.is_dir()
        {
            fs::remove_dir_all(entry.path())?;
        }
    }
    Ok(())
}
fn check(progress: Option<&crate::progress::Progress>) -> Result<()> {
    ensure!(
        !progress.is_some_and(|p| p.is_cancelled()),
        "checkpoint cancelled"
    );
    Ok(())
}
fn visit_frames(
    source: &mut File,
    start: u64,
    end: u64,
    budget: u64,
    progress: Option<&crate::progress::Progress>,
    mut visit: impl FnMut(&[Mutation]) -> Result<()>,
) -> Result<u64> {
    source.seek(SeekFrom::Start(start))?;
    let mut offset = start;
    while offset < end {
        check(progress)?;
        ensure!(
            end - offset >= 20,
            "checkpoint source has a partial committed frame"
        );
        let mut header = [0u8; 20];
        source.read_exact(&mut header)?;
        ensure!(
            &header[..8] == FRAME && crc_ok(&header),
            "checkpoint source frame corrupt"
        );
        let size = u64_at(&header, 8);
        ensure!(
            size > 0 && size <= MAX_FRAME as u64,
            "invalid checkpoint frame size"
        );
        let next = offset
            .checked_add(32 + size)
            .context("checkpoint frame overflow")?;
        ensure!(next <= end, "checkpoint end is not a frame boundary");
        if offset > start && next - start > budget {
            break;
        }
        let mut body = vec![0u8; size as usize];
        source.read_exact(&mut body)?;
        let mut tail = [0u8; 12];
        source.read_exact(&mut tail)?;
        ensure!(
            &tail[..8] == COMMIT
                && crc32fast::hash(&body) == u32::from_le_bytes(tail[8..].try_into()?),
            "checkpoint source frame checksum mismatch"
        );
        let changes: Vec<Mutation> = serde_json::from_slice(&body)?;
        visit(&changes)?;
        if let Some(p) = progress {
            p.add_journal_bytes(next - offset);
            p.add_transactions(1);
        }
        offset = next;
    }
    Ok(offset)
}
fn copy_frames(
    source: &mut File,
    start: u64,
    end: u64,
    target: &mut Journal,
    budget: u64,
    progress: Option<&crate::progress::Progress>,
) -> Result<u64> {
    visit_frames(source, start, end, budget, progress, |changes| {
        target.append(changes)
    })
}

fn validate_snapshot(
    store: &Store,
    snapshot: &fjall::Snapshot,
    old_name: &str,
    end: u64,
    progress: Option<&crate::progress::Progress>,
) -> Result<()> {
    if let Some(p) = progress {
        p.phase("validating checkpoint journal authority");
    }
    let path = store
        .root
        .join(format!(".checkpoint-work-{}", Uuid::new_v4()));
    let mut config = store.config.clone();
    config.cache.index_mib = config.cache.index_mib.min(16);
    let (db, objects, segments) = open_db(&path, &config)?;
    let tombstones = db.keyspace("tombstones", KeyspaceCreateOptions::default)?;
    let retired = db.keyspace("retired", KeyspaceCreateOptions::default)?;
    let scratch = ScratchNamespace {
        objects: Some(objects),
        segments: Some(segments),
        tombstones: Some(tombstones),
        retired: Some(retired),
        db: Some(db),
        path,
        replay_end: JOURNAL_HEADER,
    };
    let mut source = File::open(store.root.join(old_name))?;
    let replayed = visit_frames(
        &mut source,
        JOURNAL_HEADER,
        end,
        u64::MAX,
        progress,
        |changes| {
            store.ensure_write_space(MAX_FRAME as u64)?;
            apply(
                scratch.db.as_ref().unwrap(),
                scratch.objects.as_ref().unwrap(),
                scratch.segments.as_ref().unwrap(),
                scratch.tombstones.as_ref().unwrap(),
                scratch.retired.as_ref().unwrap(),
                changes,
                ApplyMode::Disposable,
            )
        },
    )?;
    ensure!(replayed == end, "checkpoint source prefix incomplete");
    if let Some(p) = progress {
        p.phase("comparing checkpoint namespace");
    }
    for (name, expected, selected) in [
        ("objects", scratch.objects.as_ref().unwrap(), &store.objects),
        (
            "segments",
            scratch.segments.as_ref().unwrap(),
            &store.segments,
        ),
        (
            "tombstones",
            scratch.tombstones.as_ref().unwrap(),
            &store.tombstones,
        ),
        ("retired", scratch.retired.as_ref().unwrap(), &store.retired),
    ] {
        let mut left = expected.iter();
        let mut right = snapshot.iter(selected);
        loop {
            check(progress)?;
            let a = left.next().map(|item| item.into_inner()).transpose()?;
            let b = right.next().map(|item| item.into_inner()).transpose()?;
            ensure!(
                a == b,
                "checkpoint index differs from journal authority in {name}"
            );
            if a.is_none() {
                break;
            }
            if let Some(p) = progress {
                p.add_index_entries(1);
            }
        }
    }
    Ok(())
}
impl Store {
    /// Snapshot scanning/catch-up occurs outside the writer lock. Only the final
    /// bounded suffix and durable activation pause namespace publication.
    pub fn checkpoint_journal(
        &self,
        force: bool,
        progress: Option<&crate::progress::Progress>,
    ) -> Result<Value> {
        let Some(_maintenance) = self.maintenance.try_lock() else {
            return Ok(json!({"status":"busy"}));
        };
        let Some(journal) = &self.journal else {
            return Ok(json!({"status":"legacy_v1"}));
        };
        check(progress)?;
        let recovery = self
            .db
            .keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)?;
        let (snapshot, old_name, mut copied) = {
            let writer = self.writer.lock();
            ensure!(
                !writer.faulted && !self.publication_fault.load(Ordering::Acquire),
                "writer faulted"
            );
            let old = journal.lock();
            let end = old.file.metadata()?.len();
            ensure!(
                old.witness()?.floor == end,
                "checkpoint source is not fully witnessed"
            );
            let base = recovery
                .get(b"checkpoint_end")?
                .filter(|v| v.len() == 8)
                .map_or(JOURNAL_HEADER, |v| u64_at(&v, 0));
            let growth = end.saturating_sub(base);
            if !force
                && growth
                    < (self.config.checkpoint.min_journal_mib as u64).saturating_mul(1024 * 1024)
            {
                return Ok(
                    json!({"status":"below_threshold","journal_bytes":end,"growth_bytes":growth}),
                );
            }
            (self.db.snapshot(), old.name.clone(), end)
        };
        validate_snapshot(self, &snapshot, &old_name, copied, progress)?;
        let reserve = (self.config.streaming.min_free_mib as u64)
            .max(self.config.storage.min_free_mib)
            .saturating_mul(1024 * 1024)
            .max(16 * 1024 * 1024);
        ensure!(
            fs2::available_space(&self.root)? > reserve + MAX_FRAME as u64,
            "insufficient checkpoint disk headroom"
        );
        let mut cleanup = Unselected {
            path: PathBuf::new(),
            activated: false,
        };
        let mut next = Journal::create(&self.root)?;
        cleanup.path = self.root.join(&next.name);
        let batch_limit = self.config.checkpoint.batch_records;
        let mut changes = Vec::with_capacity(batch_limit);
        let flush = |changes: &mut Vec<Mutation>, next: &mut Journal| -> Result<()> {
            check(progress)?;
            if !changes.is_empty() {
                ensure!(
                    fs2::available_space(&self.root)? > reserve + MAX_FRAME as u64,
                    "checkpoint disk headroom exhausted"
                );
                next.append(changes)?;
                changes.clear();
            }
            Ok(())
        };
        if let Some(p) = progress {
            p.phase("checkpoint snapshot");
        }
        for (kind, space) in [
            (0, &self.segments),
            (1, &self.objects),
            (2, &self.tombstones),
            (3, &self.retired),
        ] {
            for entry in snapshot.iter(space) {
                check(progress)?;
                let (key, value) = entry.into_inner()?;
                changes.push(match kind {
                    0 => Mutation::Segment {
                        id: key.as_ref().try_into()?,
                        metadata: value.to_vec(),
                    },
                    1 => Mutation::Put {
                        hash: key.as_ref().try_into()?,
                        location: value.to_vec(),
                    },
                    2 => {
                        ensure!(value.len() == 8, "invalid checkpoint tombstone");
                        Mutation::Delete {
                            hash: key.as_ref().try_into()?,
                            timestamp: u64_at(&value, 0),
                        }
                    }
                    _ => Mutation::Retire {
                        id: key.as_ref().try_into()?,
                    },
                });
                if let Some(p) = progress {
                    p.add_index_entries(1);
                }
                if changes.len() >= batch_limit {
                    flush(&mut changes, &mut next)?;
                }
            }
        }
        flush(&mut changes, &mut next)?;
        drop(snapshot);
        let baseline = next.file.metadata()?.len();
        let mut source = File::open(self.root.join(&old_name))?;
        let catchup = (self.config.checkpoint.max_catchup_mib as u64).saturating_mul(1024 * 1024);
        let tail_limit = (self.config.checkpoint.max_tail_kib as u64).saturating_mul(1024);
        if let Some(p) = progress {
            p.phase("checkpoint catch-up");
        }
        loop {
            check(progress)?;
            ensure!(
                fs2::available_space(&self.root)? > reserve + MAX_FRAME as u64,
                "checkpoint disk headroom exhausted"
            );
            let writer = self.writer.lock();
            ensure!(
                !writer.faulted && !self.publication_fault.load(Ordering::Acquire),
                "writer faulted"
            );
            let old = journal.lock();
            ensure!(old.name == old_name, "journal changed during checkpoint");
            let end = old.file.metadata()?.len();
            ensure!(end >= copied, "journal shortened during checkpoint");
            if end - copied <= tail_limit {
                drop(old);
                let mut writer = writer;
                // If activation fails, stop this writer: the newest activation
                // may already select the new journal, including after a crash.
                let mut publication_started = false;
                let mut failure = None;
                let result = (|| {
                    if let Some(p) = progress {
                        p.phase("activating checkpoint");
                    }
                    copied =
                        copy_frames(&mut source, copied, end, &mut next, tail_limit, progress)?;
                    ensure!(copied == end, "checkpoint tail not copied");
                    failpoint("checkpoint_before_activation")?;
                    let new_end = next.file.metadata()?.len();
                    let mut batch = self.db.batch();
                    batch.insert(
                        &recovery,
                        b"cursor",
                        serde_json::to_vec(&ReplayCursor {
                            journal: next.name.clone(),
                            end: new_end,
                        })?,
                    );
                    batch.insert(&recovery, b"checkpoint_end", baseline.to_le_bytes());
                    publication_started = true;
                    failure = Some(super::PublicationFailureGuard::new(&self.publication_fault));
                    batch.commit()?;
                    self.db.persist(PersistMode::SyncAll)?;
                    let generation = selected_generation(&self.root)?
                        .context("missing checkpoint generation")?;
                    // From this point onward keep both journals on error; the
                    // new activation may be durable even if marker sync fails.
                    cleanup.activated = true;
                    activate_version(&self.root, &generation, Some(&next.name))?;
                    failpoint("checkpoint_after_activation")?;
                    let selected = next.name.clone();
                    *journal.lock() = next;
                    Ok(
                        json!({"status":"checkpointed","journal":selected,"journal_bytes":new_end,"snapshot_bytes":baseline,"source_bytes":end}),
                    )
                })();
                if result.is_err() && publication_started {
                    writer.faulted = true;
                } else if let Some(failure) = &mut failure {
                    failure.complete();
                }
                drop(failure);
                drop(writer);
                let report = result?;
                // New authoritative activation and index cursor are durable.
                // Old immutable activation files can remain as history.
                fs::remove_file(self.root.join(&old_name))?;
                sync_dir(&self.root)?;
                if let Some(p) = progress {
                    p.finish();
                }
                return Ok(report);
            }
            drop(old);
            drop(writer);
            let next_offset = copy_frames(&mut source, copied, end, &mut next, catchup, progress)?;
            ensure!(next_offset > copied, "checkpoint catch-up made no progress");
            copied = next_offset;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checkpoint_rejects_divergent_index_and_preserves_journal_authority() {
        for remove_tombstone in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = Config::default();
            config.storage.root = dir.path().join("store");
            let store = Store::open(&config).unwrap();
            let items = store
                .put_many(&[Bytes::from_static(b"keep"), Bytes::from_static(b"deleted")])
                .unwrap();
            store.delete(&items[1].hash).unwrap();
            let old = store.journal.as_ref().unwrap().lock().name.clone();
            let activation = selected_activation(&store.root).unwrap().unwrap().sequence;
            if remove_tombstone {
                store.tombstones.remove(items[1].hash).unwrap();
            } else {
                store.objects.remove(items[0].hash).unwrap();
            }
            assert!(store.checkpoint_journal(true, None).is_err());
            assert_eq!(
                selected_activation(&store.root).unwrap().unwrap().sequence,
                activation
            );
            assert!(store.root.join(old).exists());
            drop(store);
            Store::rebuild(&config, false).unwrap();
            let store = Store::open(&config).unwrap();
            assert_eq!(store.get(&items[0].hash).unwrap().unwrap(), b"keep");
            assert!(store.get(&items[1].hash).unwrap().is_none());
        }
    }
    #[test]
    fn checkpoint_rejects_corrupt_historical_journal_frame() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = dir.path().join("store");
        let store = Store::open(&config).unwrap();
        store.put_many(&[Bytes::from_static(b"object")]).unwrap();
        let old = store.journal.as_ref().unwrap().lock().name.clone();
        let activation = selected_activation(&store.root).unwrap().unwrap().sequence;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(store.root.join(&old))
            .unwrap();
        file.seek(SeekFrom::Start(JOURNAL_HEADER + 20)).unwrap();
        let mut byte = [0];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 1;
        file.seek(SeekFrom::Start(JOURNAL_HEADER + 20)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
        assert!(store.checkpoint_journal(true, None).is_err());
        assert_eq!(
            selected_activation(&store.root).unwrap().unwrap().sequence,
            activation
        );
        assert!(store.root.join(old).exists());
    }
    #[test]
    fn checkpoint_preserves_deletion_fences_and_restarts_incrementally() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = dir.path().join("store");
        let store = Store::open(&config).unwrap();
        let items = store
            .put_many(&[
                Bytes::from_static(b"keep"),
                Bytes::from_static(b"delete"),
                Bytes::new(),
            ])
            .unwrap();
        store.delete(&items[1].hash).unwrap();
        let old = store.journal.as_ref().unwrap().lock().name.clone();
        let result = store.checkpoint_journal(true, None).unwrap();
        assert_eq!(result["status"], "checkpointed");
        assert!(!store.root.join(old).exists());
        assert!(store.get(&items[1].hash).unwrap().is_none());
        drop(store);
        let progress = Arc::new(crate::progress::Progress::new());
        let store = Store::open_with_progress(&config, &progress).unwrap();
        assert_eq!(
            progress.snapshot().recovery_mode.as_deref(),
            Some("incremental")
        );
        assert_eq!(store.get(&items[0].hash).unwrap().unwrap(), b"keep");
        assert!(store.get(&items[1].hash).unwrap().is_none());
        drop(store);
        Store::verify(&config).unwrap();
        Store::rebuild(&config, false).unwrap();
        let store = Store::open(&config).unwrap();
        assert!(store.get(&items[1].hash).unwrap().is_none());
    }
    #[test]
    fn concurrent_mutations_survive_snapshot_and_tail_catchup() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = dir.path().join("store");
        let store = Arc::new(Store::open(&config).unwrap());
        let keys = store
            .put_many(
                &(0u32..512)
                    .map(|i| Bytes::from(i.to_le_bytes().to_vec()))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let worker = store.clone();
        let thread = std::thread::spawn(move || worker.checkpoint_journal(true, None).unwrap());
        for item in keys.iter().take(64) {
            store.delete(&item.hash).unwrap();
        }
        assert_eq!(thread.join().unwrap()["status"], "checkpointed");
        drop(store);
        Store::verify(&config).unwrap();
        let store = Store::open(&config).unwrap();
        for item in keys.iter().take(64) {
            assert!(store.get(&item.hash).unwrap().is_none());
        }
    }
}
