//! Version two's committed mutation stream is the namespace authority. Physical
//! records alone are deliberately insufficient to reconstruct a deleted object.
use super::*;
use serde_json::{Value, json};

const JOURNAL_HEADER: u64 = 128;
const JOURNAL_MAGIC: &[u8; 8] = b"SKRYJNL2";
const FRAME: &[u8; 8] = b"SKRYMUT2";
const COMMIT: &[u8; 8] = b"SKRYCMT2";
const MAX_FRAME: usize = 16 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
pub(super) enum Mutation {
    Put { hash: [u8; 32], location: Vec<u8> },
    Delete { hash: [u8; 32], timestamp: u64 },
    Segment { id: [u8; 16], metadata: Vec<u8> },
    Retire { id: [u8; 16] },
    Cleaned { id: [u8; 16] },
}
impl Mutation {
    pub(super) fn segment(meta: &Meta) -> Self {
        Self::Segment {
            id: *meta.id.as_bytes(),
            metadata: encode_meta(meta),
        }
    }
}
#[derive(Serialize, Deserialize)]
struct ReplayCursor {
    journal: String,
    end: u64,
}
const RECOVERY_SPACE: &str = "recovery";

pub(super) struct Journal {
    pub name: String,
    file: File,
}
impl Journal {
    pub fn create(root: &Path) -> Result<Self> {
        let name = format!("journal-{}.log", Uuid::new_v4());
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(root.join(&name))?;
        file.write_all(&journal_header(JOURNAL_HEADER, 0))?;
        file.write_all(&journal_header(JOURNAL_HEADER, 0))?;
        file.sync_all()?;
        sync_dir(root)?;
        Ok(Self { name, file })
    }
    pub fn read_only(root: &Path, name: &str) -> Result<Self> {
        let journal = Self {
            name: name.into(),
            file: File::open(root.join(name))?,
        };
        journal.visit(|_| Ok(()))?;
        Ok(journal)
    }
    // Normal open validates the durable receipt, not historical frames.
    pub fn open_fast(root: &Path, name: &str) -> Result<Self> {
        let journal = Self {
            name: name.into(),
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .open(root.join(name))?,
        };
        journal.witness()?;
        Ok(journal)
    }
    pub fn recover_index(
        &self,
        db: &Database,
        objects: &Keyspace,
        segments: &Keyspace,
        tombstones: &Keyspace,
        retired: &Keyspace,
        progress: Option<&crate::progress::Progress>,
    ) -> Result<()> {
        if let Some(p) = progress {
            p.phase("recovering journal");
            ensure!(!p.is_cancelled(), "startup cancelled");
        }
        let recovery = db.keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)?;
        let cursor = recovery
            .get(b"cursor")?
            .map(|value| serde_json::from_slice::<ReplayCursor>(&value))
            .transpose()?;
        let witness = self.witness()?;
        let start = match cursor {
            Some(cursor)
                if cursor.journal == self.name
                    && !witness.degraded
                    && cursor.end <= witness.floor =>
            {
                ensure!(
                    cursor.end >= JOURNAL_HEADER && cursor.end <= witness.floor,
                    "index replay cursor is beyond the durable journal witness"
                );
                ensure!(
                    recovery.contains_key(b"objects")?,
                    "missing recovery object count"
                );
                if let Some(p) = progress {
                    p.recovery("incremental", "valid_cursor");
                    tracing::info!(
                        mode = "incremental",
                        reason = "valid_cursor",
                        "journal recovery selected"
                    );
                }
                cursor.end
            }
            cursor => {
                if let Some(p) = progress {
                    p.recovery(
                        "full",
                        if witness.degraded {
                            "degraded_witness"
                        } else if let Some(cursor) = &cursor {
                            if cursor.journal != self.name {
                                "journal_changed"
                            } else {
                                "cursor_ahead_of_witness"
                            }
                        } else {
                            "missing_cursor"
                        },
                    );
                    let snapshot = p.snapshot();
                    tracing::info!(
                        mode = "full",
                        reason = snapshot.recovery_reason.as_deref().unwrap_or("unknown"),
                        "journal recovery selected"
                    );
                }
                // Existing releases have no cursor. Reconstruct once, also used
                // conservatively when a witness slot needs recovery.
                self.replay_with_progress(db, objects, segments, tombstones, retired, progress)?;
                let value = recovery.get(b"cursor")?.context("missing replay cursor")?;
                serde_json::from_slice::<ReplayCursor>(&value)?.end
            }
        };
        if let Some(p) = progress
            && p.snapshot().recovery_mode.as_deref() == Some("incremental")
        {
            p.begin_journal(self.file.metadata()?.len().saturating_sub(start));
        }
        let mut previous_end = start;
        let end = self.visit_from(start, |changes, end| {
            if let Some(p) = progress {
                ensure!(!p.is_cancelled(), "startup cancelled");
            }
            // Keep the receipt ahead of any persisted index cursor, including
            // complete but previously unacknowledged post-witness frames.
            if end > self.witness()?.floor {
                let mut receipt = Self {
                    name: self.name.clone(),
                    file: self.file.try_clone()?,
                };
                receipt.write_witness(end)?;
            }
            apply(
                db,
                objects,
                segments,
                tombstones,
                retired,
                &changes,
                ApplyMode::Durable(Some((&self.name, end))),
            )?;
            if let Some(p) = progress {
                p.add_journal_bytes(end - previous_end);
                p.add_transactions(1);
            }
            previous_end = end;
            Ok(())
        })?;
        if end != start {
            if let Some(p) = progress {
                p.phase("synchronizing recovered index");
            }
            db.persist(PersistMode::SyncAll)?;
        }
        if let Some(p) = progress {
            p.finish_journal();
            p.phase("finalizing journal recovery");
        }
        let mut file = self.file.try_clone()?;
        if file.metadata()?.len() != end {
            file.set_len(end)?;
            file.sync_all()?;
        }
        if witness.degraded || end > witness.floor {
            // write_witness needs mutable access to a file handle only.
            let mut journal = Self {
                name: self.name.clone(),
                file,
            };
            journal.write_witness(end)?;
        } else {
            file.seek(SeekFrom::End(0))?;
        }
        Ok(())
    }
    pub fn open(root: &Path, name: &str) -> Result<Self> {
        let mut journal = Self {
            name: name.into(),
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .open(root.join(name))?,
        };
        let witness = journal.witness()?;
        let end = journal.visit(|_| Ok(()))?;
        if journal.file.metadata()?.len() != end {
            journal.file.set_len(end)?;
            journal.file.sync_all()?;
        }
        if witness.degraded || end > witness.floor {
            journal.write_witness(end)?;
        }
        journal.file.seek(SeekFrom::End(0))?;
        Ok(journal)
    }
    fn witness(&self) -> Result<Witness> {
        let length = self.file.metadata()?.len();
        ensure!(
            length >= JOURNAL_HEADER,
            "truncated mutation journal witness area"
        );
        let mut bytes = [0; JOURNAL_HEADER as usize];
        read_exact_at(&self.file, &mut bytes, 0)?;
        let first = parse_witness(&bytes[..64]);
        let second = parse_witness(&bytes[64..]);
        let witness = match (first, second) {
            (Some((generation, floor)), None) => Witness {
                generation,
                floor,
                slot: 0,
                degraded: true,
            },
            (None, Some((generation, floor))) => Witness {
                generation,
                floor,
                slot: 1,
                degraded: true,
            },
            (Some(a), Some(b)) => {
                if a.0 == b.0 {
                    ensure!(
                        a.1 == b.1,
                        "conflicting mutation witnesses at same generation"
                    );
                }
                let (slot, latest, older) = if a.0 > b.0 { (0, a, b) } else { (1, b, a) };
                ensure!(
                    latest.1 >= older.1,
                    "mutation witness floor moved backwards"
                );
                Witness {
                    generation: latest.0,
                    floor: latest.1,
                    slot,
                    degraded: false,
                }
            }
            (None, None) => bail!("both mutation journal witness slots are corrupt"),
        };
        ensure!(
            witness.floor <= length,
            "committed mutation journal was truncated below durable witness"
        );
        Ok(witness)
    }
    fn visit(&self, mut visit: impl FnMut(Vec<Mutation>) -> Result<()>) -> Result<u64> {
        self.visit_from(JOURNAL_HEADER, |changes, _| visit(changes))
    }
    fn visit_from(
        &self,
        start: u64,
        mut visit: impl FnMut(Vec<Mutation>, u64) -> Result<()>,
    ) -> Result<u64> {
        let witness = self.witness()?;
        let degraded = witness.degraded;
        let witness = witness.floor;
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        let length = file.metadata()?.len();
        let mut offset = start;
        while offset < length {
            // Only a short header/body is an incomplete transaction. A complete
            // frame with invalid CRC or marker is corruption, even at EOF.
            if length - offset < 20 {
                ensure!(
                    offset >= witness && !degraded,
                    "incomplete tail with uncertain/acknowledged mutation witness"
                );
                return Ok(offset);
            }
            // try_clone shares the seek position; a recovery callback can
            // update witness slots through another handle.
            file.seek(SeekFrom::Start(offset))?;
            let mut header = [0; 20];
            file.read_exact(&mut header)?;
            ensure!(
                &header[..8] == FRAME && crc_ok(&header),
                "mutation journal header corruption at {offset}"
            );
            let size = u64_at(&header, 8);
            ensure!(
                size > 0 && size <= MAX_FRAME as u64,
                "invalid mutation journal batch length"
            );
            if length - offset - 20 < size + 12 {
                ensure!(
                    offset >= witness && !degraded,
                    "incomplete tail with uncertain/acknowledged mutation witness"
                );
                return Ok(offset);
            }
            if offset < witness {
                ensure!(
                    offset + 20 + size + 12 <= witness,
                    "mutation witness is not a batch boundary"
                );
            }
            let mut body = vec![0; size as usize];
            file.read_exact(&mut body)?;
            let mut tail = [0; 12];
            file.read_exact(&mut tail)?;
            ensure!(
                &tail[..8] == COMMIT
                    && crc32fast::hash(&body) == u32::from_le_bytes(tail[8..].try_into().unwrap()),
                "mutation journal batch checksum/commit corruption at {offset}"
            );
            offset += 20 + size + 12;
            visit(
                serde_json::from_slice(&body).context("invalid committed mutation batch")?,
                offset,
            )?;
        }
        ensure!(
            !degraded || offset > witness,
            "corrupt witness without a complete post-floor batch; refusing possible acknowledged truncation"
        );
        Ok(offset)
    }
    fn append(&mut self, changes: &[Mutation]) -> Result<()> {
        let body = serde_json::to_vec(changes)?;
        ensure!(
            !body.is_empty() && body.len() <= MAX_FRAME,
            "mutation batch exceeds journal limit"
        );
        let mut header = [0; 20];
        header[..8].copy_from_slice(FRAME);
        header[8..16].copy_from_slice(&(body.len() as u64).to_le_bytes());
        finish_crc(&mut header);
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&header)?;
        self.file.write_all(&body)?;
        failpoint("journal_before_commit")?;
        self.file.write_all(COMMIT)?;
        self.file.write_all(&crc32fast::hash(&body).to_le_bytes())?;
        self.file.sync_all()?;
        failpoint("journal_after_frame_sync")?;
        let end = self.file.stream_position()?;
        self.write_witness(end)?;
        failpoint("after_mutation_sync")?;
        Ok(())
    }
    fn write_witness(&mut self, end: u64) -> Result<()> {
        let previous = self.witness()?;
        let generation = previous
            .generation
            .checked_add(1)
            .context("mutation witness generation exhausted")?;
        let next_slot = 1 - previous.slot;
        let header = journal_header(end, generation);
        self.file.seek(SeekFrom::Start(next_slot as u64 * 64))?;
        if std::env::var("SKERRY_FAILPOINT").ok().as_deref() == Some("journal_during_witness") {
            // A real partial durable overwrite of the inactive receipt, after
            // the complete mutation frame has already reached stable storage.
            self.file.write_all(&header[..24])?;
            self.file.sync_all()?;
            failpoint("journal_during_witness")?;
        }
        self.file.write_all(&header)?;
        self.file.sync_all()?;
        Ok(())
    }
    pub fn replay(
        &self,
        db: &Database,
        objects: &Keyspace,
        segments: &Keyspace,
        tombstones: &Keyspace,
        retired: &Keyspace,
    ) -> Result<()> {
        self.replay_with_progress(db, objects, segments, tombstones, retired, None)
    }
    fn replay_with_progress(
        &self,
        db: &Database,
        objects: &Keyspace,
        segments: &Keyspace,
        tombstones: &Keyspace,
        retired: &Keyspace,
        progress: Option<&crate::progress::Progress>,
    ) -> Result<()> {
        if let Some(p) = progress {
            ensure!(!p.is_cancelled(), "startup cancelled");
        }
        let recovery = db.keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)?;
        // Invalidate the old cursor durably before any destructive rebuild.
        // A process death during clearing must restart the full replay.
        if let Some(p) = progress {
            p.phase("synchronizing recovery metadata");
        }
        recovery.remove(b"cursor")?;
        db.persist(PersistMode::SyncAll)?;
        for (name, space) in [
            ("objects", objects),
            ("segments", segments),
            ("tombstones", tombstones),
            ("retired", retired),
            ("recovery", &recovery),
        ] {
            if let Some(p) = progress {
                p.clearing_keyspace(name);
                tracing::info!(keyspace = name, "clearing recovery index keyspace");
            }
            let mut cleared = 0u64;
            // Resume strictly beyond the last deleted key. Restarting at the
            // beginning repeatedly walks an ever-growing tombstone prefix.
            let mut cursor: Option<Vec<u8>> = None;
            loop {
                if let Some(p) = progress {
                    ensure!(!p.is_cancelled(), "startup cancelled");
                }
                let mut batch = db.batch();
                let mut count = 0;
                let lower = cursor.as_ref().map_or(std::ops::Bound::Unbounded, |key| {
                    std::ops::Bound::Excluded(key.clone())
                });
                let mut next = None;
                for item in space
                    .range::<Vec<u8>, _>((lower, std::ops::Bound::Unbounded))
                    .take(1024)
                {
                    let (key, _) = item.into_inner()?;
                    next = Some(key.to_vec());
                    batch.remove(space, key);
                    count += 1;
                }
                if count == 0 {
                    break;
                }
                batch.commit()?;
                cursor = next;
                cleared += count;
                if let Some(p) = progress {
                    p.add_cleared_entries(count);
                }
            }
            if progress.is_some() {
                tracing::info!(
                    keyspace = name,
                    entries_cleared = cleared,
                    "recovery index keyspace cleared"
                );
            }
        }
        if let Some(p) = progress {
            tracing::info!(
                journal_bytes = self.file.metadata()?.len().saturating_sub(JOURNAL_HEADER),
                "replaying mutation journal"
            );
            p.begin_journal(self.file.metadata()?.len().saturating_sub(JOURNAL_HEADER));
        }
        // Reset the count before replay, so each frame updates it atomically.
        recovery.insert(b"objects", 0u64.to_le_bytes())?;
        let mut previous_end = JOURNAL_HEADER;
        let end = self.visit_from(JOURNAL_HEADER, |changes, end| {
            if let Some(p) = progress {
                ensure!(!p.is_cancelled(), "startup cancelled");
            }
            // Keep the cursor absent until the entire reconstruction succeeds.
            apply(
                db,
                objects,
                segments,
                tombstones,
                retired,
                &changes,
                ApplyMode::Durable(None),
            )?;
            if let Some(p) = progress {
                p.add_journal_bytes(end - previous_end);
                p.add_transactions(1);
            }
            previous_end = end;
            Ok(())
        })?;
        recovery.insert(
            b"cursor",
            serde_json::to_vec(&ReplayCursor {
                journal: self.name.clone(),
                end,
            })?,
        )?;
        if let Some(p) = progress {
            p.phase("synchronizing recovered index");
        }
        db.persist(PersistMode::SyncAll)?;
        if let Some(p) = progress {
            p.finish_journal();
        }
        Ok(())
    }
}
struct Witness {
    generation: u64,
    floor: u64,
    slot: usize,
    degraded: bool,
}
fn parse_witness(header: &[u8]) -> Option<(u64, u64)> {
    if header.len() != 64
        || &header[..8] != JOURNAL_MAGIC
        || u16_at(header, 8) != 2
        || u16_at(header, 10) != 64
        || !crc_ok(header)
        || header[28..60].iter().any(|b| *b != 0)
    {
        return None;
    }
    let floor = u64_at(header, 12);
    (floor >= JOURNAL_HEADER).then_some((u64_at(header, 20), floor))
}
fn journal_header(end: u64, generation: u64) -> [u8; 64] {
    let mut header = [0; 64];
    header[..8].copy_from_slice(JOURNAL_MAGIC);
    header[8..10].copy_from_slice(&2u16.to_le_bytes());
    header[10..12].copy_from_slice(&64u16.to_le_bytes());
    header[12..20].copy_from_slice(&end.to_le_bytes());
    header[20..28].copy_from_slice(&generation.to_le_bytes());
    finish_crc(&mut header);
    header
}
enum ApplyMode<'a> {
    Durable(Option<(&'a str, u64)>),
    Disposable,
}
fn apply(
    db: &Database,
    objects: &Keyspace,
    segments: &Keyspace,
    tombstones: &Keyspace,
    retired: &Keyspace,
    changes: &[Mutation],
    mode: ApplyMode<'_>,
) -> Result<()> {
    let recovery = if matches!(mode, ApplyMode::Durable(_)) {
        Some(db.keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)?)
    } else {
        None
    };
    let mut count = match recovery
        .as_ref()
        .map(|space| space.get(b"objects"))
        .transpose()?
        .flatten()
    {
        Some(value) => {
            ensure!(value.len() == 8, "invalid recovery object count");
            u64_at(&value, 0)
        }
        None if recovery.is_some() => objects.len()? as u64,
        None => 0,
    };
    let mut presence = HashMap::new();
    let mut batch = db.batch();
    for change in changes {
        match change {
            Mutation::Put { hash, location } => {
                if recovery.is_some() {
                    let present = *presence.entry(*hash).or_insert(objects.contains_key(hash)?);
                    if !present {
                        count = count.checked_add(1).context("object count overflow")?;
                    }
                    presence.insert(*hash, true);
                }
                decode_loc(location)?;
                batch.insert(objects, hash, location);
                batch.remove(tombstones, hash);
            }
            Mutation::Delete { hash, timestamp } => {
                if recovery.is_some() {
                    let present = *presence.entry(*hash).or_insert(objects.contains_key(hash)?);
                    if present {
                        count = count.checked_sub(1).context("object count underflow")?;
                    }
                    presence.insert(*hash, false);
                }
                batch.remove(objects, hash);
                batch.insert(tombstones, hash, timestamp.to_le_bytes());
            }
            Mutation::Segment { id, metadata } => {
                decode_meta(Uuid::from_bytes(*id), metadata)?;
                batch.insert(segments, id, metadata);
            }
            Mutation::Retire { id } => {
                batch.remove(segments, id);
                batch.insert(retired, id, [1]);
            }
            Mutation::Cleaned { id } => batch.remove(retired, id),
        }
    }
    if let Some(recovery) = &recovery {
        batch.insert(recovery, b"objects", count.to_le_bytes());
    }
    if let ApplyMode::Durable(Some((journal, end))) = mode {
        batch.insert(
            recovery.as_ref().context("missing recovery keyspace")?,
            b"cursor",
            serde_json::to_vec(&ReplayCursor {
                journal: journal.into(),
                end,
            })?,
        );
    }
    batch.commit()?;
    Ok(())
}
impl Store {
    pub(super) fn startup_object_count(&self) -> Result<u64> {
        let recovery = self
            .db
            .keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)?;
        if let Some(value) = recovery.get(b"objects")? {
            ensure!(value.len() == 8, "invalid recovery object count");
            return Ok(u64_at(&value, 0));
        }
        // Legacy v1 indexes need one metadata-only count migration.
        let count = self.objects.len()? as u64;
        recovery.insert(b"objects", count.to_le_bytes())?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(count)
    }
    pub(super) fn publish(&self, changes: &[Mutation]) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut journal = self.journal.as_ref().map(|journal| journal.lock());
        if let Some(journal) = &mut journal {
            journal.append(changes)?;
        }
        let cursor = journal
            .as_ref()
            .map(|journal| {
                Ok::<_, anyhow::Error>((journal.name.as_str(), journal.file.metadata()?.len()))
            })
            .transpose()?;
        apply(
            &self.db,
            &self.objects,
            &self.segments,
            &self.tombstones,
            &self.retired,
            changes,
            ApplyMode::Durable(cursor),
        )?;
        failpoint("after_index_commit")?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }
    pub fn deletion_enabled(&self) -> bool {
        self.journal.is_some()
    }
    pub fn delete(&self, hash: &[u8; 32]) -> Result<DeleteResult> {
        ensure!(
            self.deletion_enabled(),
            "v1 stores require explicit upgrade before deletion"
        );
        let mut writer = self.writer.lock();
        ensure!(
            !writer.faulted,
            "write coordinator faulted; restart and recover before writing"
        );
        let _visible = self.visibility.write();
        let result = (|| {
            let location = self
                .objects
                .get(hash)?
                .map(|v| decode_loc(&v))
                .transpose()?;
            let mut changes = vec![Mutation::Delete {
                hash: *hash,
                timestamp: now(),
            }];
            if let Some(loc) = location {
                let id = Uuid::from_bytes(loc.segment);
                let value = self
                    .segments
                    .get(id.as_bytes())?
                    .context("missing delete segment")?;
                let mut meta = decode_meta(id, &value)?;
                meta.live = meta
                    .live
                    .checked_sub(loc.length)
                    .context("invalid live byte count")?;
                meta.garbage_since = now();
                changes.push(Mutation::segment(&meta));
                self.publish(&changes)?;
                if let Some(active) = &mut writer.active
                    && active.meta.id == id
                {
                    active.meta = meta;
                }
                self.object_count.fetch_sub(1, Ordering::Relaxed);
            } else {
                self.publish(&changes)?;
            }
            failpoint("delete_before_ack")?;
            Ok(DeleteResult {
                hash: *hash,
                deleted: location.is_some(),
            })
        })();
        if result.is_err() {
            writer.faulted = true;
        }
        result
    }
    pub fn upgrade(config: &Config) -> Result<Value> {
        ensure!(config.storage.root.is_dir(), "store does not exist");
        let store = Self::open(config)?;
        if store.deletion_enabled() {
            return Ok(json!({"status":"already_v2","version":2}));
        }
        let mut journal = Journal::create(&store.root)?;
        checkpoint(
            &mut journal,
            &store.objects,
            &store.segments,
            &store.tombstones,
            &store.retired,
            config.writes.batch_max_records,
        )?;
        failpoint("upgrade_before_activation")?;
        let generation = selected_generation(&store.root)?.context("missing generation")?;
        activate_version(&store.root, &generation, Some(&journal.name))?;
        Ok(json!({"status":"upgraded","version":2}))
    }
}
fn checkpoint(
    journal: &mut Journal,
    objects: &Keyspace,
    segments: &Keyspace,
    tombstones: &Keyspace,
    retired: &Keyspace,
    limit: usize,
) -> Result<()> {
    let mut changes = vec![];
    let limit = limit.clamp(1, 4096);
    for item in segments.iter() {
        let (key, value) = item.into_inner()?;
        changes.push(Mutation::Segment {
            id: key.as_ref().try_into()?,
            metadata: value.to_vec(),
        });
        if changes.len() >= limit {
            journal.append(&changes)?;
            changes.clear();
        }
    }
    for item in objects.iter() {
        let (key, value) = item.into_inner()?;
        changes.push(Mutation::Put {
            hash: key.as_ref().try_into()?,
            location: value.to_vec(),
        });
        if changes.len() >= limit {
            journal.append(&changes)?;
            changes.clear();
        }
    }
    for item in tombstones.iter() {
        let (key, value) = item.into_inner()?;
        ensure!(value.len() == 8, "invalid tombstone timestamp");
        changes.push(Mutation::Delete {
            hash: key.as_ref().try_into()?,
            timestamp: u64_at(&value, 0),
        });
        if changes.len() >= limit {
            journal.append(&changes)?;
            changes.clear();
        }
    }
    for item in retired.iter() {
        let (key, _) = item.into_inner()?;
        changes.push(Mutation::Retire {
            id: key.as_ref().try_into()?,
        });
        if changes.len() >= limit {
            journal.append(&changes)?;
            changes.clear();
        }
    }
    if !changes.is_empty() {
        journal.append(&changes)?;
    }
    journal.file.sync_all()?;
    Ok(())
}
pub(super) fn rebuild_v2(config: &Config, activation: Activation) -> Result<Value> {
    let root = &config.storage.root;
    let journal = Journal::open(
        root,
        activation
            .journal
            .as_deref()
            .context("missing v2 journal")?,
    )?;
    let generation = new_generation(root)?;
    let (db, objects, segments) = open_db(&generation, config)?;
    let tombstones = db.keyspace("tombstones", KeyspaceCreateOptions::default)?;
    let retired = db.keyspace("retired", KeyspaceCreateOptions::default)?;
    journal.replay(&db, &objects, &segments, &tombstones, &retired)?;
    let count = objects.len()?;
    // Fully validate referenced bytes, including active committed prefixes.
    for item in segments.iter() {
        let (key, value) = item.into_inner()?;
        let meta = decode_meta(Uuid::from_slice(&key)?, &value)?;
        let mut file = File::open(segment_path(root, &meta))?;
        let (count, end) =
            scan_prefix_visit(&mut file, meta.id, meta.end, false, &mut |_, _| Ok(()))?;
        ensure!(
            count == meta.count && end == meta.end,
            "journal references invalid segment"
        );
        if meta.state == 1 {
            ensure!(
                scan_file(&segment_path(root, &meta), root, false, true)?.is_some(),
                "invalid sealed segment"
            );
        }
    }
    drop(retired);
    drop(tombstones);
    drop(objects);
    drop(segments);
    drop(db);
    sync_dir(&generation)?;
    activate_version(root, &generation, Some(&journal.name))?;
    Ok(
        json!({"status":"rebuilt","version":2,"objects":count,"generation":generation.file_name().unwrap().to_string_lossy()}),
    )
}

/// A detached immutable output file; it has no namespace visibility until its
/// segment bytes are synced and the corresponding journal transaction commits.
struct Output {
    file: File,
    meta: Meta,
}
impl Output {
    fn create(root: &Path) -> Result<Self> {
        let id = Uuid::new_v4();
        let meta = Meta {
            id,
            state: 0,
            path: relative(id),
            physical: 64,
            end: 64,
            count: 0,
            live: 0,
            created: now(),
            sealed: 0,
            digest: None,
            garbage_since: 0,
            compacted: now(),
        };
        let path = segment_path(root, &meta);
        let parent = path.parent().unwrap();
        durable_mkdir(parent)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        file.write_all(&header(id, meta.created))?;
        file.sync_all()?;
        sync_dir(parent)?;
        Ok(Self { file, meta })
    }
    fn copy(
        &mut self,
        source: &File,
        hash: [u8; 32],
        location: Location,
        throttle: &mut Throttle,
    ) -> Result<Location> {
        let mut record = [0; 64];
        read_exact_at(source, &mut record, location.offset - 64)?;
        let (length, expected) = parse_record(&record)?;
        ensure!(
            length == location.length && expected == hash,
            "compaction source record/index mismatch"
        );
        self.file.seek(SeekFrom::Start(self.meta.end))?;
        self.file.write_all(&record)?;
        throttle.copied(RECORD_HEADER);
        let result = Location {
            segment: *self.meta.id.as_bytes(),
            offset: self.meta.end + 64,
            length,
        };
        let mut buffer = [0; 64 * 1024];
        let mut position = 0;
        let mut digest = blake3::Hasher::new();
        while position < length {
            let size = (length - position).min(buffer.len() as u64) as usize;
            read_exact_at(source, &mut buffer[..size], location.offset + position)?;
            self.file.write_all(&buffer[..size])?;
            digest.update(&buffer[..size]);
            position += size as u64;
            throttle.copied(size as u64);
        }
        ensure!(
            digest.finalize().as_bytes() == &hash,
            "compaction source payload BLAKE3 mismatch"
        );
        self.meta.end = result.offset + length;
        self.meta.count += 1;
        self.meta.live += length;
        self.meta.physical = self.meta.end;
        Ok(result)
    }
    fn finish(mut self) -> Result<Meta> {
        self.file.sync_all()?;
        let digest = digest_prefix(&mut self.file, self.meta.end)?;
        self.meta.sealed = now();
        self.meta.compacted = self.meta.sealed;
        self.file.seek(SeekFrom::Start(self.meta.end))?;
        self.file.write_all(&footer(&self.meta, &digest))?;
        self.file.sync_all()?;
        self.meta.state = 1;
        self.meta.physical = self.meta.end + 96;
        self.meta.digest = Some(digest);
        drop(self.file);
        Ok(self.meta)
    }
}
struct Throttle {
    start: Instant,
    bytes: u64,
    bytes_per_second: u64,
}
impl Throttle {
    fn new(bytes_per_second: u64) -> Self {
        Self {
            start: Instant::now(),
            bytes: 0,
            bytes_per_second,
        }
    }
    fn copied(&mut self, bytes: u64) {
        self.bytes += bytes;
        if self.bytes_per_second > 0 {
            // Charge each bounded write separately, so metadata scans and
            // filesystem stalls cannot accumulate a later burst allowance.
            std::thread::sleep(Duration::from_secs_f64(
                bytes as f64 / self.bytes_per_second as f64,
            ));
        }
        self.start = Instant::now();
    }
}
fn visit_locations(
    file: &File,
    meta: &Meta,
    mut visit: impl FnMut([u8; 32], Location) -> Result<()>,
) -> Result<()> {
    let mut position = 64;
    let mut count = 0;
    while position < meta.end {
        let mut header = [0; 64];
        ensure!(
            meta.end - position >= 64,
            "truncated compacted record header"
        );
        read_exact_at(file, &mut header, position)?;
        let (length, hash) = parse_record(&header)?;
        let location = Location {
            segment: *meta.id.as_bytes(),
            offset: position + 64,
            length,
        };
        position = location
            .offset
            .checked_add(length)
            .context("compaction record overflow")?;
        ensure!(position <= meta.end, "compaction record exceeds segment");
        visit(hash, location)?;
        count += 1;
    }
    ensure!(count == meta.count, "compaction segment count mismatch");
    Ok(())
}
impl Store {
    pub(crate) fn write_faulted(&self) -> bool {
        self.writer.lock().faulted
    }
    /// One pass is limited by max_copy_mib even when explicitly forced. Sealed
    /// source files stay immutable while ordinary writers and readers proceed.
    pub fn collect_garbage(&self, force: bool) -> Result<Value> {
        ensure!(
            self.deletion_enabled(),
            "v1 stores require explicit upgrade before collection"
        );
        let Some(_maintenance) = self.maintenance.try_lock() else {
            return Ok(json!({"status":"busy"}));
        };
        ensure!(!self.write_faulted(), "write coordinator faulted");
        self.cleanup_retired()?;
        let budget = self.config.gc.max_copy_mib * 1024 * 1024;
        let reserve = self.config.gc.min_free_mib * 1024 * 1024;
        let mut copied = 0u64;
        let mut reclaimed = 0u64;
        let mut retired = 0u64;
        let mut relocated = 0u64;
        let mut throttler = Throttle::new(self.config.gc.max_bytes_per_second_mib * 1024 * 1024);
        let pressure = self.update_space_pressure()?;
        // Retain at most 128 segment candidates, never a namespace map. Rank
        // fully dead files first, then actual reclaim ratio (including headers).
        let mut candidates = Vec::new();
        for item in self.segments.iter() {
            let (key, value) = item.into_inner()?;
            let source = decode_meta(Uuid::from_slice(&key)?, &value)?;
            if source.state != 1 {
                continue;
            }
            if !force
                && (now().saturating_sub(source.garbage_since.max(source.sealed))
                    < self.config.gc.delete_grace_seconds
                    || (source.compacted != 0
                        && now().saturating_sub(source.compacted)
                            < self.config.gc.cooldown_seconds))
            {
                continue;
            }
            let file = {
                let _visible = self.visibility.read();
                if !self.segments.contains_key(source.id.as_bytes())? {
                    continue;
                }
                self.acquire_file(source.id, &segment_path(&self.root, &source))?
            };
            let mut live_count = 0u64;
            let mut live_bytes = 0u64;
            visit_locations(&file, &source, |hash, location| {
                if self
                    .objects
                    .get(hash)?
                    .map(|v| decode_loc(&v))
                    .transpose()?
                    == Some(location)
                {
                    live_count += 1;
                    live_bytes += location.length;
                }
                Ok(())
            })?;
            drop(file);
            let copy_size = live_bytes
                .checked_add(
                    live_count
                        .checked_mul(RECORD_HEADER)
                        .context("GC extent overflow")?,
                )
                .context("GC copy size overflow")?;
            let garbage = source.end.saturating_sub(SEGMENT_HEADER + copy_size);
            let ratio = garbage as f64 / ((source.end - SEGMENT_HEADER).max(1) as f64);
            if live_count > 0
                && (garbage == 0
                    || (!force && !pressure && ratio < self.config.gc.min_garbage_ratio))
            {
                continue;
            }
            if copy_size > budget {
                continue;
            }
            candidates.push((source, live_count, copy_size, ratio));
            candidates.sort_by(|a, b| {
                (b.1 == 0)
                    .cmp(&(a.1 == 0))
                    .then_with(|| b.3.total_cmp(&a.3))
                    .then_with(|| b.0.physical.cmp(&a.0.physical))
            });
            candidates.truncate(128);
        }
        for (source, live_count, copy_size, _) in candidates {
            if copied.saturating_add(copy_size) > budget {
                continue;
            }
            if !force && pressure && !self.update_space_pressure()? {
                break;
            }
            let metadata = live_count
                .checked_mul(98 + 4096 + 1024)
                .context("GC metadata headroom overflow")?;
            let required = copy_size
                .checked_add(metadata)
                .and_then(|n| n.checked_add(SEGMENT_HEADER + SEGMENT_FOOTER + 64 * 1024))
                .and_then(|n| n.checked_add(reserve))
                .context("GC headroom overflow")?;
            if live_count > 0 && fs2::available_space(&self.root)? < required {
                continue;
            }
            let file = {
                let _visible = self.visibility.read();
                self.acquire_file(source.id, &segment_path(&self.root, &source))?
            };
            let mut output = if live_count > 0 {
                Some(Output::create(&self.root)?)
            } else {
                None
            };
            let spool_path = self.root.join(format!(".gc-{}.tmp", Uuid::new_v4()));
            let mut spool = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&spool_path)?;
            visit_locations(&file, &source, |hash, old| {
                if self
                    .objects
                    .get(hash)?
                    .map(|v| decode_loc(&v))
                    .transpose()?
                    != Some(old)
                {
                    return Ok(());
                }
                let destination = output
                    .as_mut()
                    .context("live object appeared in empty sealed source")?;
                let new = destination.copy(&file, hash, old, &mut throttler)?;
                spool.write_all(&hash)?;
                spool.write_all(&encode_loc(old))?;
                spool.write_all(&encode_loc(new))?;
                copied += old.length + RECORD_HEADER;
                Ok(())
            })?;
            drop(file);
            self.files_available.notify_all();
            let mut destination = output.map(Output::finish).transpose()?;
            if destination.as_ref().is_some_and(|m| m.count == 0) {
                fs::remove_file(segment_path(&self.root, destination.as_ref().unwrap()))?;
                destination = None;
            }
            if let Some(meta) = &mut destination {
                meta.live = 0;
            }
            failpoint("gc_after_copy")?;
            spool.seek(SeekFrom::Start(0))?;
            let records = spool.metadata()?.len() / 98;
            let mut done = 0;
            while done < records {
                let mut entries = Vec::new();
                for _ in
                    0..(records - done).min(self.config.writes.batch_max_records.min(4096) as u64)
                {
                    let mut entry = [0; 98];
                    spool.read_exact(&mut entry)?;
                    entries.push((
                        <[u8; 32]>::try_from(&entry[..32])?,
                        decode_loc(&entry[32..65])?,
                        decode_loc(&entry[65..])?,
                    ));
                }
                let mut writer = self.writer.lock();
                ensure!(!writer.faulted, "write coordinator faulted");
                let _visible = self.visibility.write();
                let publication = (|| {
                    let value = self
                        .segments
                        .get(source.id.as_bytes())?
                        .context("GC source disappeared")?;
                    let mut current_source = decode_meta(source.id, &value)?;
                    if let Some(meta) = &mut destination
                        && let Some(value) = self.segments.get(meta.id.as_bytes())?
                    {
                        *meta = decode_meta(meta.id, &value)?;
                    }
                    let mut changes = vec![];
                    for (hash, old, new) in &entries {
                        if self
                            .objects
                            .get(hash)?
                            .map(|v| decode_loc(&v))
                            .transpose()?
                            == Some(*old)
                        {
                            changes.push(Mutation::Put {
                                hash: *hash,
                                location: encode_loc(*new),
                            });
                            current_source.live = current_source
                                .live
                                .checked_sub(old.length)
                                .context("GC source live underflow")?;
                            destination.as_mut().unwrap().live += new.length;
                            relocated += 1;
                        }
                    }
                    if let Some(meta) = &destination {
                        changes.push(Mutation::segment(meta));
                    }
                    changes.push(Mutation::segment(&current_source));
                    self.publish(&changes)
                })();
                if publication.is_err() {
                    writer.faulted = true;
                }
                publication?;
                done += entries.len() as u64;
            }
            // All source records were considered; records deleted during copy
            // fail the compare-and-swap and cannot be resurrected by relocation.
            {
                let mut writer = self.writer.lock();
                ensure!(!writer.faulted, "write coordinator faulted");
                let _visible = self.visibility.write();
                let current = self
                    .segments
                    .get(source.id.as_bytes())?
                    .context("GC source metadata missing")?;
                let current = decode_meta(source.id, &current)?;
                if !force
                    && now().saturating_sub(current.garbage_since.max(current.sealed))
                        < self.config.gc.delete_grace_seconds
                {
                    // A concurrent deletion starts a fresh physical grace
                    // period. Relocations are already durable; retain the now
                    // dead source until that grace period expires.
                    drop(spool);
                    fs::remove_file(&spool_path)?;
                    continue;
                }
                // A sealed source never gains locations: explicit PUT writes
                // a new active location, and this pass owns the maintenance
                // mutex. Every old location was either absent during copying,
                // or compared under the writer lock during publication.
                let result = self.publish(&[Mutation::Retire {
                    id: *source.id.as_bytes(),
                }]);
                if result.is_err() {
                    writer.faulted = true;
                }
                result?;
                failpoint("gc_after_retire")?;
            }
            self.unlink_retired_id(source.id)?;
            self.mark_cleaned(source.id)?;
            drop(spool);
            fs::remove_file(&spool_path)?;
            let new_physical = destination.as_ref().map_or(0, |m| m.physical);
            reclaimed += source.physical.saturating_sub(new_physical);
            retired += 1;
            if copied >= budget {
                break;
            }
        }
        Ok(
            json!({"status":"collected","copied_bytes":copied,"reclaimed_bytes":reclaimed,"retired_segments":retired,"relocated_objects":relocated,"copy_budget_bytes":budget,"space_pressure":self.update_space_pressure()?,"candidate_limit":128}),
        )
    }
    fn update_space_pressure(&self) -> Result<bool> {
        let available = fs2::available_space(&self.root)?;
        let total = fs2::total_space(&self.root)?;
        let percent = u128::from(available) * 100 / u128::from(total.max(1));
        let reserve = self.config.gc.min_free_mib * 1024 * 1024;
        let previous = self.gc_pressure.load(Ordering::Relaxed);
        let pressure = if percent <= u128::from(self.config.gc.low_space_percent)
            || available < reserve
        {
            true
        } else if percent >= u128::from(self.config.gc.target_free_percent) && available >= reserve
        {
            false
        } else {
            previous
        };
        self.gc_pressure.store(pressure, Ordering::Relaxed);
        Ok(pressure)
    }
    fn mark_cleaned(&self, id: Uuid) -> Result<()> {
        let mut writer = self.writer.lock();
        ensure!(!writer.faulted, "write coordinator faulted");
        let _visible = self.visibility.write();
        let result = self.publish(&[Mutation::Cleaned { id: *id.as_bytes() }]);
        if result.is_err() {
            writer.faulted = true;
        }
        result
    }
    pub(super) fn cleanup_retired(&self) -> Result<()> {
        for item in self.retired.iter() {
            let (key, _) = item.into_inner()?;
            let id = Uuid::from_slice(&key)?;
            self.unlink_retired_id(id)?;
            self.mark_cleaned(id)?;
        }
        Ok(())
    }
    fn unlink_retired_id(&self, id: Uuid) -> Result<()> {
        let mut cache = self.files.lock();
        loop {
            let Some(position) = cache.entries.iter().position(|(key, _)| *key == id) else {
                break;
            };
            if Arc::strong_count(&cache.entries[position].1) == 1 {
                cache.entries.remove(position);
                break;
            }
            self.files_available
                .wait_for(&mut cache, Duration::from_millis(50));
        }
        drop(cache);
        let path = self.root.join("segments").join(relative(id));
        match fs::remove_file(&path) {
            Ok(()) => sync_dir(path.parent().unwrap())?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

impl Store {
    /// Offline full compaction. An activation record selects both the index and
    /// its checkpoint journal in one durable rename; old files are removed only
    /// after that publication and after every old handle is closed.
    pub fn vacuum(config: &Config, dry_run: bool) -> Result<Value> {
        ensure!(config.storage.root.is_dir(), "store does not exist");
        if dry_run {
            return vacuum_dry_run(config);
        }
        let store = Self::open_mode(config, true)?;
        ensure!(
            store.deletion_enabled(),
            "v1 stores require explicit upgrade before vacuum"
        );
        let root = store.root.clone();
        let old_files = segment_files(&root)?;
        let mut payload_bytes = 0u64;
        let mut objects_count = 0u64;
        for item in store.objects.iter() {
            let (_, value) = item.into_inner()?;
            payload_bytes = payload_bytes
                .checked_add(decode_loc(&value)?.length)
                .context("vacuum size overflow")?;
            objects_count += 1;
        }
        let records_bytes = payload_bytes
            .checked_add(objects_count * RECORD_HEADER)
            .context("vacuum size overflow")?;
        let old_physical: u64 = old_files
            .iter()
            .map(|p| fs::metadata(p).map(|m| m.len()))
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .sum();
        let tombstones_count =
            u64::try_from(store.tombstones.len()?).context("vacuum tombstone count overflow")?;
        let old_segments_count =
            u64::try_from(old_files.len()).context("vacuum segment count overflow")?;
        let metadata = objects_count
            .checked_mul(SEGMENT_HEADER + SEGMENT_FOOTER + 4096 + 1024)
            .and_then(|n| {
                tombstones_count
                    .checked_mul(4096 + 1024)
                    .and_then(|t| n.checked_add(t))
            })
            .and_then(|n| {
                old_segments_count
                    .checked_mul(4096 + 1024)
                    .and_then(|t| n.checked_add(t))
            })
            .context("vacuum metadata headroom overflow")?;
        let required = records_bytes
            .checked_add(metadata)
            .and_then(|n| n.checked_add(config.gc.min_free_mib * 1024 * 1024))
            .and_then(|n| n.checked_add(64 * 1024))
            .context("vacuum headroom overflow")?;
        ensure!(
            fs2::available_space(&root)? >= required,
            "insufficient free space for vacuum: requires at least {required} bytes including configured reserve"
        );
        let generation = new_generation(&root)?;
        let (db, objects, segments) = open_db(&generation, config)?;
        let tombstones = db.keyspace("tombstones", KeyspaceCreateOptions::default)?;
        let retired = db.keyspace("retired", KeyspaceCreateOptions::default)?;
        let mut output: Option<Output> = None;
        let mut batch = db.batch();
        let mut count = 0;
        let mut new_physical = 0;
        let mut throttle = Throttle::new(0); // Offline force-full is not the online rate-limited pass.
        for item in store.objects.iter() {
            let (key, value) = item.into_inner()?;
            let hash: [u8; 32] = key.as_ref().try_into()?;
            let old = decode_loc(&value)?;
            if output.as_ref().is_some_and(|o| {
                o.meta.end + RECORD_HEADER + old.length + SEGMENT_FOOTER
                    > config.segments.max_size_bytes
            }) {
                let meta = output.take().unwrap().finish()?;
                new_physical += meta.physical;
                batch.insert(&segments, meta.id.as_bytes(), encode_meta(&meta));
            }
            if output.is_none() {
                output = Some(Output::create(&root)?);
            }
            let old_meta = decode_meta(
                Uuid::from_bytes(old.segment),
                &store
                    .segments
                    .get(old.segment)?
                    .context("vacuum source metadata missing")?,
            )?;
            let source = File::open(segment_path(&root, &old_meta))?;
            let location = output
                .as_mut()
                .unwrap()
                .copy(&source, hash, old, &mut throttle)?;
            batch.insert(&objects, hash, encode_loc(location));
            count += 1;
            if count >= config.writes.batch_max_records {
                batch.commit()?;
                batch = db.batch();
                count = 0;
            }
        }
        if let Some(output) = output {
            let meta = output.finish()?;
            new_physical += meta.physical;
            batch.insert(&segments, meta.id.as_bytes(), encode_meta(&meta));
        }
        batch.commit()?;
        let mut batch = db.batch();
        let mut count = 0;
        for item in store.tombstones.iter() {
            let (key, value) = item.into_inner()?;
            batch.insert(&tombstones, key, value);
            count += 1;
            if count >= config.writes.batch_max_records {
                batch.commit()?;
                batch = db.batch();
                count = 0;
            }
        }
        batch.commit()?;
        let mut batch = db.batch();
        let mut pending = 0;
        for path in &old_files {
            let mut header = [0; 64];
            File::open(path)?.read_exact(&mut header)?;
            let (id, _) = parse_header(&header)?;
            ensure!(
                *path == root.join("segments").join(relative(id)),
                "vacuum encountered noncanonical segment path"
            );
            batch.insert(&retired, id.as_bytes(), [1]);
            pending += 1;
            if pending >= config.writes.batch_max_records {
                batch.commit()?;
                batch = db.batch();
                pending = 0;
            }
        }
        batch.commit()?;
        db.persist(PersistMode::SyncAll)?;
        let mut journal = Journal::create(&root)?;
        checkpoint(
            &mut journal,
            &objects,
            &segments,
            &tombstones,
            &retired,
            config.writes.batch_max_records,
        )?;
        let journal_name = journal.name.clone();
        drop(journal);
        drop(retired);
        drop(tombstones);
        drop(objects);
        drop(segments);
        drop(db);
        sync_dir(&generation)?;
        failpoint("vacuum_before_activation")?;
        activate_version(&root, &generation, Some(&journal_name))?;
        failpoint("vacuum_after_activation")?;
        // Keep process ownership while releasing Fjall, writer and reader file
        // handles. This also permits the cleanup sequence on Windows.
        let _lock = store.close_keep_lock();
        for path in old_files {
            if path.exists() {
                fs::remove_file(&path)?;
                sync_dir(path.parent().unwrap())?;
            }
        }
        for entry in fs::read_dir(root.join("index"))? {
            let path = entry?.path();
            if path.is_dir()
                && path != generation
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("generation-"))
            {
                fs::remove_dir_all(path)?;
            }
        }
        for entry in fs::read_dir(&root)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.starts_with("journal-") && name.ends_with(".log") && name != journal_name {
                fs::remove_file(path)?;
            }
        }
        sync_dir(&root.join("index"))?;
        sync_dir(&root)?;
        Ok(
            json!({"status":"vacuumed","objects":objects_count,"live_payload_bytes":payload_bytes,"physical_bytes_before":old_physical,"physical_bytes_after":new_physical,"reclaimed_bytes":old_physical.saturating_sub(new_physical),"version":2}),
        )
    }
    fn close_keep_lock(self) -> File {
        let Self {
            db,
            objects,
            segments,
            root,
            config,
            writer,
            visibility,
            files,
            files_available,
            object_count,
            gc_pressure,
            journal,
            tombstones,
            retired,
            maintenance,
            verification_control,
            _lock,
        } = self;
        drop((
            objects,
            segments,
            tombstones,
            retired,
            db,
            writer,
            files,
            journal,
            root,
            config,
            visibility,
            files_available,
            object_count,
            gc_pressure,
            maintenance,
            verification_control,
        ));
        _lock
    }
}

fn vacuum_dry_run(config: &Config) -> Result<Value> {
    config.validate()?;
    let root = &config.storage.root;
    let _lock = store_lock(root)?;
    let activation = selected_activation(root)?.context("store has no activation")?;
    ensure!(
        activation.version == 2,
        "v1 stores require explicit upgrade before vacuum"
    );
    let journal = Journal::read_only(
        root,
        activation
            .journal
            .as_deref()
            .context("missing v2 journal")?,
    )?;
    // Replay into disposable scratch storage outside the store: even Fjall's
    // recovery bookkeeping and an incomplete journal tail stay untouched.
    let scratch = std::env::temp_dir().join(format!("skerry-vacuum-dry-{}", Uuid::new_v4()));
    let result = (|| {
        let (db, objects, segments) = open_db(&scratch, config)?;
        let tombstones = db.keyspace("tombstones", KeyspaceCreateOptions::default)?;
        let retired = db.keyspace("retired", KeyspaceCreateOptions::default)?;
        journal.replay(&db, &objects, &segments, &tombstones, &retired)?;
        let mut count = 0u64;
        let mut live = 0u64;
        for item in objects.iter() {
            let (_, value) = item.into_inner()?;
            count += 1;
            live += decode_loc(&value)?.length;
        }
        let mut physical = 0u64;
        for path in segment_files(root)? {
            physical += fs::metadata(path)?.len();
        }
        Ok(
            json!({"status":"dry_run","objects":count,"live_payload_bytes":live,"record_bytes":live + count * RECORD_HEADER,"physical_bytes":physical,"available_bytes":fs2::available_space(root)?,"requires_exclusive_lock":true}),
        )
    })();
    let cleanup = fs::remove_dir_all(&scratch);
    match result {
        Ok(value) => {
            cleanup?;
            Ok(value)
        }
        Err(error) => Err(error),
    }
}

/// Disposable index reconstructed from the selected journal. Used whenever an
/// offline operation must inspect logical source state without touching it.
pub(super) struct ScratchNamespace {
    objects: Option<Keyspace>,
    segments: Option<Keyspace>,
    tombstones: Option<Keyspace>,
    retired: Option<Keyspace>,
    db: Option<Database>,
    path: PathBuf,
    replay_end: u64,
}
impl ScratchNamespace {
    fn build(config: &Config, root: &Path, activation: &Activation) -> Result<Self> {
        Self::build_checked(config, root, activation, None, None)
    }
    fn build_checked(
        config: &Config,
        root: &Path,
        activation: &Activation,
        check: Option<(&[Keyspace; 4], u64)>,
        progress: Option<&crate::progress::Progress>,
    ) -> Result<Self> {
        let path = std::env::temp_dir().join(format!("skerry-namespace-{}", Uuid::new_v4()));
        let (db, objects, segments) = open_db(&path, config)?;
        let tombstones = db.keyspace("tombstones", KeyspaceCreateOptions::default)?;
        let retired = db.keyspace("retired", KeyspaceCreateOptions::default)?;
        let mut scratch = Self {
            objects: Some(objects),
            segments: Some(segments),
            tombstones: Some(tombstones),
            retired: Some(retired),
            db: Some(db),
            path,
            replay_end: JOURNAL_HEADER,
        };
        let journal = Journal {
            name: activation
                .journal
                .as_deref()
                .context("missing v2 journal")?
                .into(),
            file: File::open(root.join(activation.journal.as_deref().unwrap()))?,
        };
        let compare = || -> Result<()> {
            if let Some(p) = progress {
                p.phase("comparing selected index");
            }
            if let Some((selected, _)) = check {
                let mut differences = Vec::new();
                for (name, expected, actual) in [
                    ("objects", scratch.objects.as_ref().unwrap(), &selected[0]),
                    ("segments", scratch.segments.as_ref().unwrap(), &selected[1]),
                    (
                        "tombstones",
                        scratch.tombstones.as_ref().unwrap(),
                        &selected[2],
                    ),
                    ("retired", scratch.retired.as_ref().unwrap(), &selected[3]),
                ] {
                    let mut left = expected.iter();
                    let mut right = actual.iter();
                    let mut a = left.next().map(|x| x.into_inner()).transpose()?;
                    let mut b = right.next().map(|x| x.into_inner()).transpose()?;
                    let (mut missing, mut unexpected, mut mismatched) = (0u64, 0u64, 0u64);
                    while a.is_some() || b.is_some() {
                        if let Some(p) = progress {
                            ensure!(!p.is_cancelled(), "verification cancelled");
                            p.add_index_entries(1);
                        }
                        match (&a, &b) {
                            (Some((ak, av)), Some((bk, bv))) => match ak.cmp(bk) {
                                std::cmp::Ordering::Less => {
                                    missing += 1;
                                    a = left.next().map(|x| x.into_inner()).transpose()?;
                                }
                                std::cmp::Ordering::Greater => {
                                    unexpected += 1;
                                    b = right.next().map(|x| x.into_inner()).transpose()?;
                                }
                                std::cmp::Ordering::Equal => {
                                    if av != bv {
                                        mismatched += 1;
                                    }
                                    a = left.next().map(|x| x.into_inner()).transpose()?;
                                    b = right.next().map(|x| x.into_inner()).transpose()?;
                                }
                            },
                            (Some(_), None) => {
                                missing += 1;
                                a = left.next().map(|x| x.into_inner()).transpose()?;
                            }
                            (None, Some(_)) => {
                                unexpected += 1;
                                b = right.next().map(|x| x.into_inner()).transpose()?;
                            }
                            (None, None) => break,
                        }
                    }
                    if missing + unexpected + mismatched != 0 {
                        differences.push(format!("selected index {name} differs: missing={missing} unexpected={unexpected} mismatched={mismatched}"));
                    }
                }
                ensure!(differences.is_empty(), "{}", differences.join("; "));
            }
            Ok(())
        };
        let mut compared = false;
        if check.is_some_and(|(_, cursor)| cursor == JOURNAL_HEADER) {
            compare()?;
            compared = true;
        }
        if let Some(p) = progress {
            p.phase("replaying journal");
        }
        let mut previous_end = JOURNAL_HEADER;
        let end = journal.visit_from(JOURNAL_HEADER, |changes, end| {
            if let Some(p) = progress {
                ensure!(!p.is_cancelled(), "verification cancelled");
                p.add_journal_bytes(end - previous_end);
                p.add_transactions(1);
            }
            previous_end = end;
            apply(
                scratch.db.as_ref().unwrap(),
                scratch.objects.as_ref().unwrap(),
                scratch.segments.as_ref().unwrap(),
                scratch.tombstones.as_ref().unwrap(),
                scratch.retired.as_ref().unwrap(),
                &changes,
                ApplyMode::Disposable,
            )?;
            if check.is_some_and(|(_, cursor)| cursor == end) {
                compare()?;
                compared = true;
            }
            Ok(())
        })?;
        ensure!(
            check.is_none() || compared,
            "index replay cursor is not a journal frame boundary"
        );
        scratch.replay_end = end;
        Ok(scratch)
    }
    pub fn source(config: &Config, root: &Path) -> Result<Option<Self>> {
        let Some(activation) = selected_activation(root)? else {
            return Ok(None);
        };
        if activation.version != 2 {
            return Ok(None);
        }
        Self::build(config, root, &activation).map(Some)
    }
    pub fn location(&self, hash: &[u8; 32]) -> Result<Option<Location>> {
        self.objects
            .as_ref()
            .unwrap()
            .get(hash)?
            .map(|v| decode_loc(&v))
            .transpose()
    }
    pub fn contains_segment(&self, id: Uuid) -> Result<bool> {
        Ok(self
            .segments
            .as_ref()
            .unwrap()
            .contains_key(id.as_bytes())?)
    }
}
impl Drop for ScratchNamespace {
    fn drop(&mut self) {
        drop(self.objects.take());
        drop(self.segments.take());
        drop(self.tombstones.take());
        drop(self.retired.take());
        drop(self.db.take());
        let _ = fs::remove_dir_all(&self.path);
    }
}
fn copy_index_dir(
    source: &Path,
    target: &Path,
    progress: Option<&crate::progress::Progress>,
) -> Result<()> {
    fs::create_dir_all(target)?;
    for entry in fs::read_dir(source)? {
        if let Some(p) = progress {
            ensure!(!p.is_cancelled(), "verification cancelled");
        }
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_index_dir(&entry.path(), &target.join(entry.file_name()), progress)?;
        } else if ty.is_file() {
            let mut source = File::open(entry.path())?;
            let mut destination = File::create(target.join(entry.file_name()))?;
            let mut buffer = vec![0u8; 1024 * 1024];
            loop {
                if let Some(p) = progress {
                    ensure!(!p.is_cancelled(), "verification cancelled");
                }
                let count = source.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                destination.write_all(&buffer[..count])?;
                if let Some(p) = progress {
                    p.add_index_bytes(count as u64);
                }
            }
        } else {
            bail!("unsupported entry in index generation");
        }
    }
    Ok(())
}

fn verify_orphan(
    store: &Store,
    path: &Path,
    progress: Option<&crate::progress::Progress>,
) -> Result<()> {
    let check = || -> Result<()> {
        if let Some(p) = progress {
            ensure!(!p.is_cancelled(), "verification cancelled");
        }
        Ok(())
    };
    let _permit = store
        .verification_control
        .acquire(store.config.verification.max_concurrency, check)?;
    let limit = |bytes| {
        store.verification_control.throttle(
            bytes,
            store.config.verification.max_bytes_per_second_mib,
            check,
        )
    };
    scan_file_progress(path, &store.root, true, true, progress, Some(&limit))?;
    Ok(())
}

pub(super) fn verify_v1(
    config: &Config,
    progress: Option<&crate::progress::Progress>,
) -> Result<Value> {
    config.validate()?;
    let root = &config.storage.root;
    let lock = store_lock(root)?;
    let generation = selected_generation(root)?
        .context("store has no selected index; initialize or rebuild first")?;
    let copy_path = std::env::temp_dir().join(format!("skerry-v1-index-copy-{}", Uuid::new_v4()));
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(copy_path.clone());
    if let Some(p) = progress {
        p.phase("copying selected index");
    }
    copy_index_dir(&generation, &copy_path, progress)?;
    let (db, objects, segments) = open_db(&copy_path, config)?;
    let tombstones = db.keyspace("tombstones", KeyspaceCreateOptions::default)?;
    let retired = db.keyspace("retired", KeyspaceCreateOptions::default)?;
    let store = Store {
        db,
        objects,
        segments,
        tombstones,
        retired,
        root: root.clone(),
        config: config.clone(),
        writer: Mutex::new(Writer::default()),
        visibility: RwLock::new(()),
        files: Mutex::new(FileCache {
            entries: VecDeque::new(),
            max: config.resources.max_open_segments,
        }),
        files_available: Condvar::new(),
        object_count: AtomicU64::new(0),
        gc_pressure: AtomicBool::new(false),
        verification_control: crate::verification::Control::default(),
        journal: None,
        maintenance: Mutex::new(()),
        _lock: lock,
    };
    store.recover(false, progress)?;
    let status = store.status()?;
    let mut orphaned = vec![];
    for path in segment_files(root)? {
        if let Some(p) = progress {
            ensure!(!p.is_cancelled(), "verification cancelled");
        }
        let mut header = [0; 64];
        File::open(&path)?.read_exact(&mut header)?;
        let (id, _) = parse_header(&header)?;
        if !store.segments.contains_key(id.as_bytes())? {
            if let Some(p) = progress {
                p.phase("scanning orphan segments");
                p.clear_totals();
            }
            verify_orphan(&store, &path, progress)?;
            orphaned.push(path.display().to_string());
        }
    }
    Ok(json!({"status":"verified", "storage":status, "unreferenced_segments":orphaned}))
}

pub(super) fn verify_v2(
    config: &Config,
    progress: Option<&crate::progress::Progress>,
) -> Result<Value> {
    config.validate()?;
    let lock = store_lock(&config.storage.root)?;
    let activation = selected_activation(&config.storage.root)?.context("missing v2 activation")?;
    // Opening Fjall may write recovery files, so inspect a private copy.
    let copy_path = std::env::temp_dir().join(format!("skerry-index-copy-{}", Uuid::new_v4()));
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(copy_path.clone());
    if let Some(p) = progress {
        p.phase("copying selected index");
    }
    copy_index_dir(
        &selected_generation(&config.storage.root)?.context("missing selected index")?,
        &copy_path,
        progress,
    )?;
    let (selected_db, selected_objects, selected_segments) = open_db(&copy_path, config)?;
    let selected_tombstones = selected_db.keyspace("tombstones", KeyspaceCreateOptions::default)?;
    let selected_retired = selected_db.keyspace("retired", KeyspaceCreateOptions::default)?;
    let recovery = selected_db.keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)?;
    let cursor = recovery
        .get(b"cursor")?
        .map(|v| serde_json::from_slice::<ReplayCursor>(&v))
        .transpose()?;
    let journal = Journal {
        name: activation
            .journal
            .as_deref()
            .context("missing journal")?
            .into(),
        file: File::open(
            config
                .storage
                .root
                .join(activation.journal.as_deref().unwrap()),
        )?,
    };
    let witness = journal.witness()?;
    let valid_cursor = cursor
        .as_ref()
        .filter(|c| c.journal == journal.name && c.end >= JOURNAL_HEADER && c.end <= witness.floor);
    let selected = [
        selected_objects,
        selected_segments,
        selected_tombstones,
        selected_retired,
    ];
    let mut scratch = ScratchNamespace::build_checked(
        config,
        &config.storage.root,
        &activation,
        valid_cursor.map(|c| (&selected, c.end)),
        progress,
    )?;
    let recovery_required =
        valid_cursor.is_none_or(|c| c.end < scratch.replay_end) || witness.degraded;
    drop(recovery);
    drop(selected);
    drop(selected_db);
    let store = Store {
        db: scratch.db.take().unwrap(),
        objects: scratch.objects.take().unwrap(),
        segments: scratch.segments.take().unwrap(),
        tombstones: scratch.tombstones.take().unwrap(),
        retired: scratch.retired.take().unwrap(),
        root: config.storage.root.clone(),
        config: config.clone(),
        writer: Mutex::new(Writer::default()),
        visibility: RwLock::new(()),
        files: Mutex::new(FileCache {
            entries: VecDeque::new(),
            max: config.resources.max_open_segments,
        }),
        files_available: Condvar::new(),
        object_count: AtomicU64::new(0),
        verification_control: crate::verification::Control::default(),
        gc_pressure: AtomicBool::new(false),
        journal: None,
        maintenance: Mutex::new(()),
        _lock: lock,
    };
    store.recover(false, progress)?;
    let status = store.status()?;
    let mut orphaned = vec![];
    for path in segment_files(&config.storage.root)? {
        if let Some(p) = progress {
            ensure!(!p.is_cancelled(), "verification cancelled");
        }
        let mut header = [0; 64];
        File::open(&path)?.read_exact(&mut header)?;
        let (id, _) = parse_header(&header)?;
        if !store.segments.contains_key(id.as_bytes())?
            && !store.retired.contains_key(id.as_bytes())?
        {
            if let Some(p) = progress {
                p.phase("scanning orphan segments");
                p.clear_totals();
            }
            verify_orphan(&store, &path, progress)?;
            orphaned.push(path.display().to_string());
        }
    }
    let pending = store.retired.len()?;
    drop(store);
    drop(scratch);
    Ok(
        json!({"status":"verified","version":2,"storage":status,"unreferenced_segments":orphaned,"retired_segments_pending_cleanup":pending,"recovery_required":recovery_required}),
    )
}

#[cfg(test)]
mod witness_tests {
    use super::*;

    #[test]
    fn startup_replays_only_unapplied_mutations_and_keeps_exact_count() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        let store = Store::open(&config).unwrap();
        let hash = store
            .put_many(&[Bytes::from_static(b"pending delete")])
            .unwrap()[0]
            .hash;
        let loc = decode_loc(&store.objects.get(hash).unwrap().unwrap()).unwrap();
        let id = Uuid::from_bytes(loc.segment);
        let mut meta =
            decode_meta(id, &store.segments.get(id.as_bytes()).unwrap().unwrap()).unwrap();
        meta.live = 0;
        meta.garbage_since = now();
        let mut previous_receipts = [0; JOURNAL_HEADER as usize];
        read_exact_at(
            &store.journal.as_ref().unwrap().lock().file,
            &mut previous_receipts,
            0,
        )
        .unwrap();
        // Synchronized transactions whose receipt/index publication was interrupted.
        store
            .journal
            .as_ref()
            .unwrap()
            .lock()
            .append(&[
                Mutation::Delete {
                    hash,
                    timestamp: now(),
                },
                Mutation::segment(&meta),
            ])
            .unwrap();
        {
            let mut journal = store.journal.as_ref().unwrap().lock();
            journal
                .append(&[Mutation::Delete {
                    hash: [42; 32],
                    timestamp: now(),
                }])
                .unwrap();
            journal.file.seek(SeekFrom::Start(0)).unwrap();
            journal.file.write_all(&previous_receipts).unwrap();
            journal.file.sync_all().unwrap();
        }
        drop(store);
        let store = Store::open(&config).unwrap();
        assert!(store.get(&hash).unwrap().is_none());
        assert_eq!(store.status().unwrap().objects, 0);
        let results = store
            .put_many(&[
                Bytes::from_static(b"pending delete"),
                Bytes::from_static(b"pending delete"),
            ])
            .unwrap();
        assert_eq!(results[0].hash, hash);
        assert_eq!(store.status().unwrap().objects, 1);
        drop(store);
        let store = Store::open(&config).unwrap();
        assert_eq!(store.status().unwrap().objects, 1);
        drop(store);
        Store::verify(&config).unwrap();
    }

    #[test]
    fn v1_verification_does_not_modify_source_files() {
        fn snapshot(
            root: &Path,
            dir: &Path,
            out: &mut Vec<(PathBuf, u64, std::time::SystemTime, Vec<u8>)>,
        ) {
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    snapshot(root, &path, out);
                } else {
                    let meta = fs::metadata(&path).unwrap();
                    out.push((
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        meta.len(),
                        meta.modified().unwrap(),
                        fs::read(&path).unwrap(),
                    ));
                }
            }
        }
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        let store = Store::open(&config).unwrap();
        store
            .put_many(&[Bytes::from_static(b"v1 content")])
            .unwrap();
        drop(store);
        fs::remove_file(config.storage.root.join("format-v2")).unwrap();
        let generation = selected_generation(&config.storage.root).unwrap().unwrap();
        activate(&config.storage.root, &generation).unwrap();
        let mut before = Vec::new();
        snapshot(&config.storage.root, &config.storage.root, &mut before);
        before.sort();
        Store::verify(&config).unwrap();
        let mut after = Vec::new();
        snapshot(&config.storage.root, &config.storage.root, &mut after);
        after.sort();
        assert_eq!(before, after);
    }

    #[test]
    fn corrupt_worker_stops_throttled_peer_promptly() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        config.verification.max_concurrency = 2;
        config.verification.max_bytes_per_second_mib = 1;
        let store = Store::open(&config).unwrap();
        store
            .put_many(&[Bytes::from(vec![1; 4 * 1024 * 1024])])
            .unwrap();
        store.seal().unwrap();
        store
            .put_many(&[Bytes::from(vec![2; 4 * 1024 * 1024])])
            .unwrap();
        store.seal().unwrap();
        let paths = segment_files(&config.storage.root).unwrap();
        let mut corrupt = OpenOptions::new().write(true).open(&paths[0]).unwrap();
        corrupt.seek(SeekFrom::Start(0)).unwrap();
        corrupt.write_all(b"corrupt!").unwrap();
        drop(corrupt);
        let start = Instant::now();
        assert!(store.recover(false, None).is_err());
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "peer scan did not stop promptly: {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn verify_reports_missing_unexpected_and_mismatched_index_entries() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        let store = Store::open(&config).unwrap();
        let values = store
            .put_many(&[Bytes::from_static(b"first"), Bytes::from_static(b"second")])
            .unwrap();
        store.objects.remove(values[0].hash).unwrap();
        store.objects.insert(values[1].hash, b"wrong").unwrap();
        let extra = [0u8; 32];
        assert_ne!(extra, values[0].hash);
        assert_ne!(extra, values[1].hash);
        store.objects.insert(extra, b"extra").unwrap();
        store.db.persist(PersistMode::SyncAll).unwrap();
        drop(store);
        let error = Store::verify(&config).unwrap_err().to_string();
        assert!(
            error.contains("objects differs: missing=1 unexpected=1 mismatched=1"),
            "{error}"
        );
    }

    #[test]
    fn orphan_scan_updates_progress() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        drop(Store::open(&config).unwrap());
        let mut other = config.clone();
        other.storage.root = root.path().join("other");
        let source = Store::open(&other).unwrap();
        source.put_many(&[Bytes::from_static(b"orphan")]).unwrap();
        source.seal().unwrap();
        let path = segment_files(&other.storage.root).unwrap().remove(0);
        drop(source);
        let destination = config
            .storage
            .root
            .join(path.strip_prefix(&other.storage.root).unwrap());
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::copy(path, destination).unwrap();
        let progress = Arc::new(crate::progress::Progress::new());
        let result = Store::verify_with_progress(&config, &progress).unwrap();
        assert_eq!(result["unreferenced_segments"].as_array().unwrap().len(), 1);
        let snapshot = progress.snapshot();
        assert_eq!(snapshot.segments_verified, 1);
        assert_eq!(snapshot.records_checked, 1);
        assert!(snapshot.bytes_scanned >= 64 + 64 + 6 + 96);
        assert!(snapshot.bytes_total.is_none());
    }

    #[test]
    fn orphan_scan_can_be_cancelled() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        drop(Store::open(&config).unwrap());
        let mut other = config.clone();
        other.storage.root = root.path().join("other");
        let source = Store::open(&other).unwrap();
        source
            .put_many(&[Bytes::from(vec![7; 4 * 1024 * 1024])])
            .unwrap();
        source.seal().unwrap();
        let path = segment_files(&other.storage.root).unwrap().remove(0);
        drop(source);
        let destination = config
            .storage
            .root
            .join(path.strip_prefix(&other.storage.root).unwrap());
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::copy(path, destination).unwrap();
        config.verification.max_bytes_per_second_mib = 1;
        let progress = Arc::new(crate::progress::Progress::new());
        let worker_progress = progress.clone();
        let worker =
            std::thread::spawn(move || Store::verify_with_progress(&config, &worker_progress));
        let deadline = Instant::now() + Duration::from_secs(5);
        while progress.snapshot().phase != "scanning orphan segments" {
            assert!(Instant::now() < deadline, "orphan scan never started");
            std::thread::sleep(Duration::from_millis(5));
        }
        let start = Instant::now();
        progress.cancel();
        assert!(worker.join().unwrap().is_err());
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn verify_detects_selected_index_mismatch() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        let store = Store::open(&config).unwrap();
        let hash = store.put_many(&[Bytes::from_static(b"test")]).unwrap()[0].hash;
        store.objects.remove(hash).unwrap();
        store.db.persist(PersistMode::SyncAll).unwrap();
        drop(store);
        let error = Store::verify(&config).unwrap_err().to_string();
        assert!(error.contains("selected index objects differs"), "{error}");
    }

    #[test]
    fn verify_detects_mismatch_in_each_selected_keyspace() {
        for space in ["segments", "tombstones", "retired"] {
            let root = tempfile::tempdir().unwrap();
            let mut config = Config::default();
            config.storage.root = root.path().join("store");
            let store = Store::open(&config).unwrap();
            let keyspace = match space {
                "segments" => &store.segments,
                "tombstones" => &store.tombstones,
                _ => &store.retired,
            };
            keyspace.insert([99; 16], [1]).unwrap();
            store.db.persist(PersistMode::SyncAll).unwrap();
            drop(store);
            let error = Store::verify(&config).unwrap_err().to_string();
            assert!(
                error.contains(&format!("selected index {space} differs")),
                "{error}"
            );
        }
    }

    #[test]
    fn verify_reports_unapplied_journal_without_mutating_index() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        let store = Store::open(&config).unwrap();
        store
            .journal
            .as_ref()
            .unwrap()
            .lock()
            .append(&[Mutation::Delete {
                hash: [42; 32],
                timestamp: 1,
            }])
            .unwrap();
        drop(store);
        let report = Store::verify(&config).unwrap();
        assert_eq!(report["recovery_required"], true);
        assert_eq!(Store::verify(&config).unwrap()["recovery_required"], true);
    }

    #[test]
    fn unapplied_journal_does_not_hide_index_corruption() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        let store = Store::open(&config).unwrap();
        let hash = store.put_many(&[Bytes::from_static(b"test")]).unwrap()[0].hash;
        store.objects.remove(hash).unwrap();
        store.db.persist(PersistMode::SyncAll).unwrap();
        store
            .journal
            .as_ref()
            .unwrap()
            .lock()
            .append(&[Mutation::Delete { hash, timestamp: 1 }])
            .unwrap();
        drop(store);
        assert!(
            Store::verify(&config)
                .unwrap_err()
                .to_string()
                .contains("selected index objects differs")
        );
    }

    #[test]
    fn verify_reports_missing_cursor_as_recovery_required() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        let store = Store::open(&config).unwrap();
        store.put_many(&[Bytes::from_static(b"test")]).unwrap();
        let recovery = store
            .db
            .keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)
            .unwrap();
        recovery.remove(b"cursor").unwrap();
        store.db.persist(PersistMode::SyncAll).unwrap();
        drop(recovery);
        drop(store);
        assert_eq!(Store::verify(&config).unwrap()["recovery_required"], true);
    }

    #[test]
    fn interrupted_full_replay_never_exposes_partial_cursor() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        let store = Store::open(&config).unwrap();
        let hash = store.put_many(&[Bytes::from_static(b"preserved")]).unwrap()[0].hash;
        let name = store.journal.as_ref().unwrap().lock().name.clone();
        drop(store);
        let path = config.storage.root.join(&name);
        let original = fs::read(&path).unwrap();
        let mut corrupt = original.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        fs::write(&path, corrupt).unwrap();
        let generation = selected_generation(&config.storage.root).unwrap().unwrap();
        let (db, objects, segments) = open_db(&generation, &config).unwrap();
        let tombstones = db
            .keyspace("tombstones", KeyspaceCreateOptions::default)
            .unwrap();
        let retired = db
            .keyspace("retired", KeyspaceCreateOptions::default)
            .unwrap();
        let journal = Journal {
            name,
            file: File::open(&path).unwrap(),
        };
        assert!(
            journal
                .replay(&db, &objects, &segments, &tombstones, &retired)
                .is_err()
        );
        let recovery = db
            .keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)
            .unwrap();
        assert!(recovery.get(b"cursor").unwrap().is_none());
        drop((
            recovery, objects, segments, tombstones, retired, db, journal,
        ));
        fs::write(path, original).unwrap();
        let store = Store::open(&config).unwrap();
        assert_eq!(store.get(&hash).unwrap().unwrap(), b"preserved");
    }

    #[test]
    fn startup_full_replay_reports_journal_progress() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        let store = Store::open(&config).unwrap();
        let hash = store.put_many(&[Bytes::from_static(b"preserved")]).unwrap()[0].hash;
        let recovery = store
            .db
            .keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)
            .unwrap();
        recovery.remove(b"cursor").unwrap();
        store.db.persist(PersistMode::SyncAll).unwrap();
        drop(recovery);
        drop(store);
        let progress = Arc::new(crate::progress::Progress::new());
        let store = Store::open_with_progress(&config, &progress).unwrap();
        assert_eq!(store.get(&hash).unwrap().unwrap(), b"preserved");
        let snapshot = progress.snapshot();
        assert!(snapshot.transactions_processed > 0);
        assert!(snapshot.journal_bytes > 0);
    }

    #[test]
    fn missing_cursor_rebuilds_once_and_clean_open_skips_historical_frames() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        let store = Store::open(&config).unwrap();
        let hash = store.put_many(&[Bytes::from_static(b"preserved")]).unwrap()[0].hash;
        let recovery = store
            .db
            .keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)
            .unwrap();
        recovery.remove(b"cursor").unwrap();
        store.objects.remove(hash).unwrap();
        store.db.persist(PersistMode::SyncAll).unwrap();
        drop(recovery);
        drop(store);
        let store = Store::open(&config).unwrap();
        assert_eq!(store.get(&hash).unwrap().unwrap(), b"preserved");
        assert_eq!(store.status().unwrap().objects, 1);
        let journal_name = store.journal.as_ref().unwrap().lock().name.clone();
        drop(store);
        // History already represented by the durable cursor is not rescanned.
        // Explicit verify still detects journal bit rot.
        let path = config.storage.root.join(journal_name);
        let mut bytes = fs::read(&path).unwrap();
        bytes[JOURNAL_HEADER as usize + 20] ^= 1;
        fs::write(&path, bytes).unwrap();
        let store = Store::open(&config).unwrap();
        assert_eq!(store.get(&hash).unwrap().unwrap(), b"preserved");
        assert_eq!(store.status().unwrap().objects, 1);
        drop(store);
        assert!(Store::verify(&config).is_err());
    }

    #[test]
    fn full_recovery_clears_multiple_pages_in_every_keyspace_and_reports_work() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        let store = Store::open(&config).unwrap();
        let recovery = store
            .db
            .keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)
            .unwrap();
        recovery.remove(b"cursor").unwrap();
        // Stale index contents are deliberately invalid: full recovery must
        // remove every page before rebuilding from the empty authority journal.
        let mut expected = 1u64; // persisted recovery object count
        for (space, count) in [
            (&store.objects, 5000u32),
            (&store.segments, 2049),
            (&store.tombstones, 2049),
            (&store.retired, 2049),
        ] {
            let mut batch = store.db.batch();
            for key in 0..count {
                batch.insert(space, key.to_be_bytes(), [1u8]);
            }
            batch.commit().unwrap();
            expected += u64::from(count);
        }
        store.db.persist(PersistMode::SyncAll).unwrap();
        drop(recovery);
        drop(store);
        let progress = Arc::new(crate::progress::Progress::new());
        let store = Store::open_with_progress(&config, &progress).unwrap();
        for space in [
            &store.objects,
            &store.segments,
            &store.tombstones,
            &store.retired,
        ] {
            assert!(space.is_empty().unwrap());
        }
        let snapshot = progress.snapshot();
        assert_eq!(snapshot.recovery_mode.as_deref(), Some("full"));
        assert_eq!(snapshot.recovery_reason.as_deref(), Some("missing_cursor"));
        assert_eq!(snapshot.index_entries_cleared, expected);
        assert_eq!(snapshot.journal_bytes_remaining, Some(0));
        assert_eq!(snapshot.index_keyspace, None);
        drop(store);
        let next = Arc::new(crate::progress::Progress::new());
        drop(Store::open_with_progress(&config, &next).unwrap());
        assert_eq!(
            next.snapshot().recovery_mode.as_deref(),
            Some("incremental")
        );
        assert_eq!(next.snapshot().index_entries_cleared, 0);
    }

    fn torn_latest_witness(journal: &mut Journal) -> (u64, u64) {
        journal
            .append(&[Mutation::Delete {
                hash: [1; 32],
                timestamp: 1,
            }])
            .unwrap();
        let previous = journal.witness().unwrap();
        let mut previous_headers = [0; 128];
        read_exact_at(&journal.file, &mut previous_headers, 0).unwrap();
        journal
            .append(&[Mutation::Delete {
                hash: [2; 32],
                timestamp: 2,
            }])
            .unwrap();
        let latest = journal.witness().unwrap();
        let mut latest_header = [0; 64];
        read_exact_at(&journal.file, &mut latest_header, latest.slot as u64 * 64).unwrap();
        journal.file.seek(SeekFrom::Start(0)).unwrap();
        journal.file.write_all(&previous_headers).unwrap();
        journal
            .file
            .seek(SeekFrom::Start(latest.slot as u64 * 64))
            .unwrap();
        journal.file.write_all(&latest_header[..24]).unwrap();
        journal.file.sync_all().unwrap();
        assert!(journal.witness().unwrap().degraded);
        (previous.floor, latest.floor)
    }

    #[test]
    fn partial_inactive_witness_overwrite_recovers_prior_and_complete_new_batch() {
        let root = tempfile::tempdir().unwrap();
        let mut journal = Journal::create(root.path()).unwrap();
        let (_, expected_floor) = torn_latest_witness(&mut journal);
        let name = journal.name.clone();
        drop(journal);
        let mut recovered = Journal::open(root.path(), &name).unwrap();
        assert_eq!(recovered.witness().unwrap().floor, expected_floor);
        assert!(!recovered.witness().unwrap().degraded);
        let mut records = 0;
        recovered
            .visit(|batch| {
                records += batch.len();
                Ok(())
            })
            .unwrap();
        assert_eq!(records, 2);
        recovered
            .append(&[Mutation::Delete {
                hash: [3; 32],
                timestamp: 3,
            }])
            .unwrap();
        assert!(recovered.witness().unwrap().floor > expected_floor);
    }

    #[test]
    fn corrupt_latest_witness_plus_tail_truncation_never_falls_back_to_old_namespace() {
        for tail_bytes in [0, 10] {
            let root = tempfile::tempdir().unwrap();
            let mut journal = Journal::create(root.path()).unwrap();
            let (old_floor, _) = torn_latest_witness(&mut journal);
            journal.file.set_len(old_floor + tail_bytes).unwrap();
            journal.file.sync_all().unwrap();
            let name = journal.name.clone();
            drop(journal);
            assert!(Journal::read_only(root.path(), &name).is_err());
            assert!(Journal::open(root.path(), &name).is_err());
        }
    }

    #[test]
    fn both_witness_checksums_corrupt_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let mut journal = Journal::create(root.path()).unwrap();
        journal.file.seek(SeekFrom::Start(60)).unwrap();
        journal.file.write_all(&[0; 4]).unwrap();
        journal.file.seek(SeekFrom::Start(124)).unwrap();
        journal.file.write_all(&[0; 4]).unwrap();
        journal.file.sync_all().unwrap();
        assert!(journal.visit(|_| Ok(())).is_err());
    }
}
