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
    ) -> Result<()> {
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
                cursor.end
            }
            _ => {
                // Existing releases have no cursor. Reconstruct once, also used
                // conservatively when a witness slot needs recovery.
                self.replay(db, objects, segments, tombstones, retired)?;
                let value = recovery.get(b"cursor")?.context("missing replay cursor")?;
                serde_json::from_slice::<ReplayCursor>(&value)?.end
            }
        };
        let end = self.visit_from(start, |changes, end| {
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
                Some((&self.name, end)),
            )
        })?;
        if end != start {
            db.persist(PersistMode::SyncAll)?;
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
        let recovery = db.keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)?;
        // Invalidate the old cursor durably before any destructive rebuild.
        // A process death during clearing must restart the full replay.
        recovery.remove(b"cursor")?;
        db.persist(PersistMode::SyncAll)?;
        for space in [objects, segments, tombstones, retired, &recovery] {
            // A fresh iterator for each chunk avoids retaining the complete
            // namespace or an unbounded Fjall write batch in memory.
            loop {
                let mut batch = db.batch();
                let mut count = 0;
                for item in space.iter().take(1024) {
                    let (key, _) = item.into_inner()?;
                    batch.remove(space, key);
                    count += 1;
                }
                if count == 0 {
                    break;
                }
                batch.commit()?;
            }
        }
        // Reset the count before replay, so each frame updates it atomically.
        recovery.insert(b"objects", 0u64.to_le_bytes())?;
        let end = self.visit_from(JOURNAL_HEADER, |changes, end| {
            apply(
                db,
                objects,
                segments,
                tombstones,
                retired,
                &changes,
                Some((&self.name, end)),
            )
        })?;
        recovery.insert(
            b"cursor",
            serde_json::to_vec(&ReplayCursor {
                journal: self.name.clone(),
                end,
            })?,
        )?;
        db.persist(PersistMode::SyncAll)?;
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
fn apply(
    db: &Database,
    objects: &Keyspace,
    segments: &Keyspace,
    tombstones: &Keyspace,
    retired: &Keyspace,
    changes: &[Mutation],
    cursor: Option<(&str, u64)>,
) -> Result<()> {
    let recovery = db.keyspace(RECOVERY_SPACE, KeyspaceCreateOptions::default)?;
    let mut count = match recovery.get(b"objects")? {
        Some(value) => {
            ensure!(value.len() == 8, "invalid recovery object count");
            u64_at(&value, 0)
        }
        None => objects.len()? as u64,
    };
    let mut presence = HashMap::new();
    let mut batch = db.batch();
    for change in changes {
        match change {
            Mutation::Put { hash, location } => {
                let present = *presence.entry(*hash).or_insert(objects.contains_key(hash)?);
                if !present {
                    count = count.checked_add(1).context("object count overflow")?;
                }
                presence.insert(*hash, true);
                decode_loc(location)?;
                batch.insert(objects, hash, location);
                batch.remove(tombstones, hash);
            }
            Mutation::Delete { hash, timestamp } => {
                let present = *presence.entry(*hash).or_insert(objects.contains_key(hash)?);
                if present {
                    count = count.checked_sub(1).context("object count underflow")?;
                }
                presence.insert(*hash, false);
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
    batch.insert(&recovery, b"objects", count.to_le_bytes());
    if let Some((journal, end)) = cursor {
        batch.insert(
            &recovery,
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
            cursor,
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
}
impl ScratchNamespace {
    fn build(config: &Config, root: &Path, activation: &Activation) -> Result<Self> {
        let path = std::env::temp_dir().join(format!("skerry-namespace-{}", Uuid::new_v4()));
        let (db, objects, segments) = open_db(&path, config)?;
        let tombstones = db.keyspace("tombstones", KeyspaceCreateOptions::default)?;
        let retired = db.keyspace("retired", KeyspaceCreateOptions::default)?;
        let scratch = Self {
            objects: Some(objects),
            segments: Some(segments),
            tombstones: Some(tombstones),
            retired: Some(retired),
            db: Some(db),
            path,
        };
        let journal = Journal::read_only(
            root,
            activation
                .journal
                .as_deref()
                .context("missing v2 journal")?,
        )?;
        journal.replay(
            scratch.db.as_ref().unwrap(),
            scratch.objects.as_ref().unwrap(),
            scratch.segments.as_ref().unwrap(),
            scratch.tombstones.as_ref().unwrap(),
            scratch.retired.as_ref().unwrap(),
        )?;
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
pub(super) fn verify_v2(config: &Config) -> Result<Value> {
    config.validate()?;
    let lock = store_lock(&config.storage.root)?;
    let activation = selected_activation(&config.storage.root)?.context("missing v2 activation")?;
    let mut scratch = ScratchNamespace::build(config, &config.storage.root, &activation)?;
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
        gc_pressure: AtomicBool::new(false),
        journal: None,
        maintenance: Mutex::new(()),
        _lock: lock,
    };
    store.recover(false)?;
    let status = store.status()?;
    let mut orphaned = vec![];
    for path in segment_files(&config.storage.root)? {
        let mut header = [0; 64];
        File::open(&path)?.read_exact(&mut header)?;
        let (id, _) = parse_header(&header)?;
        if !store.segments.contains_key(id.as_bytes())?
            && !store.retired.contains_key(id.as_bytes())?
        {
            scan_file(&path, &config.storage.root, true, true)?;
            orphaned.push(path.display().to_string());
        }
    }
    let pending = store.retired.len()?;
    drop(store);
    drop(scratch);
    Ok(
        json!({"status":"verified","version":2,"storage":status,"unreferenced_segments":orphaned,"retired_segments_pending_cleanup":pending}),
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
