use super::*;

type ChecksumEntry = (Location, [u8; 32], Arc<Vec<[u8; 32]>>);
#[derive(Default)]
pub(super) struct ChecksumCache {
    entries: VecDeque<ChecksumEntry>,
    pending: usize,
}

struct ChecksumReservation<'a> {
    store: &'a Store,
    charge: usize,
}
impl Drop for ChecksumReservation<'_> {
    fn drop(&mut self) {
        self.store.stream_checksums.lock().pending -= self.charge;
    }
}

struct OwnedFileLease {
    store: Arc<Store>,
    file: Option<Arc<File>>,
}
impl std::ops::Deref for OwnedFileLease {
    type Target = File;
    fn deref(&self) -> &File {
        self.file.as_ref().unwrap()
    }
}
impl Drop for OwnedFileLease {
    fn drop(&mut self) {
        self.file.take();
        self.store.files_available.notify_all();
    }
}

pub(super) fn cleanup_uploads(root: &Path) -> Result<()> {
    let dir = root.join("segments");
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(".upload-") && name.ends_with(".tmp") && entry.file_type()?.is_file() {
            fs::remove_file(entry.path())?;
        }
    }
    sync_dir(&dir)?;
    Ok(())
}

pub struct StreamUpload {
    store: Arc<Store>,
    file: Option<File>,
    path: PathBuf,
    id: Uuid,
    created: u64,
    length: u64,
    limit: u64,
    expected: Option<u64>,
    hasher: blake3::Hasher,
}
impl Drop for StreamUpload {
    fn drop(&mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.path);
    }
}
impl Store {
    pub fn begin_upload(
        self: &Arc<Self>,
        max_bytes: u64,
        expected_length: Option<u64>,
    ) -> Result<StreamUpload> {
        ensure!(self.config.streaming.enabled, "streaming disabled");
        let configured = (self.config.streaming.max_object_mib as u64)
            .checked_mul(1024 * 1024)
            .context("streaming maximum overflow")?;
        let limit = max_bytes.min(configured);
        ensure!(
            expected_length.is_none_or(|n| n <= limit),
            "object exceeds streaming maximum"
        );
        let dir = self.root.join("segments");
        durable_mkdir(&dir)?;
        let required = expected_length
            .unwrap_or(0)
            .saturating_add(SEGMENT_HEADER + RECORD_HEADER + SEGMENT_FOOTER);
        self.ensure_write_space(required)?;
        let id = Uuid::new_v4();
        let path = dir.join(format!(".upload-{id}.tmp"));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let created = now();
        file.write_all(&header(id, created))?;
        file.write_all(&[0; 64])?;
        Ok(StreamUpload {
            store: self.clone(),
            file: Some(file),
            path,
            id,
            created,
            length: 0,
            limit,
            expected: expected_length,
            hasher: blake3::Hasher::new(),
        })
    }

    pub fn prepare_stream_read(
        self: &Arc<Self>,
        hash: &[u8; 32],
        cancel: Arc<AtomicBool>,
    ) -> Result<Option<StreamRead>> {
        let (loc, file) = loop {
            ensure!(!cancel.load(Ordering::Acquire), "stream read cancelled");
            let visible = self.visibility.read();
            let Some(loc) = self
                .objects
                .get(hash)?
                .map(|v| decode_loc(&v))
                .transpose()?
            else {
                return Ok(None);
            };
            let id = Uuid::from_bytes(loc.segment);
            let m = decode_meta(
                id,
                &self
                    .segments
                    .get(id.as_bytes())?
                    .context("missing segment metadata")?,
            )?;
            ensure!(
                m.state != 2
                    && loc
                        .offset
                        .checked_add(loc.length)
                        .is_some_and(|end| end <= m.end),
                "invalid stream location"
            );
            let mut cache = self.files.lock();
            let file = if let Some(index) = cache.entries.iter().position(|(key, _)| *key == id) {
                let entry = cache.entries.remove(index).unwrap();
                let file = entry.1.clone();
                cache.entries.push_back(entry);
                Some(file)
            } else {
                if cache.entries.len() >= cache.max
                    && let Some(index) = cache
                        .entries
                        .iter()
                        .position(|(_, file)| Arc::strong_count(file) == 1)
                {
                    cache.entries.remove(index);
                }
                if cache.entries.len() < cache.max {
                    let file = Arc::new(File::open(segment_path(&self.root, &m))?);
                    cache.entries.push_back((id, file.clone()));
                    Some(file)
                } else {
                    None
                }
            };
            if let Some(file) = file {
                break (
                    loc,
                    OwnedFileLease {
                        store: self.clone(),
                        file: Some(file),
                    },
                );
            }
            // The namespace is resampled after each wait. Never wait for a
            // descriptor while holding visibility, which would block writers.
            drop(visible);
            self.files_available
                .wait_for(&mut cache, Duration::from_millis(50));
        };
        let mut h = [0; 64];
        read_exact_at(&file, &mut h, loc.offset - 64)?;
        let (len, expected) = parse_record(&h)?;
        ensure!(
            len == loc.length && expected == *hash,
            "record/index mismatch"
        );
        let chunk = self
            .config
            .streaming
            .chunk_kib
            .checked_mul(1024)
            .context("chunk overflow")?;
        ensure!(chunk > 0, "invalid chunk size");
        let cache_enabled = self.config.streaming.checksum_cache_mib != 0;
        let count = usize::try_from(loc.length.div_ceil(chunk as u64))?;
        let charge = count
            .max(1)
            .checked_mul(32)
            .and_then(|n| n.checked_add(512))
            .context("checksum table overflow")?;
        // With retention disabled, bound in-flight metadata by the configured maximum
        // object size and read concurrency. No checksum tables survive their readers.
        let budget = if cache_enabled {
            self.config
                .streaming
                .checksum_cache_mib
                .checked_mul(1024 * 1024)
                .context("checksum budget overflow")?
        } else {
            let maximum = (self.config.streaming.max_object_mib as u64)
                .checked_mul(1024 * 1024)
                .context("maximum overflow")?;
            usize::try_from(maximum.div_ceil(chunk as u64))?
                .max(1)
                .checked_mul(32)
                .and_then(|n| n.checked_add(512))
                .and_then(|n| n.checked_mul(self.config.streaming.max_reads))
                .context("checksum budget overflow")?
        };
        ensure!(
            charge <= budget,
            "object checksum table exceeds configured budget"
        );
        let cached = {
            let mut cache = self.stream_checksums.lock();
            if cache_enabled {
                cache
                    .entries
                    .iter()
                    .position(|(l, h, _)| *l == loc && h == hash)
                    .map(|index| {
                        let entry = cache.entries.remove(index).unwrap();
                        let table = entry.2.clone();
                        cache.entries.push_back(entry);
                        table
                    })
            } else {
                None
            }
        };
        let table = if let Some(table) = cached {
            table
        } else {
            {
                let mut cache = self.stream_checksums.lock();
                while cache
                    .pending
                    .saturating_add(
                        cache
                            .entries
                            .iter()
                            .map(|e| e.2.len().max(1) * 32 + 512)
                            .sum::<usize>(),
                    )
                    .saturating_add(charge)
                    > budget
                {
                    let Some(index) = cache
                        .entries
                        .iter()
                        .position(|e| Arc::strong_count(&e.2) == 1)
                    else {
                        bail!("stream checksum capacity exhausted");
                    };
                    cache.entries.remove(index);
                }
                cache.pending += charge;
            }
            let reservation = ChecksumReservation {
                store: self,
                charge,
            };
            let mut table = Vec::with_capacity(count);
            let mut whole = blake3::Hasher::new();
            let mut offset = 0u64;
            let mut buffer = vec![0; chunk.min(usize::try_from(loc.length).unwrap_or(chunk))];
            while offset < loc.length {
                ensure!(!cancel.load(Ordering::Acquire), "stream read cancelled");
                let n = (loc.length - offset).min(chunk as u64) as usize;
                read_exact_at(&file, &mut buffer[..n], loc.offset + offset)?;
                whole.update(&buffer[..n]);
                table.push(*blake3::hash(&buffer[..n]).as_bytes());
                offset += n as u64;
            }
            ensure!(
                whole.finalize().as_bytes() == hash,
                "payload BLAKE3 mismatch"
            );
            let table = Arc::new(table);
            {
                let mut cache = self.stream_checksums.lock();
                cache.entries.push_back((loc, *hash, table.clone()));
                cache.pending -= charge;
            }
            std::mem::forget(reservation);
            table
        };
        Ok(Some(StreamRead {
            store: self.clone(),
            file,
            loc,
            table,
            chunk,
            offset: 0,
            cancel,
        }))
    }
}
impl StreamUpload {
    pub fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        let next = self
            .length
            .checked_add(bytes.len() as u64)
            .context("upload length overflow")?;
        ensure!(
            next <= self.limit && self.expected.is_none_or(|n| next <= n),
            "object exceeds upload limit"
        );
        self.store
            .ensure_write_space((bytes.len() as u64).saturating_add(SEGMENT_FOOTER))?;
        self.file
            .as_mut()
            .context("upload closed")?
            .write_all(bytes)?;
        self.hasher.update(bytes);
        self.length = next;
        Ok(())
    }
    pub fn finish(self) -> Result<PutResult> {
        self.prepare_finish()?.finish()
    }
    pub(crate) fn prepare_finish(mut self) -> Result<PreparedStreamUpload> {
        ensure!(
            self.expected.is_none_or(|n| n == self.length),
            "upload length mismatch"
        );
        let hash = *self.hasher.finalize().as_bytes();
        let file = self.file.as_mut().context("upload closed")?;
        let mut rh = prefix(REC_MAGIC, 64);
        rh[12..20].copy_from_slice(&self.length.to_le_bytes());
        rh[20..52].copy_from_slice(&hash);
        finish_crc(&mut rh);
        file.seek(SeekFrom::Start(SEGMENT_HEADER))?;
        file.write_all(&rh)?;
        let end = SEGMENT_HEADER + RECORD_HEADER + self.length;
        let digest = digest_prefix(file, end)?;
        let meta = Meta {
            id: self.id,
            state: 1,
            path: relative(self.id),
            physical: end + SEGMENT_FOOTER,
            end,
            count: 1,
            live: self.length,
            created: self.created,
            sealed: now(),
            digest: Some(digest),
            garbage_since: 0,
            compacted: 0,
        };
        file.seek(SeekFrom::Start(end))?;
        file.write_all(&footer(&meta, &digest))?;
        let started = Instant::now();
        let synced = file.sync_all();
        self.store
            .write_timings
            .data_sync_us
            .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        self.store
            .write_timings
            .data_sync_calls
            .fetch_add(1, Ordering::Relaxed);
        synced?;
        failpoint("stream_after_payload_sync")?;
        let final_path = segment_path(&self.store.root, &meta);
        let parent = final_path.parent().unwrap();
        durable_mkdir(parent)?;
        // A durable orphan is intentionally retained if publication fails.
        self.file.take();
        fs::rename(&self.path, &final_path)?;
        let prepared = PreparedStreamUpload {
            store: self.store.clone(),
            meta,
            hash,
            publication_started: false,
        };
        sync_dir(parent)?;
        sync_dir(&self.store.root.join("segments"))?;
        failpoint("stream_after_rename")?;
        Ok(prepared)
    }
}
pub(crate) struct PreparedStreamUpload {
    store: Arc<Store>,
    meta: Meta,
    hash: [u8; 32],
    publication_started: bool,
}
impl Drop for PreparedStreamUpload {
    fn drop(&mut self) {
        if !self.publication_started {
            let path = segment_path(&self.store.root, &self.meta);
            match fs::remove_file(&path) {
                Ok(()) => {
                    if let Some(parent) = path.parent() {
                        let _ = sync_dir(parent);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(%error, "could not remove unpublished prepared stream")
                }
            }
        }
    }
}
impl PreparedStreamUpload {
    pub(crate) fn hash(&self) -> [u8; 32] {
        self.hash
    }
    pub(crate) fn len(&self) -> u64 {
        self.meta.live
    }
    pub(crate) fn finish(mut self) -> Result<PutResult> {
        let hash = self.hash;
        let meta = self.meta.clone();
        let mut writer = self.store.writer.lock();
        ensure!(
            !writer.faulted && !self.store.publication_fault.load(Ordering::Acquire),
            "write coordinator faulted"
        );
        let _visible = self.store.visibility.write();
        if self.store.objects.contains_key(hash)? {
            // Preserve namespace authority for the durable duplicate segment.
            let mut dead = meta.clone();
            dead.live = 0;
            dead.garbage_since = now();
            self.publication_started = true;
            if let Err(e) = self.store.publish(&[Mutation::segment(&dead)]) {
                writer.faulted = true;
                return Err(e);
            }
            return Ok(PutResult {
                hash,
                duplicate: true,
            });
        }
        let loc = Location {
            segment: *meta.id.as_bytes(),
            offset: SEGMENT_HEADER + RECORD_HEADER,
            length: meta.live,
        };
        self.publication_started = true;
        if let Err(e) = self.store.publish(&[
            Mutation::Put {
                hash,
                location: encode_loc(loc),
            },
            Mutation::segment(&meta),
        ]) {
            writer.faulted = true;
            return Err(e);
        }
        self.store.object_count.fetch_add(1, Ordering::Relaxed);
        Ok(PutResult {
            hash,
            duplicate: false,
        })
    }
}
pub struct StreamRead {
    store: Arc<Store>,
    file: OwnedFileLease,
    loc: Location,
    table: Arc<Vec<[u8; 32]>>,
    chunk: usize,
    offset: u64,
    cancel: Arc<AtomicBool>,
}
impl StreamRead {
    pub fn chunk_size(&self) -> usize {
        self.chunk
    }
    pub fn length(&self) -> u64 {
        self.loc.length
    }
    pub fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        ensure!(
            !self.cancel.load(Ordering::Acquire),
            "stream read cancelled"
        );
        if self.offset == self.loc.length {
            return Ok(None);
        }
        let n = (self.loc.length - self.offset).min(self.chunk as u64) as usize;
        let mut bytes = vec![0; n];
        read_exact_at(&self.file, &mut bytes, self.loc.offset + self.offset)?;
        let index = (self.offset / self.chunk as u64) as usize;
        ensure!(
            blake3::hash(&bytes).as_bytes() == &self.table[index],
            "stream chunk BLAKE3 mismatch"
        );
        self.offset += n as u64;
        Ok(Some(bytes))
    }
}
impl Drop for StreamRead {
    fn drop(&mut self) {
        if self.store.config.streaming.checksum_cache_mib == 0 {
            let mut cache = self.store.stream_checksums.lock();
            if let Some(index) = cache
                .entries
                .iter()
                .position(|e| Arc::ptr_eq(&e.2, &self.table))
                && Arc::strong_count(&self.table) == 2
            {
                cache.entries.remove(index);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn store() -> (tempfile::TempDir, Arc<Store>) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = dir.path().to_path_buf();
        config.streaming.min_free_mib = 0;
        let store = Arc::new(Store::open(&config).unwrap());
        (dir, store)
    }
    #[test]
    fn roundtrip_duplicate_and_cleanup() {
        let (dir, store) = store();
        let data = vec![42; 9 * 1024 * 1024 + 3];
        let mut upload = store
            .begin_upload(data.len() as u64, Some(data.len() as u64))
            .unwrap();
        for chunk in data.chunks(65536) {
            upload.write_chunk(chunk).unwrap();
        }
        let result = upload.finish().unwrap();
        assert!(!result.duplicate);
        let mut read = store
            .prepare_stream_read(&result.hash, Arc::new(AtomicBool::new(false)))
            .unwrap()
            .unwrap();
        let mut output = Vec::new();
        while let Some(chunk) = read.next_chunk().unwrap() {
            output.extend(chunk);
        }
        assert_eq!(output, data);
        let mut upload = store.begin_upload(data.len() as u64, None).unwrap();
        upload.write_chunk(&data).unwrap();
        assert!(upload.finish().unwrap().duplicate);
        let mut interrupted = store.begin_upload(10, None).unwrap();
        interrupted.write_chunk(b"test").unwrap();
        drop(interrupted);
        assert!(fs::read_dir(dir.path().join("segments")).unwrap().all(|e| {
            !e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".upload-")
        }));
    }
    #[test]
    fn rejects_corruption_during_initial_scan() {
        let (_dir, store) = store();
        let data = vec![7; 2 * 1024 * 1024];
        let mut upload = store.begin_upload(data.len() as u64, None).unwrap();
        upload.write_chunk(&data).unwrap();
        let result = upload.finish().unwrap();
        let loc = store.location(&result.hash).unwrap().unwrap();
        let id = Uuid::from_bytes(loc.segment);
        let meta = decode_meta(id, &store.segments.get(id.as_bytes()).unwrap().unwrap()).unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .open(segment_path(&store.root, &meta))
            .unwrap();
        file.seek(SeekFrom::Start(loc.offset + loc.length - 1))
            .unwrap();
        file.write_all(&[9]).unwrap();
        assert!(
            store
                .prepare_stream_read(&result.hash, Arc::new(AtomicBool::new(false)))
                .is_err()
        );
    }
    #[test]
    fn cached_read_checks_changed_chunks() {
        let (_dir, store) = store();
        let data = vec![3; 2 * 1024 * 1024];
        let mut upload = store.begin_upload(data.len() as u64, None).unwrap();
        upload.write_chunk(&data).unwrap();
        let result = upload.finish().unwrap();
        drop(
            store
                .prepare_stream_read(&result.hash, Arc::new(AtomicBool::new(false)))
                .unwrap(),
        );
        let loc = store.location(&result.hash).unwrap().unwrap();
        let id = Uuid::from_bytes(loc.segment);
        let meta = decode_meta(id, &store.segments.get(id.as_bytes()).unwrap().unwrap()).unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .open(segment_path(&store.root, &meta))
            .unwrap();
        file.seek(SeekFrom::Start(loc.offset + loc.length - 1))
            .unwrap();
        file.write_all(&[9]).unwrap();
        let mut read = store
            .prepare_stream_read(&result.hash, Arc::new(AtomicBool::new(false)))
            .unwrap()
            .unwrap();
        assert!(read.next_chunk().unwrap().is_some());
        assert!(read.next_chunk().is_err());
    }
    #[test]
    fn disabled_cache_does_not_retain_tables() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = dir.path().into();
        config.streaming.min_free_mib = 0;
        config.streaming.checksum_cache_mib = 0;
        let store = Arc::new(Store::open(&config).unwrap());
        let mut upload = store.begin_upload(10, None).unwrap();
        upload.write_chunk(b"hello").unwrap();
        let result = upload.finish().unwrap();
        let read = store
            .prepare_stream_read(&result.hash, Arc::new(AtomicBool::new(false)))
            .unwrap();
        assert_eq!(store.stream_checksums.lock().entries.len(), 1);
        drop(read);
        assert!(store.stream_checksums.lock().entries.is_empty());
    }
    #[test]
    fn waiting_for_file_is_cancellable_and_does_not_block_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = dir.path().into();
        config.streaming.min_free_mib = 0;
        config.resources.max_open_segments = 1;
        let store = Arc::new(Store::open(&config).unwrap());
        let mut a = store.begin_upload(10, None).unwrap();
        a.write_chunk(b"one").unwrap();
        let a = a.finish().unwrap().hash;
        let mut b = store.begin_upload(10, None).unwrap();
        b.write_chunk(b"two").unwrap();
        let b = b.finish().unwrap().hash;
        let read = store
            .prepare_stream_read(&a, Arc::new(AtomicBool::new(false)))
            .unwrap()
            .unwrap();
        let cancel = Arc::new(AtomicBool::new(false));
        let other = store.clone();
        let flag = cancel.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            tx.send(other.prepare_stream_read(&b, flag).is_err())
                .unwrap();
        });
        std::thread::sleep(Duration::from_millis(100));
        let (tx, done) = std::sync::mpsc::channel();
        let writer = store.clone();
        let writer = std::thread::spawn(move || {
            tx.send(writer.put_many(&[Bytes::from_static(b"small")]))
                .unwrap();
        });
        done.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
        cancel.store(true, Ordering::Release);
        assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap());
        worker.join().unwrap();
        writer.join().unwrap();
        drop(read);
    }
    #[test]
    fn reader_lease_survives_delete_and_gc() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = dir.path().into();
        config.streaming.min_free_mib = 0;
        config.resources.max_open_segments = 1;
        let store = Arc::new(Store::open(&config).unwrap());
        let mut upload = store.begin_upload(10, None).unwrap();
        upload.write_chunk(b"hello").unwrap();
        let hash = upload.finish().unwrap().hash;
        let mut read = store
            .prepare_stream_read(&hash, Arc::new(AtomicBool::new(false)))
            .unwrap()
            .unwrap();
        store.delete(&hash).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let other = store.clone();
        let worker = std::thread::spawn(move || {
            tx.send(other.collect_garbage(true)).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        assert_eq!(read.next_chunk().unwrap().unwrap(), b"hello");
        drop(read);
        rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
        worker.join().unwrap();
    }
    #[test]
    fn detects_corruption_before_emission() {
        let (_dir, store) = store();
        let data = vec![3; 2 * 1024 * 1024];
        let mut upload = store.begin_upload(data.len() as u64, None).unwrap();
        upload.write_chunk(&data).unwrap();
        let result = upload.finish().unwrap();
        let mut read = store
            .prepare_stream_read(&result.hash, Arc::new(AtomicBool::new(false)))
            .unwrap()
            .unwrap();
        let loc = store.location(&result.hash).unwrap().unwrap();
        let id = Uuid::from_bytes(loc.segment);
        let meta = decode_meta(id, &store.segments.get(id.as_bytes()).unwrap().unwrap()).unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .open(segment_path(&store.root, &meta))
            .unwrap();
        file.seek(SeekFrom::Start(loc.offset)).unwrap();
        file.write_all(&[9]).unwrap();
        assert!(read.next_chunk().is_err());
    }
    #[test]
    fn dropping_unpublished_prepared_upload_reclaims_file_but_publication_attempt_retains_it() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = directory.path().into();
        let store = Arc::new(Store::open(&config).unwrap());
        let mut upload = store.begin_upload(1024, Some(7)).unwrap();
        upload.write_chunk(b"pending").unwrap();
        let prepared = upload.prepare_finish().unwrap();
        let path = segment_path(&store.root, &prepared.meta);
        assert!(path.exists());
        drop(prepared);
        assert!(!path.exists());
        assert_eq!(store.status().unwrap().objects, 0);
        let mut upload = store.begin_upload(1024, Some(7)).unwrap();
        upload.write_chunk(b"unknown").unwrap();
        let mut prepared = upload.prepare_finish().unwrap();
        let path = segment_path(&store.root, &prepared.meta);
        // Once journal publication may have started, cleanup cannot infer
        // whether this UUID became authoritative. Process crashes likewise
        // bypass Drop and leave the orphan for explicit vacuum.
        prepared.publication_started = true;
        drop(prepared);
        assert!(path.exists());
    }
}
