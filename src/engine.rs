//! Blocking storage primitives. Call from a bounded blocking pool, never an async executor.
use crate::config::Config;
use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;
mod maintenance;
use maintenance::{Journal, Mutation};

pub const SEGMENT_HEADER: u64 = 64;
pub const RECORD_HEADER: u64 = 64;
pub const SEGMENT_FOOTER: u64 = 96;
const SEG_MAGIC: &[u8; 8] = b"SKRYSEG1";
const REC_MAGIC: &[u8; 8] = b"SKRYREC1";
const END_MAGIC: &[u8; 8] = b"SKRYEND1";

#[derive(Clone, Debug)]
pub struct DeleteResult {
    pub hash: [u8; 32],
    pub deleted: bool,
}
#[derive(Clone, Debug)]
pub struct PutResult {
    pub hash: [u8; 32],
    pub duplicate: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Location {
    pub segment: [u8; 16],
    pub offset: u64,
    pub length: u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageStatus {
    pub objects: u64,
    pub payload_bytes: u64,
    pub physical_bytes: u64,
    pub segments: usize,
    pub active_segment: Option<String>,
    pub active_bytes: u64,
    pub index_bytes: u64,
    pub open_files: usize,
}
#[derive(Clone, Debug)]
struct Meta {
    id: Uuid,
    state: u8,
    path: String,
    physical: u64,
    end: u64,
    count: u64,
    live: u64,
    created: u64,
    sealed: u64,
    digest: Option<[u8; 32]>,
    garbage_since: u64,
    compacted: u64,
}
struct Active {
    file: File,
    meta: Meta,
}
#[derive(Default)]
struct Writer {
    active: Option<Active>,
    faulted: bool,
}
struct FileCache {
    entries: VecDeque<(Uuid, Arc<File>)>,
    max: usize,
}

struct FileLease<'a> {
    file: Option<Arc<File>>,
    available: &'a Condvar,
}
impl std::ops::Deref for FileLease<'_> {
    type Target = File;
    fn deref(&self) -> &File {
        self.file.as_ref().unwrap()
    }
}
impl Drop for FileLease<'_> {
    fn drop(&mut self) {
        drop(self.file.take());
        self.available.notify_all();
    }
}

pub struct Store {
    // Keep ownership until all other fields have dropped, including Fjall handles.
    db: Database,
    objects: Keyspace,
    segments: Keyspace,
    root: PathBuf,
    config: Config,
    writer: Mutex<Writer>,
    visibility: RwLock<()>,
    files: Mutex<FileCache>,
    files_available: Condvar,
    object_count: AtomicU64,
    gc_pressure: AtomicBool,
    journal: Option<Mutex<Journal>>,
    tombstones: Keyspace,
    retired: Keyspace,
    maintenance: Mutex<()>,
    _lock: File,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn u16_at(b: &[u8], p: usize) -> u16 {
    u16::from_le_bytes(b[p..p + 2].try_into().unwrap())
}
fn u64_at(b: &[u8], p: usize) -> u64 {
    u64::from_le_bytes(b[p..p + 8].try_into().unwrap())
}
fn crc_ok(b: &[u8]) -> bool {
    b.len() >= 4
        && crc32fast::hash(&b[..b.len() - 4])
            == u32::from_le_bytes(b[b.len() - 4..].try_into().unwrap())
}
fn finish_crc(b: &mut [u8]) {
    let n = b.len();
    let sum = crc32fast::hash(&b[..n - 4]);
    b[n - 4..].copy_from_slice(&sum.to_le_bytes());
}
fn prefix(magic: &[u8; 8], len: usize) -> Vec<u8> {
    let mut b = vec![0; len];
    b[..8].copy_from_slice(magic);
    b[8..10].copy_from_slice(&1u16.to_le_bytes());
    b[10..12].copy_from_slice(&(len as u16).to_le_bytes());
    b
}
fn header(id: Uuid, created: u64) -> Vec<u8> {
    let mut b = prefix(SEG_MAGIC, 64);
    b[12..28].copy_from_slice(id.as_bytes());
    b[28..36].copy_from_slice(&created.to_le_bytes());
    finish_crc(&mut b);
    b
}
fn parse_header(b: &[u8]) -> Result<(Uuid, u64)> {
    ensure!(
        b.len() == 64 && &b[..8] == SEG_MAGIC,
        "invalid segment header magic/length"
    );
    ensure!(
        u16_at(b, 8) == 1 && u16_at(b, 10) == 64,
        "unsupported segment format"
    );
    ensure!(crc_ok(b), "segment header checksum mismatch");
    ensure!(
        b[36..60].iter().all(|x| *x == 0),
        "unsupported segment flags"
    );
    let id = Uuid::from_slice(&b[12..28])?;
    ensure!(
        id.get_version_num() == 4 && id.get_variant() == uuid::Variant::RFC4122,
        "segment ID is not UUIDv4"
    );
    Ok((id, u64_at(b, 28)))
}
fn record_header(payload: &[u8], hash: &[u8; 32]) -> Vec<u8> {
    let mut b = prefix(REC_MAGIC, 64);
    b[12..20].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    b[20..52].copy_from_slice(hash);
    finish_crc(&mut b);
    b
}
fn parse_record(b: &[u8]) -> Result<(u64, [u8; 32])> {
    ensure!(
        b.len() == 64 && &b[..8] == REC_MAGIC,
        "invalid record magic/length"
    );
    ensure!(
        u16_at(b, 8) == 1 && u16_at(b, 10) == 64,
        "unsupported record format"
    );
    ensure!(crc_ok(b), "record header checksum mismatch");
    ensure!(
        b[52..60].iter().all(|x| *x == 0),
        "unsupported record flags"
    );
    Ok((u64_at(b, 12), b[20..52].try_into().unwrap()))
}
fn footer(meta: &Meta, digest: &[u8; 32]) -> Vec<u8> {
    let mut b = prefix(END_MAGIC, 96);
    b[12..28].copy_from_slice(meta.id.as_bytes());
    for (p, v) in [(28, meta.end), (36, meta.count), (44, meta.sealed)] {
        b[p..p + 8].copy_from_slice(&v.to_le_bytes());
    }
    b[52..84].copy_from_slice(digest);
    finish_crc(&mut b);
    b
}
#[derive(Debug)]
struct Footer {
    id: Uuid,
    end: u64,
    count: u64,
    sealed: u64,
    digest: [u8; 32],
}
fn parse_footer(b: &[u8]) -> Result<Footer> {
    ensure!(b.len() == 96 && &b[..8] == END_MAGIC, "invalid seal footer");
    ensure!(
        u16_at(b, 8) == 1 && u16_at(b, 10) == 96,
        "unsupported footer format"
    );
    ensure!(crc_ok(b), "footer checksum mismatch");
    ensure!(
        b[84..92].iter().all(|x| *x == 0),
        "unsupported footer flags"
    );
    Ok(Footer {
        id: Uuid::from_slice(&b[12..28])?,
        end: u64_at(b, 28),
        count: u64_at(b, 36),
        sealed: u64_at(b, 44),

        digest: b[52..84].try_into().unwrap(),
    })
}
fn encode_loc(l: Location) -> Vec<u8> {
    let mut b = vec![1];
    b.extend_from_slice(&l.segment);
    b.extend_from_slice(&l.offset.to_le_bytes());
    b.extend_from_slice(&l.length.to_le_bytes());
    b
}
fn decode_loc(b: &[u8]) -> Result<Location> {
    ensure!(b.len() == 33 && b[0] == 1, "invalid object-index value");
    let l = Location {
        segment: b[1..17].try_into().unwrap(),
        offset: u64_at(b, 17),
        length: u64_at(b, 25),
    };
    ensure!(
        l.offset >= SEGMENT_HEADER + RECORD_HEADER,
        "invalid payload offset"
    );
    l.offset
        .checked_add(l.length)
        .context("object-index length overflow")?;
    Ok(l)
}
fn encode_meta(m: &Meta) -> Vec<u8> {
    let extended = m.garbage_since != 0 || m.compacted != 0;
    let mut b = vec![if extended { 2 } else { 1 }, m.state];
    b.extend_from_slice(&(m.path.len() as u16).to_le_bytes());
    for v in [m.physical, m.end, m.count, m.live, m.created, m.sealed] {
        b.extend_from_slice(&v.to_le_bytes());
    }
    b.push(u8::from(m.digest.is_some()));
    b.extend_from_slice(&m.digest.unwrap_or([0; 32]));
    b.extend_from_slice(m.path.as_bytes());
    if extended {
        b.extend_from_slice(&m.garbage_since.to_le_bytes());
        b.extend_from_slice(&m.compacted.to_le_bytes());
    }
    b
}
fn decode_meta(id: Uuid, b: &[u8]) -> Result<Meta> {
    ensure!(
        b.len() >= 85 && (b[0] == 1 || b[0] == 2) && b[1] <= 2,
        "invalid segment metadata version/state"
    );
    let n = u16_at(b, 2) as usize;
    let extra = if b[0] == 2 { 16 } else { 0 };
    ensure!(b.len() == 85 + n + extra, "invalid segment metadata length");
    let path = String::from_utf8(b[85..85 + n].to_vec())?;
    validate_relative(&path)?;
    ensure!(path == relative(id), "noncanonical segment path");
    ensure!(b[52] <= 1, "invalid segment digest flag");
    ensure!(
        u64_at(b, 12) >= 64 && u64_at(b, 4) >= u64_at(b, 12),
        "invalid segment logical/physical extent"
    );
    ensure!(b[1] != 1 || b[52] == 1, "sealed segment missing digest");
    Ok(Meta {
        id,
        state: b[1],
        path,
        physical: u64_at(b, 4),
        end: u64_at(b, 12),
        count: u64_at(b, 20),
        live: u64_at(b, 28),
        created: u64_at(b, 36),
        sealed: u64_at(b, 44),
        garbage_since: if extra > 0 { u64_at(b, 85 + n) } else { 0 },
        compacted: if extra > 0 { u64_at(b, 93 + n) } else { 0 },
        digest: if b[52] == 1 {
            Some(b[53..85].try_into().unwrap())
        } else {
            None
        },
    })
}
fn validate_relative(s: &str) -> Result<()> {
    ensure!(!s.contains('\\'), "segment path must use slash separators");
    ensure!(
        !s.is_empty()
            && Path::new(s)
                .components()
                .all(|c| matches!(c, Component::Normal(_))),
        "unsafe segment path"
    );
    Ok(())
}
fn relative(id: Uuid) -> String {
    format!("{}/{}.seg", &id.simple().to_string()[..2], id)
}
fn segment_path(root: &Path, m: &Meta) -> PathBuf {
    root.join("segments").join(&m.path)
}
fn durable_mkdir(path: &Path) -> Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    durable_mkdir(parent)?;
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {}
        Err(e) => return Err(e.into()),
    }
    sync_dir(parent)?;
    Ok(())
}
fn store_lock(root: &Path) -> Result<File> {
    durable_mkdir(root)?;
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join("store.lock"))?;
    fs2::FileExt::try_lock_exclusive(&f).context("store is already locked by another process")?;
    Ok(f)
}
fn sync_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}
fn failpoint(name: &str) -> Result<()> {
    // Explicit opt-in fault injection for subprocess tests only. Never enabled by default.
    if std::env::var("SKERRY_FAILPOINT").ok().as_deref() == Some(name) {
        if std::env::var("SKERRY_FAILPOINT_ACTION").ok().as_deref() == Some("abort") {
            std::process::abort();
        }
        bail!("injected storage failure: {name}");
    }
    Ok(())
}
fn digest_prefix(file: &mut File, end: u64) -> Result<[u8; 32]> {
    file.seek(SeekFrom::Start(0))?;
    let mut h = blake3::Hasher::new();
    let mut left = end;
    let mut buf = [0; 64 * 1024];
    while left > 0 {
        let n = (left.min(buf.len() as u64)) as usize;
        file.read_exact(&mut buf[..n])?;
        h.update(&buf[..n]);
        left -= n as u64;
    }
    Ok(*h.finalize().as_bytes())
}

#[derive(Serialize, Deserialize)]
struct Activation {
    version: u8,
    generation: String,
    sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    journal: Option<String>,
}
fn activation_files(root: &Path) -> Result<Vec<PathBuf>> {
    let index = root.join("index");
    if !index.exists() {
        return Ok(vec![]);
    }
    let mut files = vec![];
    for e in fs::read_dir(index)? {
        let p = e?.path();
        if p.file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|s| s.starts_with("activation-") && s.ends_with(".json"))
        {
            files.push(p)
        }
    }
    files.sort();
    Ok(files)
}
fn read_activation(path: &Path) -> Result<Activation> {
    let b = fs::read(path)?;
    ensure!(
        b.len() > 4,
        "incomplete activation record; explicit maintenance required"
    );
    ensure!(
        crc_ok(&b),
        "newest activation checksum invalid; refusing stale fallback"
    );
    let a: Activation = serde_json::from_slice(&b[..b.len() - 4])?;
    ensure!(
        (a.version == 1 || a.version == 2)
            && a.generation.starts_with("generation-")
            && Path::new(&a.generation).components().count() == 1,
        "invalid activation record"
    );
    if a.version == 2 {
        let journal = a
            .journal
            .as_ref()
            .context("v2 activation missing journal")?;
        ensure!(
            journal.starts_with("journal-") && Path::new(journal).components().count() == 1,
            "invalid mutation journal path"
        );
    }
    Ok(a)
}
fn selected_activation(root: &Path) -> Result<Option<Activation>> {
    let indexed = activation_files(root)?
        .last()
        .map(|p| read_activation(p))
        .transpose()?;
    let marker = root.join("format-v2");
    let fallback = if marker.exists() {
        Some(read_activation(&marker)?)
    } else {
        None
    };
    match (indexed, fallback) {
        (Some(a), Some(marker)) => {
            ensure!(
                marker.version == 2 && a.version == 2,
                "activation contradicts v2 root marker; refusing format rollback"
            );
            if a.sequence == marker.sequence {
                ensure!(
                    a.generation == marker.generation && a.journal == marker.journal,
                    "conflicting activation records at same sequence"
                );
            }
            Ok(Some(if marker.sequence > a.sequence {
                marker
            } else {
                a
            }))
        }
        (a, None) => Ok(a),
        (None, marker) => Ok(marker),
    }
}
fn sync_root_marker(root: &Path, activation: &Activation) -> Result<()> {
    let mut bytes = serde_json::to_vec(activation)?;
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    let temp = root.join(format!(".format-v2-{}.tmp", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temp, root.join("format-v2"))?;
    sync_dir(root)?;
    Ok(())
}
fn selected_generation(root: &Path) -> Result<Option<PathBuf>> {
    let Some(a) = selected_activation(root)? else {
        return Ok(None);
    };
    let generation = root.join("index").join(a.generation);
    ensure!(
        generation.join("version").exists(),
        "selected index generation is missing/incomplete; rebuild required"
    );
    Ok(Some(generation))
}
fn activate(root: &Path, generation: &Path) -> Result<()> {
    activate_version(root, generation, None)
}
fn activate_version(root: &Path, generation: &Path, journal: Option<&str>) -> Result<()> {
    let index = root.join("index");
    fs::create_dir_all(&index)?;
    let sequence = selected_activation(root)?
        .map_or(0, |a| a.sequence)
        .checked_add(1)
        .context("activation sequence exhausted")?;
    let a = Activation {
        version: if journal.is_some() { 2 } else { 1 },
        journal: journal.map(str::to_owned),
        generation: generation
            .file_name()
            .context("generation path")?
            .to_string_lossy()
            .into_owned(),
        sequence,
    };
    let mut b = serde_json::to_vec(&a)?;
    let sum = crc32fast::hash(&b);
    b.extend_from_slice(&sum.to_le_bytes());
    let final_path = index.join(format!("activation-{sequence:020}.json"));
    let temp = index.join(format!(".activation-{}.tmp", Uuid::new_v4()));
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        f.write_all(&b)?;
        f.sync_all()?;
    }
    failpoint("before_activation")?;
    fs::rename(&temp, &final_path)?;
    sync_dir(&index)?;
    failpoint("activation_before_marker")?;
    if journal.is_some() {
        sync_root_marker(root, &a)?;
    }
    failpoint("after_activation")?;
    Ok(())
}
fn new_generation(root: &Path) -> Result<PathBuf> {
    let p = root
        .join("index")
        .join(format!("generation-{}", Uuid::new_v4()));
    fs::create_dir_all(&p)?;
    sync_dir(&root.join("index"))?;
    sync_dir(root)?;
    Ok(p)
}
fn open_db(path: &Path, config: &Config) -> Result<(Database, Keyspace, Keyspace)> {
    let db = Database::builder(path)
        .cache_size((config.cache.index_mib as u64) * 1024 * 1024)
        .open()?;
    let objects = db.keyspace("objects", || {
        KeyspaceCreateOptions::default().with_kv_separation(None)
    })?;
    let segments = db.keyspace("segments", || {
        KeyspaceCreateOptions::default().with_kv_separation(None)
    })?;
    Ok((db, objects, segments))
}
fn segment_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = vec![];
    let dir = root.join("segments");
    if !dir.exists() {
        return Ok(out);
    }
    for e in fs::read_dir(&dir)? {
        let p = e?.path();
        ensure!(!p.is_symlink(), "symlink in segment directory");
        if p.is_dir() {
            for e in fs::read_dir(p)? {
                let p = e?.path();
                ensure!(!p.is_symlink(), "symlink in segment directory");
                if p.extension().is_some_and(|s| s == "seg") {
                    out.push(p)
                }
            }
        } else if p.extension().is_some_and(|s| s == "seg") {
            out.push(p)
        }
    }
    out.sort();
    Ok(out)
}
#[derive(Debug)]
struct Scanned {
    meta: Meta,
}
fn scan_prefix_visit(
    file: &mut File,
    id: Uuid,
    end: u64,
    allow_incomplete: bool,
    visit: &mut dyn FnMut([u8; 32], Location) -> Result<()>,
) -> Result<(u64, u64)> {
    ensure!(end >= 64, "invalid segment end");
    let mut offset = 64;
    let mut count = 0;
    let mut buf = [0; 64 * 1024];
    while offset < end {
        if end - offset < 64 {
            ensure!(
                allow_incomplete,
                "truncated record header below committed end"
            );
            break;
        }
        let mut h = [0; 64];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut h)?;
        if allow_incomplete && h.iter().all(|x| *x == 0) {
            break;
        }
        let (len, hash) =
            parse_record(&h).with_context(|| format!("segment {id} record at {offset}"))?;
        let payload = offset.checked_add(64).context("record offset overflow")?;
        let next = payload.checked_add(len).context("record length overflow")?;
        if next > end {
            ensure!(allow_incomplete, "truncated payload below committed end");
            break;
        }
        let mut hasher = blake3::Hasher::new();
        let mut left = len;
        while left > 0 {
            let n = left.min(buf.len() as u64) as usize;
            file.read_exact(&mut buf[..n])?;
            hasher.update(&buf[..n]);
            left -= n as u64;
        }
        ensure!(
            hasher.finalize().as_bytes() == &hash,
            "payload BLAKE3 mismatch in segment {id} at {payload}"
        );
        visit(
            hash,
            Location {
                segment: *id.as_bytes(),
                offset: payload,
                length: len,
            },
        )?;
        count += 1;
        offset = next;
    }
    Ok((count, offset))
}
fn scan_file(path: &Path, root: &Path, salvage: bool, canonical: bool) -> Result<Option<Scanned>> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    ensure!(len >= 64, "truncated segment header: {}", path.display());
    let mut h = [0; 64];
    file.read_exact(&mut h)?;
    let (id, created) = parse_header(&h)?;
    ensure!(
        path.file_name().and_then(|s| s.to_str()) == Some(format!("{id}.seg").as_str()),
        "segment filename UUID mismatch"
    );
    let mut fbuf = [0; 96];
    let sealed = if len >= 160 {
        file.seek(SeekFrom::Start(len - 96))?;
        file.read_exact(&mut fbuf)?;
        &fbuf[..8] == END_MAGIC
    } else {
        false
    };
    let footer = if sealed {
        Some(parse_footer(&fbuf)?)
    } else {
        None
    };
    let end = footer.as_ref().map_or(len, |f| f.end);
    ensure!(end <= len, "footer end outside segment");
    let (count, valid_end) =
        scan_prefix_visit(&mut file, id, end, footer.is_none(), &mut |_, _| Ok(()))?;
    if let Some(f) = &footer {
        ensure!(
            f.id == id && f.end.checked_add(96) == Some(len) && f.count == count,
            "seal footer identity/count/end mismatch"
        );
        ensure!(
            digest_prefix(&mut file, end)? == f.digest,
            "whole-segment digest mismatch"
        );
    }
    if footer.is_none() && !salvage {
        return Ok(None);
    }
    let rel = if canonical {
        let rel = path
            .strip_prefix(root.join("segments"))?
            .to_string_lossy()
            .replace('\\', "/");
        validate_relative(&rel)?;
        ensure!(
            rel == relative(id),
            "segment path does not match UUID shard"
        );
        rel
    } else {
        relative(id)
    };
    Ok(Some(Scanned {
        meta: Meta {
            id,
            state: if sealed { 1 } else { 0 },
            path: rel,
            physical: len,
            end: valid_end,
            count,
            live: 0,
            created,
            sealed: footer.as_ref().map_or(0, |f| f.sealed),
            digest: footer.map(|f| f.digest),
            garbage_since: 0,
            compacted: 0,
        },
    }))
}

fn parse_segment_cursor(cursor: &str) -> Result<Uuid> {
    let encoded = cursor
        .strip_prefix("v1:")
        .context("invalid segment cursor version")?;
    ensure!(
        encoded.len() == 32
            && encoded
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid segment cursor encoding"
    );
    let id = Uuid::parse_str(encoded).context("invalid segment cursor UUID")?;
    ensure!(
        id.get_version_num() == 4 && id.get_variant() == uuid::Variant::RFC4122,
        "invalid segment cursor UUID version"
    );
    Ok(id)
}

impl Store {
    pub fn verify(config: &Config) -> Result<serde_json::Value> {
        verify(config)
    }
    pub fn rebuild(config: &Config, salvage: bool) -> Result<serde_json::Value> {
        rebuild(config, salvage)
    }
    pub fn import(config: &Config, source: &Path) -> Result<serde_json::Value> {
        import(config, source)
    }
    pub fn open(config: &Config) -> Result<Self> {
        Self::open_mode(config, true)
    }
    fn open_mode(config: &Config, repair: bool) -> Result<Self> {
        config.validate()?;
        let root = config.storage.root.clone();
        let lock = store_lock(&root)?;
        if repair {
            fs::create_dir_all(root.join("segments"))?;
            fs::create_dir_all(root.join("logs"))?;
        }
        let (generation, new) = match selected_generation(&root)? {
            Some(p) => (p, false),
            None => {
                ensure!(
                    repair,
                    "store has no selected index; initialize or rebuild first"
                );
                ensure!(
                    segment_files(&root)?.is_empty(),
                    "segments exist without an index; run rebuild-index (active records require --salvage)"
                );
                (new_generation(&root)?, true)
            }
        };
        let (db, objects, segments) = open_db(&generation, config)?;
        let tombstones = db.keyspace("tombstones", KeyspaceCreateOptions::default)?;
        let retired = db.keyspace("retired", KeyspaceCreateOptions::default)?;
        let journal = if new {
            Some(Journal::create(&root)?)
        } else {
            selected_activation(&root)?
                .filter(|a| a.version == 2)
                .map(|a| {
                    if repair {
                        Journal::open(&root, a.journal.as_deref().unwrap())
                    } else {
                        Journal::read_only(&root, a.journal.as_deref().unwrap())
                    }
                })
                .transpose()?
        };
        if repair && let Some(journal) = &journal {
            journal.replay(&db, &objects, &segments, &tombstones, &retired)?;
        }
        if new {
            db.persist(PersistMode::SyncAll)?;
            sync_dir(&generation)?;
            activate_version(
                &root,
                &generation,
                journal.as_ref().map(|j| j.name.as_str()),
            )?;
        }
        let store = Self {
            db,
            objects,
            segments,
            root,
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
            journal: journal.map(Mutex::new),
            tombstones,
            retired,
            maintenance: Mutex::new(()),
            _lock: lock,
        };
        if repair && store.deletion_enabled() {
            // Reconcile selection and finish known retirements before scanning
            // physical orphan files; retired bytes need no further validation.
            let activation = selected_activation(&store.root)?.context("missing v2 activation")?;
            sync_root_marker(&store.root, &activation)?;
            store.cleanup_retired()?;
        }
        store.recover(repair)?;
        Ok(store)
    }
    fn recover(&self, repair: bool) -> Result<()> {
        let mut writer = self.writer.lock();
        let mut object_count = 0u64;
        for item in self.segments.iter() {
            let (k, v) = item.into_inner()?;
            let id = Uuid::from_slice(&k)?;
            let mut meta = decode_meta(id, &v)?;
            ensure!(
                meta.state != 2,
                "segment {id} is quarantined; offline repair required"
            );
            let path = segment_path(&self.root, &meta);
            ensure!(!path.is_symlink(), "referenced segment is a symlink");
            let mut file = OpenOptions::new()
                .read(true)
                .write(meta.state == 0 && repair)
                .open(&path)
                .with_context(|| format!("missing/inaccessible referenced segment {id}"))?;
            let mut head = [0; 64];
            file.read_exact(&mut head)?;
            let (header_id, created) = parse_header(&head)?;
            ensure!(
                header_id == id && created == meta.created,
                "segment header/manifest mismatch"
            );
            let physical = file.metadata()?.len();
            ensure!(
                physical >= meta.end,
                "committed data truncated in segment {id}"
            );
            let mut live = 0u64;
            let (count, end) =
                scan_prefix_visit(&mut file, id, meta.end, false, &mut |hash, loc| {
                    if let Some(v) = self.objects.get(hash)?
                        && decode_loc(&v)? == loc
                    {
                        object_count += 1;
                        live += loc.length;
                    }
                    Ok(())
                })?;
            ensure!(
                end == meta.end && count == meta.count,
                "committed segment count/end mismatch"
            );
            ensure!(
                live == meta.live,
                "segment live-byte metadata mismatch for {id}"
            );
            if meta.state == 1 {
                ensure!(
                    physical == meta.end + 96 && meta.physical == physical,
                    "sealed segment length mismatch"
                );
                let mut buf = [0; 96];
                file.seek(SeekFrom::Start(meta.end))?;
                file.read_exact(&mut buf)?;
                let f = parse_footer(&buf)?;
                ensure!(
                    f.id == id
                        && f.end == meta.end
                        && f.count == meta.count
                        && Some(f.digest) == meta.digest
                        && f.sealed == meta.sealed,
                    "sealed segment metadata mismatch"
                );
                ensure!(
                    digest_prefix(&mut file, meta.end)? == f.digest,
                    "sealed segment digest mismatch"
                );
            } else {
                let mut sealed = false;
                if physical == meta.end + 96 {
                    let mut b = [0; 96];
                    file.seek(SeekFrom::Start(meta.end))?;
                    file.read_exact(&mut b)?;
                    if let Ok(f) = parse_footer(&b) {
                        ensure!(
                            f.id == id && f.end == meta.end && f.count == meta.count,
                            "interrupted seal footer mismatch"
                        );
                        ensure!(
                            digest_prefix(&mut file, meta.end)? == f.digest,
                            "interrupted seal digest mismatch"
                        );
                        meta.state = 1;
                        meta.digest = Some(f.digest);
                        meta.sealed = f.sealed;
                        meta.physical = physical;
                        sealed = true;
                    }
                }
                if sealed && repair {
                    self.publish(&[Mutation::segment(&meta)])?;
                } else if !sealed {
                    ensure!(
                        writer.active.is_none(),
                        "multiple active segments in single-volume store"
                    );
                    if repair && (physical != meta.end || meta.physical != meta.end) {
                        file.set_len(meta.end)?;
                        file.sync_all()?;
                        meta.physical = meta.end;
                        self.publish(&[Mutation::segment(&meta)])?;
                    }
                    file.seek(SeekFrom::Start(meta.end))?;
                    writer.active = Some(Active {
                        file,
                        meta: meta.clone(),
                    });
                }
            }
        }
        let mut indexed = 0u64;
        for item in self.objects.iter() {
            let (k, v) = item.into_inner()?;
            ensure!(k.len() == 32, "invalid hash index key");
            decode_loc(&v)?;
            indexed += 1;
        }
        ensure!(
            indexed == object_count,
            "object index contains missing segments or invalid record locations"
        );
        self.object_count.store(object_count, Ordering::Relaxed);
        if repair {
            for p in segment_files(&self.root)? {
                let mut h = [0; 64];
                File::open(&p)?.read_exact(&mut h)?;
                let (id, _) = parse_header(&h)?;
                if !self.segments.contains_key(id.as_bytes())? {
                    tracing::warn!(segment=%id,"unreferenced segment retained; inspect with verify/rebuild-index");
                }
            }
            for entry in fs::read_dir(self.root.join("segments"))? {
                let p = entry?.path();
                if p.is_dir() && !p.is_symlink() {
                    for entry in fs::read_dir(&p)? {
                        let staged = entry?.path();
                        if staged
                            .file_name()
                            .and_then(|s| s.to_str())
                            .is_some_and(|s| s.starts_with(".import-") && s.ends_with(".tmp"))
                        {
                            ensure!(!staged.is_symlink(), "staged import symlink rejected");
                            fs::remove_file(&staged)?;
                            sync_dir(&p)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    fn create_active(&self, writer: &mut Writer) -> Result<()> {
        let id = Uuid::new_v4();
        let path = relative(id);
        let full = self.root.join("segments").join(&path);
        let parent = full.parent().unwrap();
        fs::create_dir_all(parent)?;
        sync_dir(&self.root.join("segments"))?;
        let created = now();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&full)?;
        file.write_all(&header(id, created))?;
        file.sync_all()?;
        sync_dir(parent)?;
        let meta = Meta {
            id,
            state: 0,
            path,
            physical: 64,
            end: 64,
            count: 0,
            live: 0,
            created,
            sealed: 0,
            digest: None,
            garbage_since: 0,
            compacted: 0,
        };
        let _visible = self.visibility.write();
        self.publish(&[Mutation::segment(&meta)])?;
        writer.active = Some(Active { file, meta });
        Ok(())
    }
    fn seal_locked(&self, writer: &mut Writer) -> Result<()> {
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
        // Close writer before marking immutable (important on Windows).
        drop(a.file);
        a.meta.state = 1;
        a.meta.physical = a.meta.end + 96;
        a.meta.digest = Some(digest);
        let _visible = self.visibility.write();
        self.publish(&[Mutation::segment(&a.meta)])?;
        Ok(())
    }
    pub fn put_many(&self, payloads: &[Bytes]) -> Result<Vec<PutResult>> {
        ensure!(
            payloads.len() <= self.config.writes.batch_max_records,
            "write batch exceeds configured record limit"
        );
        for p in payloads {
            ensure!(
                p.len() <= self.config.max_object_bytes(),
                "object exceeds configured maximum"
            );
            ensure!(
                (p.len() as u64)
                    .checked_add(64 + 64 + 96)
                    .is_some_and(|n| n <= self.config.segments.max_size_bytes),
                "object does not fit segment"
            );
        }
        let mut writer = self.writer.lock();
        ensure!(
            !writer.faulted,
            "write coordinator faulted; restart and recover before writing"
        );
        let result = (|| {
            let mut results = vec![];
            let mut pending: Vec<([u8; 32], Location)> = vec![];
            let mut seen = HashSet::new();
            for p in payloads {
                let hash = *blake3::hash(p).as_bytes();
                if self.objects.contains_key(hash)? || !seen.insert(hash) {
                    results.push(PutResult {
                        hash,
                        duplicate: true,
                    });
                    continue;
                }
                if writer.active.as_ref().is_some_and(|a| {
                    a.meta.end + 64 + p.len() as u64 + 96 > self.config.segments.max_size_bytes
                }) {
                    self.commit_pending(&mut writer, &mut pending)?;
                    self.seal_locked(&mut writer)?;
                }
                if writer.active.is_none() {
                    self.create_active(&mut writer)?;
                }
                let a = writer.active.as_mut().unwrap();
                let needed = a.meta.end + 64 + p.len() as u64;
                if self.config.segments.growth_factor == 2 && needed > a.meta.physical {
                    let mut capacity = a.meta.physical.max(64);
                    while capacity < needed {
                        capacity = capacity
                            .saturating_mul(2)
                            .min(self.config.segments.max_size_bytes - 96);
                    }
                    a.file.set_len(capacity)?;
                    a.meta.physical = capacity;
                }
                a.file.seek(SeekFrom::Start(a.meta.end))?;
                a.file.write_all(&record_header(p, &hash))?;
                failpoint("after_record_header")?;
                let offset = a.meta.end + 64;
                a.file.write_all(p)?;
                failpoint("after_payload")?;
                a.meta.end = offset + p.len() as u64;
                a.meta.physical = a.meta.physical.max(a.meta.end);
                a.meta.count += 1;
                a.meta.live += p.len() as u64;
                pending.push((
                    hash,
                    Location {
                        segment: *a.meta.id.as_bytes(),
                        offset,
                        length: p.len() as u64,
                    },
                ));
                results.push(PutResult {
                    hash,
                    duplicate: false,
                });
            }
            self.commit_pending(&mut writer, &mut pending)?;
            failpoint("before_ack")?;
            Ok(results)
        })();
        if result.is_err() {
            writer.faulted = true;
        }
        result
    }
    fn commit_pending(
        &self,
        writer: &mut Writer,
        pending: &mut Vec<([u8; 32], Location)>,
    ) -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let a = writer
            .active
            .as_mut()
            .context("no active segment for pending writes")?;
        a.file.sync_all()?;
        failpoint("after_segment_sync")?;
        let _visible = self.visibility.write();
        let mut changes: Vec<_> = pending
            .iter()
            .map(|(hash, l)| Mutation::Put {
                hash: *hash,
                location: encode_loc(*l),
            })
            .collect();
        changes.push(Mutation::segment(&a.meta));
        failpoint("before_index_commit")?;
        self.publish(&changes)?;
        self.object_count
            .fetch_add(pending.len() as u64, Ordering::Relaxed);
        failpoint("after_index_sync")?;
        pending.clear();
        Ok(())
    }
    /// Probe only when no writer is publishing. Index visibility alone is not
    /// sufficient: a failed journal sync can leave an unacknowledged mapping.
    /// A busy writer falls back to the normal coordinated write path.
    pub(crate) fn committed_duplicate(&self, payload: &[u8]) -> Result<Option<PutResult>> {
        let hash = *blake3::hash(payload).as_bytes();
        let Some(writer) = self.writer.try_lock() else {
            return Ok(None);
        };
        ensure!(
            !writer.faulted,
            "write coordinator faulted; restart and recover before writing"
        );
        Ok(self.exists(&hash)?.then_some(PutResult {
            hash,
            duplicate: true,
        }))
    }
    pub fn location(&self, hash: &[u8; 32]) -> Result<Option<Location>> {
        let _visible = self.visibility.read();
        self.objects.get(hash)?.map(|v| decode_loc(&v)).transpose()
    }
    pub fn exists(&self, hash: &[u8; 32]) -> Result<bool> {
        Ok(self.location(hash)?.is_some())
    }
    fn acquire_file(&self, id: Uuid, path: &Path) -> Result<FileLease<'_>> {
        let mut cache = self.files.lock();
        loop {
            if let Some(n) = cache.entries.iter().position(|(key, _)| *key == id) {
                let entry = cache.entries.remove(n).unwrap();
                let file = entry.1.clone();
                cache.entries.push_back(entry);
                return Ok(FileLease {
                    file: Some(file),
                    available: &self.files_available,
                });
            }
            if cache.entries.len() < cache.max {
                let f = Arc::new(File::open(path)?);
                cache.entries.push_back((id, f.clone()));
                return Ok(FileLease {
                    file: Some(f),
                    available: &self.files_available,
                });
            }
            if let Some(n) = cache
                .entries
                .iter()
                .position(|(_, f)| Arc::strong_count(f) == 1)
            {
                cache.entries.remove(n);
                self.files_available.notify_all();
                continue;
            }
            self.files_available
                .wait_for(&mut cache, Duration::from_millis(50));
        }
    }
    pub fn get(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>> {
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
        let v = self
            .segments
            .get(id.as_bytes())?
            .context("missing segment metadata")?;
        let m = decode_meta(id, &v)?;
        ensure!(m.state != 2, "segment quarantined");
        ensure!(
            loc.offset
                .checked_add(loc.length)
                .is_some_and(|end| end <= m.end),
            "location beyond committed end"
        );
        ensure!(
            loc.length <= self.config.max_object_bytes() as u64,
            "stored object exceeds configured read limit"
        );
        let file = self.acquire_file(id, &segment_path(&self.root, &m))?;
        drop(visible);
        let result = (|| {
            let mut h = [0; 64];
            read_exact_at(&file, &mut h, loc.offset - 64)?;
            let (len, expected) = parse_record(&h)?;
            ensure!(
                len == loc.length && &expected == hash,
                "record/index mismatch"
            );
            let mut payload = vec![0; usize::try_from(len)?];
            read_exact_at(&file, &mut payload, loc.offset)?;
            ensure!(
                blake3::hash(&payload).as_bytes() == hash,
                "payload BLAKE3 mismatch"
            );
            Ok(Some(payload))
        })();
        drop(file);
        self.files_available.notify_all();
        result
    }
    /// Return at most `limit` segment summaries in raw UUID/index order.
    /// The cursor is an exclusive position, not a chronological timestamp.
    pub fn list_segments(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<crate::inspection::SegmentPage> {
        use std::ops::Bound::{Excluded, Unbounded};
        ensure!(
            (1..=1000).contains(&limit),
            "segment page limit must be 1..1000"
        );
        let cursor = cursor.map(parse_segment_cursor).transpose()?;
        let _visible = self.visibility.read();
        let metas = {
            if let Some(id) = cursor {
                ensure!(
                    self.segments.contains_key(id.as_bytes())?,
                    "unknown segment cursor"
                );
            }
            let mut metas = Vec::with_capacity(limit + 1);
            let range = if let Some(id) = cursor {
                self.segments
                    .range((Excluded(id.as_bytes().to_vec()), Unbounded))
            } else {
                self.segments.iter()
            };
            for item in range.take(limit + 1) {
                let (key, value) = item.into_inner()?;
                metas.push(decode_meta(Uuid::from_slice(&key)?, &value)?);
            }
            metas
        };
        let has_more = metas.len() > limit;
        let mut segments = Vec::with_capacity(metas.len().min(limit));
        for meta in metas.into_iter().take(limit) {
            segments.push(self.inspection_summary(&meta)?);
        }
        let next_cursor = if has_more {
            segments.last().map(|s| {
                format!(
                    "v1:{}",
                    Uuid::parse_str(&s.segment_id)
                        .expect("encoded UUID")
                        .simple()
                )
            })
        } else {
            None
        };
        Ok(crate::inspection::SegmentPage {
            segments,
            next_cursor,
        })
    }

    pub fn segment_info(&self, id: Uuid) -> Result<Option<crate::inspection::SegmentInfo>> {
        let _visible = self.visibility.read();
        let meta = {
            self.segments
                .get(id.as_bytes())?
                .map(|v| decode_meta(id, &v))
                .transpose()?
        };
        meta.map(|m| self.inspection_summary(&m)).transpose()
    }

    fn inspection_summary(&self, meta: &Meta) -> Result<crate::inspection::SegmentInfo> {
        Ok(crate::inspection::SegmentInfo {
            segment_id: meta.id.to_string(),
            state: match meta.state {
                0 => "active",
                1 => "sealed",
                _ => "quarantined",
            }
            .to_owned(),
            physical_bytes: fs::metadata(segment_path(&self.root, meta))?.len(),
            committed_bytes: meta.end,
            object_count: meta.count,
            live_payload_bytes: meta.live,
            created_timestamp: meta.created,
            sealed_timestamp: (meta.state == 1).then_some(meta.sealed),
            whole_segment_digest: meta.digest.map(hex::encode),
            utilization: meta.end as f64 / self.config.segments.max_size_bytes as f64,
        })
    }

    pub fn object_info(&self, hash: [u8; 32]) -> Result<crate::inspection::ObjectInfo> {
        let _visible = self.visibility.read();
        let location = {
            self.objects
                .get(hash)?
                .map(|v| decode_loc(&v))
                .transpose()?
        };
        if let Some(location) = location {
            let id = Uuid::from_bytes(location.segment);
            let value = self
                .segments
                .get(id.as_bytes())?
                .context("missing segment metadata")?;
            let meta = decode_meta(id, &value)?;
            ensure!(meta.state != 2, "segment quarantined");
            ensure!(
                location
                    .offset
                    .checked_add(location.length)
                    .is_some_and(|end| end <= meta.end),
                "object location exceeds committed extent"
            );
            Ok(crate::inspection::ObjectInfo {
                hash: hex::encode(hash),
                exists: true,
                payload_length: Some(location.length),
                segment_id: Some(id.to_string()),
                payload_offset: Some(location.offset),
            })
        } else {
            Ok(crate::inspection::ObjectInfo {
                hash: hex::encode(hash),
                exists: false,
                payload_length: None,
                segment_id: None,
                payload_offset: None,
            })
        }
    }

    pub fn verify_segment(
        &self,
        id: Uuid,
        progress: Arc<AtomicU64>,
    ) -> Result<crate::inspection::VerificationReport> {
        self.verify_segment_bounded(
            id,
            progress,
            Arc::new(AtomicBool::new(false)),
            Duration::from_secs(300),
        )
    }

    /// Verify a committed metadata snapshot without modifying files or index state.
    /// Both buffer size and descriptor leases are shared with ordinary reads.
    /// The deadline/cancellation checks also cover descriptor-capacity waits.
    pub fn verify_segment_bounded(
        &self,
        id: Uuid,
        progress: Arc<AtomicU64>,
        cancelled: Arc<AtomicBool>,
        max_duration: Duration,
    ) -> Result<crate::inspection::VerificationReport> {
        ensure!(
            !max_duration.is_zero(),
            "verification duration must be positive"
        );
        let deadline = Instant::now()
            .checked_add(max_duration)
            .context("verification deadline overflow")?;
        let check = || -> Result<()> {
            ensure!(
                !cancelled.load(Ordering::Acquire),
                "segment verification cancelled"
            );
            ensure!(
                Instant::now() < deadline,
                "segment verification deadline exceeded"
            );
            Ok(())
        };
        check()?;
        progress.store(0, Ordering::Release);
        let lease_visibility = self.visibility.read();
        let meta = {
            let value = self
                .segments
                .get(id.as_bytes())?
                .context("segment not found")?;
            decode_meta(id, &value)?
        };
        ensure!(meta.state != 2, "segment quarantined");
        let path = segment_path(&self.root, &meta);
        let file = {
            let mut cache = self.files.lock();
            loop {
                check()?;
                if let Some(n) = cache.entries.iter().position(|(key, _)| *key == id) {
                    let entry = cache.entries.remove(n).unwrap();
                    let file = entry.1.clone();
                    cache.entries.push_back(entry);
                    break file;
                }
                if cache.entries.len() < cache.max {
                    let file = Arc::new(File::open(&path)?);
                    cache.entries.push_back((id, file.clone()));
                    break file;
                }
                if let Some(n) = cache
                    .entries
                    .iter()
                    .position(|(_, file)| Arc::strong_count(file) == 1)
                {
                    cache.entries.remove(n);
                    continue;
                }
                self.files_available
                    .wait_for(&mut cache, Duration::from_millis(50));
            }
        };
        let file = FileLease {
            file: Some(file),
            available: &self.files_available,
        };
        drop(lease_visibility);
        let result = (|| {
            let physical = file.metadata()?.len();
            ensure!(
                meta.end >= SEGMENT_HEADER && physical >= meta.end,
                "committed segment extent truncated"
            );
            let mut header = [0; 64];
            read_exact_at(&file, &mut header, 0)?;
            let (header_id, created) = parse_header(&header)?;
            ensure!(
                header_id == id && created == meta.created,
                "segment identity differs from index"
            );
            let mut whole = blake3::Hasher::new();
            whole.update(&header);
            progress.store(64, Ordering::Release);
            let mut position = SEGMENT_HEADER;
            let mut count = 0;
            let mut buffer = [0; 64 * 1024];
            while position < meta.end {
                check()?;
                ensure!(
                    meta.end - position >= RECORD_HEADER,
                    "truncated committed record header"
                );
                let mut record = [0; 64];
                read_exact_at(&file, &mut record, position)?;
                let (length, expected_hash) = parse_record(&record)?;
                let payload = position
                    .checked_add(RECORD_HEADER)
                    .context("record offset overflow")?;
                let next = payload
                    .checked_add(length)
                    .context("record length overflow")?;
                ensure!(next <= meta.end, "committed record exceeds segment extent");
                whole.update(&record);
                progress.fetch_add(RECORD_HEADER, Ordering::Release);
                let mut object = blake3::Hasher::new();
                let mut offset = payload;
                while offset < next {
                    check()?;
                    let n = (next - offset).min(buffer.len() as u64) as usize;
                    read_exact_at(&file, &mut buffer[..n], offset)?;
                    object.update(&buffer[..n]);
                    whole.update(&buffer[..n]);
                    offset += n as u64;
                    progress.fetch_add(n as u64, Ordering::Release);
                }
                ensure!(
                    object.finalize().as_bytes() == &expected_hash,
                    "segment payload BLAKE3 mismatch"
                );
                count += 1;
                position = next;
            }
            ensure!(
                position == meta.end && count == meta.count,
                "segment committed count/end mismatch"
            );
            if meta.state == 1 {
                check()?;
                ensure!(
                    physical
                        == meta
                            .end
                            .checked_add(SEGMENT_FOOTER)
                            .context("footer offset overflow")?
                        && physical == meta.physical,
                    "sealed segment length mismatch"
                );
                let mut footer_bytes = [0; 96];
                read_exact_at(&file, &mut footer_bytes, meta.end)?;
                let footer = parse_footer(&footer_bytes)?;
                ensure!(
                    footer.id == id
                        && footer.end == meta.end
                        && footer.count == count
                        && footer.sealed == meta.sealed,
                    "seal footer differs from index"
                );
                let digest = *whole.finalize().as_bytes();
                ensure!(
                    footer.digest == digest && meta.digest == Some(digest),
                    "whole-segment BLAKE3 mismatch"
                );
                progress.fetch_add(SEGMENT_FOOTER, Ordering::Release);
            }
            check()?;
            Ok(crate::inspection::VerificationReport {
                segment_id: id.to_string(),
                verified_bytes: progress.load(Ordering::Acquire),
                object_count: count,
                sealed: meta.state == 1,
            })
        })();
        drop(file);
        self.files_available.notify_all();
        result
    }

    pub fn status(&self) -> Result<StorageStatus> {
        let _visible = self.visibility.read();
        let mut status = StorageStatus {
            index_bytes: self.db.disk_space()?,
            open_files: self.files.lock().entries.len(),
            ..StorageStatus::default()
        };
        status.objects = self.object_count.load(Ordering::Relaxed);
        for item in self.segments.iter() {
            let (k, v) = item.into_inner()?;
            let meta = decode_meta(Uuid::from_slice(&k)?, &v)?;
            status.segments += 1;
            status.payload_bytes += meta.live;
            status.physical_bytes += fs::metadata(segment_path(&self.root, &meta))?.len();
            if meta.state == 0 {
                status.active_segment = Some(meta.id.to_string());
                status.active_bytes = meta.end;
                status.open_files += 1;
            }
        }
        Ok(status)
    }
    pub fn sync(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        ensure!(
            !writer.faulted,
            "write coordinator faulted; final synchronization cannot be reported as successful"
        );
        let _visible = self.visibility.write();
        let result = (|| {
            if let Some(a) = &writer.active {
                a.file.sync_all()?;
            }
            self.db.persist(PersistMode::SyncAll)?;
            Ok(())
        })();
        if result.is_err() {
            writer.faulted = true;
        }
        result
    }
    pub fn seal(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        ensure!(!writer.faulted, "write coordinator faulted");
        let result = self.seal_locked(&mut writer);
        if result.is_err() {
            writer.faulted = true;
        }
        result
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut out: &mut [u8], mut offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;
    while !out.is_empty() {
        match file.read_at(out, offset) {
            Ok(0) => bail!("truncated positional read"),
            Ok(n) => {
                offset += n as u64;
                out = &mut out[n..];
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
#[cfg(windows)]
fn read_exact_at(file: &File, mut out: &mut [u8], mut offset: u64) -> Result<()> {
    use std::os::windows::fs::FileExt;
    while !out.is_empty() {
        match file.seek_read(out, offset) {
            Ok(0) => bail!("truncated positional read"),
            Ok(n) => {
                offset += n as u64;
                out = &mut out[n..];
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

pub fn verify(config: &Config) -> Result<serde_json::Value> {
    // Verify committed bytes under exclusive ownership, without truncating or adopting tails.
    ensure!(config.storage.root.is_dir(), "store does not exist");
    if selected_activation(&config.storage.root)?.is_some_and(|a| a.version == 2) {
        return maintenance::verify_v2(config);
    }
    let store = Store::open_mode(config, false)?;
    let status = store.status()?;
    let mut orphaned = vec![];
    for p in segment_files(&config.storage.root)? {
        let mut h = [0; 64];
        File::open(&p)?.read_exact(&mut h)?;
        let (id, _) = parse_header(&h)?;
        if !store.segments.contains_key(id.as_bytes())? {
            scan_file(&p, &config.storage.root, true, true)?;
            orphaned.push(p.display().to_string());
        }
    }
    Ok(serde_json::json!({"status":"verified","storage":status,"unreferenced_segments":orphaned}))
}

pub fn rebuild(config: &Config, salvage: bool) -> Result<serde_json::Value> {
    config.validate()?;
    let root = &config.storage.root;
    let _lock = store_lock(root)?;
    if let Some(a) = selected_activation(root)?
        && a.version == 2
    {
        return maintenance::rebuild_v2(config, a);
    }
    let files = segment_files(root)?;
    let generation = new_generation(root)?;
    let (db, objects, segments) = open_db(&generation, config)?;
    let mut ids = HashSet::new();
    let mut skipped = vec![];
    let mut total = 0u64;
    let mut duplicates = 0u64;
    let mut active = 0;
    // Deterministic winner: lexicographic segment path then increasing payload offset.
    for p in files {
        let Some(mut scanned) = scan_file(&p, root, salvage, true)? else {
            skipped.push(p.display().to_string());
            continue;
        };
        ensure!(
            ids.insert(scanned.meta.id),
            "duplicate segment UUID in rebuild"
        );
        if scanned.meta.state == 0 {
            active += 1;
            ensure!(
                active <= 1,
                "salvage encountered multiple active files; export/select one active segment first"
            );
        }
        let mut file = File::open(&p)?;
        let mut live = 0u64;
        let mut batch = db.batch();
        let mut batch_seen = HashSet::new();
        let mut batch_count = 0;
        scan_prefix_visit(
            &mut file,
            scanned.meta.id,
            scanned.meta.end,
            false,
            &mut |hash, l| {
                if objects.contains_key(hash)? || !batch_seen.insert(hash) {
                    duplicates += 1;
                } else {
                    batch.insert(&objects, hash, encode_loc(l));
                    live += l.length;
                    total += 1;
                    batch_count += 1;
                }
                if batch_count >= config.writes.batch_max_records {
                    scanned.meta.live = live;
                    batch.insert(
                        &segments,
                        scanned.meta.id.as_bytes(),
                        encode_meta(&scanned.meta),
                    );
                    std::mem::replace(&mut batch, db.batch()).commit()?;
                    batch_seen.clear();
                    batch_count = 0;
                }
                Ok(())
            },
        )?;
        scanned.meta.live = live;
        batch.insert(
            &segments,
            scanned.meta.id.as_bytes(),
            encode_meta(&scanned.meta),
        );
        batch.commit()?;
    }
    db.persist(PersistMode::SyncAll)?;
    drop(objects);
    drop(segments);
    drop(db);
    sync_dir(&generation)?;
    // Validate the candidate without activating it. This catches inconsistent object mappings.
    let (db, objects, segments) = open_db(&generation, config)?;
    let mut counts = 0u64;
    for item in objects.iter() {
        let (_, v) = item.into_inner()?;
        decode_loc(&v)?;
        counts += 1;
    }
    drop(objects);
    drop(segments);
    drop(db);
    activate(root, &generation)?;
    Ok(
        serde_json::json!({"status":"rebuilt","generation":generation.file_name().unwrap().to_string_lossy(),"objects":counts,"excluded_active_segments":skipped,"salvage":salvage,"duplicate_records":duplicates,"scanned_records":total}),
    )
}

pub fn import(config: &Config, source: &Path) -> Result<serde_json::Value> {
    ensure!(source.is_dir(), "source directory does not exist");
    let source_root = if source.file_name().is_some_and(|s| s == "segments")
        && source
            .parent()
            .is_some_and(|p| p.join("store.lock").exists())
    {
        source.parent().unwrap().to_path_buf()
    } else {
        source.to_path_buf()
    };
    ensure!(
        fs::canonicalize(&source_root)?
            != fs::canonicalize(&config.storage.root)
                .unwrap_or_else(|_| config.storage.root.clone()),
        "cannot import store into itself"
    );
    let _source_lock = if source_root.join("store.lock").exists() {
        Some(store_lock(&source_root).context("source must be offline during import")?)
    } else {
        None
    };
    let source_namespace = maintenance::ScratchNamespace::source(config, &source_root)?;
    let store = Store::open(config)?;
    let mut imported = 0;
    let mut duplicates = 0u64;
    let mut redundant = 0u64;
    let mut skipped = 0;
    let mut seen_ids = HashMap::new();
    let source_files = if source_root.join("segments").is_dir() {
        segment_files(&source_root)?
    } else {
        export_files(&source_root, 0)?
    };
    for p in source_files {
        if let Some(namespace) = &source_namespace {
            let id = p
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| Uuid::parse_str(s).ok());
            if !id
                .map(|id| namespace.contains_segment(id))
                .transpose()?
                .unwrap_or(false)
            {
                continue;
            }
        }
        let Some(mut scanned) = scan_file(&p, &source_root, false, false)? else {
            skipped += 1;
            continue;
        };
        let id = scanned.meta.id;
        if let Some(namespace) = &source_namespace
            && !namespace.contains_segment(id)?
        {
            continue;
        }
        if let Some(previous) = seen_ids.insert(id, p.clone()) {
            ensure!(
                files_equal(&previous, &p)?,
                "conflicting source segment UUID"
            );
            continue;
        }
        let mut already_present = false;
        if let Some(value) = store.segments.get(id.as_bytes())? {
            let existing = decode_meta(id, &value)?;
            ensure!(
                existing.state == 1
                    && existing.digest == scanned.meta.digest
                    && existing.end == scanned.meta.end,
                "conflicting destination segment UUID {id}"
            );
            ensure!(
                files_equal(&segment_path(&store.root, &existing), &p)?,
                "same UUID but nonidentical segment bytes"
            );
            scanned.meta.live = existing.live;
            already_present = true;
        }
        scanned.meta.path = relative(id);
        let final_path = segment_path(&store.root, &scanned.meta);
        let parent = final_path.parent().unwrap();
        fs::create_dir_all(parent)?;
        let staging = parent.join(format!(".import-{}.tmp", Uuid::new_v4()));
        {
            let mut src = File::open(&p)?;
            let mut dst = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staging)?;
            std::io::copy(&mut src, &mut dst)?;
            dst.sync_all()?;
        }
        // Source was verified before copy; verify the staged destination independently too.
        {
            let mut file = File::open(&staging)?;
            ensure!(
                digest_prefix(&mut file, scanned.meta.end)? == scanned.meta.digest.unwrap(),
                "staged import digest mismatch"
            );
            let mut buf = [0; 96];
            file.seek(SeekFrom::Start(scanned.meta.end))?;
            file.read_exact(&mut buf)?;
            ensure!(
                parse_footer(&buf)?.id == id,
                "staged footer identity mismatch"
            );
        }
        failpoint("import_after_copy")?;
        if final_path.exists() {
            ensure!(
                files_equal(&final_path, &staging)?,
                "conflicting unreferenced destination segment"
            );
            fs::remove_file(&staging)?;
        } else {
            fs::rename(&staging, &final_path)?;
        }
        sync_dir(parent)?;
        sync_dir(&store.root.join("segments"))?;
        failpoint("import_after_publish_file")?;
        let mut file = File::open(&final_path)?;
        let mut pending = vec![];
        let data_end = scanned.meta.end;
        let mut publish = |pending: &mut Vec<([u8; 32], Location)>| -> Result<()> {
            let _visible = store.visibility.write();
            let mut changes = vec![];
            let mut added = 0;
            let mut seen = HashSet::new();
            for (hash, l) in pending.iter() {
                let existing = store
                    .objects
                    .get(hash)?
                    .map(|v| decode_loc(&v))
                    .transpose()?;
                if existing == Some(*l) {
                    // An idempotent retry encountered this segment's live record.
                    continue;
                }
                if existing.is_some() || store.tombstones.contains_key(hash)? || !seen.insert(*hash)
                {
                    duplicates += 1;
                    redundant += l.length;
                } else {
                    scanned.meta.live += l.length;
                    changes.push(Mutation::Put {
                        hash: *hash,
                        location: encode_loc(*l),
                    });
                    added += 1;
                }
            }
            changes.push(Mutation::segment(&scanned.meta));
            store.publish(&changes)?;
            store.object_count.fetch_add(added, Ordering::Relaxed);
            pending.clear();
            Ok(())
        };
        scan_prefix_visit(&mut file, id, data_end, false, &mut |hash, l| {
            if let Some(namespace) = &source_namespace
                && namespace.location(&hash)? != Some(l)
            {
                return Ok(());
            }
            pending.push((hash, l));
            if pending.len() >= config.writes.batch_max_records {
                publish(&mut pending)?;
            }
            Ok(())
        })?;
        publish(&mut pending)?;
        if !already_present {
            imported += 1;
        }
    }
    Ok(
        serde_json::json!({"imported_segments":imported,"skipped_active_segments":skipped,"redundant_records":duplicates,"redundant_payload_bytes":redundant}),
    )
}

fn files_equal(a: &Path, b: &Path) -> Result<bool> {
    let mut a = File::open(a)?;
    let mut b = File::open(b)?;
    if a.metadata()?.len() != b.metadata()?.len() {
        return Ok(false);
    }
    let mut left = a.metadata()?.len();
    let mut ba = [0; 65536];
    let mut bb = [0; 65536];
    while left > 0 {
        let n = left.min(65536) as usize;
        a.read_exact(&mut ba[..n])?;
        b.read_exact(&mut bb[..n])?;
        if ba[..n] != bb[..n] {
            return Ok(false);
        }
        left -= n as u64;
    }
    Ok(true)
}

fn export_files(root: &Path, depth: usize) -> Result<Vec<PathBuf>> {
    ensure!(depth <= 16, "source directory nesting exceeds 16 levels");
    let mut out = vec![];
    for entry in fs::read_dir(root)? {
        let p = entry?.path();
        ensure!(!p.is_symlink(), "source symlinks are not supported");
        if p.is_dir() {
            out.extend(export_files(&p, depth + 1)?);
        } else if p.extension().is_some_and(|s| s == "seg") {
            out.push(p);
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod format_tests {
    use super::*;
    #[test]
    fn duplicate_probe_requires_idle_healthy_writer() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        let store = Store::open(&config).unwrap();
        let bytes = Bytes::from_static(b"durable duplicate");
        assert!(store.committed_duplicate(&bytes).unwrap().is_none());
        store.put_many(std::slice::from_ref(&bytes)).unwrap();
        assert!(
            store
                .committed_duplicate(&bytes)
                .unwrap()
                .unwrap()
                .duplicate
        );
        {
            let mut writer = store.writer.lock();
            assert!(store.committed_duplicate(&bytes).unwrap().is_none());
            // An index mapping may exist after a failed sync; never acknowledge it.
            writer.faulted = true;
        }
        assert!(store.committed_duplicate(&bytes).is_err());
    }
    #[test]
    fn golden_v1_headers() {
        let id = Uuid::parse_str("00112233-4455-4677-8899-aabbccddeeff").unwrap();
        let h = header(id, 0x0102030405060708);
        assert_eq!(
            hex::encode(&h),
            "534b5259534547310100400000112233445546778899aabbccddeeff0807060504030201000000000000000000000000000000000000000000000000a2f8fd80"
        );
        assert_eq!(parse_header(&h).unwrap(), (id, 0x0102030405060708));
        let hash = *blake3::hash(b"abc").as_bytes();
        let r = record_header(b"abc", &hash);
        assert_eq!(
            hex::encode(&r),
            "534b5259524543310100400003000000000000006437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d850000000000000000dde8f237"
        );
        assert_eq!(parse_record(&r).unwrap(), (3, hash));
    }
    #[test]
    fn parser_rejects_versions_checksums_flags_and_length_overflow() {
        let mut h = header(Uuid::new_v4(), 1);
        h[8] = 2;
        finish_crc(&mut h);
        assert!(parse_header(&h).is_err());
        let mut r = record_header(b"abc", blake3::hash(b"abc").as_bytes());
        r[52] = 1;
        finish_crc(&mut r);
        assert!(parse_record(&r).is_err());
        let r = record_header(b"abc", blake3::hash(b"abc").as_bytes());
        assert!(parse_record(&r[..63]).is_err());
        let l = Location {
            segment: *Uuid::new_v4().as_bytes(),
            offset: 128,
            length: u64::MAX,
        };
        assert!(decode_loc(&encode_loc(l)).is_err());
    }
}
