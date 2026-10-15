use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Mutex, TryLockError},
};
pub mod api;
pub mod broker;
const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub payload: Vec<u8>,
}
fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(14695981039346656037u64, |h, b| {
        (h ^ (*b as u64)).wrapping_mul(1099511628211)
    })
}
pub struct Partition {
    file: File,
    path: PathBuf,
    base_offset: u64,
    next: u64,
    max_record_bytes: usize,
}
impl Partition {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_limit(path, DEFAULT_MAX_RECORD_BYTES)
    }
    pub fn open_with_limit(path: impl AsRef<Path>, max_record_bytes: usize) -> io::Result<Self> {
        if max_record_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "record limit must be positive",
            ));
        }
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let base_offset = read_base_offset(&path)?;
        let next = base_offset + read_all(&file, base_offset)?.len() as u64;
        Ok(Self {
            file,
            path,
            base_offset,
            next,
            max_record_bytes,
        })
    }
    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        if payload.len() > self.max_record_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "record exceeds partition limit",
            ));
        }
        let offset = self.next;
        self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
        self.file.write_all(&checksum(payload).to_le_bytes())?;
        self.file.write_all(payload)?;
        self.file.sync_data()?;
        self.next += 1;
        Ok(offset)
    }
    pub fn read_all(&self) -> io::Result<Vec<Record>> {
        read_all(&self.file, self.base_offset)
    }
    pub fn read_from(&self, offset: u64, limit: usize) -> io::Result<Vec<Record>> {
        Ok(self
            .read_all()?
            .into_iter()
            .filter(|record| record.offset >= offset)
            .take(limit)
            .collect())
    }
    pub fn retain_last(&mut self, keep: usize) -> io::Result<usize> {
        let records = self.read_all()?;
        let dropped = records.len().saturating_sub(keep);
        if dropped == 0 {
            return Ok(0);
        }
        let retained = &records[dropped..];
        let temporary = self.path.with_extension("retention.tmp");
        {
            let mut output = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)?;
            for record in retained {
                write_record(&mut output, &record.payload)?;
            }
            output.sync_all()?;
        }
        fs::rename(&temporary, &self.path)?;
        self.base_offset += dropped as u64;
        write_base_offset(&self.path, self.base_offset)?;
        self.file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.path)?;
        Ok(dropped)
    }
    pub fn earliest_offset(&self) -> u64 {
        self.base_offset
    }
    pub fn next_offset(&self) -> u64 {
        self.next
    }
}

pub struct SharedPartition {
    inner: Mutex<Partition>,
}

impl SharedPartition {
    pub fn new(partition: Partition) -> Self {
        Self {
            inner: Mutex::new(partition),
        }
    }
    pub fn append(&self, payload: &[u8]) -> io::Result<u64> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("partition lock poisoned"))?
            .append(payload)
    }
    pub fn try_append(&self, payload: &[u8]) -> io::Result<u64> {
        match self.inner.try_lock() {
            Ok(mut partition) => partition.append(payload),
            Err(TryLockError::WouldBlock) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "partition is applying backpressure",
            )),
            Err(TryLockError::Poisoned(_)) => Err(io::Error::other("partition lock poisoned")),
        }
    }
    pub fn read_from(&self, offset: u64, limit: usize) -> io::Result<Vec<Record>> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("partition lock poisoned"))?
            .read_from(offset, limit)
    }
    pub fn retain_last(&self, keep: usize) -> io::Result<usize> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("partition lock poisoned"))?
            .retain_last(keep)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderLease {
    pub owner: String,
    pub epoch: u64,
    pub expires_at_ms: u64,
}

#[derive(Default)]
pub struct LeaderRegistry {
    leases: HashMap<u32, LeaderLease>,
}

impl LeaderRegistry {
    pub fn claim(
        &mut self,
        partition: u32,
        owner: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> io::Result<LeaderLease> {
        if owner.is_empty() || ttl_ms == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "owner and positive lease ttl are required",
            ));
        }
        if let Some(current) = self.leases.get(&partition)
            && current.expires_at_ms > now_ms
            && current.owner != owner
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "partition already has a live leader",
            ));
        }
        let epoch = self
            .leases
            .get(&partition)
            .map(|lease| {
                lease.epoch + u64::from(lease.owner != owner || lease.expires_at_ms <= now_ms)
            })
            .unwrap_or(1);
        let lease = LeaderLease {
            owner: owner.to_owned(),
            epoch,
            expires_at_ms: now_ms.saturating_add(ttl_ms),
        };
        self.leases.insert(partition, lease.clone());
        Ok(lease)
    }
    pub fn validate(&self, partition: u32, owner: &str, epoch: u64, now_ms: u64) -> bool {
        self.leases.get(&partition).is_some_and(|lease| {
            lease.owner == owner && lease.epoch == epoch && lease.expires_at_ms > now_ms
        })
    }
}

#[derive(Default)]
pub struct ConsumerOffsets {
    committed: HashMap<(String, u32), u64>,
}
impl ConsumerOffsets {
    pub fn commit(&mut self, group: &str, partition: u32, next_offset: u64) -> bool {
        let key = (group.to_owned(), partition);
        let current = self.committed.get(&key).copied().unwrap_or(0);
        if next_offset < current {
            return false;
        }
        self.committed.insert(key, next_offset);
        true
    }
    pub fn committed(&self, group: &str, partition: u32) -> u64 {
        self.committed
            .get(&(group.to_owned(), partition))
            .copied()
            .unwrap_or(0)
    }
}

pub struct PartitionRouter {
    count: u32,
}
impl PartitionRouter {
    pub fn new(count: u32) -> io::Result<Self> {
        if count == 0 {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "partition count must be positive",
            ))
        } else {
            Ok(Self { count })
        }
    }
    pub fn route(&self, key: &[u8]) -> u32 {
        (checksum(key) % (self.count as u64)) as u32
    }
}
fn read_all(file: &File, base_offset: u64) -> io::Result<Vec<Record>> {
    let mut f = file.try_clone()?;
    f.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    f.read_to_end(&mut bytes)?;
    let mut cursor = 0;
    let mut out = Vec::new();
    while cursor < bytes.len() {
        if bytes.len() - cursor < 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated header",
            ));
        }
        let len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        let expected = u64::from_le_bytes(bytes[cursor + 4..cursor + 12].try_into().unwrap());
        cursor += 12;
        if bytes.len() - cursor < len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated payload",
            ));
        }
        let payload = bytes[cursor..cursor + len].to_vec();
        cursor += len;
        if checksum(&payload) != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checksum mismatch",
            ));
        }
        out.push(Record {
            offset: base_offset + out.len() as u64,
            payload,
        });
    }
    Ok(out)
}

fn write_record(file: &mut File, payload: &[u8]) -> io::Result<()> {
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    file.write_all(&checksum(payload).to_le_bytes())?;
    file.write_all(payload)
}

fn base_offset_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.base", path.display()))
}

fn read_base_offset(path: &Path) -> io::Result<u64> {
    match fs::read(base_offset_path(path)) {
        Ok(bytes) if bytes.len() == 8 => Ok(u64::from_le_bytes(bytes.try_into().unwrap())),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid base offset metadata",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error),
    }
}

fn write_base_offset(path: &Path, offset: u64) -> io::Result<()> {
    let target = base_offset_path(path);
    let temporary = PathBuf::from(format!("{}.tmp", target.display()));
    fs::write(&temporary, offset.to_le_bytes())?;
    fs::rename(temporary, target)
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    fn path(suffix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("streamforge-{}-{suffix}", std::process::id()))
    }
    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(base_offset_path(path));
        let _ = std::fs::remove_file(path.with_extension("retention.tmp"));
    }
    #[test]
    fn persists_ordered_records() {
        let p = path("ordered");
        cleanup(&p);
        {
            let mut log = Partition::open(&p).unwrap();
            assert_eq!(log.append(b"a").unwrap(), 0);
            assert_eq!(log.append(b"b").unwrap(), 1);
        }
        let log = Partition::open(&p).unwrap();
        assert_eq!(
            log.read_all()
                .unwrap()
                .iter()
                .map(|r| r.payload.clone())
                .collect::<Vec<_>>(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        cleanup(&p);
    }
    #[test]
    fn reads_bounded_batches_from_an_offset() {
        let p = path("batches");
        cleanup(&p);
        let mut log = Partition::open(&p).unwrap();
        for value in [b"a", b"b", b"c"] {
            log.append(value).unwrap();
        }
        let records = log.read_from(1, 1).unwrap();
        assert_eq!(
            records,
            vec![Record {
                offset: 1,
                payload: b"b".to_vec()
            }]
        );
        cleanup(&p);
    }
    #[test]
    fn rejects_oversized_records_without_advancing_offset() {
        let p = path("limit");
        cleanup(&p);
        let mut log = Partition::open_with_limit(&p, 2).unwrap();
        assert_eq!(
            log.append(b"abc").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(log.next_offset(), 0);
        cleanup(&p);
    }
    #[test]
    fn detects_corruption_on_recovery() {
        let p = path("corrupt");
        cleanup(&p);
        {
            let mut log = Partition::open(&p).unwrap();
            log.append(b"safe").unwrap();
        }
        let mut bytes = std::fs::read(&p).unwrap();
        *bytes.last_mut().unwrap() ^= 0xff;
        std::fs::write(&p, bytes).unwrap();
        assert_eq!(
            Partition::open(&p).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );
        cleanup(&p);
    }
    #[test]
    fn consumer_offsets_are_monotonic_and_isolated() {
        let mut offsets = ConsumerOffsets::default();
        assert!(offsets.commit("billing", 0, 4));
        assert!(!offsets.commit("billing", 0, 3));
        assert!(offsets.commit("search", 0, 2));
        assert_eq!(offsets.committed("billing", 0), 4);
        assert_eq!(offsets.committed("search", 0), 2);
    }
    #[test]
    fn routing_is_stable_and_bounded() {
        let router = PartitionRouter::new(8).unwrap();
        let first = router.route(b"customer-42");
        assert_eq!(first, router.route(b"customer-42"));
        assert!(first < 8);
        assert!(PartitionRouter::new(0).is_err());
    }
    #[test]
    fn retention_preserves_absolute_offsets_across_recovery() {
        let p = path("retention");
        cleanup(&p);
        {
            let mut log = Partition::open(&p).unwrap();
            for value in [b"a", b"b", b"c", b"d"] {
                log.append(value).unwrap();
            }
            assert_eq!(log.retain_last(2).unwrap(), 2);
            assert_eq!(log.earliest_offset(), 2);
            assert_eq!(log.next_offset(), 4);
            assert_eq!(log.append(b"e").unwrap(), 4);
        }
        let log = Partition::open(&p).unwrap();
        assert_eq!(
            log.read_all().unwrap(),
            vec![
                Record {
                    offset: 2,
                    payload: b"c".to_vec()
                },
                Record {
                    offset: 3,
                    payload: b"d".to_vec()
                },
                Record {
                    offset: 4,
                    payload: b"e".to_vec()
                },
            ]
        );
        cleanup(&p);
    }
    #[test]
    fn concurrent_writers_receive_unique_ordered_offsets() {
        let p = path("concurrent");
        cleanup(&p);
        let partition = Arc::new(SharedPartition::new(Partition::open(&p).unwrap()));
        let mut writers = Vec::new();
        for value in 0..16u8 {
            let partition = Arc::clone(&partition);
            writers.push(std::thread::spawn(move || {
                partition.append(&[value]).unwrap()
            }));
        }
        let mut offsets = writers
            .into_iter()
            .map(|writer| writer.join().unwrap())
            .collect::<Vec<_>>();
        offsets.sort_unstable();
        assert_eq!(offsets, (0..16).collect::<Vec<_>>());
        assert_eq!(partition.read_from(0, 32).unwrap().len(), 16);
        cleanup(&p);
    }
    #[test]
    fn try_append_reports_backpressure_instead_of_blocking() {
        let p = path("backpressure");
        cleanup(&p);
        let partition = SharedPartition::new(Partition::open(&p).unwrap());
        let _held = partition.inner.lock().unwrap();
        assert_eq!(
            partition.try_append(b"later").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(_held);
        assert_eq!(partition.try_append(b"accepted").unwrap(), 0);
        cleanup(&p);
    }
    #[test]
    fn leader_leases_fence_stale_owners() {
        let mut leaders = LeaderRegistry::default();
        let first = leaders.claim(0, "node-a", 100, 50).unwrap();
        assert!(leaders.validate(0, "node-a", first.epoch, 149));
        assert_eq!(
            leaders.claim(0, "node-b", 120, 50).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let second = leaders.claim(0, "node-b", 150, 50).unwrap();
        assert!(second.epoch > first.epoch);
        assert!(!leaders.validate(0, "node-a", first.epoch, 151));
        assert!(leaders.validate(0, "node-b", second.epoch, 151));
    }
}
