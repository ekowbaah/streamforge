use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};
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
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        let next = read_all(&file)?.len() as u64;
        Ok(Self {
            file,
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
        read_all(&self.file)
    }
    pub fn read_from(&self, offset: u64, limit: usize) -> io::Result<Vec<Record>> {
        Ok(self
            .read_all()?
            .into_iter()
            .skip(offset as usize)
            .take(limit)
            .collect())
    }
    pub fn next_offset(&self) -> u64 {
        self.next
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
fn read_all(file: &File) -> io::Result<Vec<Record>> {
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
            offset: out.len() as u64,
            payload,
        });
    }
    Ok(out)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn path(suffix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("streamforge-{}-{suffix}", std::process::id()))
    }
    #[test]
    fn persists_ordered_records() {
        let p = path("ordered");
        let _ = std::fs::remove_file(&p);
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
        std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn reads_bounded_batches_from_an_offset() {
        let p = path("batches");
        let _ = std::fs::remove_file(&p);
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
        std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn rejects_oversized_records_without_advancing_offset() {
        let p = path("limit");
        let _ = std::fs::remove_file(&p);
        let mut log = Partition::open_with_limit(&p, 2).unwrap();
        assert_eq!(
            log.append(b"abc").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(log.next_offset(), 0);
        std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn detects_corruption_on_recovery() {
        let p = path("corrupt");
        let _ = std::fs::remove_file(&p);
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
        std::fs::remove_file(p).unwrap();
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
}
