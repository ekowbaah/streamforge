use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};
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
}
impl Partition {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        let next = read_all(&file)?.len() as u64;
        Ok(Self { file, next })
    }
    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
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
    #[test]
    fn persists_ordered_records() {
        let p = std::env::temp_dir().join(format!("streamforge-{}", std::process::id()));
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
}
