use crate::{ConsumerOffsets, Partition, PartitionRouter, Record, SharedPartition};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

pub struct Broker {
    root: PathBuf,
    partition_count: u32,
    topics: RwLock<HashMap<String, Arc<Topic>>>,
    offsets: Mutex<ConsumerOffsets>,
}

struct Topic {
    router: PartitionRouter,
    partitions: Vec<SharedPartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendedRecord {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerRecord {
    pub offset: u64,
    pub payload: Vec<u8>,
}

impl From<Record> for BrokerRecord {
    fn from(value: Record) -> Self {
        Self {
            offset: value.offset,
            payload: value.payload,
        }
    }
}

impl Broker {
    pub fn open(root: impl AsRef<Path>, partition_count: u32) -> io::Result<Self> {
        PartitionRouter::new(partition_count)?;
        fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
            partition_count,
            topics: RwLock::new(HashMap::new()),
            offsets: Mutex::new(ConsumerOffsets::default()),
        })
    }

    pub fn append(&self, topic: &str, key: &[u8], payload: &[u8]) -> io::Result<AppendedRecord> {
        let topic_log = self.topic(topic)?;
        let partition = topic_log.router.route(key);
        let offset = topic_log.partitions[partition as usize].try_append(payload)?;
        Ok(AppendedRecord {
            topic: topic.to_owned(),
            partition,
            offset,
        })
    }

    pub fn read(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        limit: usize,
    ) -> io::Result<Vec<BrokerRecord>> {
        let topic_log = self.topic(topic)?;
        let selected = topic_log
            .partitions
            .get(partition as usize)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "partition is out of range")
            })?;
        selected
            .read_from(offset, limit.min(1_000))
            .map(|records| records.into_iter().map(Into::into).collect())
    }

    pub fn commit(&self, group: &str, partition: u32, next_offset: u64) -> io::Result<()> {
        if group.is_empty() || partition >= self.partition_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "group and valid partition are required",
            ));
        }
        let accepted = self
            .offsets
            .lock()
            .map_err(|_| io::Error::other("offset lock poisoned"))?
            .commit(group, partition, next_offset);
        if accepted {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "consumer offsets cannot move backwards",
            ))
        }
    }

    pub fn committed(&self, group: &str, partition: u32) -> io::Result<u64> {
        Ok(self
            .offsets
            .lock()
            .map_err(|_| io::Error::other("offset lock poisoned"))?
            .committed(group, partition))
    }

    pub fn retain_last(&self, topic: &str, partition: u32, keep: usize) -> io::Result<usize> {
        let topic_log = self.topic(topic)?;
        topic_log
            .partitions
            .get(partition as usize)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "partition is out of range")
            })?
            .retain_last(keep)
    }

    fn topic(&self, name: &str) -> io::Result<Arc<Topic>> {
        validate_topic(name)?;
        if let Some(topic) = self
            .topics
            .read()
            .map_err(|_| io::Error::other("topic lock poisoned"))?
            .get(name)
            .cloned()
        {
            return Ok(topic);
        }
        let mut topics = self
            .topics
            .write()
            .map_err(|_| io::Error::other("topic lock poisoned"))?;
        if let Some(topic) = topics.get(name).cloned() {
            return Ok(topic);
        }
        let directory = self.root.join(name);
        fs::create_dir_all(&directory)?;
        let partitions = (0..self.partition_count)
            .map(|partition| {
                Partition::open(directory.join(format!("partition-{partition}.log")))
                    .map(SharedPartition::new)
            })
            .collect::<io::Result<Vec<_>>>()?;
        let topic = Arc::new(Topic {
            router: PartitionRouter::new(self.partition_count)?,
            partitions,
        });
        topics.insert(name.to_owned(), Arc::clone(&topic));
        Ok(topic)
    }
}

fn validate_topic(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "topic names may contain only letters, numbers, dashes, and underscores",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(suffix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "streamforge-broker-{}-{suffix}",
            std::process::id()
        ))
    }

    #[test]
    fn routes_persists_replays_and_retains_records() {
        let root = root("roundtrip");
        let _ = fs::remove_dir_all(&root);
        let broker = Broker::open(&root, 2).unwrap();
        let first = broker.append("orders", b"customer-1", b"created").unwrap();
        let second = broker.append("orders", b"customer-1", b"paid").unwrap();
        assert_eq!(first.partition, second.partition);
        assert_eq!((first.offset, second.offset), (0, 1));
        drop(broker);

        let recovered = Broker::open(&root, 2).unwrap();
        let records = recovered.read("orders", first.partition, 0, 10).unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.payload.as_slice())
                .collect::<Vec<_>>(),
            vec![b"created".as_slice(), b"paid".as_slice()]
        );
        assert_eq!(
            recovered.retain_last("orders", first.partition, 1).unwrap(),
            1
        );
        assert_eq!(
            recovered.read("orders", first.partition, 0, 10).unwrap()[0].offset,
            1
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_path_traversal_and_offset_rewinds() {
        let root = root("validation");
        let _ = fs::remove_dir_all(&root);
        let broker = Broker::open(&root, 2).unwrap();
        assert_eq!(
            broker
                .append("../escape", b"key", b"value")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        broker.commit("billing", 0, 4).unwrap();
        assert_eq!(
            broker.commit("billing", 0, 3).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(broker.committed("billing", 0).unwrap(), 4);
        fs::remove_dir_all(root).unwrap();
    }
}
