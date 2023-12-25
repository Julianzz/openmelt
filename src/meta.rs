use std::sync::{Arc, Mutex};

use ahash::AHashMap;

#[derive(Clone, Debug)]
pub struct FileMeta {
    segment: String,
    partition: String,
    min_timestamp: i64,
    max_timestamp: i64,
}
impl FileMeta {
    pub fn new(partition: String, segment: String, min_ts: i64, max_ts: i64) -> FileMeta {
        FileMeta {
            partition,
            segment,
            min_timestamp: min_ts,
            max_timestamp: max_ts,
        }
    }
    pub fn segment(&self) -> &str {
        &self.segment
    }
    pub fn partition(&self) -> &str {
        &self.partition
    }
}

type MetaStorage = Arc<Mutex<AHashMap<String, Vec<FileMeta>>>>;

pub struct MetaService {
    files: MetaStorage,
}

impl MetaService {
    pub fn new() -> MetaService {
        MetaService {
            files: Arc::new(Mutex::new(AHashMap::new())),
        }
    }

    pub fn add_file(&self, table_name: &str, file: FileMeta) {
        log::info!("add file {} {:?}", table_name, file);
        let mut write = self.files.lock().unwrap();
        write.entry(table_name.to_string()).or_default().push(file);
    }

    pub fn query_files(
        &self,
        table_name: &str,
        begin: Option<i64>,
        end: Option<i64>,
    ) -> Vec<FileMeta> {
        let files = self.files.lock().unwrap();
        if let Some(files) = files.get(table_name) {
            files
                .into_iter()
                .cloned()
                .filter(|v| {
                    if let Some(t) = begin {
                        if v.max_timestamp < t {
                            return false;
                        }
                    }
                    if let Some(t) = end {
                        if v.min_timestamp > t {
                            return false;
                        }
                    }
                    return true;
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        }
    }
}
