use std::path::{Path, PathBuf};

use bytes::Bytes;

pub struct Storage {
    root: PathBuf,
}

impl Storage {
    pub fn new(root: impl AsRef<Path>) -> Storage {
        Storage {
            root: root.as_ref().into(),
        }
    }
}
impl Storage {
    pub async fn put(&self, filename: &str, datas: Bytes) -> Result<(), anyhow::Error> {
        let path = self.root.join(filename);
        tokio::fs::write(path, datas).await?;
        Ok(())
    }

    pub fn ensure_dir(&self, name: &str) -> Result<(), anyhow::Error> {
        let path = self.root.join(name);
        std::fs::create_dir_all(path)?;
        Ok(())
    }
}
