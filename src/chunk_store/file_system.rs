use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};
use tokio::fs;

use super::*;

pub struct FsChunkStore {
    path: PathBuf,
    id: u64,
}

impl FsChunkStore {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();

        let id_path = path.join("directory-id");
        std::fs::create_dir_all(id_path.parent().unwrap())?;
        let id = match std::fs::read(&id_path) {
            Ok(bytes) => serde_cbor::from_slice(&bytes)?,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let id = rand::random();
                std::fs::write(&id_path, &serde_cbor::to_vec(&id)?)?;
                id
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        };

        Ok(FsChunkStore {
            path: path.to_path_buf(),
            id,
        })
    }

    fn path(&self, key: &str) -> PathBuf {
        let key_part = if key.len() >= 8 {
            format!("{}/{}", &key[..4], &key[4..])
        } else {
            key.to_string()
        };
        self.path.join(key_part)
    }

    async fn writable_path(&self, key: &str) -> IoResult<PathBuf> {
        let path = self.path(key);
        fs::create_dir_all(path.parent().expect("keys are in directory")).await?;
        Ok(path)
    }
}

#[async_trait::async_trait]
impl ChunkStore for FsChunkStore {
    async fn read(&self, id: &str) -> IoResult<Vec<u8>> {
        Ok(fs::read(self.path(id)).await?)
    }

    async fn store(&self, chunk: &[u8]) -> IoResult<String> {
        let id = id_for(chunk);
        fs::write(self.writable_path(&id).await?, chunk).await?;
        Ok(id)
    }

    async fn store_at(&self, chunk: &[u8], location: &Location) -> IoResult<String> {
        if *location != Location::Directory(self.id) {
            return Err(IoError::NotFound);
        }

        self.store(chunk).await
    }

    async fn drop(&self, id: &str) -> IoResult<()> {
        Ok(fs::remove_file(self.path(id)).await?)
    }

    async fn locations(&self) -> IoResult<HashSet<Location>> {
        Ok(maplit::hashset! { Location::Directory(self.id) })
    }
}
