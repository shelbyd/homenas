#![allow(warnings)]

use std::path::{Path, PathBuf};
use tokio::{
    fs::{self, *},
    io::AsyncReadExt,
};

use super::*;

pub struct FileSystem {
    path: PathBuf,
    // paths: Vec<PathBuf>,
}

impl FileSystem {
    pub fn new(paths: &[impl AsRef<Path>]) -> Self {
        // Could use a non-empty list type.
        assert_ne!(paths.len(), 0);
        assert_eq!(paths.len(), 1);

        FileSystem {
            path: paths
                .iter()
                .map(|p| p.as_ref().to_path_buf())
                .next()
                .unwrap(),
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for FileSystem {
    async fn set(&self, key: String, value: Vec<u8>) -> IoResult<()> {
        todo!("set")
    }

    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        // fs::read(self.path.join(key)).await.ok()
        todo!()
    }

    async fn compare_exchange(
        &self,
        key: &str,
        current: Option<Vec<u8>>,
        new: Vec<u8>,
    ) -> IoResult<bool> {
        // let path = self.path.join(key);
        // fs::create_dir_all(path.parent().unwrap()).await.unwrap();

        // let mut open = OpenOptions::new()
        //     .read(true)
        //     .write(true)
        //     .create(true)
        //     .open(self.path.join(key))
        //     .await
        //     .unwrap();

        // let mut contents = Vec::new();
        // open.read_to_end(&mut contents).await.unwrap();

        // dbg!(contents);

        todo!("compare_exchange")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
