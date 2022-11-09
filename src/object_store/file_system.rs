use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{self, *},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use super::*;

pub struct FileSystem {
    path: PathBuf,
    id: u64,
}

impl FileSystem {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();

        let id_path = path.join("directory-id");
        let id = match std::fs::read(&id_path) {
            Ok(bytes) => serde_cbor::from_slice(&bytes)?,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let id = rand::random();
                std::fs::write(&id_path, &serde_cbor::to_vec(&id)?)?;
                id
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        };

        Ok(FileSystem {
            path: path.to_path_buf(),
            id,
        })
    }

    async fn writable_path(&self, key: &str) -> IoResult<PathBuf> {
        let path = self.path.join(key);
        fs::create_dir_all(path.parent().expect("keys are in directory")).await?;
        Ok(path)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FileSystem {
    async fn set(&self, key: &str, value: &[u8]) -> IoResult<()> {
        let path = self.writable_path(&key).await?;
        fs::write(path, &value).await?;

        Ok(())
    }

    async fn get(&self, key: &str) -> IoResult<Vec<u8>> {
        match fs::read(self.path.join(key)).await {
            Ok(buf) => Ok(buf),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                log::debug!("key not found: {}", key);
                Err(IoError::NotFound)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn clear(&self, key: &str) -> IoResult<()> {
        fs::remove_file(self.path.join(key)).await?;
        Ok(())
    }

    async fn compare_exchange(
        &self,
        key: &str,
        current: Option<&[u8]>,
        new: &[u8],
    ) -> IoResult<bool> {
        let path = self.writable_path(&key).await?;
        let mut file = match current {
            None => {
                let open = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .await;

                match open {
                    Err(e) if e.kind() == ErrorKind::AlreadyExists => return Ok(false),
                    Err(e) => return Err(e.into()),
                    Ok(f) => f,
                }
            }
            Some(current) => {
                let open = OpenOptions::new().read(true).write(true).open(path).await;

                match open {
                    Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
                    Err(e) => return Err(e.into()),
                    Ok(mut f) => {
                        let mut contents = Vec::new();
                        f.read_to_end(&mut contents).await?;
                        if contents != current {
                            return Ok(false);
                        }
                        f
                    }
                }
            }
        };

        file.set_len(0).await?;
        file.rewind().await?;
        file.write_all(&new).await?;
        Ok(true)
    }

    async fn locations(&self) -> IoResult<Vec<Location>> {
        Ok(vec![Location::Directory(self.id)])
    }

    async fn connect(&self, location: &Location) -> IoResult<Box<dyn ObjectStore + '_>> {
        if *location != Location::Directory(self.id) {
            return Err(IoError::NotFound);
        }

        Ok(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::*;

    fn bar() -> Vec<u8> {
        b"bar".to_vec()
    }

    #[tokio::test]
    async fn empty() {
        let dir = tempdir().unwrap();
        let fs = FileSystem::new(dir.path()).unwrap();

        assert_eq!(fs.get("foo").await, Err(IoError::NotFound));
    }

    #[tokio::test]
    async fn set_has_get() {
        let dir = tempdir().unwrap();
        let fs = FileSystem::new(dir.path()).unwrap();

        fs.set("foo", b"bar").await.unwrap();

        assert_eq!(fs.get("foo").await, Ok(bar()));
    }

    #[tokio::test]
    async fn set_with_slashes() {
        let dir = tempdir().unwrap();
        let fs = FileSystem::new(dir.path()).unwrap();

        fs.set("foo/bar/baz", b"bar").await.unwrap();

        assert_eq!(fs.get("foo/bar/baz").await, Ok(bar()));
    }

    #[cfg(test)]
    mod compare_exchange {
        use super::*;

        #[tokio::test]
        async fn no_value() {
            let dir = tempdir().unwrap();
            let fs = FileSystem::new(dir.path()).unwrap();

            assert_eq!(fs.compare_exchange("foo", None, b"bar").await, Ok(true));
            assert_eq!(fs.get("foo").await, Ok(bar()));
        }

        #[tokio::test]
        async fn existing_value_with_none() {
            let dir = tempdir().unwrap();
            let fs = FileSystem::new(dir.path()).unwrap();

            fs.set("foo", b"bar").await.unwrap();

            assert_eq!(fs.compare_exchange("foo", None, b"baz").await, Ok(false));
            assert_eq!(fs.get("foo").await, Ok(bar()));
        }

        #[tokio::test]
        async fn no_value_with_some() {
            let dir = tempdir().unwrap();
            let fs = FileSystem::new(dir.path()).unwrap();

            assert_eq!(
                fs.compare_exchange("foo", Some(b"bar"), b"bar").await,
                Ok(false)
            );
            assert_eq!(fs.get("foo").await, Err(IoError::NotFound));
        }

        #[tokio::test]
        async fn incorrect_value() {
            let dir = tempdir().unwrap();
            let fs = FileSystem::new(dir.path()).unwrap();

            fs.set("foo", b"bar").await.unwrap();

            assert_eq!(
                fs.compare_exchange("foo", Some(b"baz"), b"bar").await,
                Ok(false)
            );
            assert_eq!(fs.get("foo").await, Ok(bar()));
        }

        #[tokio::test]
        async fn correct_some() {
            let dir = tempdir().unwrap();
            let fs = FileSystem::new(dir.path()).unwrap();

            fs.set("foo", b"bar").await.unwrap();

            assert_eq!(
                fs.compare_exchange("foo", Some(b"bar"), b"baz").await,
                Ok(true)
            );
            assert_eq!(fs.get("foo").await, Ok(b"baz".to_vec()));
        }
    }
}
