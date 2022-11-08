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
    // paths: Vec<PathBuf>,
}

impl FileSystem {
    pub fn new(path: impl AsRef<Path>) -> Self {
        FileSystem {
            path: path.as_ref().to_path_buf(),
        }
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

    async fn get(&self, key: &str) -> IoResult<Option<Vec<u8>>> {
        match fs::read(self.path.join(key)).await {
            Ok(buf) => Ok(Some(buf)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
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
        let fs = FileSystem::new(dir.path());

        assert_eq!(fs.get("foo").await, Ok(None));
    }

    #[tokio::test]
    async fn set_has_get() {
        let dir = tempdir().unwrap();
        let fs = FileSystem::new(dir.path());

        fs.set("foo", b"bar").await.unwrap();

        assert_eq!(fs.get("foo").await, Ok(Some(bar())));
    }

    #[tokio::test]
    async fn set_with_slashes() {
        let dir = tempdir().unwrap();
        let fs = FileSystem::new(dir.path());

        fs.set("foo/bar/baz", b"bar").await.unwrap();

        assert_eq!(fs.get("foo/bar/baz").await, Ok(Some(bar())));
    }

    #[cfg(test)]
    mod compare_exchange {
        use super::*;

        #[tokio::test]
        async fn no_value() {
            let dir = tempdir().unwrap();
            let fs = FileSystem::new(dir.path());

            assert_eq!(fs.compare_exchange("foo", None, b"bar").await, Ok(true));
            assert_eq!(fs.get("foo").await, Ok(Some(bar())));
        }

        #[tokio::test]
        async fn existing_value_with_none() {
            let dir = tempdir().unwrap();
            let fs = FileSystem::new(dir.path());

            fs.set("foo", b"bar").await.unwrap();

            assert_eq!(
                fs.compare_exchange("foo", None, b"baz").await,
                Ok(false)
            );
            assert_eq!(fs.get("foo").await, Ok(Some(bar())));
        }

        #[tokio::test]
        async fn no_value_with_some() {
            let dir = tempdir().unwrap();
            let fs = FileSystem::new(dir.path());

            assert_eq!(
                fs.compare_exchange("foo", Some(b"bar"), b"bar").await,
                Ok(false)
            );
            assert_eq!(fs.get("foo").await, Ok(None));
        }

        #[tokio::test]
        async fn incorrect_value() {
            let dir = tempdir().unwrap();
            let fs = FileSystem::new(dir.path());

            fs.set("foo", b"bar").await.unwrap();

            assert_eq!(
                fs.compare_exchange("foo", Some(b"baz"), b"bar")
                    .await,
                Ok(false)
            );
            assert_eq!(fs.get("foo").await, Ok(Some(bar())));
        }

        #[tokio::test]
        async fn correct_some() {
            let dir = tempdir().unwrap();
            let fs = FileSystem::new(dir.path());

            fs.set("foo", b"bar").await.unwrap();

            assert_eq!(
                fs.compare_exchange("foo", Some(b"bar"), b"baz")
                    .await,
                Ok(true)
            );
            assert_eq!(fs.get("foo").await, Ok(Some(b"baz".to_vec())));
        }
    }
}
