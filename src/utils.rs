use crate::io::*;

use std::{io::ErrorKind, path::Path};

pub trait FoundResult<T> {
    fn into_found(self) -> Result<Option<T>, IoError>;
}

impl<T> FoundResult<T> for Result<T, IoError> {
    fn into_found(self) -> Result<Option<T>, IoError> {
        match self {
            Ok(t) => Ok(Some(t)),
            Err(IoError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub fn opt_vec(opt: Option<&[u8]>) -> Option<Vec<u8>> {
    Some(opt?.to_vec())
}

pub fn opt_slice(opt: &Option<Vec<u8>>) -> Option<&[u8]> {
    match opt {
        None => None,
        Some(v) => Some(v.as_slice()),
    }
}

pub fn obtain_id(file_path: impl AsRef<Path>) -> anyhow::Result<u64> {
    let file_path = file_path.as_ref();
    std::fs::create_dir_all(file_path.parent().unwrap())?;

    match std::fs::read(&file_path) {
        Ok(bytes) => Ok(serde_cbor::from_slice(&bytes)?),
        Err(e) if e.kind() == ErrorKind::NotFound => {
            let id = rand::random();
            std::fs::write(&file_path, &serde_cbor::to_vec(&id)?)?;
            Ok(id)
        }
        Err(e) => Err(anyhow::anyhow!(e)),
    }
}

pub fn from_slice<T: serde::de::DeserializeOwned>(vec: impl AsRef<[u8]>) -> anyhow::Result<T> {
    Ok(serde_cbor::from_slice(vec.as_ref())?)
}

pub fn to_vec<T: serde::Serialize>(t: &T) -> anyhow::Result<Vec<u8>> {
    Ok(serde_cbor::to_vec(t)?)
}

pub async fn ensure_dir_exists(path: impl AsRef<Path>) -> IoResult<()> {
    tokio::fs::create_dir_all(path).await?;
    Ok(())
}

#[macro_export]
macro_rules! log_err {
    ($expr:expr) => {{
        match $expr {
            Ok(v) => Some(v),
            Err(e) => {
                log::error!("{}", e);
                None
            }
        }
    }};
}

#[macro_export]
macro_rules! just_log {
    ($expr:expr) => {{
        match $expr {
            Ok(v) => Ok(v),
            Err(e) => {
                log::error!("{}", e);
                Err(e)
            }
        }
    }};
}

pub fn diff<T: PartialEq>(a: T, b: T) -> Option<(T, T)> {
    if a == b {
        None
    } else {
        Some((a, b))
    }
}

#[derive(Default)]
pub struct NotifyOnDrop {
    wakers: Vec<tokio::sync::oneshot::Sender<()>>,
}

impl NotifyOnDrop {
    pub fn push(&mut self) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.wakers.push(tx);
        rx
    }
}

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        for waker in self.wakers.drain(..) {
            let _ = waker.send(());
        }
    }
}
