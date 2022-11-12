use crate::io::*;

use std::{fmt::Display, io::ErrorKind, path::Path};

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

pub trait ResultExt<T, E> {
    fn log_err(self) -> Option<T>
    where
        E: Display;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn log_err(self) -> Option<T>
    where
        E: Display,
    {
        match self {
            Ok(t) => Some(t),
            Err(e) => {
                log::error!("{}", e);
                None
            }
        }
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
