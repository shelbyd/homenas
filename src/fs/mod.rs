use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, ffi::OsString, time::Duration};

mod chunk_store;
use chunk_store::*;

mod file_handle;
use file_handle::*;

mod file_system;
pub use file_system::*;

pub type HandleId = u64;
pub type NodeId = u64;

pub type IoResult<T> = Result<T, IoError>;

#[derive(thiserror::Error, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IoError {
    #[error("not found")]
    NotFound,
    #[error("out of range")]
    OutOfRange,
    #[error("unimplemented")]
    Unimplemented,
    #[error("not a file")]
    NotAFile,
    #[error("not a directory")]
    NotADirectory,
    #[error("timeout")]
    Timeout,
    #[error("i/o")]
    Io,
    #[error("parsing")]
    Parse,
    #[error("uncategorized")]
    Uncategorized,
    #[error("invalid data")]
    InvalidData,
    #[error("temporarily unavailable")]
    TempUnavailable,
    #[error("bad file descriptor")]
    BadDescriptor,
}

// TODO(shelbyd): Delete and use std::io::Result.
impl From<std::io::Error> for IoError {
    fn from(e: std::io::Error) -> Self {
        use std::io::ErrorKind::*;

        match e.kind() {
            NotFound => IoError::NotFound,
            unhandled => {
                log::warn!("Unhandled std::io::ErrorKind: {:?}", unhandled);
                IoError::Uncategorized
            }
        }
    }
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, Clone)]
pub struct Entry<K = DetailedKind> {
    pub node_id: NodeId,
    pub name: OsString,
    pub kind: K,
}

impl Entry {
    pub fn created_since_epoch(&self) -> Duration {
        Duration::default()
    }
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, Clone)]
pub enum EntryKind<F = (), D = ()> {
    File(F),
    Directory(D),
}

#[allow(dead_code)]
impl<F, D> EntryKind<F, D> {
    pub fn as_file(&self) -> Option<&F> {
        match self {
            EntryKind::File(f) => Some(f),
            _ => None,
        }
    }

    pub fn as_file_mut(&mut self) -> Option<&mut F> {
        match self {
            EntryKind::File(f) => Some(f),
            _ => None,
        }
    }

    pub fn as_dir(&self) -> Option<&D> {
        match self {
            EntryKind::Directory(d) => Some(d),
            _ => None,
        }
    }

    pub fn as_dir_mut(&mut self) -> Option<&mut D> {
        match self {
            EntryKind::Directory(d) => Some(d),
            _ => None,
        }
    }
}

pub type DetailedKind = EntryKind<FileData, DirectoryData>;

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, Clone)]
pub struct FileData {
    pub size: u64,
}

#[derive(Default, PartialEq, Eq, Debug, Deserialize, Serialize, Clone)]
pub struct DirectoryData {
    children: BTreeMap<OsString, NodeId>,
}
