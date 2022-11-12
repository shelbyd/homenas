use serde::{Deserialize, Serialize};

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
    #[error("internal error")]
    Internal,
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

impl From<sled::Error> for IoError {
    fn from(e: sled::Error) -> Self {
        use sled::Error::*;

        match e {
            CollectionNotFound(_) => IoError::Internal,
            Unsupported(_) => IoError::Internal,
            ReportableBug(bug) => {
                log::error!("Sled bug: {}", bug);
                IoError::Internal
            }
            Io(e) => e.into(),
            Corruption { .. } => IoError::InvalidData,
        }
    }
}

impl From<async_raft::error::RaftError> for IoError {
    fn from(e: async_raft::error::RaftError) -> Self {
        use async_raft::error::RaftError::*;

        match e {
            ShuttingDown => IoError::Internal,
            unhandled => {
                unimplemented!("unhandled: {:?}", unhandled);
            }
        }
    }
}
