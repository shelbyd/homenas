use serde::{Deserialize, Serialize};
use std::{ffi::OsString, time::Duration};

mod file_system;
pub use file_system::*;

pub type NodeId = u64;

pub type IoResult<T> = Result<T, IoError>;

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum IoError {
    #[error("not found")]
    NotFound,
    #[error("out of range")]
    OutOfRange,
    #[error("unimplemented")]
    Unimplemented,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub enum FileKind {
    File,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct Entry {
    pub node_id: NodeId,
    pub name: OsString,
    pub kind: FileKind,
}

impl Entry {
    pub fn created_since_epoch(&self) -> Duration {
        unimplemented!("created_since_epoch");
    }
}
