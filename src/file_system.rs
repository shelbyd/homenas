use std::{
    ffi::{c_int, OsStr},
    time::SystemTime,
};

#[async_trait::async_trait]
pub trait FileSystem {
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<File, c_int>;
}

pub struct File {
    pub created_at: SystemTime,
    pub node_id: u64,
    pub size: u64,
}

pub struct Main;

#[async_trait::async_trait]
impl FileSystem for Main {
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<File, c_int> {
        log::info!("lookup(parent, name): {:?}", (parent, name.to_str()));

        if parent == 1 && name.to_str() == Some("hello.txt") {
            return Ok(File {
                created_at: std::time::UNIX_EPOCH,
                node_id: 2,
                size: 13,
            });
        }

        Err(libc::ENOENT)
    }
}
