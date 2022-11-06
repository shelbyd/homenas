use std::{
    ffi::{c_int, OsStr},
    time::SystemTime,
};

#[async_trait::async_trait]
pub trait FileSystem {
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<Attributes, c_int>;
    async fn get_attributes(&self, node: u64) -> Result<Attributes, c_int>;
}

pub struct Attributes {
    pub created_at: SystemTime,
    pub node_id: u64,

    pub kind: KindedAttributes,
}

pub enum KindedAttributes {
    File { size: u64 },
    Dir {},
}

impl Attributes {
    fn file() -> Self {
        Attributes {
            created_at: std::time::UNIX_EPOCH,
            node_id: 2,
            kind: KindedAttributes::File { size: 13 },
        }
    }

    fn dir() -> Self {
        Attributes {
            created_at: std::time::UNIX_EPOCH,
            node_id: 1,
            kind: KindedAttributes::Dir {},
        }
    }
}

pub struct Main;

#[async_trait::async_trait]
impl FileSystem for Main {
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<Attributes, c_int> {
        log::info!("lookup(parent, name): {:?}", (parent, name.to_str()));

        if parent == 1 && name.to_str() == Some("hello.txt") {
            return Ok(Attributes::file());
        }

        Err(libc::ENOENT)
    }

    async fn get_attributes(&self, node: u64) -> Result<Attributes, c_int> {
        log::info!("get_attributes(node): {:?}", node);

        match node {
            1 => Ok(Attributes::dir()),
            _ => Err(libc::ENOENT),
        }
    }
}
