use std::{
    ffi::{c_int, OsStr, OsString},
    time::SystemTime,
};

#[async_trait::async_trait]
pub trait FileSystem {
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<Attributes, c_int>;
    async fn get_attributes(&self, node: u64) -> Result<Attributes, c_int>;
    async fn list_children(&self, parent: u64) -> Result<Vec<ChildItem>, c_int>;
    async fn read(&self, node: u64, offset: u64, size: u32) -> Result<Vec<u8>, c_int>;
}

#[derive(Debug)]
pub struct Attributes {
    pub created_at: SystemTime,
    pub node_id: u64,

    pub kind: KindedAttributes,
}

#[derive(Debug)]
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

    pub fn created_since_epoch(&self) -> std::time::Duration {
        self.created_at
            .duration_since(std::time::UNIX_EPOCH)
            .expect("should always be after epoch")
    }
}

#[derive(Debug)]
pub struct ChildItem {
    pub node_id: u64,
    pub kind: FileKind,
    pub path: OsString,
}

#[derive(Debug)]
pub enum FileKind {
    File,
    Directory,
}

pub struct Main;

#[async_trait::async_trait]
impl FileSystem for Main {
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<Attributes, c_int> {
        log::debug!("lookup(parent, name): {:?}", (parent, name.to_str()));

        if parent == 1 && name.to_str() == Some("hello.txt") {
            return Ok(Attributes::file());
        }

        Err(libc::ENOENT)
    }

    async fn get_attributes(&self, node: u64) -> Result<Attributes, c_int> {
        log::debug!("get_attributes(node): {:?}", node);

        match node {
            1 => Ok(Attributes::dir()),
            2 => Ok(Attributes::file()),
            _ => Err(libc::ENOENT),
        }
    }

    async fn list_children(&self, parent: u64) -> Result<Vec<ChildItem>, c_int> {
        log::debug!("list_children(parent): {:?}", parent);

        if parent != 1 {
            return Err(libc::ENOENT);
        }

        Ok(vec![
            ChildItem {
                node_id: 1,
                kind: FileKind::Directory,
                path: ".".into(),
            },
            ChildItem {
                node_id: 1,
                kind: FileKind::Directory,
                path: "..".into(),
            },
            ChildItem {
                node_id: 2,
                kind: FileKind::File,
                path: "hello.txt".into(),
            },
        ])
    }

    async fn read(&self, node: u64, offset: u64, size: u32) -> Result<Vec<u8>, c_int> {
        log::debug!("read(node, offset): {:?}", (node, offset));

        if node != 2 {
            return Err(libc::ENOENT);
        }

        let bytes = b"Hello World!\n";

        let offset = &bytes[offset as usize..];
        let len = core::cmp::min(offset.len(), size as usize);
        Ok(offset[..len].to_vec())
    }
}
