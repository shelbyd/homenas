use std::{
    collections::HashMap,
    ffi::{c_int, OsStr, OsString},
    io::BufRead,
    sync::RwLock,
    time::SystemTime,
};

#[async_trait::async_trait]
pub trait FileSystem {
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<Attributes, c_int>;
    async fn get_attributes(&self, node: u64) -> Result<Attributes, c_int>;
    async fn list_children(&self, parent: u64) -> Result<Vec<ChildItem>, c_int>;
    async fn read(&self, node: u64, offset: u64, size: u32) -> Result<Vec<u8>, c_int>;
    async fn write<T: BufRead + Send>(&self, node: u64, offset: u64, data: T)
        -> Result<u32, c_int>;
}

#[derive(Debug, Clone)]
pub struct Attributes {
    pub created_at: SystemTime,
    pub node_id: u64,

    pub kind: KindedAttributes,
}

#[derive(Debug, Clone)]
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

#[derive(Default)]
pub struct Main {
    inner: RwLock<Inner>,
}

struct Inner {
    next_node_id: u64,
    nodes: HashMap<u64, INode>,
}

impl Default for Inner {
    fn default() -> Self {
        Inner {
            next_node_id: 1,
            nodes: maplit::hashmap! {
                1 => INode {
                    attributes: Attributes {
                        created_at: std::time::UNIX_EPOCH,
                        node_id: 1,
                        kind: KindedAttributes::Dir {},
                    },
                    kind: INodeKind::Directory(Directory {
                        children: maplit::hashmap! {
                            "hello.txt".into() => 2,
                        },
                        parent: None,
                    }),
                },
                2 => INode {
                    attributes: Attributes::file(),
                    kind: INodeKind::RegularFile(b"Hello World!\n".to_vec()),
                },
            },
        }
    }
}

struct INode {
    attributes: Attributes,
    kind: INodeKind,
}

enum INodeKind {
    RegularFile(Vec<u8>),
    Directory(Directory),
}

struct Directory {
    children: HashMap<OsString, u64>,
    parent: Option<u64>,
}

#[async_trait::async_trait]
impl FileSystem for Main {
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<Attributes, c_int> {
        log::debug!("lookup(parent, name): {:?}", (parent, name.to_str()));

        let read = self.inner.read().unwrap();

        let node = read.nodes.get(&parent).ok_or(libc::ENOENT)?;
        let child_id = match &node.kind {
            INodeKind::Directory(dir) => dir.children.get(name).ok_or(libc::ENOENT)?,
            _ => return Err(libc::ENOTDIR),
        };

        let child = read
            .nodes
            .get(&child_id)
            .expect("should have child if parent knows");
        Ok(child.attributes.clone())
    }

    async fn get_attributes(&self, node: u64) -> Result<Attributes, c_int> {
        log::debug!("get_attributes(node): {:?}", node);

        let read = self.inner.read().unwrap();
        let node = read.nodes.get(&node).ok_or(libc::ENOENT)?;
        Ok(node.attributes.clone())
    }

    async fn list_children(&self, parent: u64) -> Result<Vec<ChildItem>, c_int> {
        log::debug!("list_children(parent): {:?}", parent);

        let read = self.inner.read().unwrap();
        let parent = read.nodes.get(&parent).ok_or(libc::ENOENT)?;

        let children = match &parent.kind {
            INodeKind::Directory(dir) => &dir.children,
            _ => return Err(libc::ENOTDIR),
        };

        let children_entries = children.iter().map(|(path, id)| {
            let child = read.nodes.get(&id).expect("parent has dead child link");
            let kind = match child.kind {
                INodeKind::RegularFile(_) => FileKind::File,
                INodeKind::Directory(_) => FileKind::Directory,
            };
            ChildItem {
                node_id: *id,
                kind,
                path: path.into(),
            }
        });

        Ok([
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
        ]
        .into_iter()
        .chain(children_entries)
        .collect())
    }

    async fn read(&self, node: u64, offset: u64, size: u32) -> Result<Vec<u8>, c_int> {
        log::debug!("read(node, offset): {:?}", (node, offset));

        let read = self.inner.read().unwrap();
        let node = read.nodes.get(&node).ok_or(libc::ENOENT)?;

        let contents = match &node.kind {
            INodeKind::RegularFile(v) => v,
            _ => return Err(libc::EINVAL),
        };

        let offset = &contents[offset as usize..];
        let len = core::cmp::min(offset.len(), size as usize);
        Ok(offset[..len].to_vec())
    }

    async fn write<T: BufRead + Send>(
        &self,
        node: u64,
        offset: u64,
        mut data: T,
    ) -> Result<u32, c_int> {
        log::debug!("write(node, offset): {:?}", (node, offset));

        if offset != 0 {
            log::warn!("Not Implemented: write(offset): {:?}", offset);
            return Err(libc::ENOSYS);
        }

        let mut write = self.inner.write().unwrap();
        let node = write.nodes.get_mut(&node).ok_or(libc::ENOENT)?;

        let mut contents = match &mut node.kind {
            INodeKind::RegularFile(b) => b,
            _ => return Err(libc::EINVAL),
        };

        contents.clear();
        let written = data
            .read_to_end(contents)
            .expect("read/write in memory suceeds");

        Ok(written as u32)
    }
}
