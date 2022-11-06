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
    async fn create_file(&self, parent: u64, name: &OsStr) -> Result<Attributes, c_int>;
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
            next_node_id: 2,
            nodes: maplit::hashmap! {
                1 => INode {
                    attributes: Attributes {
                        created_at: std::time::UNIX_EPOCH,
                        node_id: 1,
                        kind: KindedAttributes::Dir {},
                    },
                    kind: INodeKind::Directory(Directory {
                        children: maplit::hashmap! {},
                        parent: None,
                    }),
                },
            },
        }
    }
}

#[derive(Debug)]
struct INode {
    attributes: Attributes,
    kind: INodeKind,
}

#[derive(Debug)]
enum INodeKind {
    RegularFile(Vec<u8>),
    Directory(Directory),
}

#[derive(Debug)]
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

        match &mut node.attributes.kind {
            KindedAttributes::File { size, .. } => {
                *size = written as u64;
            }
            _ => unreachable!(),
        }

        Ok(written as u32)
    }

    async fn create_file(&self, parent: u64, name: &OsStr) -> Result<Attributes, c_int> {
        log::debug!("create_file(parent, name): {:?}", (parent, name.to_str()));

        let mut write = self.inner.write().unwrap();

        let new_node_id = write.next_node_id;
        write.next_node_id += 1;

        let new_node = INode {
            attributes: Attributes {
                created_at: SystemTime::now(),
                node_id: new_node_id,
                kind: KindedAttributes::File { size: 0 },
            },
            kind: INodeKind::RegularFile(Vec::new()),
        };
        let new_node = write.nodes.try_insert(new_node_id, new_node).expect("already had node with id");
        let attr = new_node.attributes.clone();

        let parent = write.nodes.get_mut(&parent).ok_or(libc::ENOENT)?;
        let mut dir = match &mut parent.kind {
            INodeKind::Directory(d) => d,
            _ => return Err(libc::ENOTDIR),
        };

        dir.children
            .try_insert(name.to_owned(), new_node_id)
            .map_err(|_| libc::EEXIST)?;

        Ok(attr)
    }
}
