#![allow(dead_code)] // DO NOT SUBMIT

use std::{ffi::OsStr, io::BufRead};

use super::*;
use crate::object_store::*;

pub struct FileSystem<O> {
    store: O,
}

impl<O> FileSystem<O> {
    pub fn new(store: O) -> Self {
        Self { store }
    }
}

impl<O: ObjectStore> FileSystem<O> {
    pub async fn lookup(&self, parent: NodeId, name: &OsStr) -> IoResult<()> {
        assert_eq!(parent, 1);
        let root = self.store.get("root").await.ok_or(IoError::NotFound)?;

        let root_children: Vec<_> = serde_cbor::from_slice(&root).unwrap();
        root_children
            .into_iter()
            .find(|child: &ChildItem| child.name == name)
            .ok_or(IoError::NotFound)?;

        Ok(())
    }

    pub async fn list_children(&self, _parent: NodeId) -> IoResult<Vec<ChildItem>> {
        let bytes = match self.store.get("root").await {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };
        let root = serde_cbor::from_slice(&bytes).unwrap();
        Ok(root)
    }

    pub async fn create_file(&self, _parent: NodeId, name: &OsStr) -> IoResult<Entry> {
        assert_eq!(_parent, 1);

        let new_node_id = self.get_next_node_id().await?;

        let file_location = format!("file/{}", new_node_id);

        self.store.set(file_location, Vec::new()).await;

        self.store
            .update(String::from("root"), |buf| {
                let mut children: Vec<_> = buf
                    .map(|buf| serde_cbor::from_slice(buf).unwrap())
                    .unwrap_or_default();

                children.push(ChildItem {
                    kind: FileKind::File,
                    name: name.to_owned(),
                    node_id: new_node_id,
                });

                (serde_cbor::to_vec(&children).unwrap(), ())
            })
            .await;

        Ok(Entry {
            node_id: new_node_id,
        })
    }

    async fn get_next_node_id(&self) -> IoResult<u64> {
        Ok(self
            .store
            .update("meta/next_node_id".to_string(), |next| {
                let next = next
                    .map(|bytes| serde_cbor::from_slice(bytes).unwrap())
                    .unwrap_or(2);
                (serde_cbor::to_vec(&(next + 1)).unwrap(), next)
            })
            .await)
    }

    pub async fn write<B: BufRead>(&self, node: NodeId, offset: u64, mut data: B) -> IoResult<u64> {
        assert_eq!(offset, 0);

        let mut vec = Vec::new();
        let amount = data.read_to_end(&mut vec).unwrap();

        self.store.set(format!("file/{}", node), vec).await;

        Ok(amount as u64)
    }

    pub async fn read(&self, node: NodeId, offset: u64, amount: u32) -> IoResult<Vec<u8>> {
        let offset = offset as usize;

        let mut contents = self.store.get(&format!("file/{}", node)).await.ok_or(IoError::NotFound)?;

        if offset > contents.len() {
            return Err(IoError::OutOfRange);
        }

        let mut offset = contents.split_off(offset);
        let amount = core::cmp::min(amount, offset.len() as u32);
        offset.truncate(amount as usize);

        Ok(offset)
    }
}

#[cfg(target_family = "unix")]
fn bytes(os_str: &OsStr) -> &[u8] {
    use std::os::unix::ffi::OsStrExt;

    os_str.as_bytes()
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut s = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut s, "{:x}", byte).expect("write to string is infallible");
    }

    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_root() {
        let mem_os = crate::object_store::Memory::default();
        let fs = FileSystem::new(mem_os);

        assert_eq!(
            fs.lookup(1, &OsStr::new("foo.txt")).await,
            Err(IoError::NotFound)
        );
        assert_eq!(fs.list_children(1).await, Ok(vec![]));
    }

    #[tokio::test]
    async fn create_child() {
        let mem_os = crate::object_store::Memory::default();
        let fs = FileSystem::new(mem_os);

        fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();

        fs.lookup(1, &OsStr::new("foo.txt")).await.unwrap();
    }

    #[tokio::test]
    async fn create_child_does_not_have_random_children() {
        let mem_os = crate::object_store::Memory::default();
        let fs = FileSystem::new(mem_os);

        fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();

        assert_eq!(
            fs.lookup(1, &OsStr::new("bar.txt")).await,
            Err(IoError::NotFound)
        );
    }

    #[tokio::test]
    async fn list_children_after_create() {
        let mem_os = crate::object_store::Memory::default();
        let fs = FileSystem::new(mem_os);

        fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();

        assert_eq!(
            fs.list_children(1).await,
            Ok(vec![ChildItem {
                node_id: 2,
                name: OsString::from("foo.txt"),
                kind: FileKind::File
            }])
        );
    }

    #[tokio::test]
    async fn write_read() {
        let mem_os = crate::object_store::Memory::default();
        let fs = FileSystem::new(mem_os);

        let attrs = fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();
        assert_eq!(fs.write(attrs.node_id, 0, &[1, 2, 3][..]).await, Ok(3));

        assert_eq!(fs.read(attrs.node_id, 0, u32::MAX).await, Ok(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn read_past_end() {
        let mem_os = crate::object_store::Memory::default();
        let fs = FileSystem::new(mem_os);

        let attrs = fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();
        fs.write(attrs.node_id, 0, &[1, 2, 3][..]).await.unwrap();

        assert_eq!(fs.read(attrs.node_id, 3, u32::MAX).await, Ok(vec![]));
        assert_eq!(fs.read(attrs.node_id, 4, u32::MAX).await, Err(IoError::OutOfRange));
    }
}
