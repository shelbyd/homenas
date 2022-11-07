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
    pub async fn lookup(&self, parent: NodeId, name: &OsStr) -> IoResult<Entry> {
        assert_eq!(parent, 1);
        let root = self.store.get("root").await.ok_or(IoError::NotFound)?;

        let root_children: Vec<NodeId> = serde_cbor::from_slice(&root).unwrap();
        for child_id in root_children {
            let child = self
                .store
                .get(&format!("file/{}.meta", child_id))
                .await
                .expect("parent has broken link to child");

            let child: Entry = serde_cbor::from_slice(&child).unwrap();
            if child.name == name {
                return Ok(child);
            }
        }

        Err(IoError::NotFound)
    }

    pub async fn read_entry(&self, node: NodeId) -> IoResult<Entry> {
        log::debug!("read_entry(node): {:?}", node);

        if node == 1 {
            return Ok(Entry {
                node_id: 1,
                kind: FileKind::Directory,
                name: OsString::from("."),
            })
        }

        let read = self
            .store
            .get(&format!("file/{}.meta", node))
            .await
            .expect("parent has broken link to child");
        Ok(serde_cbor::from_slice(&read).unwrap())
    }

    pub async fn list_children(&self, _parent: NodeId) -> IoResult<Vec<Entry>> {
        let bytes = match self.store.get("root").await {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };
        let root_children: Vec<NodeId> = serde_cbor::from_slice(&bytes).unwrap();

        futures::future::try_join_all(root_children.into_iter().map(|id| self.read_entry(id))).await
    }

    pub async fn create_file(&self, _parent: NodeId, name: &OsStr) -> IoResult<Entry> {
        assert_eq!(_parent, 1);

        let new_node_id = self.get_next_node_id().await?;

        let file_location = format!("file/{}", new_node_id);
        self.store.set(file_location, Vec::new()).await;

        self.store
            .set(
                format!("file/{}.meta", new_node_id),
                serde_cbor::to_vec(&Entry {
                    kind: FileKind::File,
                    name: name.to_owned(),
                    node_id: new_node_id,
                })
                .unwrap(),
            )
            .await;

        self.store
            .update(String::from("root"), |buf| {
                let mut children: Vec<_> = buf
                    .map(|buf| serde_cbor::from_slice(buf).unwrap())
                    .unwrap_or_default();

                children.push(new_node_id);

                (serde_cbor::to_vec(&children).unwrap(), ())
            })
            .await;

        Ok(Entry {
            node_id: new_node_id,
            kind: FileKind::File,
            name: name.into(),
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

    pub async fn write<B: BufRead>(&self, node: NodeId, offset: u64, mut data: B) -> IoResult<u32> {
        assert_eq!(offset, 0);

        let mut vec = Vec::new();
        let amount = data.read_to_end(&mut vec).unwrap();

        self.store.set(format!("file/{}", node), vec).await;

        Ok(amount as u32)
    }

    pub async fn read(&self, node: NodeId, offset: u64, amount: u32) -> IoResult<Vec<u8>> {
        let offset = offset as usize;

        let mut contents = self
            .store
            .get(&format!("file/{}", node))
            .await
            .ok_or(IoError::NotFound)?;

        if offset > contents.len() {
            return Err(IoError::OutOfRange);
        }

        let mut offset = contents.split_off(offset);
        let amount = core::cmp::min(amount, offset.len() as u32);
        offset.truncate(amount as usize);

        Ok(offset)
    }
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

        let created = fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();

        let lookup = fs.lookup(1, &OsStr::new("foo.txt")).await.unwrap();
        assert_eq!(lookup, created);

        let read_entry = fs.read_entry(created.node_id).await.unwrap();
        assert_eq!(read_entry, created);
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
            Ok(vec![Entry {
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
        assert_eq!(
            fs.read(attrs.node_id, 4, u32::MAX).await,
            Err(IoError::OutOfRange)
        );
    }

    // Lookup file returns written size.
}
