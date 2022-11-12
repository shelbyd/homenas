use dashmap::{mapref::entry::Entry as DashMapEntry, DashMap};
use std::{ffi::OsStr, io::BufRead};

use super::*;
use crate::{
    db::{update_typed, *},
    object_store::*,
};

pub struct FileSystem<O, C, T> {
    #[allow(unused)]
    object_store: O,
    chunk_store: C,
    tree: T,
    open_handles: DashMap<NodeId, FileHandle<C, T>>,
}

impl<O, C, T> FileSystem<O, C, T> {
    pub fn new(object_store: O, chunk_store: C, tree: T) -> Self {
        Self {
            object_store,
            chunk_store,
            tree,
            open_handles: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Contents {
    Raw(Vec<u8>),
}

impl<O: ObjectStore + Clone, C: ChunkStore + Clone, T: Tree + Clone> FileSystem<O, C, T> {
    pub async fn lookup(&self, parent: NodeId, name: &OsStr) -> IoResult<Entry> {
        let parent = self.read_entry(parent).await?;

        self.read_entry(
            *parent
                .kind
                .as_dir()
                .ok_or(IoError::NotADirectory)?
                .children
                .get(name)
                .ok_or(IoError::NotFound)?,
        )
        .await
    }

    pub async fn read_entry(&self, node: NodeId) -> IoResult<Entry> {
        log::debug!("read_entry(node): {:?}", node);

        match self
            .tree
            .get_typed::<Entry>(&format!("file/{}.meta", node))
            .await?
        {
            Some(e) => Ok(e),
            None if node == 1 => return Ok(root_entry()),
            None => Err(IoError::NotFound),
        }
    }

    async fn write_entry(&self, entry: &Entry) -> IoResult<()> {
        self.tree
            .set_typed(&format!("file/{}.meta", entry.node_id), Some(&entry))
            .await
    }

    async fn clear_entry(&self, node: NodeId) -> IoResult<()> {
        self.tree.set(&format!("file/{}.meta", node), None).await
    }

    pub async fn update_entry<R: Send>(
        &self,
        node: NodeId,
        mut update: impl FnMut(&mut Entry) -> IoResult<R> + Send,
    ) -> IoResult<(Entry, R)> {
        update_typed(&self.tree, &format!("file/{}.meta", node), |entry| {
            let mut entry = match entry {
                Some(e) => e,
                None if node == 1 => root_entry(),
                None => return Err(IoError::NotFound),
            };

            let r = update(&mut entry)?;

            Ok((Some(entry.clone()), (entry, r)))
        })
        .await
    }

    pub async fn list_children(&self, node: NodeId) -> IoResult<Vec<Entry>> {
        let mut node = self.read_entry(node).await?;
        node.name = OsString::from(".");

        let mut fake_parent = node.clone();
        fake_parent.name = OsString::from("..");

        let children = futures::future::try_join_all(
            node.kind
                .as_dir()
                .ok_or(IoError::NotADirectory)?
                .children
                .values()
                .map(|id| self.read_entry(*id)),
        )
        .await?;

        Ok([node, fake_parent].into_iter().chain(children).collect())
    }

    pub async fn create_file(&self, parent: NodeId, name: &OsStr) -> IoResult<Entry> {
        log::info!(
            "Creating file, (parent, name): ({}, {:?})",
            parent,
            name.to_str()
        );

        let entry = self
            .add_entry_to_parent(parent, name, |node_id| Entry {
                kind: DetailedKind::File(FileData { size: 0 }),
                name: name.to_owned(),
                node_id,
            })
            .await?;

        log::info!("Created file {}", entry.node_id);
        Ok(entry)
    }

    async fn add_entry_to_parent(
        &self,
        parent: NodeId,
        name: &OsStr,
        entry: impl FnOnce(NodeId) -> Entry,
    ) -> IoResult<Entry> {
        let new_node_id = self.get_next_node_id().await?;
        let entry = entry(new_node_id);

        self.write_entry(&entry).await?;

        self.update_entry(parent, |parent| {
            parent
                .kind
                .as_dir_mut()
                .ok_or(IoError::NotADirectory)?
                .children
                .insert(name.into(), new_node_id);
            Ok(())
        })
        .await?;

        Ok(entry)
    }

    async fn get_next_node_id(&self) -> IoResult<NodeId> {
        update_typed(&self.tree, "meta/next_node_id", |next| {
            let next = next.unwrap_or(2);
            Ok((Some(next + 1), next))
        })
        .await
    }

    pub async fn create_dir(&self, parent: NodeId, name: &OsStr) -> IoResult<Entry> {
        log::info!(
            "Creating directory, (parent, name): ({}, {:?})",
            parent,
            name.to_str()
        );

        let entry = self
            .add_entry_to_parent(parent, name, |node_id| Entry {
                kind: DetailedKind::Directory(DirectoryData {
                    children: Default::default(),
                }),
                name: name.to_owned(),
                node_id,
            })
            .await?;

        log::info!("Created directory {}", entry.node_id);
        Ok(entry)
    }

    pub async fn unlink(&self, parent: NodeId, name: &OsStr) -> IoResult<NodeId> {
        log::info!(
            "Unlinking entry, (parent, name): ({}, {:?})",
            parent,
            name.to_str()
        );

        let (_, node_id) = self
            .update_entry(parent, |parent| {
                Ok(parent
                    .kind
                    .as_dir_mut()
                    .ok_or(IoError::NotADirectory)?
                    .children
                    .remove(name)
                    .ok_or(IoError::NotFound)?)
            })
            .await?;

        log::info!("Unlinked node {}", node_id);
        Ok(node_id)
    }

    pub async fn forget(&self, node: NodeId) -> IoResult<()> {
        log::info!("Forgetting node {}", node);

        let entry = self.read_entry(node).await?;
        self.clear_entry(node).await?;

        match entry.kind {
            EntryKind::File(_) => {
                let handle = self.create_handle(node).await?;
                handle.forget().await?;
            }
            EntryKind::Directory(_) => {}
        }

        Ok(())
    }

    pub async fn open(&self, node: NodeId) -> IoResult<HandleId> {
        match self.open_handles.entry(node) {
            DashMapEntry::Occupied(_) => Err(IoError::TempUnavailable),
            DashMapEntry::Vacant(v) => {
                let handle = self.create_handle(node).await?;
                v.insert(handle);
                Ok(node)
            }
        }
    }

    async fn create_handle(&self, node: NodeId) -> IoResult<FileHandle<C, T>> {
        let handle = FileHandle::create(
            self.chunk_store.clone(),
            1024 * 1024,
            &format!("file/{}.chunks", node),
            self.tree.clone(),
        )
        .await?;
        Ok(handle)
    }

    pub async fn write<B: BufRead>(
        &self,
        node: NodeId,
        offset: u64,
        amount: u32,
        data: B,
    ) -> IoResult<u32> {
        // Experiment showed negligible to 0 contention here with large file writes.
        self.open_handles
            .get_mut(&node)
            .ok_or(IoError::BadDescriptor)?
            .write(offset, amount, data)
            .await
    }

    pub async fn read(&self, node: NodeId, offset: u64, amount: u32) -> IoResult<Vec<u8>> {
        self.open_handles
            .get(&node)
            .ok_or(IoError::BadDescriptor)?
            .read(offset, amount)
            .await
    }

    pub async fn flush(&self, node: NodeId) -> IoResult<()> {
        self.open_handles
            .get_mut(&node)
            .ok_or(IoError::BadDescriptor)?
            .flush()
            .await
    }

    pub async fn release(&self, node: NodeId) -> IoResult<()> {
        let mut handle = self
            .open_handles
            .remove(&node)
            .ok_or(IoError::BadDescriptor)?
            .1;
        handle.flush().await?;

        self.update_entry(node, |entry| {
            entry.kind.as_file_mut().ok_or(IoError::NotAFile)?.size = handle.size();
            Ok(())
        })
        .await?;

        Ok(())
    }
}

fn root_entry() -> Entry {
    Entry {
        node_id: 1,
        name: OsString::from("."),
        kind: DetailedKind::Directory(DirectoryData::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::Memory;

    #[tokio::test]
    async fn empty_root() {
        let mem = memory_chunk_store();
        let mem_os = Memory::default();
        let mem_tree = MemoryTree::default();
        let fs = FileSystem::new(&mem_os, &mem, &mem_tree);

        assert_eq!(
            fs.lookup(1, &OsStr::new("foo.txt")).await,
            Err(IoError::NotFound)
        );

        assert_eq!(
            fs.list_children(1).await,
            Ok(vec![
                Entry {
                    node_id: 1,
                    name: OsString::from("."),
                    kind: EntryKind::Directory(DirectoryData {
                        children: maplit::btreemap! {},
                    }),
                },
                Entry {
                    node_id: 1,
                    name: OsString::from(".."),
                    kind: EntryKind::Directory(DirectoryData {
                        children: maplit::btreemap! {},
                    }),
                },
            ])
        );
    }

    #[tokio::test]
    async fn create_child() {
        let mem = memory_chunk_store();
        let mem_os = Memory::default();
        let mem_tree = MemoryTree::default();
        let fs = FileSystem::new(&mem_os, &mem, &mem_tree);

        let created = fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();

        let lookup = fs.lookup(1, &OsStr::new("foo.txt")).await.unwrap();
        assert_eq!(lookup, created);

        let read_entry = fs.read_entry(created.node_id).await.unwrap();
        assert_eq!(read_entry, created);
    }

    #[tokio::test]
    async fn create_child_does_not_have_random_children() {
        let mem = memory_chunk_store();
        let mem_os = Memory::default();
        let mem_tree = MemoryTree::default();
        let fs = FileSystem::new(&mem_os, &mem, &mem_tree);

        fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();

        assert_eq!(
            fs.lookup(1, &OsStr::new("bar.txt")).await,
            Err(IoError::NotFound)
        );
    }

    #[tokio::test]
    async fn list_children_after_create() {
        let mem = memory_chunk_store();
        let mem_os = Memory::default();
        let mem_tree = MemoryTree::default();
        let fs = FileSystem::new(&mem_os, &mem, &mem_tree);

        let created = fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();

        let entries = fs.list_children(1).await.unwrap();
        assert!(entries.contains(&created));
    }

    #[tokio::test]
    async fn write_read() {
        let mem = memory_chunk_store();
        let mem_os = Memory::default();
        let mem_tree = MemoryTree::default();
        let fs = FileSystem::new(&mem_os, &mem, &mem_tree);

        let attrs = fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();
        fs.open(attrs.node_id).await.unwrap();
        assert_eq!(fs.write(attrs.node_id, 0, 3, &[1, 2, 3][..]).await, Ok(3));

        assert_eq!(fs.read(attrs.node_id, 0, u32::MAX).await, Ok(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn read_past_end() {
        let mem = memory_chunk_store();
        let mem_os = Memory::default();
        let mem_tree = MemoryTree::default();
        let fs = FileSystem::new(&mem_os, &mem, &mem_tree);

        let attrs = fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();
        fs.open(attrs.node_id).await.unwrap();
        fs.write(attrs.node_id, 0, 3, &[1, 2, 3][..]).await.unwrap();

        assert_eq!(fs.read(attrs.node_id, 3, u32::MAX).await, Ok(vec![]));
        assert_eq!(
            fs.read(attrs.node_id, 4, u32::MAX).await,
            Err(IoError::OutOfRange)
        );
    }

    #[tokio::test]
    async fn write_read_includes_size() {
        let mem = memory_chunk_store();
        let mem_os = Memory::default();
        let mem_tree = MemoryTree::default();
        let fs = FileSystem::new(&mem_os, &mem, &mem_tree);

        let attrs = fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();
        fs.open(attrs.node_id).await.unwrap();
        assert_eq!(fs.write(attrs.node_id, 0, 3, &[1, 2, 3][..]).await, Ok(3));
        fs.release(attrs.node_id).await.unwrap();

        let entry = fs.read_entry(attrs.node_id).await.unwrap();
        assert_eq!(entry.kind.as_file().unwrap().size, 3);
    }

    #[tokio::test]
    async fn unlink_deletes_file() {
        let mem = memory_chunk_store();
        let mem_os = Memory::default();
        let mem_tree = MemoryTree::default();
        let fs = FileSystem::new(&mem_os, &mem, &mem_tree);

        let created = fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();
        fs.unlink(1, &OsStr::new("foo.txt")).await.unwrap();

        let entries = fs.list_children(1).await.unwrap();
        assert!(!entries.contains(&created));

        assert_eq!(
            Memory::default()
                .get(&format!("files/{}.meta", created.node_id))
                .await,
            Err(IoError::NotFound)
        );
    }

    #[tokio::test]
    async fn rmdir_deletes_dir() {
        let mem = memory_chunk_store();
        let mem_os = Memory::default();
        let mem_tree = MemoryTree::default();
        let fs = FileSystem::new(&mem_os, &mem, &mem_tree);

        let created = fs.create_dir(1, &OsStr::new("foo")).await.unwrap();
        fs.unlink(1, &OsStr::new("foo")).await.unwrap();
        fs.forget(created.node_id).await.unwrap();

        let entries = fs.list_children(1).await.unwrap();
        assert!(!entries.contains(&created));

        assert_eq!(
            mem_tree
                .get(&format!("files/{}.meta", created.node_id))
                .await,
            Ok(None)
        );
    }
}
