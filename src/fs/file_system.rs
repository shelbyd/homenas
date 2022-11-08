use std::{ffi::OsStr, io::BufRead};

use super::*;
use crate::object_store::{self, CborTyped, *};

pub struct FileSystem<O> {
    store: CborTyped<O>,
}

impl<O> FileSystem<O> {
    pub fn new(store: O) -> Self {
        Self {
            store: CborTyped::new(store),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Contents {
    Raw(Vec<u8>),
}

impl<O: ObjectStore> FileSystem<O> {
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
            .store
            .get_typed::<Entry>(&format!("file/{}.meta", node))
            .await?
        {
            Some(e) => Ok(e),
            None if node == 1 => {
                return Ok(Entry {
                    node_id: 1,
                    name: OsString::from("."),
                    kind: DetailedKind::Directory(DirectoryData::default()),
                });
            }
            None => unreachable!("parent has broken link to child"),
        }
    }

    async fn write_entry(&self, entry: Entry) -> IoResult<()> {
        self.store
            .set_typed(format!("file/{}.meta", entry.node_id), &entry)
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

    pub async fn create_file(&self, parent: NodeId, name: &OsStr) -> IoResult<Entry<DetailedKind>> {
        let new_node_id = self.get_next_node_id().await?;

        let contents_location = format!("file/{}", new_node_id);
        self.store
            .set_typed::<Contents>(contents_location, &Contents::Raw(Vec::new()))
            .await?;

        let file_entry = Entry {
            kind: DetailedKind::File(FileData { size: 0 }),
            name: name.to_owned(),
            node_id: new_node_id,
        };
        self.write_entry(file_entry).await?;

        let mut parent = self.read_entry(parent).await?;
        parent
            .kind
            .as_dir_mut()
            .ok_or(IoError::NotADirectory)?
            .children
            .insert(name.into(), new_node_id);
        self.write_entry(parent).await?;

        Ok(Entry {
            node_id: new_node_id,
            kind: EntryKind::File(FileData { size: 0 }),
            name: name.into(),
        })
    }

    async fn get_next_node_id(&self) -> IoResult<u64> {
        object_store::update_typed(&*self.store, "meta/next_node_id", |next| {
            let next = next.unwrap_or(2);
            Ok((next + 1, next))
        })
        .await
    }

    pub async fn write<B: BufRead>(&self, node: NodeId, offset: u64, mut data: B) -> IoResult<u32> {
        assert_eq!(offset, 0);

        let mut entry = self.read_entry(node).await?;
        let file_data = entry.kind.as_file_mut().ok_or(IoError::NotAFile)?;

        let mut vec = Vec::new();
        let amount = data.read_to_end(&mut vec).unwrap();
        file_data.size = amount as u64;

        self.store.set_typed::<Contents>(format!("file/{}", node), &Contents::Raw(vec)).await?;
        self.write_entry(entry).await?;

        Ok(amount as u32)
    }

    pub async fn read(&self, node: NodeId, offset: u64, amount: u32) -> IoResult<Vec<u8>> {
        let offset = offset as usize;

        let contents = self
            .store
            .get_typed::<Contents>(&format!("file/{}", node))
            .await?
            .ok_or(IoError::NotFound)?;

        let mut buf = match contents {
            Contents::Raw(buf) => buf,
        };

        if offset > buf.len() {
            return Err(IoError::OutOfRange);
        }

        let mut offset = buf.split_off(offset);
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

        let created = fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();

        let entries = fs.list_children(1).await.unwrap();
        assert!(entries.contains(&created));
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

    #[tokio::test]
    async fn write_read_includes_size() {
        let mem_os = crate::object_store::Memory::default();
        let fs = FileSystem::new(mem_os);

        let attrs = fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();
        assert_eq!(fs.write(attrs.node_id, 0, &[1, 2, 3][..]).await, Ok(3));

        let entry = fs.read_entry(attrs.node_id).await.unwrap();
        assert_eq!(entry.kind.as_file().unwrap().size, 3);
    }
}
