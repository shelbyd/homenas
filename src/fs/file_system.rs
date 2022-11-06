#![allow(dead_code)] // DO NOT SUBMIT

use std::ffi::OsStr;

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
    pub async fn lookup(&self, _node: NodeId, name: &OsStr) -> IoResult<()> {
        let file_location = format!("file/{}", hex(bytes(name)));
        self.store
            .get(&file_location)
            .await
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

    pub async fn create_file(&self, _parent: NodeId, name: &OsStr) -> IoResult<()> {
        let file_location = format!("file/{}", hex(bytes(name)));
        self.store
            .set(file_location, serde_cbor::to_vec(&()).unwrap())
            .await;

        self.store
            .update(String::from("root"), |buf| {
                let mut children: Vec<_> = buf
                    .map(|buf| serde_cbor::from_slice(buf).unwrap())
                    .unwrap_or_default();

                children.push(ChildItem {
                    kind: FileKind::File,
                    name: name.to_owned(),
                    node_id: 2,
                });

                (serde_cbor::to_vec(&children).unwrap(), ())
            })
            .await;

        Ok(())
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
    async fn list_children_after_create() {
        let mem_os = crate::object_store::Memory::default();
        let fs = FileSystem::new(mem_os);

        fs.create_file(1, &OsStr::new("foo.txt")).await.unwrap();

        dbg!(&fs.store);

        assert_eq!(
            fs.list_children(1).await,
            Ok(vec![ChildItem {
                node_id: 2,
                name: OsString::from("foo.txt"),
                kind: FileKind::File
            }])
        );
    }
}
