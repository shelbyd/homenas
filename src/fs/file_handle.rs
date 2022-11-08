#![allow(dead_code)]

use std::{io::BufRead, ops::Range};

use super::*;
use crate::object_store::*;

#[derive(Serialize, Deserialize)]
struct ChunkIds {
    chunk_size: u32,
    chunks: BTreeMap<u64, String>,
}

pub struct FileHandle<O> {
    store: CborTyped<O>,
    chunk_size: u32,
    chunks: BTreeMap<u64, Chunk>,
    meta_key: String,
}

#[derive(Debug)]
enum Chunk {
    Id(String),
    Cached(Vec<u8>),
}

impl<O: ObjectStore> FileHandle<O> {
    pub async fn create(store: O, chunk_size: u32, meta_key: &str) -> IoResult<Self> {
        let store = CborTyped::new(store);

        let ids = store
            .get_typed::<ChunkIds>(meta_key)
            .await?
            .unwrap_or_else(|| ChunkIds::new(chunk_size));

        if chunk_size != ids.chunk_size {
            log::error!(
                "Mismatched chunk_size, (specified, file had): {:?}",
                (chunk_size, ids.chunk_size)
            );
            return Err(IoError::InvalidData);
        }

        let chunks = ids
            .chunks
            .into_iter()
            .map(|(key, value)| (key, Chunk::Id(value)))
            .collect();

        Ok(Self {
            store,
            chunk_size: chunk_size,
            chunks,
            meta_key: meta_key.to_string(),
        })
    }

    pub fn chunk_key(&self, index: usize) -> Option<String> {
        let chunk = self.chunks.iter().nth(index)?.1;
        Some(chunk.storage_key())
    }

    pub async fn read(&self, range: Range<u64>) -> IoResult<Vec<u8>> {
        use std::borrow::Cow;

        let mut ret = Vec::new();
        let mut start = range.start;

        while start < range.end {
            let (chunk_offset, buf_start, buf_end) = self.chunk_range(start, range.end);

            let buf = match self.chunks.get(&chunk_offset) {
                None => return Ok(ret),

                Some(Chunk::Cached(v)) => Cow::Borrowed(v),
                Some(c @ Chunk::Id(_)) => Cow::Owned(
                    self.store
                        .get(&c.storage_key())
                        .await?
                        .ok_or(IoError::NotFound)?,
                ),
            };

            let buf_end = core::cmp::min(buf_end, buf.len());
            let buf = &buf[buf_start..buf_end];
            start += buf.len() as u64;
            if buf.len() == 0 {
                break;
            }
            ret.extend(buf);
        }

        Ok(ret)
    }

    pub async fn write(
        &mut self,
        offset: u64,
        amount: u32,
        mut buf: impl BufRead,
    ) -> IoResult<u32> {
        let mut write_start = offset;
        let write_end = write_start + amount as u64;

        while write_start < write_end {
            let (chunk_offset, buf_start, buf_end) = self.chunk_range(write_start, write_end);
            let buffer = self.write_to(chunk_offset).await?;

            if buf_end > buffer.len() {
                buffer.resize(buf_end, 0);
            }

            let mut buffer = &mut buffer[buf_start..buf_end];
            buf.read_exact(&mut buffer)?;

            assert_ne!(buffer.len(), 0);
            write_start += buffer.len() as u64;
        }

        self.flush_full_chunks().await?;

        Ok(amount)
    }

    fn chunk_range(&self, start: u64, end: u64) -> (u64, usize, usize) {
        let chunk_offset = (start / self.chunk_size as u64) * self.chunk_size as u64;

        let buf_start = (start - chunk_offset) as usize;
        let buf_end = core::cmp::min(end - chunk_offset, self.chunk_size as u64) as usize;

        (chunk_offset, buf_start, buf_end)
    }

    async fn write_to(&mut self, offset: u64) -> IoResult<&mut Vec<u8>> {
        assert_eq!(offset % self.chunk_size as u64, 0);

        let entry = self
            .chunks
            .entry(offset)
            .or_insert(Chunk::Cached(Vec::new()));
        if let Chunk::Cached(v) = entry {
            return Ok(v);
        }

        let buf = self
            .store
            .get(&entry.storage_key())
            .await?
            .ok_or(IoError::NotFound)?;
        *entry = Chunk::Cached(buf);

        match entry {
            Chunk::Cached(v) => Ok(v),
            _ => unreachable!(),
        }
    }

    async fn flush_full_chunks(&mut self) -> IoResult<()> {
        for chunk in self.chunks.values_mut() {
            match chunk {
                Chunk::Id(_) => continue,
                Chunk::Cached(buf) if buf.len() != self.chunk_size as usize => continue,
                Chunk::Cached(buf) => {
                    let owned = buf.clone();

                    self.store.set(chunk.storage_key(), owned).await?;

                    *chunk = Chunk::Id(chunk.id());
                }
            }
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> IoResult<()> {
        let mut chunk_ids = ChunkIds::new(self.chunk_size);

        for (offset, chunk) in &mut self.chunks {
            let id = match chunk {
                Chunk::Id(id) => id.clone(),
                Chunk::Cached(buf) => {
                    let owned = buf.clone();

                    self.store.set(chunk.storage_key(), owned).await?;

                    *chunk = Chunk::Id(chunk.id());
                    chunk.id()
                }
            };
            chunk_ids.chunks.insert(*offset, id);
        }

        self.store
            .set_typed::<ChunkIds>(self.meta_key.clone(), &chunk_ids)
            .await
    }
}

impl Chunk {
    fn id(&self) -> String {
        match self {
            Chunk::Id(s) => s.clone(),
            Chunk::Cached(v) => hex::encode(blake3::hash(v).as_bytes()),
        }
    }

    fn storage_key(&self) -> String {
        format!("chunks/{}", self.id())
    }
}

impl ChunkIds {
    fn new(size: u32) -> Self {
        ChunkIds {
            chunks: Default::default(),
            chunk_size: size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_MB: u32 = 1024 * 1024; // 1 MiB

    #[tokio::test]
    async fn created_file_has_no_contents() {
        let mem = Memory::default();
        let fh = FileHandle::create(&mem, ONE_MB, "meta").await.unwrap();

        assert_eq!(fh.read(0..4096).await, Ok(Vec::new()));
    }

    #[tokio::test]
    async fn after_write_has_written() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, ONE_MB, "meta").await.unwrap();

        assert_eq!(fh.write(0, 3, &b"foo"[..]).await, Ok(3));

        assert_eq!(fh.read(0..4096).await, Ok(b"foo".to_vec()));
    }

    #[tokio::test]
    async fn write_with_offset() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, ONE_MB, "meta").await.unwrap();

        fh.write(3, 3, &[1, 2, 3][..]).await.unwrap();

        assert_eq!(fh.read(0..4096).await, Ok(vec![0, 0, 0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn write_chunk_prefix() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, ONE_MB, "meta").await.unwrap();

        fh.write(3, 3, &[3, 4, 5][..]).await.unwrap();
        fh.write(0, 3, &[0, 1, 2][..]).await.unwrap();

        assert_eq!(fh.read(0..4096).await, Ok(vec![0, 1, 2, 3, 4, 5]));
    }

    #[tokio::test]
    async fn write_full_chunk_flushes() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, 4, "meta").await.unwrap();

        fh.write(0, 4, &[1, 2, 3, 4][..]).await.unwrap();

        assert_eq!(fh.read(0..4096).await, Ok(vec![1, 2, 3, 4]));
        assert_eq!(
            mem.get(&fh.chunk_key(0).unwrap()).await,
            Ok(Some(vec![1, 2, 3, 4]))
        );
    }

    #[tokio::test]
    async fn multipart_write_flushes_chunk() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, 4, "meta").await.unwrap();

        fh.write(0, 2, &[1, 2][..]).await.unwrap();
        fh.write(2, 2, &[3, 4][..]).await.unwrap();

        assert_eq!(fh.read(0..4096).await, Ok(vec![1, 2, 3, 4]));
        assert_eq!(
            mem.get(&fh.chunk_key(0).unwrap()).await,
            Ok(Some(vec![1, 2, 3, 4]))
        );
    }

    #[tokio::test]
    async fn multipart_write_past_chunk() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, 4, "meta").await.unwrap();

        fh.write(0, 2, &[1, 2][..]).await.unwrap();
        fh.write(2, 4, &[3, 4, 5, 6][..]).await.unwrap();

        assert_eq!(fh.read(0..4096).await, Ok(vec![1, 2, 3, 4, 5, 6]));
        assert_eq!(
            mem.get(&fh.chunk_key(0).unwrap()).await,
            Ok(Some(vec![1, 2, 3, 4]))
        );
    }

    #[tokio::test]
    async fn three_chunks_of_writes() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, 4, "meta").await.unwrap();

        let zero_to_twelve = (0..12).collect::<Vec<_>>();
        fh.write(0, 12, zero_to_twelve.as_slice()).await.unwrap();

        assert_eq!(fh.read(0..4096).await, Ok(zero_to_twelve));
        assert_eq!(
            mem.get(&fh.chunk_key(0).unwrap()).await,
            Ok(Some(vec![0, 1, 2, 3]))
        );
        assert_eq!(
            mem.get(&fh.chunk_key(1).unwrap()).await,
            Ok(Some(vec![4, 5, 6, 7]))
        );
        assert_eq!(
            mem.get(&fh.chunk_key(2).unwrap()).await,
            Ok(Some(vec![8, 9, 10, 11]))
        );
    }

    #[tokio::test]
    async fn another_instance_has_written() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, 4, "meta").await.unwrap();

        fh.write(0, 4, &[0, 1, 2, 3][..]).await.unwrap();
        fh.flush().await.unwrap();

        let new_fh = FileHandle::create(&mem, 4, "meta").await.unwrap();
        assert_eq!(new_fh.read(0..4096).await, Ok(vec![0, 1, 2, 3]));
    }
}
