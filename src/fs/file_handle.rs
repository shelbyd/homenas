use std::io::BufRead;

use super::*;
use crate::db::*;

#[derive(Serialize, Deserialize)]
struct ChunkIds {
    chunk_size: u32,
    chunks: BTreeMap<u64, ChunkRef>,
}

pub struct FileHandle<C, T> {
    chunk_store: C,
    tree: T,
    chunk_size: u32,
    chunks: BTreeMap<u64, Chunk>,
    meta_key: String,
}

#[derive(Debug)]
enum Chunk {
    Ref(ChunkRef),
    InMemory(
        Vec<u8>,
        Option<String>, // Previous id.
    ),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ChunkRef {
    id: String,
    size: u32,
}

impl<C: ChunkStore, T: Tree> FileHandle<C, T> {
    pub async fn create(
        chunk_store: C,
        chunk_size: u32,
        meta_key: &str,
        tree: T,
    ) -> IoResult<Self> {
        let ids = tree
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
            .map(|(key, value)| (key, Chunk::Ref(value)))
            .collect();

        Ok(Self {
            chunk_store,
            tree,
            chunk_size,
            chunks,
            meta_key: meta_key.to_string(),
        })
    }

    pub async fn read(&self, offset: u64, amount: u32) -> IoResult<Vec<u8>> {
        use std::borrow::Cow;

        let mut ret = Vec::new();
        let mut start = offset;
        let end = offset + amount as u64;

        while start < end {
            let (chunk_offset, buf_start, buf_end) = self.chunk_range(start, end);

            let buf = match self.chunks.get(&chunk_offset) {
                None => return Ok(ret),

                Some(Chunk::InMemory(v, _)) => Cow::Borrowed(v),
                Some(Chunk::Ref(ref_)) => Cow::Owned(self.chunk_store.read(&ref_.id).await?),
            };

            if buf_start > buf.len() {
                return Err(IoError::OutOfRange);
            }

            let buf_end = core::cmp::min(buf_end, buf.len());
            let buf = &buf[buf_start..buf_end];
            start += buf.len() as u64;
            if buf.is_empty() {
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

            let buffer = &mut buffer[buf_start..buf_end];
            buf.read_exact(buffer)?;

            assert_ne!(buffer.len(), 0);
            write_start += buffer.len() as u64;
        }

        self.flush_full_chunks().await?;

        Ok(amount)
    }

    fn chunk_range(&self, start: u64, end: u64) -> (u64, usize, usize) {
        chunk_range(self.chunk_size, start, end)
    }

    async fn write_to(&mut self, offset: u64) -> IoResult<&mut Vec<u8>> {
        assert_eq!(offset % self.chunk_size as u64, 0);

        let entry = self
            .chunks
            .entry(offset)
            .or_insert(Chunk::InMemory(Vec::new(), None));
        entry.load(&self.chunk_store).await
    }

    async fn flush_full_chunks(&mut self) -> IoResult<()> {
        for chunk in self.chunks.values_mut() {
            if chunk.size() == self.chunk_size {
                chunk.flush(&self.chunk_store).await?;
            }
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> IoResult<()> {
        let mut chunk_ids = ChunkIds::new(self.chunk_size);

        for (offset, chunk) in &mut self.chunks {
            let ref_ = chunk.flush(&self.chunk_store).await?;
            chunk_ids.chunks.insert(*offset, ref_);
        }

        self.tree
            .set_typed::<ChunkIds>(&self.meta_key, Some(&chunk_ids))
            .await
    }

    pub fn size(&self) -> u64 {
        self.chunks.values().map(|c| c.size() as u64).sum()
    }

    pub async fn forget(self) -> IoResult<()> {
        for chunk in self.chunks.into_values() {
            chunk.forget(&self.chunk_store).await?;
        }

        self.tree.set(&self.meta_key, None).await?;
        Ok(())
    }
}

impl Chunk {
    fn size(&self) -> u32 {
        match self {
            Chunk::InMemory(v, _) => v.len() as u32,
            Chunk::Ref(r) => r.size,
        }
    }

    async fn flush(&mut self, chunk_store: &impl ChunkStore) -> IoResult<ChunkRef> {
        match self {
            Chunk::Ref(r) => Ok(r.clone()),
            Chunk::InMemory(buf, previous_id) => {
                let id = chunk_store.store(buf).await?;

                if let Some(prev) = previous_id {
                    chunk_store.drop(prev).await?;
                }

                let ref_ = ChunkRef {
                    id,
                    size: buf.len() as u32,
                };
                *self = Chunk::Ref(ref_.clone());
                Ok(ref_)
            }
        }
    }

    async fn forget(self, chunk_store: &impl ChunkStore) -> IoResult<()> {
        match self {
            Chunk::Ref(ref_) => chunk_store.drop(&ref_.id).await?,
            Chunk::InMemory(_, Some(id)) => chunk_store.drop(&id).await?,
            Chunk::InMemory(_, None) => {}
        }

        Ok(())
    }

    async fn load(&mut self, chunk_store: &impl ChunkStore) -> IoResult<&mut Vec<u8>> {
        match self {
            Chunk::InMemory(buf, _) => Ok(buf),
            Chunk::Ref(ref_) => {
                let read = chunk_store.read(&ref_.id).await?;
                *self = Chunk::InMemory(read, Some(ref_.id.clone()));

                match self {
                    Chunk::InMemory(buf, _) => Ok(buf),
                    _ => unreachable!(),
                }
            }
        }
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

fn chunk_range(chunk_size: u32, start: u64, end: u64) -> (u64, usize, usize) {
    let chunk_offset = (start / chunk_size as u64) * chunk_size as u64;

    let buf_start = (start - chunk_offset) as usize;
    let buf_end = core::cmp::min(end - chunk_offset, chunk_size as u64) as usize;

    (chunk_offset, buf_start, buf_end)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_MB: u32 = 1024 * 1024; // 1 MiB

    async fn create<'l, C: ChunkStore, T: Tree>(c: C, o: T, size: u32) -> FileHandle<C, T> {
        FileHandle::create(c, size, "files/test.meta", o)
            .await
            .unwrap()
    }

    #[test]
    fn test_chunk_range() {
        assert_eq!(
            chunk_range(ONE_MB, 4, u32::MAX as u64),
            (0, 4_usize, ONE_MB as usize)
        );
    }

    #[tokio::test]
    async fn created_file_has_no_contents() {
        let mem = memory_chunk_store();
        let fh = create(&mem, MemoryTree::default(), ONE_MB).await;

        assert_eq!(fh.read(0, 4096).await, Ok(Vec::new()));
    }

    #[tokio::test]
    async fn after_write_has_written() {
        let mem = memory_chunk_store();
        let mut fh = create(&mem, MemoryTree::default(), ONE_MB).await;

        assert_eq!(fh.write(0, 3, &b"foo"[..]).await, Ok(3));

        assert_eq!(fh.read(0, 4096).await, Ok(b"foo".to_vec()));
    }

    #[tokio::test]
    async fn write_with_offset() {
        let mem = memory_chunk_store();
        let mut fh = create(&mem, MemoryTree::default(), ONE_MB).await;

        fh.write(3, 3, &[1, 2, 3][..]).await.unwrap();

        assert_eq!(fh.read(0, 4096).await, Ok(vec![0, 0, 0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn write_chunk_prefix() {
        let mem = memory_chunk_store();
        let mut fh = create(&mem, MemoryTree::default(), ONE_MB).await;

        fh.write(3, 3, &[3, 4, 5][..]).await.unwrap();
        fh.write(0, 3, &[0, 1, 2][..]).await.unwrap();

        assert_eq!(fh.read(0, 4096).await, Ok(vec![0, 1, 2, 3, 4, 5]));
    }

    #[tokio::test]
    async fn write_full_chunk_flushes() {
        let mem = memory_chunk_store();
        let mut fh = create(&mem, MemoryTree::default(), 4).await;

        fh.write(0, 4, &[1, 2, 3, 4][..]).await.unwrap();

        assert_eq!(fh.read(0, 4096).await, Ok(vec![1, 2, 3, 4]));
        assert_eq!(mem.read(&id_for(&[1, 2, 3, 4])).await, Ok(vec![1, 2, 3, 4]));
    }

    #[tokio::test]
    async fn multipart_write_flushes_chunk() {
        let mem = memory_chunk_store();
        let mut fh = create(&mem, MemoryTree::default(), 4).await;

        fh.write(0, 2, &[1, 2][..]).await.unwrap();
        fh.write(2, 2, &[3, 4][..]).await.unwrap();

        assert_eq!(fh.read(0, 4096).await, Ok(vec![1, 2, 3, 4]));
        assert_eq!(mem.read(&id_for(&[1, 2, 3, 4])).await, Ok(vec![1, 2, 3, 4]));
    }

    #[tokio::test]
    async fn multipart_write_past_chunk() {
        let mem = memory_chunk_store();
        let mut fh = create(&mem, MemoryTree::default(), 4).await;

        fh.write(0, 2, &[1, 2][..]).await.unwrap();
        fh.write(2, 4, &[3, 4, 5, 6][..]).await.unwrap();

        assert_eq!(fh.read(0, 4096).await, Ok(vec![1, 2, 3, 4, 5, 6]));
        assert_eq!(mem.read(&id_for(&[1, 2, 3, 4])).await, Ok(vec![1, 2, 3, 4]));
    }

    #[tokio::test]
    async fn three_chunks_of_writes() {
        let mem = memory_chunk_store();
        let mut fh = create(&mem, MemoryTree::default(), 4).await;

        let zero_to_twelve = (0..12).collect::<Vec<_>>();
        fh.write(0, 12, zero_to_twelve.as_slice()).await.unwrap();

        assert_eq!(fh.read(0, 4096).await, Ok(zero_to_twelve));
        assert_eq!(mem.read(&id_for(&[0, 1, 2, 3])).await, Ok(vec![0, 1, 2, 3]));
        assert_eq!(mem.read(&id_for(&[4, 5, 6, 7])).await, Ok(vec![4, 5, 6, 7]));
        assert_eq!(
            mem.read(&id_for(&[8, 9, 10, 11])).await,
            Ok(vec![8, 9, 10, 11])
        );
    }

    #[tokio::test]
    async fn another_instance_has_written() {
        let mem = memory_chunk_store();
        let mem_os = MemoryTree::default();
        let mut fh = create(&mem, &mem_os, 4).await;

        fh.write(0, 4, &[0, 1, 2, 3][..]).await.unwrap();
        fh.flush().await.unwrap();

        let new_fh = create(&mem, &mem_os, 4).await;
        assert_eq!(new_fh.read(0, 4096).await, Ok(vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn read_starts_past_data() {
        let mem = memory_chunk_store();
        let mut fh = create(&mem, MemoryTree::default(), ONE_MB).await;

        assert_eq!(fh.write(0, 3, &b"foo"[..]).await, Ok(3));

        assert_eq!(fh.read(4, 4096).await, Err(IoError::OutOfRange));
    }

    #[tokio::test]
    async fn forget_clears_store() {
        let mem = memory_chunk_store();
        let mut fh = create(&mem, MemoryTree::default(), ONE_MB).await;

        fh.write(0, 3, &b"foo"[..]).await.unwrap();
        fh.flush().await.unwrap();

        assert_eq!(fh.forget().await, Ok(()));
        assert!(!mem.values().contains(&b"foo".to_vec()));
    }

    #[tokio::test]
    async fn forget_after_change_is_empty() {
        let mem = memory_chunk_store();
        let mut fh = create(&mem, MemoryTree::default(), ONE_MB).await;

        fh.write(0, 3, &b"foo"[..]).await.unwrap();
        fh.flush().await.unwrap();

        fh.write(0, 3, &b"bar"[..]).await.unwrap();
        fh.flush().await.unwrap();

        assert_eq!(fh.forget().await, Ok(()));
        assert!(!mem.values().contains(&b"foo".to_vec()));
        assert!(!mem.values().contains(&b"bar".to_vec()));
    }
}
