#![allow(dead_code)]

use std::io::BufRead;

use super::*;
use crate::object_store::*;

#[derive(Serialize, Deserialize)]
struct ChunkIds {
    chunk_size: u32,
    chunks: BTreeMap<u64, ChunkRef>,
}

pub struct FileHandle<O> {
    store: CborTyped<O>,
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
            .map(|(key, value)| (key, Chunk::Ref(value)))
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
                Some(c @ Chunk::Ref(_)) => Cow::Owned(
                    self.store
                        .get(&c.storage_key())
                        .await?
                        .ok_or(IoError::NotFound)?,
                ),
            };

            if buf_start > buf.len() {
                return Err(IoError::OutOfRange);
            }

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
        chunk_range(self.chunk_size, start, end)
    }

    async fn write_to(&mut self, offset: u64) -> IoResult<&mut Vec<u8>> {
        assert_eq!(offset % self.chunk_size as u64, 0);

        let entry = self
            .chunks
            .entry(offset)
            .or_insert(Chunk::InMemory(Vec::new(), None));
        entry.load(&self.store).await
    }

    async fn flush_full_chunks(&mut self) -> IoResult<()> {
        for chunk in self.chunks.values_mut() {
            if chunk.size() == self.chunk_size {
                chunk.flush(&self.store).await?;
            }
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> IoResult<()> {
        let mut chunk_ids = ChunkIds::new(self.chunk_size);

        for (offset, chunk) in &mut self.chunks {
            chunk.flush(&self.store).await?;
            chunk_ids.chunks.insert(*offset, chunk.ref_());
        }

        self.store
            .set_typed::<ChunkIds>(&self.meta_key, &chunk_ids)
            .await
    }

    pub fn size(&self) -> u64 {
        self.chunks.values().map(|c| c.size() as u64).sum()
    }

    pub async fn forget(self) -> IoResult<()> {
        for chunk in self.chunks.into_values() {
            chunk.forget(&self.store).await?;
        }

        self.store.clear(&self.meta_key).await?;
        Ok(())
    }
}

impl Chunk {
    fn id(&self) -> String {
        match self {
            Chunk::Ref(r) => r.id.clone(),
            Chunk::InMemory(v, _) => Self::id_of(v),
        }
    }

    fn storage_key(&self) -> String {
        Self::build_storage_key(&self.id())
    }

    fn build_storage_key(id: &str) -> String {
        format!("chunks/{}/{}", &id[..4], &id[4..])
    }

    fn id_of(data: &[u8]) -> String {
        hex::encode(blake3::hash(data).as_bytes())
    }

    fn size(&self) -> u32 {
        match self {
            Chunk::InMemory(v, _) => v.len() as u32,
            Chunk::Ref(r) => r.size,
        }
    }

    async fn flush(&mut self, store: &CborTyped<impl ObjectStore>) -> IoResult<()> {
        match self {
            Chunk::Ref(_) => {}
            Chunk::InMemory(v, Some(prev)) if *prev == Self::id_of(v) => {}

            Chunk::InMemory(buf, previous_id) => {
                let id = Self::id_of(buf);
                log::info!("{}: Flushing chunk", id);
                store.set(&Self::build_storage_key(&id), &buf).await?;

                if let Some(prev) = previous_id {
                    store.clear(&Self::build_storage_key(&prev)).await?;
                }

                *self = Chunk::Ref(ChunkRef {
                    id,
                    size: buf.len() as u32,
                });
            }
        }

        Ok(())
    }

    fn ref_(&self) -> ChunkRef {
        ChunkRef {
            id: self.id(),
            size: self.size(),
        }
    }

    async fn forget(self, store: &CborTyped<impl ObjectStore>) -> IoResult<()> {
        store.clear(&self.storage_key()).await?;
        Ok(())
    }

    async fn load(&mut self, store: &CborTyped<impl ObjectStore>) -> IoResult<&mut Vec<u8>> {
        match self {
            Chunk::InMemory(buf, _) => Ok(buf),
            Chunk::Ref(ref_) => {
                let id = ref_.id.clone();

                let read = store
                    .get(&self.storage_key())
                    .await?
                    .ok_or(IoError::NotFound)?;
                *self = Chunk::InMemory(read, Some(id));

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

    #[test]
    fn test_chunk_range() {
        assert_eq!(
            chunk_range(ONE_MB, 4, u32::MAX as u64),
            (0, 4 as usize, ONE_MB as usize)
        );
    }

    #[test]
    fn chunk_splits_to_subdirs() {
        let chunk = Chunk::Ref(ChunkRef {
            id: String::from("deadbeef"),
            size: 42,
        });
        assert_eq!(&chunk.storage_key(), "chunks/dead/beef");
    }

    #[tokio::test]
    async fn created_file_has_no_contents() {
        let mem = Memory::default();
        let fh = FileHandle::create(&mem, ONE_MB, "meta").await.unwrap();

        assert_eq!(fh.read(0, 4096).await, Ok(Vec::new()));
    }

    #[tokio::test]
    async fn after_write_has_written() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, ONE_MB, "meta").await.unwrap();

        assert_eq!(fh.write(0, 3, &b"foo"[..]).await, Ok(3));

        assert_eq!(fh.read(0, 4096).await, Ok(b"foo".to_vec()));
    }

    #[tokio::test]
    async fn write_with_offset() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, ONE_MB, "meta").await.unwrap();

        fh.write(3, 3, &[1, 2, 3][..]).await.unwrap();

        assert_eq!(fh.read(0, 4096).await, Ok(vec![0, 0, 0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn write_chunk_prefix() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, ONE_MB, "meta").await.unwrap();

        fh.write(3, 3, &[3, 4, 5][..]).await.unwrap();
        fh.write(0, 3, &[0, 1, 2][..]).await.unwrap();

        assert_eq!(fh.read(0, 4096).await, Ok(vec![0, 1, 2, 3, 4, 5]));
    }

    #[tokio::test]
    async fn write_full_chunk_flushes() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, 4, "meta").await.unwrap();

        fh.write(0, 4, &[1, 2, 3, 4][..]).await.unwrap();

        assert_eq!(fh.read(0, 4096).await, Ok(vec![1, 2, 3, 4]));
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

        assert_eq!(fh.read(0, 4096).await, Ok(vec![1, 2, 3, 4]));
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

        assert_eq!(fh.read(0, 4096).await, Ok(vec![1, 2, 3, 4, 5, 6]));
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

        assert_eq!(fh.read(0, 4096).await, Ok(zero_to_twelve));
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
        assert_eq!(new_fh.read(0, 4096).await, Ok(vec![0, 1, 2, 3]));
    }

    #[tokio::test]
    async fn read_starts_past_data() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, ONE_MB, "meta").await.unwrap();

        assert_eq!(fh.write(0, 3, &b"foo"[..]).await, Ok(3));

        assert_eq!(fh.read(4, 4096).await, Err(IoError::OutOfRange));
    }

    #[tokio::test]
    async fn forget_clears_store() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, ONE_MB, "meta").await.unwrap();

        fh.write(0, 3, &b"foo"[..]).await.unwrap();
        fh.flush().await.unwrap();

        assert_eq!(fh.forget().await, Ok(()));
        assert_eq!(mem.len(), 0);
    }

    #[tokio::test]
    async fn forget_after_change_is_empty() {
        let mem = Memory::default();
        let mut fh = FileHandle::create(&mem, ONE_MB, "meta").await.unwrap();

        fh.write(0, 3, &b"foo"[..]).await.unwrap();
        fh.flush().await.unwrap();

        fh.write(0, 3, &b"bar"[..]).await.unwrap();
        fh.flush().await.unwrap();

        assert_eq!(fh.forget().await, Ok(()));
        assert_eq!(mem.len(), 0);
    }
}