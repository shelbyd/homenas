use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use structopt::*;

use crate::{chunk_store::*, db::*, object_store::*, stores::*};

#[derive(StructOpt, Debug)]
#[allow(dead_code)] // TODO: Remove.
pub struct StartCommand {
    /// Local port to listen for peer connections.
    #[structopt(long, default_value = "36686")]
    pub(crate) listen_on: u16,

    /// Peers to try to connect to.
    #[structopt(long)]
    pub(crate) peers: Vec<SocketAddr>,

    /// Directories to store data to. If empty, will only store in memory.
    #[structopt(long)]
    pub(crate) backing_dir: Vec<PathBuf>,

    #[structopt(long)]
    /// By default, clear existing mount before trying to mount. If provided, will just try to
    /// mount without clearing first.
    pub(crate) fail_on_existing_mount: bool,

    /// Where to mount the homenas directory.
    pub(crate) mount_path: PathBuf,
}

impl StartCommand {
    pub async fn run(&self, _opts: &crate::commands::Options) -> anyhow::Result<()> {
        let tree: Box<dyn Tree> = match &self.backing_dir[..] {
            [] => {
                log::warn!("No backing-dir provided, only persisting to memory");
                Box::new(crate::db::MemoryTree::default())
            }
            [single] => Box::new(crate::db::Sled::new(single)?),
            [leader, others @ ..] => Box::new(crate::db::Multi::new(
                crate::db::Sled::new(leader)?,
                others
                    .iter()
                    .map(crate::db::Sled::new)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        };
        let tree = Arc::new(tree);

        let object_store: Box<dyn ObjectStore> = match &self.backing_dir[..] {
            [] => Box::new(Memory::default()),
            [single] => Box::new(FileSystem::new(single)?),
            [dirs @ ..] => Box::new(crate::stores::Multi::new(
                dirs.iter()
                    .map(FileSystem::new)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        };

        let object_store = Network::new(object_store, self.listen_on, &self.peers).await?;
        let object_store = Arc::new(object_store);

        let chunk_store: Box<dyn ChunkStore> = match &self.backing_dir[..] {
            [] => todo!(),
            [single] => Box::new(FsChunkStore::new(single)?),
            [dirs @ ..] => Box::new(crate::chunk_store::Multi::new(
                dirs.iter()
                    .map(FsChunkStore::new)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        };
        let chunk_store = Striping::new(chunk_store, "meta/chunks", Arc::clone(&tree));
        let chunk_store = RefCount::new(chunk_store, "meta/chunks", Arc::clone(&tree));

        let fs = crate::fs::FileSystem::new(object_store, Arc::new(chunk_store), Arc::clone(&tree));

        if !self.fail_on_existing_mount {
            crate::operating_system::unmount(&self.mount_path)?;
        }
        crate::operating_system::mount(fs, &self.mount_path)?;

        Ok(())
    }
}
