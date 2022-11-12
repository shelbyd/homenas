use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use structopt::*;

use crate::{chunk_store::*, db::*, PROJECT_DIRS, *};

#[derive(StructOpt, Debug)]
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

        let chunk_store: Box<dyn ChunkStore> = match &self.backing_dir[..] {
            [] => Box::new(MemChunkStore::default()),
            [single] => Box::new(FsChunkStore::new(single)?),
            [dirs @ ..] => Box::new(crate::chunk_store::Multi::new(
                dirs.iter()
                    .map(FsChunkStore::new)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        };

        let store = Arc::new(
            NetworkStore::create(
                Arc::new(tree),
                chunk_store,
                self.listen_on,
                &self.peers,
                PROJECT_DIRS.data_dir(),
            )
            .await?,
        );

        let chunk_store = Arc::clone(&store);
        let chunk_store = Striping::new(chunk_store, Arc::clone(&store));
        let chunk_store = RefCount::new(chunk_store, Arc::clone(&store));

        let fs = crate::fs::FileSystem::new(Arc::new(chunk_store), Arc::clone(&store));

        if !self.fail_on_existing_mount {
            crate::operating_system::unmount(&self.mount_path)?;
        }
        crate::operating_system::mount(fs, &self.mount_path)?;

        Ok(())
    }
}
