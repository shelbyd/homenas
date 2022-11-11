use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use structopt::*;

use crate::{chunk_store::*, object_store::*};

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
        let object_store: Box<dyn ObjectStore> = match &self.backing_dir[..] {
            [] => {
                log::warn!("No backing-dir provided, only persisting to memory");
                Box::new(Memory::default())
            }
            [single] => Box::new(FileSystem::new(single)?),
            [dirs @ ..] => Box::new(Multi::new(
                dirs.iter()
                    .map(FileSystem::new)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        };

        let object_store = Network::new(object_store, self.listen_on, &self.peers).await?;
        let object_store = Arc::new(object_store);

        let chunk_store = Direct::new(Arc::clone(&object_store), "chunks");
        let chunk_store = Striping::new(chunk_store, "meta/chunks");
        let chunk_store = RefCount::new(chunk_store, "meta/chunks");

        let fs = crate::fs::FileSystem::new(object_store, Arc::new(chunk_store));

        if !self.fail_on_existing_mount {
            crate::fuse::unmount(&self.mount_path)?;
        }
        crate::fuse::mount(fs, &self.mount_path)?;

        Ok(())
    }
}
