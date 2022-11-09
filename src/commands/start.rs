use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use structopt::*;

use crate::{chunk_store::*, object_store::*};

#[derive(StructOpt, Debug)]
#[allow(dead_code)] // TODO: Remove.
pub struct StartCommand {
    /// Local port to listen for peer connections.
    #[structopt(long, default_value = "42000")]
    listen_on: u16,

    /// Peers to try to connect to.
    #[structopt(long)]
    peers: Vec<SocketAddr>,

    /// Directories to store data to. If empty, will only store in memory.
    #[structopt(long)]
    backing_dir: Vec<PathBuf>,

    /// Where to mount the homenas directory.
    mount_path: PathBuf,
}

impl StartCommand {
    pub async fn run(&self, _opts: &crate::Options) -> anyhow::Result<()> {
        let object_store: Box<dyn ObjectStore> = match &self.backing_dir[..] {
            [] => Box::new(Memory::default()),
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

        crate::fuse::mount(fs, &self.mount_path)?;
        Ok(())
    }
}
