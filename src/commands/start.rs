use std::{net::SocketAddr, path::PathBuf};
use structopt::*;

#[derive(StructOpt, Debug)]
#[allow(dead_code)] // TODO: Remove.
pub struct StartCommand {
    /// Local port to listen for peer connections.
    #[structopt(long, default_value = "42000")]
    listen_on: u16,

    /// Peers to try to connect to.
    #[structopt(long)]
    peers: Vec<SocketAddr>,

    /// Where to mount the homenas directory.
    mount_path: PathBuf,
}

impl StartCommand {
    pub async fn run(&self, _opts: &crate::Options) -> anyhow::Result<()> {
        let mem_store = crate::object_store::Memory::default();
        let fs = crate::fs::FileSystem::new(mem_store);

        crate::fuse::mount(fs, &self.mount_path)?;
        Ok(())
    }
}
