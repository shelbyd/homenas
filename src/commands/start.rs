use std::{net::SocketAddr, path::PathBuf};
use structopt::*;

#[derive(StructOpt, Debug)]
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
        crate::fuse::mount(crate::file_system::Main::default(), &self.mount_path)?;
        Ok(())
    }
}
