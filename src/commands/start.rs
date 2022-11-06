use std::path::PathBuf;
use structopt::*;

#[derive(StructOpt, Debug)]
pub struct StartCommand {
    /// Where to mount the homenas directory.
    mount_path: PathBuf,
}

impl StartCommand {
    pub async fn run(&self, _opts: &crate::Options) -> anyhow::Result<()> {
        crate::fuse::mount(crate::file_system::Main, &self.mount_path)?;
        Ok(())
    }
}
