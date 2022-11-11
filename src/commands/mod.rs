use super::*;

mod start;
pub use start::*;

#[derive(StructOpt, Debug)]
pub struct Options {
    /// Command to run.
    #[structopt(subcommand)]
    pub command: Option<Command>,
}

#[derive(StructOpt, Debug)]
pub enum Command {
    /// Start a node with the provided arguments.
    Start(commands::StartCommand),
}

#[cfg(target_family = "unix")]
pub fn create_smart_start() -> StartCommand {
    StartCommand {
        listen_on: 36686,
        peers: Vec::new(),
        backing_dir: Vec::new(),
        mount_path: std::path::PathBuf::from("/mnt/homenas"),
    }
}
