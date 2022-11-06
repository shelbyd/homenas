use structopt::*;

mod commands;
pub mod file_system;
mod fuse;

#[derive(StructOpt, Debug)]
pub struct Options {
    /// Command to run.
    #[structopt(subcommand)]
    pub command: Command,
}

#[derive(StructOpt, Debug)]
pub enum Command {
    Start(commands::StartCommand),
}
