use structopt::*;

mod chunk_store;
mod commands;
mod fs;
mod fuse;
mod io;
mod object_store;

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
