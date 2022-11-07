#![feature(map_try_insert)]

use structopt::*;

mod commands;
mod fs;
mod fuse;
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
