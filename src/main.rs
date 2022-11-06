use structopt::*;

mod commands;
mod file_system;
mod fuse;

#[derive(StructOpt, Debug)]
pub struct Options {
    /// Command to run.
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt, Debug)]
enum Command {
    Start(commands::StartCommand),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let options = Options::from_args();

    match &options.command {
        Command::Start(cmd) => cmd.run(&options).await?,
    }

    Ok(())
}
