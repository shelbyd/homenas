use structopt::*;

use homenas::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let options = Options::from_args();

    match &options.command {
        Command::Start(cmd) => cmd.run(&options).await?,
    }

    Ok(())
}
