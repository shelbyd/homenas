use structopt::*;

use homenas::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let options = Options::from_args();

    match &options.command {
        Command::Start(cmd) => cmd.run(&options).await?,
    }

    Ok(())
}
