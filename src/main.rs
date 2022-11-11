use structopt::*;

use homenas::commands::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    homenas::logging::init()?;

    let options = Options::from_args();

    match &options.command {
        Some(Command::Start(cmd)) => cmd.run(&options).await?,
        None => create_smart_start().run(&options).await?,
    }

    Ok(())
}
