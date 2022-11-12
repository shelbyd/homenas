use structopt::*;

// TODO(shelbyd): Store combinators implement all store types.

mod chunk_store;
pub mod commands;
mod db;
mod fs;
mod io;
pub mod logging;
pub mod operating_system;
mod utils;
pub use utils::*;

#[cfg(test)]
mod testing;

use directories::*;

lazy_static::lazy_static! {
    static ref PROJECT_DIRS: ProjectDirs =
        ProjectDirs::from("com", "51", "HomeNAS").expect("missing $HOME from operating system");
}
