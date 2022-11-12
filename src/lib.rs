use structopt::*;

// TODO(shelbyd): Store combinators implement all store types.

mod chunk_store;
pub mod commands;
mod db;
mod fs;
mod io;
pub mod logging;
mod object_store;
pub mod operating_system;

#[cfg(test)]
mod testing;

use directories::*;

lazy_static::lazy_static! {
    static ref PROJECT_DIRS: ProjectDirs =
        ProjectDirs::from("com", "51", "HomeNAS").expect("missing $HOME from operating system");
}
