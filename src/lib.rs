use structopt::*;

mod chunk_store;
pub mod commands;
mod fs;
mod io;
pub mod logging;
mod object_store;
pub mod operating_system;
mod stores;

#[cfg(test)]
mod testing;

use directories::*;

lazy_static::lazy_static! {
    static ref PROJECT_DIRS: ProjectDirs =
        ProjectDirs::from("com", "51", "HomeNAS").expect("missing $HOME from operating system");
}
