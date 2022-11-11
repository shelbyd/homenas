use super::*;

use human_bytes::*;
use std::{ffi::*, path::*};
use sysinfo::*;

mod start;
pub use start::*;

#[derive(StructOpt, Debug)]
pub struct Options {
    /// Command to run.
    #[structopt(subcommand)]
    pub command: Option<Command>,
}

#[derive(StructOpt, Debug)]
pub enum Command {
    /// Start a node with the provided arguments.
    Start(commands::StartCommand),
}

#[cfg(target_family = "unix")]
pub fn create_smart_start() -> StartCommand {
    StartCommand {
        listen_on: 36686,
        peers: Vec::new(),
        backing_dir: smart_backing_dirs(),
        mount_path: PathBuf::from("/mnt/homenas"),
    }
}

fn smart_backing_dirs() -> Vec<PathBuf> {
    let mut system = System::new();
    system.refresh_disks_list();

    log::info!("Detecting disks");
    let paths = system
        .disks()
        .iter()
        .filter(|d| should_use_disk(d))
        .map(|d| d.mount_point().join(".homenas_storage"))
        .collect();
    log::info!("Starting with paths: {:?}", &paths);
    paths
}

fn should_use_disk(disk: &Disk) -> bool {
    log::info!("Determining if usable: {:?}", disk.name());

    if disk.name() == OsString::from("/dev/mapper/data-root") {
        log::info!("  Using unix data root disk");
        return true;
    }

    if disk.mount_point() == OsString::from("/recovery") {
        log::info!("Skipping unix recovery disk");
        return false;
    }

    let is_uefi = disk.mount_point() == PathBuf::from("/boot/efi");
    if is_uefi {
        log::info!("  Skipping uefi partition");
        return false;
    }

    log::warn!("Skipping unrecognized disk: {:?}", disk.name());

    log::info!("  mount_point: {:?}", disk.mount_point());
    log::info!(
        "  available space: {}",
        human_bytes(disk.available_space() as f64),
    );
    log::info!("  total space: {}", human_bytes(disk.total_space() as f64),);
    log::info!(
        "  format: {:?}",
        String::from_utf8_lossy(disk.file_system())
    );
    log::info!("  removable: {}", disk.is_removable());

    false
}
