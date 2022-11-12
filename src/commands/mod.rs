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
        fail_on_existing_mount: false,
        network_state_dir: None,
        mount_path: crate::operating_system::default_mount_path(),
    }
}

fn smart_backing_dirs() -> Vec<PathBuf> {
    let mut system = System::new();
    system.refresh_disks_list();

    log::info!("Detecting disks");
    let paths: Vec<_> = system.disks().iter().filter_map(disk_path).collect();
    if paths.is_empty() {
        log::warn!("Starting with no paths");
    } else {
        log::info!("Starting with paths:");
        for path in &paths {
            log::info!("  - {}", path.display());
        }
    }
    paths
}

fn disk_path(disk: &Disk) -> Option<PathBuf> {
    log::info!("Determining if usable: {:?}", disk.name());

    if disk.name() == "/dev/mapper/data-root" {
        log::info!("  Using unix data root disk");
        if disk.mount_point() == PathBuf::from("/") {
            let path = PROJECT_DIRS.data_dir().to_path_buf();
            log::info!("    path: {:?}", path);
            return Some(path);
        }

        log::warn!(
            "Skipping unrecognized mount point for data root {:?}",
            disk.mount_point()
        );
        return None;
    }

    if disk.mount_point() == OsString::from("/recovery") {
        log::info!("Skipping unix recovery disk");
        return None;
    }

    let is_uefi = disk.mount_point() == PathBuf::from("/boot/efi");
    if is_uefi {
        log::info!("  Skipping uefi partition");
        return None;
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

    None
}
