#[cfg(target_family = "unix")]
pub use self::unix::*;

#[cfg(target_family = "unix")]
mod unix {
    use std::path::*;

    pub fn default_mount_path() -> PathBuf {
        PathBuf::from("/mnt/homenas")
    }
}
