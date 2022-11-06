use std::path::Path;

#[cfg(target_family = "unix")]
mod unix;

#[cfg(target_family = "unix")]
pub fn mount(
    fs: impl crate::file_system::FileSystem + Send + Sync + 'static,
    path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    ::fuse::mount(unix::UnixWrapper::new(fs), &path, &[])?;
    Ok(())
}
