use fuse::*;
use std::{ffi::OsStr, sync::Arc};
use time::Timespec;

pub struct UnixWrapper<F>(Arc<F>);

impl<F> UnixWrapper<F> {
    pub fn new(f: F) -> Self {
        UnixWrapper(Arc::new(f))
    }
}

impl<F> fuse::Filesystem for UnixWrapper<F>
where
    F: crate::file_system::FileSystem + Send + Sync + 'static,
{
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_owned();
        let wrapped = Arc::clone(&self.0);
        ::tokio::spawn(async move {
            let result = wrapped.lookup(parent, &name).await;
            match result {
                Ok(file) => {
                    let passed = file
                        .created_at
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("should always be after epoch");
                    let create_time =
                        Timespec::new(passed.as_secs() as i64, passed.subsec_nanos() as i32);
                    // TODO(shelbyd): Don't hardcode these values.
                    let attr = FileAttr {
                        ino: file.node_id,
                        size: file.size,
                        blocks: 1,
                        atime: create_time,
                        mtime: create_time,
                        ctime: create_time,
                        crtime: create_time,
                        kind: FileType::RegularFile,
                        perm: 0o644,
                        nlink: 1,
                        uid: 501,
                        gid: 20,
                        rdev: 0,
                        flags: 0,
                    };
                    reply.entry(&::time::Timespec::new(1, 0), &attr, 0);
                }
                Err(c_int) => reply.error(c_int),
            }
        });
    }
}
