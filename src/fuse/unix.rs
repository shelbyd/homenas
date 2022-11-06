use fuse::*;
use std::{ffi::OsStr, sync::Arc};
use time::Timespec;

use crate::file_system::{Attributes, KindedAttributes};

pub struct UnixWrapper<F>(Arc<F>);

impl<F> UnixWrapper<F> {
    pub fn new(f: F) -> Self {
        UnixWrapper(Arc::new(f))
    }

    fn inner<G, Fut>(&self, g: G)
    where
        G: FnOnce(Arc<F>) -> Fut,
        Fut: core::future::Future<Output = ()> + Send + 'static,
    {
        let inner = Arc::clone(&self.0);
        tokio::spawn(g(inner));
    }
}

impl<F> fuse::Filesystem for UnixWrapper<F>
where
    F: crate::file_system::FileSystem + Send + Sync + 'static,
{
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_owned();

        self.inner(|inner| async move {
            match inner.lookup(parent, &name).await {
                Ok(file) => reply.entry(&Timespec::new(1, 0), &attr_to_unix(file), 0),
                Err(c_int) => reply.error(c_int),
            }
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        self.inner(|inner| async move {
            let attrs = match inner.get_attributes(ino).await {
                Err(e) => return reply.error(e),
                Ok(v) => v,
            };
            reply.attr(&Timespec::new(1, 0), &attr_to_unix(attrs));
        });
    }
}

fn attr_to_unix(attrs: Attributes) -> FileAttr {
    let passed = attrs
        .created_at
        .duration_since(std::time::UNIX_EPOCH)
        .expect("should always be after epoch");
    let create_time = Timespec::new(passed.as_secs() as i64, passed.subsec_nanos() as i32);

    let size = match attrs.kind {
        KindedAttributes::File { size, .. } => size,
        KindedAttributes::Dir { .. } => 0,
    };

    let kind = match attrs.kind {
        KindedAttributes::File { .. } => FileType::RegularFile,
        KindedAttributes::Dir { .. } => FileType::Directory,
    };

    // TODO(shelbyd): Don't hardcode these values.
    FileAttr {
        ino: attrs.node_id,
        size: size,
        blocks: size / 256,
        atime: create_time,
        mtime: create_time,
        ctime: create_time,
        crtime: create_time,
        kind,
        perm: 0o755,
        nlink: 1,
        uid: 501,
        gid: 20,
        rdev: 0,
        flags: 0,
    }
}
