use polyfuse::{bytes::Bytes, reply, Data, KernelConfig, Operation, Session};
use std::{path::Path, sync::Arc, time::Duration};

use crate::file_system::{Attributes, FileKind, FileSystem, KindedAttributes};

pub fn mount(
    fs: impl FileSystem + Send + Sync + 'static,
    path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let session = Session::mount(path.as_ref().to_path_buf(), KernelConfig::default())?;

    let fs = Arc::new(fs);
    while let Some(req) = session.next_request()? {
        let fs = Arc::clone(&fs);
        tokio::spawn(async move {
            let op = req.operation().expect("always has operation");
            log::debug!("Got operation: {:?}", op);

            match process_operation(fs, op).await {
                Ok(b) => {
                    log::debug!("Replying OK");
                    req.reply(b)
                },
                Err(e) => {
                    log::debug!("Replying with error: {}", e);
                    req.reply_error(e)
                },
            }
        });
    }

    Ok(())
}

async fn process_operation<'r>(
    fs: Arc<impl crate::file_system::FileSystem>,
    op: Operation<'r, Data<'r>>,
) -> Result<Box<dyn Bytes>, libc::c_int> {
    match op {
        Operation::Lookup(op) => {
            let entry = fs.lookup(op.parent(), op.name()).await?;

            let mut out = reply::EntryOut::default();
            out.ino(entry.node_id);
            file_attr(entry, out.attr());
            out.ttl_attr(Duration::from_secs(1));
            out.ttl_entry(Duration::from_secs(1));

            Ok(Box::new(out))
        }
        Operation::Getattr(op) => {
            let entry = fs.get_attributes(op.ino()).await?;

            let mut out = reply::AttrOut::default();
            out.ttl(Duration::from_secs(1));
            file_attr(entry, out.attr());

            Ok(Box::new(out))
        }
        Operation::Readdir(op) => {
            let entries = fs.list_children(op.ino()).await?;

            let mut out = reply::ReaddirOut::new(op.size() as usize);

            let to_skip = if op.offset() == 0 { 0 } else { op.offset() + 1 } as usize;
            let entry_offsets = entries
                .iter()
                .enumerate()
                .skip(to_skip)
                .take(op.size() as usize);
            for (i, entry) in entry_offsets {
                let kind = match entry.kind {
                    FileKind::File => libc::DT_REG,
                    FileKind::Directory => libc::DT_DIR,
                } as u32;

                let is_full = out.entry(entry.path.as_ref(), entry.node_id, kind, i as u64);
                if is_full {
                    break;
                }
            }

            Ok(Box::new(out))
        }
        unhandled => {
            log::warn!("Unhandled operation: {:?}", unhandled);
            Err(libc::ENOSYS)
        }
    }
}

fn file_attr(entry: Attributes, attr: &mut reply::FileAttr) {
    attr.nlink(1);
    attr.uid(unsafe { libc::getuid() });
    attr.gid(unsafe { libc::getgid() });

    attr.ino(entry.node_id);
    attr.ctime(entry.created_since_epoch());
    attr.atime(entry.created_since_epoch());
    attr.mtime(entry.created_since_epoch());

    match entry.kind {
        KindedAttributes::File { size, .. } => {
            attr.size(size);
            attr.blocks(1);
            attr.blksize(u32::MAX);
            attr.mode(libc::S_IFREG as u32 | 0o444);
        }
        KindedAttributes::Dir { .. } => {
            attr.mode(libc::S_IFDIR as u32 | 0o555);
        }
    }
}
