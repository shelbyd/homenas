use polyfuse::{reply, Data, KernelConfig, Operation, Session};
use std::{path::Path, sync::Arc, time::Duration};

use crate::{
    chunk_store::ChunkStore,
    fs::{Entry, EntryKind, FileSystem},
    io::*,
    object_store::ObjectStore,
};

pub fn unmount(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let path = path.as_ref();

    let status = std::process::Command::new("fusermount")
        .arg("-u")
        .arg(path)
        .status()?;

    if status.success() {
        log::info!("Stopped FUSE running at {:?}", path);
    } else {
        log::info!("FUSE not running at {:?}", path);
    }

    Ok(())
}

pub fn mount<O, C>(fs: FileSystem<O, C>, path: impl AsRef<Path>) -> anyhow::Result<()>
where
    O: ObjectStore + 'static,
    C: ChunkStore + Clone + 'static,
{
    let path = path.as_ref();

    log::info!("Creating path {:?}", path);
    match std::fs::create_dir_all(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }

    log::info!("Mounting to {:?}", path);
    let session = Session::mount(path.to_path_buf(), KernelConfig::default()).map_err(|e| {
        if e.to_string().contains("too short control message length") {
            anyhow::anyhow!("FUSE already mounted at {}", path.to_string_lossy())
        } else {
            e.into()
        }
    })?;

    log::info!("Listening for FUSE requests");
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
                }
                Err(e) => {
                    log::debug!("Replying with error: {}", e);
                    req.reply_error(libc_error(e))
                }
            }
        });
    }

    Ok(())
}

async fn process_operation<'r, O, C>(
    fs: Arc<FileSystem<O, C>>,
    op: Operation<'r, Data<'r>>,
) -> Result<Box<dyn polyfuse::bytes::Bytes>, IoError>
where
    O: ObjectStore + 'static,
    C: ChunkStore + Clone + 'static,
{
    match op {
        Operation::Lookup(op) => {
            let entry = fs.lookup(op.parent(), op.name()).await?;
            Ok(Box::new(entry_out(entry)))
        }
        Operation::Getattr(op) => {
            let entry = fs.read_entry(op.ino()).await?;

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
                    EntryKind::File(_) => libc::DT_REG,
                    EntryKind::Directory(_) => libc::DT_DIR,
                } as u32;

                let is_full = out.entry(entry.name.as_ref(), entry.node_id, kind, i as u64);
                if is_full {
                    break;
                }
            }

            Ok(Box::new(out))
        }
        Operation::Open(op) => {
            let handle_id = fs.open(op.ino()).await?;

            let mut out = reply::OpenOut::default();
            out.fh(handle_id);

            Ok(Box::new(out))
        }
        Operation::Flush(op) => {
            fs.flush(op.ino()).await?;
            Ok(Box::new(Vec::<u8>::new()))
        }
        Operation::Release(op) => {
            fs.release(op.ino()).await?;
            Ok(Box::new(Vec::<u8>::new()))
        }
        Operation::Write(op, data) => {
            let written = fs.write(op.ino(), op.offset(), op.size(), data).await?;

            let mut out = reply::WriteOut::default();
            out.size(written);

            Ok(Box::new(out))
        }
        Operation::Read(op) => {
            let read = fs.read(op.ino(), op.offset(), op.size()).await?;
            Ok(Box::new(read))
        }
        Operation::Mknod(op) => match node_type(op.mode())? {
            EntryKind::File(()) => {
                let entry = fs.create_file(op.parent(), op.name()).await?;
                Ok(Box::new(entry_out(entry)))
            }
            file_type => {
                log::warn!("Unhandled operation: Mknod {:?}", file_type);
                Err(IoError::Unimplemented)
            }
        },
        Operation::Mkdir(op) => {
            let entry = fs.create_dir(op.parent(), op.name()).await?;
            Ok(Box::new(entry_out(entry)))
        }
        Operation::Unlink(op) => {
            fs.unlink(op.parent(), op.name()).await?;
            Ok(Box::new(Vec::<u8>::new()))
        }
        Operation::Forget(op) => {
            for forget in &*op {
                fs.forget(forget.ino()).await?;
            }
            Ok(Box::new(Vec::<u8>::new()))
        }
        Operation::Rmdir(op) => {
            let node = fs.unlink(op.parent(), op.name()).await?;
            fs.forget(node).await?;
            Ok(Box::new(Vec::<u8>::new()))
        }

        unhandled => {
            log::warn!("Unhandled operation: {:?}", unhandled);
            Err(IoError::Unimplemented)
        }
    }
}

fn entry_out(entry: Entry) -> reply::EntryOut {
    let mut out = reply::EntryOut::default();
    out.ino(entry.node_id);
    file_attr(entry, out.attr());

    out.ttl_attr(Duration::from_secs(1));
    out.ttl_entry(Duration::from_secs(1));

    out
}

fn file_attr(entry: Entry, attr: &mut reply::FileAttr) {
    attr.nlink(1);
    attr.uid(unsafe { libc::getuid() });
    attr.gid(unsafe { libc::getgid() });

    attr.ino(entry.node_id);
    attr.ctime(entry.created_since_epoch());
    attr.atime(entry.created_since_epoch());
    attr.mtime(entry.created_since_epoch());

    match entry.kind {
        EntryKind::File(f) => {
            attr.size(f.size);
            attr.blocks(1);
            attr.blksize(u32::MAX);
            attr.mode(libc::S_IFREG as u32 | 0o444);
        }
        EntryKind::Directory(_) => {
            attr.mode(libc::S_IFDIR as u32 | 0o555);
        }
    }
}

fn node_type(mode: u32) -> Result<EntryKind, IoError> {
    if mode & libc::S_IFREG > 0 {
        return Ok(EntryKind::File(()));
    }

    Err(IoError::Unimplemented)
}

fn libc_error(io: IoError) -> libc::c_int {
    match io {
        IoError::NotFound => libc::ENOENT,
        IoError::Unimplemented => libc::ENOSYS,
        IoError::OutOfRange => libc::EINVAL,
        IoError::NotAFile => libc::EINVAL,
        IoError::NotADirectory => libc::EINVAL,
        IoError::Timeout => libc::ETIMEDOUT,
        IoError::Io => libc::EIO,
        IoError::Parse => libc::EIO,
        IoError::Uncategorized => libc::EIO,
        IoError::InvalidData => libc::EIO,
        IoError::TempUnavailable => libc::EAGAIN,
        IoError::BadDescriptor => libc::EBADF,
        IoError::Internal => libc::EIO,
    }
}
