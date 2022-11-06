use polyfuse::{bytes::Bytes, reply, Data, KernelConfig, Operation, Session};
use std::{path::Path, sync::Arc, time::Duration};

use crate::file_system::{FileSystem, KindedAttributes};

pub fn mount(
    fs: impl FileSystem + Send + Sync + 'static,
    path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let session = Session::mount(path.as_ref().to_path_buf(), KernelConfig::default())?;

    let fs = Arc::new(fs);
    while let Some(req) = session.next_request()? {
        let fs = Arc::clone(&fs);
        tokio::spawn(async move {
            match process_operation(fs, req.operation().expect("always has operation")).await {
                Ok(b) => req.reply(b),
                Err(e) => req.reply_error(e),
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
            out.generation(0);
            out.ttl_attr(Duration::from_secs(1));

            let attr = out.attr();
            attr.ino(entry.node_id);
            attr.ctime(entry.created_since_epoch());
            attr.atime(entry.created_since_epoch());
            attr.mtime(entry.created_since_epoch());
            match entry.kind {
                KindedAttributes::File { size, .. } => {
                    attr.size(size);
                    attr.blocks(1);
                }
                KindedAttributes::Dir { .. } => {}
            }

            Ok(Box::new(out))
        }
        unhandled => {
            log::warn!("Unhandled operation: {:?}", unhandled);
            Err(libc::ENOSYS)
        }
    }
}
