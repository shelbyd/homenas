use super::*;

use flexi_logger::*;

pub fn init() -> anyhow::Result<()> {
    let file_spec = FileSpec::default().directory(PROJECT_DIRS.data_dir().join("logs"));

    #[allow(non_snake_case)]
    let MiB = 1024 * 1024;
    let total_log_size = 256 * MiB;
    let max_file_size = 8 * MiB;

    Logger::try_with_str("info")?
        .log_to_file(file_spec.clone())
        .format_for_files(file_format)
        .print_message()
        .rotate(
            Criterion::Size(max_file_size),
            Naming::Timestamps,
            Cleanup::KeepLogFiles((total_log_size / max_file_size) as usize),
        )
        .duplicate_to_stderr(Duplicate::Info)
        .format_for_stderr(stderr_format)
        .start()?;

    Ok(())
}

pub fn file_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    write!(
        w,
        "[{}] {} [{}] {}",
        now.format("%Y-%m-%d %H:%M:%S%.3f"),
        record.level(),
        record.module_path().unwrap_or("<unnamed>"),
        &record.args()
    )
}

pub fn stderr_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    let level = record.level();
    let painter = style(record.level());
    write!(
        w,
        "[{}] {} [{}:{}] {}",
        now.format("%Y-%m-%d %H:%M:%S%.3f"),
        painter.paint(level.to_string()),
        record.file().unwrap_or("<unnamed>"),
        record.line().unwrap_or(0),
        painter.paint(&record.args().to_string())
    )
}
