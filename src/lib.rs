use std::{
    io,
    path::{Path, PathBuf},
    time::Instant,
};

use clap::{Args, Subcommand};
use read_exact::ReadExact;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
};

mod read_exact;

const CLOSE: u8 = 0;

#[derive(Debug, Clone, Subcommand)]
pub enum FileTransferCommand {
    Push(PushFileArgs),
    Pull(PullFileArgs),
}

impl FileTransferCommand {
    pub async fn perform<R, W>(
        &self,
        mut read: R,
        mut write: W,
    ) -> io::Result<FileTransferResult<R, W>>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin,
    {
        let start = Instant::now();
        let (bytes, read, write) = match self {
            FileTransferCommand::Push(args) => {
                let (bytes, write) = args.push_file(write).await?;
                let msg = read.read_u8().await?;
                assert_eq!(msg, CLOSE);
                (bytes, read, write)
            }
            FileTransferCommand::Pull(args) => {
                let (bytes, read) = args.pull_file(read).await?;
                write.write_u8(CLOSE).await?;
                (bytes, read, write)
            }
        };
        let duration = start.elapsed();
        let throughput = bytes as f64 / duration.as_secs_f64();
        let throughput_mib_s = throughput / 1024. / 1024.;
        let latency_ms = duration.as_secs_f64() * 1000.;
        let stats = FileTransferStats {
            bytes,
            throughput_mib_s,
            latency_ms,
        };
        Ok(FileTransferResult { stats, read, write })
    }
}

#[derive(Debug)]
pub struct FileTransferResult<R, W> {
    pub stats: FileTransferStats,
    pub read: R,
    pub write: W,
}

#[derive(Debug, Clone, Args)]
pub struct PushFileArgs {
    pub source_file: PathBuf,
}

impl PushFileArgs {
    pub async fn push_file<W>(&self, write: W) -> io::Result<(usize, W)>
    where
        W: AsyncWrite + Unpin,
    {
        push_file(&self.source_file, write).await
    }
}

pub async fn push_file<W>(source_file: impl AsRef<Path>, mut write: W) -> io::Result<(usize, W)>
where
    W: AsyncWrite + Unpin,
{
    let file = File::open(source_file).await?;
    let bytes = file.metadata().await?.len();
    let mut file = BufReader::new(file);

    write.write_u64(bytes).await?;
    let read_bytes = tokio::io::copy(&mut file, &mut write).await?;

    assert_eq!(bytes, read_bytes, "file modified during transmission");

    Ok((usize::try_from(read_bytes).unwrap(), write))
}

#[derive(Debug, Clone, Args)]
pub struct PullFileArgs {
    pub output_file: PathBuf,
}

impl PullFileArgs {
    pub async fn pull_file<R>(&self, read: R) -> io::Result<(usize, R)>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        pull_file(&self.output_file, read).await
    }
}

pub async fn pull_file<R>(output_file: impl AsRef<Path>, mut read: R) -> io::Result<(usize, R)>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let _ = tokio::fs::remove_file(&output_file).await;
    let mut file = File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open(output_file)
        .await?;

    let bytes = read.read_u64().await?;
    let read_exact = ReadExact::new(read, usize::try_from(bytes).unwrap());
    let mut read = read_exact.into_async_read();
    let written = tokio::io::copy(&mut read, &mut file).await?;

    Ok((
        usize::try_from(written).unwrap(),
        read.into_inner().into_inner(),
    ))
}

#[derive(Debug, Clone)]
pub struct FileTransferStats {
    pub bytes: usize,
    pub throughput_mib_s: f64,
    pub latency_ms: f64,
}
impl core::fmt::Display for FileTransferStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bytes: {bytes}; throughput: {throughput_mib_s:.2} MiB/s; latency: {latency_ms:.2} ms;",
            bytes = self.bytes,
            throughput_mib_s = self.throughput_mib_s,
            latency_ms = self.latency_ms,
        )
    }
}
