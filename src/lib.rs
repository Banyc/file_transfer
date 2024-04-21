use std::{
    io,
    path::{Path, PathBuf},
    time::Instant,
};

use clap::{Args, Subcommand};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncWrite, BufReader},
};

#[derive(Debug, Clone, Subcommand)]
pub enum FileTransferCommand {
    Push(PushFileArgs),
    Pull(PullFileArgs),
}

impl FileTransferCommand {
    pub async fn perform(
        &self,
        read: impl AsyncRead + Unpin,
        write: impl AsyncWrite + Unpin,
    ) -> io::Result<FileTransferStats> {
        let start = Instant::now();
        let bytes = match self {
            FileTransferCommand::Push(args) => args.push_file(write).await?,
            FileTransferCommand::Pull(args) => args.pull_file(read).await?,
        };
        let duration = start.elapsed();
        let throughput = bytes as f64 / duration.as_secs_f64();
        let throughput_mib_s = throughput / 1024. / 1024.;
        let latency_ms = duration.as_secs_f64() * 1000.;
        Ok(FileTransferStats {
            bytes,
            throughput_mib_s,
            latency_ms,
        })
    }
}

#[derive(Debug, Clone, Args)]
pub struct PushFileArgs {
    pub source_file: PathBuf,
}

impl PushFileArgs {
    pub async fn push_file(&self, write: impl AsyncWrite + Unpin) -> io::Result<usize> {
        push_file(&self.source_file, write).await
    }
}

pub async fn push_file(
    source_file: impl AsRef<Path>,
    mut write: impl AsyncWrite + Unpin,
) -> io::Result<usize> {
    let file = File::open(source_file).await.unwrap();
    let mut file = BufReader::new(file);

    let read = tokio::io::copy(&mut file, &mut write).await.unwrap();

    Ok(usize::try_from(read).unwrap())
}

#[derive(Debug, Clone, Args)]
pub struct PullFileArgs {
    pub output_file: PathBuf,
}

impl PullFileArgs {
    pub async fn pull_file(&self, read: impl AsyncRead + Unpin) -> io::Result<usize> {
        pull_file(&self.output_file, read).await
    }
}

pub async fn pull_file(
    output_file: impl AsRef<Path>,
    mut read: impl AsyncRead + Unpin,
) -> io::Result<usize> {
    let _ = tokio::fs::remove_file(&output_file).await;
    let mut file = File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open(output_file)
        .await
        .unwrap();

    let written = tokio::io::copy(&mut read, &mut file).await.unwrap();

    Ok(usize::try_from(written).unwrap())
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
