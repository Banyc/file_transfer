use async_async_io::read::{AsyncAsyncRead, PollRead};
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct ReadExact<R> {
    read: R,
    remaining: usize,
}
impl<R> ReadExact<R> {
    pub fn new(read: R, bytes: usize) -> Self {
        Self {
            read,
            remaining: bytes,
        }
    }

    pub fn into_async_read(self) -> PollRead<Self> {
        PollRead::new(self)
    }

    pub fn into_inner(self) -> R {
        self.read
    }
}
impl<R> AsyncAsyncRead for ReadExact<R>
where
    R: AsyncRead + Unpin + Send,
{
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes = self.remaining.min(buf.len());
        if bytes == 0 {
            return Ok(0);
        }
        self.read.read_exact(&mut buf[..bytes]).await?;
        self.remaining -= bytes;
        Ok(bytes)
    }
}
