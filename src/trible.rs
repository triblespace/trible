use std::convert::From;
use std::io::Result;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Trible([u8; Trible::SIZE]);

impl Trible {
    pub const ENTITY_SIZE: usize = 16;
    pub const ATTRIBUTE_SIZE: usize = 16;
    pub const VALUE_SIZE: usize = 32;
    pub const VALUE_START: usize = 32;
    pub const SIZE: usize = Self::ENTITY_SIZE + Self::ATTRIBUTE_SIZE + Self::VALUE_SIZE;

    pub const TXN_ZEROS: usize = 16;

    pub async fn read(&mut self, mut reader: impl AsyncRead + Unpin) -> Result<usize> {
        reader.read_exact(&mut self.0).await
    }

    pub async fn write(&self, mut writer: impl AsyncWrite + Unpin) -> Result<()> {
        writer.write_all(&self.0).await
    }
}
