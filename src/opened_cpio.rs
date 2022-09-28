use std::path::Path;

use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;

use crate::cpio::CachedPathBuf;
use crate::error::CpioError;

pub struct OpenedCpio {
    size: u64,
    file: File,
    path: CachedPathBuf,
}

impl OpenedCpio {
    pub fn new(path: CachedPathBuf, size: u64) -> Result<Self, CpioError> {
        let file = std::fs::File::open(&path.0).map_err(|e| CpioError::Fs {
            ctx: "Failed to open CPIO file",
            path: path.0.clone(),
            e,
        })?;

        Ok(OpenedCpio {
            path,
            size,
            file: File::from_std(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path.0
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub async fn reader_stream(
        self,
    ) -> std::io::Result<ReaderStream<tokio::io::BufReader<tokio::fs::File>>> {
        Ok(ReaderStream::new(BufReader::new(self.file)))
    }
}
