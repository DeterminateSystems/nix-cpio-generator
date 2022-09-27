use std::path::Path;

use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;

use crate::cpio_cache::{CachedPathBuf, CpioError};

pub struct OpenedCpio {
    size: u64,
    file: File,
    path: CachedPathBuf,
}

impl OpenedCpio {
    pub fn new(path: CachedPathBuf, size: u64) -> Result<Self, CpioError> {
        let file = std::fs::File::open(&path.0).map_err(|e| CpioError::Io {
            ctx: "Failed to open CPIO file",
            src: Some(path.0.clone()),
            dest: None,
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
