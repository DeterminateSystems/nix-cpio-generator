use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::error::CpioError;

#[derive(Debug, Clone)]
pub struct Cpio {
    pub size: u64,
    pub path: CachedPathBuf,
}

impl Cpio {
    pub fn new(path: CachedPathBuf) -> Result<Self, CpioError> {
        let metadata = std::fs::metadata(&path.0).map_err(|e| CpioError::Fs {
            ctx: "Reading the CPIO's file metadata",
            path: path.0.clone(),
            e,
        })?;

        Ok(Self {
            size: metadata.len(),
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path.0
    }
}

/// A path that points to a cached CPIO.
#[derive(Debug, Clone)]
pub struct CachedPathBuf(pub PathBuf);

impl CachedPathBuf {
    pub fn new(src: PathBuf, cache_dir: &Path) -> Result<Self, CpioError> {
        let cached_path =
            if let Some(std::path::Component::Normal(pathname)) = src.components().last() {
                let mut cache_name = OsString::from(pathname);
                cache_name.push(".cpio.zstd");

                Ok(cache_dir.join(cache_name))
            } else {
                Err(CpioError::Uncachable(format!(
                    "Cannot calculate a cache path for: {:?}",
                    src
                )))
            };

        cached_path.map(CachedPathBuf)
    }

    /// This function assumes the passed PathBuf already exists in the cache.
    pub fn new_preexisting(src: PathBuf) -> Self {
        CachedPathBuf(src)
    }
}
