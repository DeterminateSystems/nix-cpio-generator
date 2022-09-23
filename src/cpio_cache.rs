use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use log::{info, trace};
use lru::LruCache;
use tempfile::NamedTempFile;
use tokio::fs::{self, File};
use tokio::io::BufReader;
use tokio::sync::Semaphore;
use tokio_util::io::ReaderStream;

use crate::cpio::{make_archive_from_dir, make_registration};

#[derive(Clone)]
pub struct CpioCache {
    cache_dir: PathBuf,
    cache: Arc<RwLock<CpioLruCache>>,
    semaphore: Option<Arc<Semaphore>>,
}

struct CpioLruCache {
    // TODO: turn into LRU, delete file once it falls out
    lru_cache: LruCache<PathBuf, Cpio>,
    current_size_in_bytes: u64,
    max_size_in_bytes: u64,
}

impl CpioLruCache {
    pub fn new(max_size_in_bytes: u64) -> Self {
        Self {
            lru_cache: LruCache::unbounded(),
            // FIXME: implement current size, max size
            current_size_in_bytes: 0,
            max_size_in_bytes,
        }
    }

    fn prune_lru(&mut self) -> Result<(), CpioError> {
        if let Some((path, cpio)) = self.lru_cache.pop_lru() {
            std::fs::remove_file(&path).map_err(|e| CpioError::Io {
                ctx: "Removing the LRU CPIO",
                src: path.to_path_buf(),
                dest: path.to_path_buf(),
                e,
            })?;

            self.current_size_in_bytes -= cpio.size;
        }

        Ok(())
    }

    fn push(&mut self, path: PathBuf, cpio: Cpio) -> anyhow::Result<()> {
        while cpio.size + self.current_size_in_bytes > self.max_size_in_bytes {
            self.prune_lru()?;
        }

        if let Some((_path, cpio)) = self.lru_cache.push(path, cpio) {
            self.current_size_in_bytes -= cpio.size;
        }

        Ok(())
    }
}
impl CpioCache {
    pub fn new(cache_dir: PathBuf, parallelism: Option<usize>) -> anyhow::Result<Self> {
        let cache = Arc::new(RwLock::new(CpioLruCache::new(0))); // FIXME: max size should be provided

        // TODO: enumerate cache dir and put into LRU

        let epsilon = 2 * 1024 * 1024 * 1024; // 2 GiB
        loop {
            let cache_read = cache
                .read()
                .expect("Failed to get read lock on the cpio cache");
            if cache_read.current_size_in_bytes > cache_read.max_size_in_bytes + epsilon {
                cache
                    .write()
                    .expect("Failed to get write lock on the cpio cache")
                    .prune_lru()?;
            } else {
                break;
            }
        }

        Ok(Self {
            cache,
            cache_dir,
            semaphore: parallelism.map(|cap| Arc::new(Semaphore::new(cap))),
        })
    }

    pub async fn dump_cpio(&self, path: PathBuf) -> anyhow::Result<Cpio> {
        if let Some(cpio) = self.get_cached(&path)? {
            trace!("Found CPIO in the memory cache {:?}", path);
            Ok(cpio)
        } else if let Ok(cpio) = self.get_directory_cached(&path).await {
            trace!("Found CPIO in the directory cache {:?}", path);
            Ok(cpio)
        } else {
            info!("Making a new CPIO for {:?}", path);
            self.make_cpio(&path).await
        }
    }

    fn get_cached(&self, path: &Path) -> anyhow::Result<Option<Cpio>> {
        let mut cache_write = self
            .cache
            .write()
            .expect("Failed to get a write lock on the cpio cache (for LRU updating)");
        let path_buf = path.to_path_buf();
        let cpio = cache_write.lru_cache.get(&path_buf);

        let cpio = match cpio {
            Some(cpio) => {
                if cpio.path.exists()
                    && cpio.path.is_file()
                    && std::fs::File::open(&cpio.path).is_ok()
                {
                    Some(cpio)
                } else {
                    cache_write.lru_cache.demote(&path_buf);
                    cache_write.prune_lru()?;
                    None
                }
            }
            None => None,
        };

        Ok(cpio.cloned())
    }

    async fn get_directory_cached(&self, path: &Path) -> anyhow::Result<Cpio> {
        let cached_location = self.cache_path(path)?;
        let cpio = Cpio::new(cached_location.clone())
            .await
            .map_err(|e| CpioError::Io {
                ctx: "Loading a cached CPIO",
                src: path.to_path_buf(),
                dest: cached_location,
                e,
            })?;

        self.cache
            .write()
            .expect("Failed to get a write lock on the cpio cache")
            .push(path.to_path_buf(), cpio.clone())?;

        Ok(cpio)
    }

    async fn make_cpio(&self, path: &Path) -> anyhow::Result<Cpio> {
        let _semaphore = if let Some(sem) = &self.semaphore {
            trace!("Waiting for the semaphore ...");
            let taken = Some(sem.acquire().await.map_err(CpioError::Semaphore)?);
            trace!("Got for the semaphore ...");
            taken
        } else {
            None
        };

        let final_dest = self.cache_path(path)?;
        let temp_dest = NamedTempFile::new_in(&self.cache_dir).map_err(|e| CpioError::Io {
            ctx: "Creating a new named temporary file.",
            src: path.to_path_buf(),
            dest: final_dest.clone(),
            e,
        })?;

        trace!(
            "Constructing CPIO for {:?} at {:?}, to be moved to {:?}",
            &path,
            &temp_dest,
            &final_dest
        );

        let tdf = temp_dest.as_file().try_clone().unwrap();
        let insidedest = temp_dest.path().to_path_buf();
        let insidepath = path.to_path_buf();

        let mut compressor =
            zstd::stream::write::Encoder::new(tdf, 10).map_err(|e| CpioError::Io {
                ctx: "Instantiating the zstd write-stream encoder",
                src: insidepath.clone(),
                dest: insidedest.clone(),
                e,
            })?;

        compressor
            .include_checksum(true)
            .map_err(|e| CpioError::Io {
                ctx: "Including checksums",
                src: insidepath.clone(),
                dest: insidedest.clone(),
                e,
            })?;

        let mut compressor = tokio::task::spawn_blocking(move || -> Result<_, CpioError> {
            make_archive_from_dir(Path::new("/"), &insidepath, &mut compressor).map_err(|e| {
                CpioError::Io {
                    ctx: "Constructing a CPIO",
                    src: insidepath,
                    dest: insidedest,
                    e,
                }
            })?;

            Ok(compressor)
        })
        .await
        .unwrap()?;

        make_registration(path, &mut compressor)
            .await
            .map_err(CpioError::RegistrationError)?;
        compressor.finish().map_err(|e| CpioError::Io {
            ctx: "Finishing the zstd write-stream encoder",
            src: path.to_path_buf(),
            dest: temp_dest.path().to_path_buf(),
            e,
        })?;

        temp_dest.persist(&final_dest).map_err(|e| CpioError::Io {
            ctx: "Persisting the temporary file to the final location.",
            src: path.to_path_buf(),
            dest: final_dest.clone(),
            e: e.error,
        })?;

        self.get_directory_cached(path).await
    }

    fn cache_path(&self, src: &Path) -> Result<PathBuf, CpioError> {
        if let Some(std::path::Component::Normal(pathname)) = src.components().last() {
            let mut cache_name = OsString::from(pathname);
            cache_name.push(".cpio.zstd");

            Ok(self.cache_dir.join(cache_name))
        } else {
            Err(CpioError::Uncachable(format!(
                "Cannot calculate a cache path for: {:?}",
                src
            )))
        }
    }
}

pub struct Cpio {
    size: u64,
    path: PathBuf,
}

impl Clone for Cpio {
    fn clone(&self) -> Self {
        Cpio {
            size: self.size,
            path: self.path.clone(),
        }
    }
}

impl Cpio {
    pub async fn new(path: PathBuf) -> std::io::Result<Self> {
        let metadata = fs::metadata(&path).await?;

        Ok(Self {
            size: metadata.len(),
            path,
        })
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn reader_stream(
        mut self,
    ) -> std::io::Result<ReaderStream<tokio::io::BufReader<tokio::fs::File>>> {
        Ok(ReaderStream::new(BufReader::new(self.handle().await?)))
    }

    async fn handle(&mut self) -> std::io::Result<tokio::fs::File> {
        File::open(&self.path).await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CpioError {
    #[error("An IO error")]
    Io {
        ctx: &'static str,
        src: PathBuf,
        dest: PathBuf,
        e: std::io::Error,
    },

    #[error("Generating the Nix DB registration failed")]
    RegistrationError(crate::cpio::MakeRegistrationError),

    #[error(
        "The path we tried to generate a cache for can't turn in to a cache key for some reason"
    )]
    Uncachable(String),

    #[error("failed to acquire a semaphore")]
    Semaphore(tokio::sync::AcquireError),
}
