use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use log::{info, trace};
use lru::LruCache;
use tempfile::NamedTempFile;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::sync::Semaphore;
use tokio_util::io::ReaderStream;

use crate::cpio::{make_archive_from_dir, make_registration};

pub const SIZE_EPSILON_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

#[derive(Clone)]
pub struct CpioCache {
    cache_dir: PathBuf,
    cache: Arc<RwLock<CpioLruCache>>,
    semaphore: Option<Arc<Semaphore>>,
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct NixStorePath(PathBuf);

impl NixStorePath {
    pub fn new(store_path: PathBuf) -> Self {
        assert!(store_path.starts_with("/nix/store"));
        NixStorePath(store_path)
    }

    pub fn from_cached(
        cached_location: &CachedPathBuf,
        cache_dir: &PathBuf,
    ) -> Result<Self, CpioError> {
        let nix_store = PathBuf::from("/nix/store");
        let stripped = cached_location
            .0
            .strip_prefix(cache_dir)
            .map_err(CpioError::StripCachePrefix)?;
        let store_hash_path = stripped.with_extension("").with_extension("");

        Ok(NixStorePath(nix_store.join(store_hash_path)))
    }
}

struct CpioLruCache {
    lru_cache: LruCache<NixStorePath, Cpio>,
    current_size_in_bytes: u64,
    max_size_in_bytes: u64,
}

impl CpioLruCache {
    fn new(max_size_in_bytes: u64) -> Self {
        Self {
            lru_cache: LruCache::unbounded(),
            current_size_in_bytes: 0,
            max_size_in_bytes,
        }
    }

    fn prune_single_lru(&mut self) -> Result<(), CpioError> {
        if let Some((path, cpio)) = self.lru_cache.pop_lru() {
            if cpio.path().exists() {
                std::fs::remove_file(&cpio.path()).map_err(|e| CpioError::Io {
                    ctx: "Removing the LRU CPIO",
                    src: Some(path.0),
                    dest: Some(cpio.path().to_path_buf()),
                    e,
                })?;

                self.current_size_in_bytes -= cpio.size;
            }
        }

        Ok(())
    }

    fn prune_lru(&mut self) -> Result<(), CpioError> {
        while self.current_size_in_bytes > self.max_size_in_bytes + SIZE_EPSILON_BYTES {
            self.prune_single_lru()?;
        }

        Ok(())
    }

    fn push(&mut self, path: NixStorePath, cpio: Cpio) -> Result<(), CpioError> {
        let cpio_size = cpio.size;

        while cpio_size + self.current_size_in_bytes > self.max_size_in_bytes {
            self.prune_single_lru()?;
        }

        if let Some((_path, replaced_cpio)) = self.lru_cache.push(path, cpio) {
            self.current_size_in_bytes -= replaced_cpio.size;
        }

        self.current_size_in_bytes += cpio_size;

        Ok(())
    }

    fn get(&mut self, path: &NixStorePath) -> Option<&Cpio> {
        self.lru_cache.get(path)
    }

    fn demote(&mut self, path: &NixStorePath) {
        self.lru_cache.demote(path)
    }
}

impl CpioCache {
    pub fn new(
        cache_dir: PathBuf,
        parallelism: Option<usize>,
        max_cache_size_in_bytes: u64,
    ) -> Result<Self, CpioError> {
        let mut cache = CpioLruCache::new(max_cache_size_in_bytes);

        log::info!("enumerating cache dir {:?} to place into lru", cache_dir);
        for entry in std::fs::read_dir(&cache_dir).map_err(|e| CpioError::Io {
            ctx: "Reading the cache dir",
            src: None,
            dest: Some(cache_dir.clone()),
            e,
        })? {
            let entry = entry.map_err(|e| CpioError::Io {
                ctx: "Reading cache dir entry",
                src: None,
                dest: None,
                e,
            })?;
            let path = entry.path();

            if path.is_dir() {
                log::warn!("{:?} was a directory but it shouldn't be", path);
            } else {
                let cached_location = CachedPathBuf::new_preexisting(path.to_path_buf());
                let store_path = NixStorePath::from_cached(&cached_location, &cache_dir)?;
                let cpio = Cpio::new(cached_location)?;
                cache.push(store_path, cpio)?;
            }
        }

        log::info!(
            "attempting to prune lru cache to be less than max size {} bytes (currently {} bytes)",
            cache.max_size_in_bytes,
            cache.current_size_in_bytes
        );
        cache.prune_lru()?;

        Ok(Self {
            cache: Arc::new(RwLock::new(cache)),
            cache_dir,
            semaphore: parallelism.map(|cap| Arc::new(Semaphore::new(cap))),
        })
    }

    pub async fn dump_cpio(&self, path: PathBuf) -> Result<Cpio, CpioError> {
        if let Some(cpio) = self.get_cached(&path)? {
            trace!("Found CPIO in the cache {:?}", path);
            Ok(cpio)
        } else {
            info!("Making a new CPIO for {:?}", path);
            self.make_cpio(&path).await
        }
    }

    fn get_cached(&self, path: &Path) -> Result<Option<Cpio>, CpioError> {
        let mut cache_write = self
            .cache
            .write()
            .expect("Failed to get a write lock on the cpio cache (for LRU updating)");
        let store_path = NixStorePath::new(path.to_path_buf());
        let cpio = cache_write.get(&store_path);

        let cpio = match cpio {
            Some(cpio) => {
                let cpio_path = cpio.path();
                if cpio_path.exists()
                    && cpio_path.is_file()
                    && std::fs::File::open(&cpio_path).is_ok()
                {
                    Some(cpio)
                } else {
                    cache_write.demote(&store_path);
                    cache_write.prune_single_lru()?;
                    None
                }
            }
            None => None,
        };

        Ok(cpio.cloned())
    }

    async fn make_cpio(&self, path: &Path) -> Result<Cpio, CpioError> {
        let _semaphore = if let Some(sem) = &self.semaphore {
            trace!("Waiting for the semaphore ...");
            let taken = Some(sem.acquire().await.map_err(CpioError::Semaphore)?);
            trace!("Got for the semaphore ...");
            taken
        } else {
            None
        };

        let final_dest = CachedPathBuf::new(path.to_path_buf(), &self.cache_dir)?;
        let temp_dest = NamedTempFile::new_in(&self.cache_dir).map_err(|e| CpioError::Io {
            ctx: "Creating a new named temporary file.",
            src: Some(path.to_path_buf()),
            dest: Some(final_dest.0.clone()),
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
                src: Some(insidepath.clone()),
                dest: Some(insidedest.clone()),
                e,
            })?;

        compressor
            .include_checksum(true)
            .map_err(|e| CpioError::Io {
                ctx: "Including checksums",
                src: Some(insidepath.clone()),
                dest: Some(insidedest.clone()),
                e,
            })?;

        let mut compressor = tokio::task::spawn_blocking(move || -> Result<_, CpioError> {
            make_archive_from_dir(Path::new("/"), &insidepath, &mut compressor).map_err(|e| {
                CpioError::Io {
                    ctx: "Constructing a CPIO",
                    src: Some(insidepath),
                    dest: Some(insidedest),
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
            src: Some(path.to_path_buf()),
            dest: Some(temp_dest.path().to_path_buf()),
            e,
        })?;

        temp_dest
            .persist(&final_dest.0)
            .map_err(|e| CpioError::Io {
                ctx: "Persisting the temporary file to the final location.",
                src: Some(path.to_path_buf()),
                dest: Some(final_dest.0.clone()),
                e: e.error,
            })?;

        let store_path = NixStorePath::new(path.to_path_buf());
        let cached_location = CachedPathBuf::new(path.to_path_buf(), &self.cache_dir)?;
        let cpio = Cpio::new(cached_location)?;

        self.cache
            .write()
            .expect("Failed to get a write lock on the cpio cache")
            .push(store_path, cpio.clone())?;

        Ok(cpio)
    }
}

#[derive(Debug, Clone)]
pub struct CachedPathBuf(PathBuf);

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

#[derive(Debug)]
pub struct Cpio {
    size: u64,
    path: CachedPathBuf,
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
    pub fn new(path: CachedPathBuf) -> Result<Self, CpioError> {
        let metadata = std::fs::metadata(&path.0).map_err(|e| CpioError::Io {
            ctx: "Reading the CPIO's file metadata",
            src: None,
            dest: Some(path.0.clone()),
            e,
        })?;

        Ok(Self {
            size: metadata.len(),
            path,
        })
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn path(&self) -> &Path {
        &self.path.0
    }

    pub async fn reader_stream(
        mut self,
    ) -> std::io::Result<ReaderStream<tokio::io::BufReader<tokio::fs::File>>> {
        Ok(ReaderStream::new(BufReader::new(self.handle().await?)))
    }

    async fn handle(&mut self) -> std::io::Result<tokio::fs::File> {
        File::open(&self.path()).await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CpioError {
    #[error("An IO error")]
    Io {
        ctx: &'static str,
        src: Option<PathBuf>,
        dest: Option<PathBuf>,
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

    #[error("Failed to strip cache prefix")]
    StripCachePrefix(std::path::StripPrefixError),
}
