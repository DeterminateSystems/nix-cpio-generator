use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use log::{info, trace};
use lru::LruCache;
use tempfile::NamedTempFile;
use tokio::sync::Semaphore;

use crate::cpio::{make_archive_from_dir, make_registration};
use crate::opened_cpio::OpenedCpio;

#[derive(Clone)]
pub struct CpioCache {
    cache_dir: PathBuf,
    cache: Arc<RwLock<CpioLruCache>>,
    semaphore: Option<Arc<Semaphore>>,
}

/// A path that exists in the Nix store.
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
            // NOTE: We use an unbounded LruCache because we don't care about the number of items,
            // but the size of the items on disk (which we need to track ourselves).
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
                    src: path.0,
                    dest: cpio.path().to_path_buf(),
                    e,
                })?;

                self.current_size_in_bytes -= cpio.size;

                log::trace!("Removed {:?} ({} bytes)", cpio.path(), cpio.size);
            }
        }

        Ok(())
    }

    fn prune_lru(&mut self) -> Result<(), CpioError> {
        while self.current_size_in_bytes > self.max_size_in_bytes {
            self.prune_single_lru()?;
        }

        Ok(())
    }

    fn push(&mut self, path: NixStorePath, cpio: Cpio) -> Result<(), CpioError> {
        let cpio_size = cpio.size;

        // Ensure we are still below (or at) the max cache size.
        self.prune_lru()?;

        // This branch is necessary, or else setting a max cache size of e.g. 0 will loop here
        // infinitely (since the CPIOs aren't 0 bytes large, and 1 + 0 > 0).
        if cpio_size < self.max_size_in_bytes {
            // Ensure pushing this CPIO will not exceed the max cache size.
            while cpio_size + self.current_size_in_bytes > self.max_size_in_bytes {
                self.prune_single_lru()?;
            }
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

        log::info!("Enumerating cache dir {:?} to place into lru", cache_dir);
        for entry in std::fs::read_dir(&cache_dir).map_err(|e| CpioError::Fs {
            ctx: "Reading the cache dir",
            path: cache_dir.clone(),
            e,
        })? {
            let entry = entry.map_err(|e| CpioError::Fs {
                ctx: "Reading cache dir entry",
                path: cache_dir.clone(),
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

        if cache.current_size_in_bytes > cache.max_size_in_bytes {
            log::info!(
                "Pruning lru cache to be less than max size {} bytes (currently {} bytes)",
                cache.max_size_in_bytes,
                cache.current_size_in_bytes,
            );
            cache.prune_lru()?;
        }

        Ok(Self {
            cache: Arc::new(RwLock::new(cache)),
            cache_dir,
            semaphore: parallelism.map(|cap| Arc::new(Semaphore::new(cap))),
        })
    }

    pub async fn dump_cpio(&self, path: PathBuf) -> Result<OpenedCpio, CpioError> {
        let cpio = if let Some(cpio) = self.get_cached(&path)? {
            trace!("Found CPIO in the cache {:?}", path);
            cpio
        } else {
            info!("Making a new CPIO for {:?}", path);
            self.make_cpio(&path).await?
        };

        // Get a handle to the CPIO...
        let cpio = OpenedCpio::new(cpio.path, cpio.size);

        // ...and then prune the LRU to ensure we're not over the max cache size.
        // The order is important, so we don't prune the CPIO file before we have a handle to it.
        self.cache
            .write()
            .expect("Failed to get a write lock on the cpio cache (for LRU pruning)")
            .prune_lru()?;

        cpio
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
            src: path.to_path_buf(),
            dest: final_dest.0.clone(),
            e,
        })?;

        trace!(
            "Constructing CPIO for {:?} at {:?}, to be moved to {:?}",
            &path,
            &temp_dest,
            &final_dest
        );

        let tdf = temp_dest.as_file().try_clone().unwrap();
        let inside_dest = temp_dest.path().to_path_buf();
        let inside_path = path.to_path_buf();

        let mut compressor =
            zstd::stream::write::Encoder::new(tdf, 10).map_err(|e| CpioError::Io {
                ctx: "Instantiating the zstd write-stream encoder",
                src: inside_path.clone(),
                dest: inside_dest.clone(),
                e,
            })?;

        compressor
            .include_checksum(true)
            .map_err(|e| CpioError::Io {
                ctx: "Including checksums",
                src: inside_path.clone(),
                dest: inside_dest.clone(),
                e,
            })?;

        let mut compressor = tokio::task::spawn_blocking(move || -> Result<_, CpioError> {
            make_archive_from_dir(Path::new("/"), &inside_path, &mut compressor).map_err(|e| {
                CpioError::Io {
                    ctx: "Constructing a CPIO",
                    src: inside_path,
                    dest: inside_dest,
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

        temp_dest
            .persist(&final_dest.0)
            .map_err(|e| CpioError::Io {
                ctx: "Persisting the temporary file to the final location.",
                src: path.to_path_buf(),
                dest: final_dest.0.clone(),
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

#[derive(Debug, thiserror::Error)]
pub enum CpioError {
    #[error("A filesystem error")]
    Fs {
        ctx: &'static str,
        path: PathBuf,
        #[source]
        e: std::io::Error,
    },

    #[error("An IO error")]
    Io {
        ctx: &'static str,
        src: PathBuf,
        dest: PathBuf,
        #[source]
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

#[cfg(test)]
mod tests {
    use super::*;

    // This test is ignored by default because it will only succeed from inside of the nix dev shell
    // that provides the zstd and cpio tools, as well as the CPIO_TEST_CLOSURE environment variable.
    #[ignore]
    #[tokio::test]
    async fn cpio_cache_0_max_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let cpio_cache = CpioCache::new(temp_dir.path().to_owned(), None, 0).unwrap();
        let cpio = cpio_cache
            .make_cpio(&PathBuf::from(env!("CPIO_TEST_CLOSURE")))
            .await
            .unwrap();

        let mut command = std::process::Command::new("sh");
        command.args(["-c", "zstdcat \"$1\" | cpio -div", "--"]);
        command.arg(cpio.path());
        command.current_dir(temp_dir.path());

        let stderr = &command.output().unwrap().stderr;
        let path_part = std::str::from_utf8(stderr).unwrap().lines().nth(0).unwrap();
        assert!(env!("CPIO_TEST_CLOSURE").contains(path_part));

        let extracted_path = temp_dir.path().join(path_part);
        let out = std::fs::read_to_string(extracted_path).unwrap();
        assert_eq!(out, "Hello, CPIO!\n");

        cpio_cache.cache.write().unwrap().prune_lru().unwrap();

        assert_eq!(cpio_cache.cache.read().unwrap().lru_cache.len(), 0);
        assert_eq!(cpio_cache.cache.read().unwrap().max_size_in_bytes, 0);
        assert_eq!(cpio_cache.cache.read().unwrap().current_size_in_bytes, 0);
    }
}
