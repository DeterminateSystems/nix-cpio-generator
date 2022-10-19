use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use log::{info, trace};
use tempfile::NamedTempFile;
use tokio::sync::Semaphore;

use crate::cpio::{CachedPathBuf, Cpio};
use crate::cpio_lru_cache::{CpioLruCache, NixStorePath};
use crate::cpio_maker::{make_archive_from_dir, make_registration};
use crate::error::CpioError;
use crate::opened_cpio::OpenedCpio;

#[derive(Clone)]
pub struct CpioCache {
    cache_dir: PathBuf,
    cache: Arc<RwLock<CpioLruCache>>,
    semaphore: Option<Arc<Semaphore>>,
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
        let temp_dest =
            NamedTempFile::new_in_respecting_umask(&self.cache_dir).map_err(|e| CpioError::Io {
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
