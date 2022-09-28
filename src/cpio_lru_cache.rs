use std::path::PathBuf;

use lru::LruCache;

use crate::cpio::{CachedPathBuf, Cpio};
use crate::error::CpioError;

pub struct CpioLruCache {
    pub lru_cache: LruCache<NixStorePath, Cpio>,
    pub current_size_in_bytes: u64,
    pub max_size_in_bytes: u64,
}

impl CpioLruCache {
    pub fn new(max_size_in_bytes: u64) -> Self {
        Self {
            // NOTE: We use an unbounded LruCache because we don't care about the number of items,
            // but the size of the items on disk (which we need to track ourselves).
            lru_cache: LruCache::unbounded(),
            current_size_in_bytes: 0,
            max_size_in_bytes,
        }
    }

    pub fn prune_single_lru(&mut self) -> Result<(), CpioError> {
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

    pub fn prune_lru(&mut self) -> Result<(), CpioError> {
        while self.current_size_in_bytes > self.max_size_in_bytes {
            self.prune_single_lru()?;
        }

        Ok(())
    }

    pub fn push(&mut self, path: NixStorePath, cpio: Cpio) -> Result<(), CpioError> {
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

    pub fn get(&mut self, path: &NixStorePath) -> Option<&Cpio> {
        self.lru_cache.get(path)
    }

    pub fn demote(&mut self, path: &NixStorePath) {
        self.lru_cache.demote(path)
    }
}

/// A path that exists in the Nix store.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct NixStorePath(PathBuf);

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
