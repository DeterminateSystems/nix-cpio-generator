use std::path::PathBuf;

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
    RegistrationError(crate::cpio_maker::MakeRegistrationError),

    #[error(
        "The path we tried to generate a cache for can't turn in to a cache key for some reason"
    )]
    Uncachable(String),

    #[error("failed to acquire a semaphore")]
    Semaphore(tokio::sync::AcquireError),

    #[error("Failed to strip cache prefix")]
    StripCachePrefix(std::path::StripPrefixError),
}
