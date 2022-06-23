use std::ffi::OsString;
use std::io;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};

use tokio::process::Command;

pub async fn get_closure_paths(path: &Path) -> io::Result<Vec<PathBuf>> {
    let output = Command::new(env!("NIX_STORE_BIN"))
        .arg("--query")
        .arg("--requisites")
        .arg(path)
        .output()
        .await?;

    let lines = output
        .stdout
        .split(|&ch| ch == b'\n')
        .filter_map(|line| {
            if line.is_empty() {
                None
            } else {
                let line = Vec::from(line);
                let line = OsString::from_vec(line);
                Some(PathBuf::from(line))
            }
        })
        .collect();

    Ok(lines)
}
