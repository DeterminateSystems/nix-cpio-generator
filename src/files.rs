use std::ffi::OsStr;
use std::path::Path;

pub fn basename(path: &Path) -> Option<&OsStr> {
    if let Some(std::path::Component::Normal(pathname)) = path.components().last() {
        Some(pathname)
    } else {
        None
    }
}
