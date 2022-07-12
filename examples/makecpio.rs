use std::path::Path;

use tempfile::TempDir;

#[tokio::main]
async fn main() {
    let threads = 4;
    let src = std::env::args().nth(1).unwrap();
    let dest = std::env::args().nth(2).unwrap();

    let scratch = TempDir::new().unwrap();
    let mut dest = tokio::fs::OpenOptions::new().create(true).write(true).open(dest).await.unwrap();

    let mut cpiocache =
        nix_cpio_generator::cpio_cache::CpioCache::new(scratch.path().to_path_buf(), Some(threads)).unwrap();
    let (size, stream) = nix_cpio_generator::stream::stream(
        &mut cpiocache,
        Path::new(&src),
    )
    .await
    .unwrap();
    println!("Bytes: {}", size);
    tokio::io::copy(
        &mut Box::pin(tokio_util::io::StreamReader::new(stream)),
        &mut dest).await.unwrap();
}
