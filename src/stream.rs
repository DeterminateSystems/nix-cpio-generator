#[allow(unused_imports)]
use futures::stream::{FuturesOrdered, FuturesUnordered, TryStreamExt};
use futures::StreamExt;
use std::path::Path;

use log::{error, info, trace, warn};

use crate::cpio_cache::CpioCache;
use crate::cpio_maker::{make_load_cpio, LEADER_CPIO_BYTES, LEADER_CPIO_LEN};
use crate::nix::get_closure_paths;

pub async fn stream(
    cpio_cache: &CpioCache,
    store_path: &Path,
) -> Result<
    (
        u64,
        impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>>,
    ),
    (),
> {
    info!("Sending closure: {:?}", &store_path);

    let closure_paths = get_closure_paths(store_path).await.map_err(|e| {
        warn!("Error calculating closure for {:?}: {:?}", store_path, e);
        panic!();
    })?;

    let mut cpio_makers = closure_paths
        .iter()
        .cloned()
        .map(|path| cpio_cache.dump_cpio(path))
        .collect::<FuturesUnordered<_>>();

    let mut size: u64 = 0;
    let mut readers: Vec<_> = vec![];

    while let Some(result) = cpio_makers.next().await {
        let cpio = result.map_err(|e| {
            error!("Failure generating a CPIO: {:?}", e);
            panic!();
        })?;
        size += cpio.size();
        readers.push(cpio);
    }

    readers.sort_unstable_by(|left, right| {
        left.path()
            .partial_cmp(right.path())
            .expect("Sorting &Path should have no chance for NaNs, thus no unwrap")
    });

    let mut streams = FuturesOrdered::new();
    for cpio in readers.into_iter() {
        streams.push(async {
            trace!("Handing over the reader for {:?}", cpio.path());
            cpio.reader_stream().await.map_err(|e| {
                error!("Failed to get a reader stream: {:?}", e);
                e
            })
        });
    }

    let size = size
        .checked_add(*LEADER_CPIO_LEN)
        .expect("Failed to sum the leader length with the total initrd size");
    let leader_stream = futures::stream::once(async {
        Ok::<_, std::io::Error>(bytes::Bytes::from_static(&LEADER_CPIO_BYTES))
    });

    let store_loader = make_load_cpio(&closure_paths).map_err(|e| {
        error!("Failed to generate a load CPIO: {:?}", e);
        panic!();
    })?;
    let size = size
        .checked_add(
            store_loader
                .len()
                .try_into()
                .expect("Failed to convert a usize to u64"),
        )
        .expect("Failed to sum the loader length with the total initrd size");

    let body_stream = leader_stream
        .chain(futures::stream::once(async move {
            Ok::<_, std::io::Error>(bytes::Bytes::from(store_loader))
        }))
        .chain(streams.try_flatten());

    Ok((size, body_stream))
}
