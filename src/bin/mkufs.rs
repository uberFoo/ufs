use std::path::PathBuf;

use failure::Error;
use log::debug;
use pretty_env_logger;
use structopt::StructOpt;

use ufs::{BlockCardinality, BlockManager, BlockSize, FileStore};

#[derive(Debug, StructOpt)]
#[structopt(name = "mkufs", about = "create an on-disk ufs file system")]
struct Opt {
    /// File system bundle
    #[structopt(parse(from_os_str))]
    bundle_path: PathBuf,
    /// Block size
    #[structopt(short = "s", long = "block-size", default_value = "2048")]
    block_size: BlockSize,
    /// Number of blocks
    #[structopt(short = "c", long = "block-count", default_value = "256")]
    block_count: BlockCardinality,
}

fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    let opt = Opt::from_args();
    debug!("running with options {:?}", opt);

    match FileStore::new(&opt.bundle_path, opt.block_size, opt.block_count) {
        Ok(store) => {
            BlockManager::new(store);
            println!(
                "Created new ufs file system with {} {} blocks at {:?}.",
                opt.block_count, opt.block_size, opt.bundle_path
            );
            Ok(())
        }
        Err(e) => {
            println!("Problem creating file system at {:?}.", opt.bundle_path);
            Err(e)
        }
    }
}
