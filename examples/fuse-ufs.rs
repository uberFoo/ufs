use std::path::PathBuf;

use ::fuse::mount;
use env_logger;
use failure::Error;
use structopt::StructOpt;
use ufs::fuse::UberFSFuse;

/// Mount the file system using FUSE.
///
/// FIXME: create options to mount or create new
#[derive(Debug, StructOpt)]
#[structopt(name = "fuse-ufs", about = "mount a ufs using fuse")]
struct Opt {
    /// File system bundle
    #[structopt(parse(from_os_str))]
    bundle_path: PathBuf,
    /// Mount point
    #[structopt(parse(from_os_str))]
    mount_path: PathBuf,
}

fn main() -> Result<(), Error> {
    env_logger::init();
    let opt = Opt::from_args();
    mount(UberFSFuse::new(&opt.bundle_path)?, &opt.mount_path, &[])?;
    Ok(())
}
