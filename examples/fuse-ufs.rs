use std::{fs, path::PathBuf};

use ::fuse::mount;
use env_logger;
use failure::Error;
use structopt::StructOpt;
// use ufs::{fuse::UberFSFuse, UberFileSystem};

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
    // let mut ufs = if fs::read_dir(&opt.bundle_path).is_ok() {
    //     UberFileSystem::load_file_backed(&opt.bundle_path)?
    // } else {
    //     UberFileSystem::new_file_backed(&opt.bundle_path, 2048, 0x100)?
    // };

    // let mut ufs_fuse = UberFSFuse::new(&mut ufs);
    // ufs_fuse.load_root_directory();

    // mount(ufs_fuse, &opt.mount_path, &[])?;

    Ok(())
}
