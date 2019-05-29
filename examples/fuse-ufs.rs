use std::{fs, path::PathBuf};

use ::fuse::mount;
use failure::Error;
use pretty_env_logger;
use structopt::StructOpt;
use ufs::{fuse::UberFSFuse, UberFileSystem};

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
    pretty_env_logger::init();
    let opt = Opt::from_args();
    if fs::read_dir(&opt.bundle_path).is_ok() {
        let ufs = UberFileSystem::load_file_backed(&opt.bundle_path)?;
        let mut ufs_fuse = UberFSFuse::new(ufs);
        ufs_fuse.load_root_directory();

        mount(ufs_fuse, &opt.mount_path, &[])?;
    } else {
        println!(
            "No file system found at {}",
            &opt.bundle_path.to_str().unwrap()
        );
    };

    Ok(())
}
