use std::fs;

use ::fuse::mount;
use clap::{App, Arg};
use pretty_env_logger;
use reqwest::Url;
use ufs::{UberFSFuse, UberFileSystem, UfsMounter};

fn main() -> Result<(), failure::Error> {
    let opts = App::new("fuse-ufs")
        .version("0.2.0")
        .author("Keith T. Star <keith@uberfoo.com>")
        .about("mount a ufs volume using fuse")
        .arg(
            Arg::with_name("bundle")
                .short("b")
                .long("bundle")
                .value_name("BUNDLE_DIR")
                .help("Read blocks from a ufs bundle")
                .group("source")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("network")
                .short("n")
                .long("url")
                .value_name("URL")
                .help("Read blocks from a remote block server")
                .group("source")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("mnt")
                .value_name("MOUNT_POINT")
                .help("File system mount point")
                .required(true)
                .requires("source")
                .index(1),
        )
        .get_matches();

    pretty_env_logger::init();

    match opts.value_of("bundle") {
        Some(path) => match fs::read_dir(&path) {
            Ok(_) => {
                let ufs = UberFileSystem::load_file_backed(&path)?;
                let mounter = UfsMounter::new(ufs);
                let ufs_fuse = UberFSFuse::new(mounter);
                mount(ufs_fuse, &opts.value_of("mnt").unwrap(), &[])?;
            }
            Err(e) => {
                eprintln!("error reading bundle: {}", e);
                std::process::exit(-1);
            }
        },
        None => {
            // We know it's one or the other, so unwrap is ok here.
            match Url::parse(opts.value_of("network").unwrap()) {
                Ok(url) => {
                    let ufs = UberFileSystem::new_networked(url)?;
                    let mounter = UfsMounter::new(ufs);
                    let ufs_fuse = UberFSFuse::new(mounter);
                    mount(ufs_fuse, &opts.value_of("mnt").unwrap(), &[])?;
                }
                Err(e) => {
                    eprintln!("invalid URL: {}", e);
                    std::process::exit(-2);
                }
            }
        }
    };

    Ok(())
}
