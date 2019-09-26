use std::{
    fs,
    io::{self, Write},
};

use ::fuse::mount;
use clap::{App, AppSettings, Arg};
use pretty_env_logger;
use reqwest::Url;
use ufs::{UberFSFuse, UberFileSystem, UfsMounter, UfsUuid};

fn main() -> Result<(), failure::Error> {
    let opts = App::new("fuse-ufs")
        .version("0.2.1")
        .author("Keith T. Star <keith@uberfoo.com>")
        .about("mount a ufs volume using fuse")
        .arg(
            Arg::with_name("password")
                .short("p")
                .long("password")
                .value_name("PASSWORD")
                .help("File system master password")
                .takes_value(true),
        )
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
        .arg(
            Arg::with_name("remote")
                .short("r")
                .long("port")
                .value_name("PORT")
                .help("Port to listen for remote file system support")
                .required(false)
                .takes_value(true),
        )
        .setting(AppSettings::ColoredHelp)
        .get_matches();

    pretty_env_logger::init();

    let port = if let Some(port) = opts.value_of("remote") {
        port.parse::<u16>().ok()
    } else {
        None
    };

    match opts.value_of("bundle") {
        Some(path) => match fs::read_dir(&path) {
            Ok(_) => {
                let master_password = if let Some(password) = opts.value_of("password") {
                    password.to_owned()
                } else {
                    rpassword::read_password_from_tty(Some("master password: ")).unwrap()
                };

                io::stdout().write_all(b"user: ")?;
                io::stdout().flush()?;
                let mut user = String::new();
                io::stdin().read_line(&mut user);
                let user = user.trim();
                let password = rpassword::read_password_from_tty(Some("password: ")).unwrap();

                let ufs = UberFileSystem::load_file_backed(
                    master_password,
                    user.to_string(),
                    password,
                    &path,
                )?;
                let mounter = UfsMounter::new(ufs, port);
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
                    io::stdout().write_all(b"user: ")?;
                    io::stdout().flush()?;
                    let mut user = String::new();
                    io::stdin().read_line(&mut user);
                    let user = user.trim();
                    let password = rpassword::read_password_from_tty(Some("password: ")).unwrap();

                    let fs_name = url.path_segments().unwrap().last().unwrap();
                    let ufs = UberFileSystem::new_networked(
                        user.to_string(),
                        password,
                        fs_name.to_string(),
                        url,
                    )?;
                    let mounter = UfsMounter::new(ufs, port);
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
