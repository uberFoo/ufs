use std::{
    io::{self, Write},
    path::PathBuf,
};

use log::debug;
use pretty_env_logger;
use structopt::StructOpt;

use ufs::{BlockCardinality, BlockManager, BlockMap, BlockSize, FileStore, UfsUuid};

#[derive(Debug, StructOpt)]
#[structopt(
    name = "mkufs",
    about = "Create an on-disk ufs file system.  The file system UUID is the same as the on-disk bundle location.",
    global_settings(&[structopt::clap::AppSettings::ColoredHelp])
)]
struct Opt {
    /// File system bundle directory
    #[structopt(parse(from_os_str))]
    bundle_path: PathBuf,
    /// Block size
    #[structopt(short = "s", long = "block-size", default_value = "2048")]
    block_size: BlockSize,
    /// Number of blocks
    #[structopt(short = "c", long = "block-count", default_value = "1024")]
    block_count: BlockCardinality,
    /// File system master password
    #[structopt(short = "p", long = "password")]
    password: Option<String>,
}

fn main() -> Result<(), failure::Error> {
    pretty_env_logger::init();

    let opt = Opt::from_args();
    debug!("running with options {:?}", opt);

    let master_password = if let Some(password) = opt.password {
        password
    } else {
        let p = rpassword::read_password_from_tty(Some("master password: ")).unwrap();
        let c = rpassword::read_password_from_tty(Some("confirm master password: ")).unwrap();
        if p == c {
            p
        } else {
            panic!("Passwords do not match.");
        }
    };

    io::stdout().write_all(b"user: ")?;
    io::stdout().flush()?;
    let mut user = String::new();
    io::stdin().read_line(&mut user)?;
    let user = user.trim();
    let password = rpassword::read_password_from_tty(Some("password: ")).unwrap();
    let password2 = rpassword::read_password_from_tty(Some("confirm password: ")).unwrap();
    if password != password2 {
        panic!("Passwords do not match.")
    }

    let map = BlockMap::new(
        UfsUuid::new_root_fs(opt.bundle_path.file_name().unwrap().to_str().unwrap()),
        opt.block_size,
        opt.block_count,
    );

    match FileStore::new(&master_password, &opt.bundle_path, map) {
        Ok(store) => {
            BlockManager::new(user, &password, store);
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
