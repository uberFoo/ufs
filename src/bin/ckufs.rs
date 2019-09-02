use std::path::PathBuf;

use log::debug;
use pretty_env_logger;
use structopt::StructOpt;

use ufs::FileStore;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "ckufs",
    about = "check an on-disk ufs file system",
    global_settings(&[structopt::clap::AppSettings::ColoredHelp])
)]
struct Opt {
    /// File system bundle
    #[structopt(parse(from_os_str))]
    bundle_path: PathBuf,
    /// Display verbose BlockMap information
    #[structopt(short = "v", long = "verbose")]
    show_map: bool,
}

fn main() -> Result<(), failure::Error> {
    pretty_env_logger::init();

    let opt = Opt::from_args();
    debug!("running with options {:?}", opt);

    FileStore::check(&opt.bundle_path, opt.show_map)
}
