use std::{
    io::{self, Write},
    path::PathBuf,
};

use {log::debug, pretty_env_logger, structopt::StructOpt};

use ufs::UberFileSystem;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "useradd",
    about = "add a user to a ufs file system",
    global_settings(&[structopt::clap::AppSettings::ColoredHelp])
)]
struct Opt {
    /// File system bundle
    #[structopt(parse(from_os_str))]
    bundle_path: PathBuf,
    /// List existing users
    #[structopt(short = "l", long = "list")]
    list: bool,
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
        rpassword::read_password_from_tty(Some("master password: ")).unwrap()
    };

    io::stdout().write_all(b"user: ")?;
    io::stdout().flush()?;
    let mut user = String::new();
    io::stdin().read_line(&mut user)?;
    let user = user.trim();
    let password = rpassword::read_password_from_tty(Some("password: ")).unwrap();

    let mut ufs = UberFileSystem::load_file_backed(
        master_password,
        user.to_string(),
        password,
        &opt.bundle_path,
    )?;

    if opt.list {
        for user in ufs.get_users() {
            println!(" - '{}'", user);
        }
    } else {
        io::stdout().write_all(b"user: ")?;
        io::stdout().flush()?;
        let mut user = String::new();
        io::stdin().read_line(&mut user)?;
        let user = user.trim();
        let password = rpassword::read_password_from_tty(Some("password: ")).unwrap();
        let password2 = rpassword::read_password_from_tty(Some("confirm password: ")).unwrap();
        if password != password2 {
            panic!("Passwords do not match.");
        }

        ufs.add_user(user.to_owned());
    }

    Ok(())
}
