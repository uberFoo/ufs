use {
    fuse::mount,
    std::{
        path::Path,
        process::Command,
        thread::{self, spawn, JoinHandle},
        time,
    },
    ufs::{UberFSFuse, UberFileSystem, UfsMounter},
};

fn mount_fs(
    bundle: &'static Path,
    mnt: &'static str,
    port: u16,
) -> JoinHandle<Result<(), failure::Error>> {
    spawn(move || {
        Command::new("mkdir")
            .arg(mnt)
            .status()
            .expect("unable to create mount point");

        let ufs = UberFileSystem::load_file_backed(
            "".to_string(),
            "".to_string(),
            "".to_string(),
            bundle,
        )?;
        let mounter = UfsMounter::new(ufs, Some(port));
        let ufs_fuse = UberFSFuse::new(mounter);
        mount(ufs_fuse, &mnt.to_string(), &[])?;

        Command::new("rmdir")
            .arg(mnt)
            .status()
            .expect("unable to remove mount point");
        Ok(())
    })
}

#[test]
fn file_operations() {
    let fs = mount_fs(Path::new("tests/bundles/integration_tests"), "mnt1", 8887);
    // Sleep to allow the file system to mount.
    thread::sleep(time::Duration::from_millis(500));

    // Test file creation/removal, reading/writing
    assert_eq!(false, Path::new("mnt1/wasm.rs").exists());

    let status = Command::new("cp")
        .arg("src/wasm.rs")
        .arg("mnt1")
        .status()
        .expect("failed to copy file");
    assert!(status.success());

    let status = Command::new("diff")
        .arg("src/wasm.rs")
        .arg("mnt1/wasm.rs")
        .status()
        .expect("diff failed");
    assert!(status.success());

    let status = Command::new("rm")
        .arg("mnt1/wasm.rs")
        .status()
        .expect("unable to remove file");
    assert!(status.success());
    thread::sleep(time::Duration::from_millis(500));

    assert_eq!(false, Path::new("mnt1/wasm.rs").exists());

    // Test directory creation and removal
    assert_eq!(false, Path::new("mnt1/fubar").exists());

    let status = Command::new("mkdir")
        .arg("mnt1/fubar")
        .status()
        .expect("failed to create directory");
    assert!(status.success());

    assert_eq!(true, Path::new("mnt1/fubar").exists());

    let status = Command::new("rmdir")
        .arg("mnt1/fubar")
        .status()
        .expect("failed to remove directory");
    assert!(status.success());
    thread::sleep(time::Duration::from_millis(500));

    assert_eq!(false, Path::new("mnt1/fubar").exists());

    Command::new("umount")
        .arg("mnt1")
        .status()
        .expect("unable to unmount mnt1");
    fs.join()
        .expect("unable to join fs thread")
        .expect("error starting fs");
}
