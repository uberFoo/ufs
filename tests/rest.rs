use {
    fuse::mount,
    reqwest::Url,
    std::{
        collections::HashMap,
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
        Command::new("mkdir").arg(mnt).status();

        let ufs = UberFileSystem::load_file_backed(
            "".to_string(),
            "".to_string(),
            "".to_string(),
            bundle,
        )?;
        let mounter = UfsMounter::new(ufs, Some(port));
        let ufs_fuse = UberFSFuse::new(mounter);
        mount(ufs_fuse, &mnt.to_string(), &[])?;

        Command::new("rmdir").arg(mnt).status();
        Ok(())
    })
}

fn get_token(port: u16) -> String {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let mut creds = HashMap::new();
    creds.insert("id", "");
    creds.insert("password", "");
    client
        .post(&format!("https://localhost:{}/login", port))
        .json(&creds)
        .send()
        .unwrap()
        .text()
        .unwrap()
}

#[test]
fn missing_endpoints() {
    let fs = mount_fs(Path::new("bundles/integration_tests"), "mnt1", 8887);
    // Sleep to allow the file system to mount.
    thread::sleep(time::Duration::from_millis(500));

    let token = get_token(8887);

    // Test GET
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let body = client
        .get("https://localhost:8887/wasm/foo")
        .query(&[("token", &token)])
        .send()
        .unwrap()
        .text()
        .unwrap();

    assert_eq!("no such endpoint".to_string(), body);

    // Test POST
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let content: HashMap<String, String> = HashMap::new();
    let body = client
        .post("https://localhost:8887/wasm/foo")
        .json(&content)
        .query(&[("token", &token)])
        .send()
        .unwrap()
        .text()
        .unwrap();

    assert_eq!("no such endpoint".to_string(), body);

    // Test PUT
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let content: HashMap<String, String> = HashMap::new();
    let body = client
        .put("https://localhost:8887/wasm/foo")
        .json(&content)
        .query(&[("token", &token)])
        .send()
        .unwrap()
        .text()
        .unwrap();

    assert_eq!("no such endpoint".to_string(), body);

    // Test DELETE
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let body = client
        .delete("https://localhost:8887/wasm/foo")
        .query(&[("token", &token)])
        .json(&content)
        .send()
        .unwrap()
        .text()
        .unwrap();

    assert_eq!("no such endpoint".to_string(), body);

    // Test PATCH
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let body = client
        .patch("https://localhost:8887/wasm/foo")
        .json(&content)
        .query(&[("token", &token)])
        .send()
        .unwrap()
        .text()
        .unwrap();

    assert_eq!("no such endpoint".to_string(), body);

    Command::new("umount").arg("mnt1").status();
    fs.join()
        .expect("unable to join fs thread")
        .expect("error starting fs");
}

#[test]
fn wasm_rest() {
    let fs = mount_fs(Path::new("bundles/test_for_echo"), "mnt2", 8889);
    // Sleep to allow the file system to mount.
    thread::sleep(time::Duration::from_millis(500));

    let token = get_token(8889);

    // Test GET
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let body = client
        .get("https://localhost:8889/wasm/foo")
        .query(&[("token", &token)])
        .send()
        .unwrap()
        .text()
        .unwrap();

    assert_eq!(
        "{\n  \"index\": 42,\n  \"value\": 433494437\n}".to_string(),
        body
    );

    // Test POST
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let content: HashMap<String, String> = HashMap::new();
    let body = client
        .post("https://localhost:8889/wasm/foo")
        .json(&content)
        .query(&[("token", &token)])
        .send()
        .unwrap()
        .text()
        .unwrap();

    assert_eq!(
        "{\n  \"index\": 42,\n  \"value\": 433494437\n}".to_string(),
        body
    );

    // Test PUT not allowed
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let content: HashMap<String, String> = HashMap::new();
    let body = client
        .put("https://localhost:8889/wasm/foo")
        .json(&content)
        .query(&[("token", &token)])
        .send()
        .unwrap()
        .text()
        .unwrap();

    assert_eq!("insufficient permissions".to_string(), body);

    // Test DELETE not allowed
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let body = client
        .delete("https://localhost:8889/wasm/foo")
        .query(&[("token", &token)])
        .json(&content)
        .send()
        .unwrap()
        .text()
        .unwrap();

    assert_eq!("insufficient permissions".to_string(), body);

    // Test PATCH not allowed
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let body = client
        .patch("https://localhost:8889/wasm/foo")
        .json(&content)
        .query(&[("token", &token)])
        .send()
        .unwrap()
        .text()
        .unwrap();

    assert_eq!("insufficient permissions".to_string(), body);

    Command::new("umount").arg("mnt2").status();
    fs.join()
        .expect("unable to join fs thread")
        .expect("error starting fs");
}
