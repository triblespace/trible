use assert_cmd::Command;
use ed25519_dalek::SigningKey;
use predicates::prelude::*;
use rand::rngs::OsRng;
use tempfile::tempdir;
use tribles::repo::pile::Pile;
use tribles::repo::Repository;
use tribles::value::schemas::hash::Blake3;

#[test]
fn store_blob_list_outputs_file() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("file.bin");
    std::fs::write(&file, b"hi").unwrap();

    let url = format!("file://{}", dir.path().display());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "list", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains("file.bin"));
}

#[test]
fn store_blob_put_uploads_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("input.bin");
    let contents = b"hi there";
    std::fs::write(&file_path, contents).unwrap();

    let url = format!("file://{}", dir.path().display());

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!("^{handle}\\n$");

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "put", &url, file_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match(&pattern).unwrap());

    let blob_path = dir.path().join("blobs").join(digest);
    assert!(blob_path.exists());
}

#[test]
fn store_blob_forget_removes_blob() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("input.bin");
    let contents = b"remove me";
    std::fs::write(&file_path, contents).unwrap();

    let url = format!("file://{}", dir.path().display());

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!("^{handle}\\n$");

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "put", &url, file_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match(&pattern).unwrap());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "forget", &url, &handle])
        .assert()
        .success();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "list", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains(&digest).not());
}

#[test]
fn store_blob_get_downloads_file() {
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("input.bin");
    let output_path = dir.path().join("output.bin");
    let contents = b"remote blob";
    std::fs::write(&input_path, contents).unwrap();

    let url = format!("file://{}", dir.path().display());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "put", &url, input_path.to_str().unwrap()])
        .assert()
        .success();

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "store",
            "blob",
            "get",
            &url,
            &handle,
            output_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());

    let out = std::fs::read(&output_path).unwrap();
    assert_eq!(contents, &out[..]);
}

#[test]
fn store_blob_inspect_outputs_metadata() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("inspect.bin");
    let contents = b"remote";
    std::fs::write(&file_path, contents).unwrap();

    let url = format!("file://{}", dir.path().display());

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!("^{handle}\\n$");

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "put", &url, file_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match(&pattern).unwrap());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "inspect", &url, &handle])
        .assert()
        .success()
        .stdout(predicate::str::contains("Length:"));
}

#[test]
fn store_branch_list_outputs_id() {
    let dir = tempdir().unwrap();
    let branch_id = [1u8; 16];
    let branch_hex = hex::encode(branch_id);
    let branches_dir = dir.path().join("branches");
    std::fs::create_dir_all(&branches_dir).unwrap();
    std::fs::write(branches_dir.join(&branch_hex), b"branch").unwrap();

    let url = format!("file://{}", dir.path().display());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "branch", "list", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains(branch_hex.to_ascii_uppercase()));
}

#[test]
fn branch_push_pull_transfers_branch() {
    // const MAX_SIZE removed; new Pile API accepts a hash protocol type parameter
    let dir = tempdir().unwrap();
    let local = dir.path().join("local.pile");
    let remote_dir = dir.path().join("remote");
    std::fs::create_dir_all(remote_dir.join("branches")).unwrap();
    std::fs::create_dir_all(remote_dir.join("blobs")).unwrap();
    let url = format!("file://{}", remote_dir.display());

    let branch_id = {
        let pile: Pile<Blake3> = Pile::open(&local).unwrap();
        let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));
        let ws = repo.branch("main").unwrap();
        ws.branch_id()
    };
    let branch_hex = hex::encode(branch_id);

    Command::cargo_bin("trible")
        .unwrap()
        .args(["branch", "push", &url, local.to_str().unwrap(), &branch_hex])
        .assert()
        .success();

    let other = dir.path().join("other.pile");
    Command::cargo_bin("trible")
        .unwrap()
        .args(["branch", "pull", &url, other.to_str().unwrap(), &branch_hex])
        .assert()
        .success();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "branch", "list", other.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains(branch_hex.to_ascii_uppercase()));
}
