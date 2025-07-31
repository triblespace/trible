use assert_cmd::Command;
use blake3;
use ed25519_dalek::SigningKey;
use hex;
use predicates::prelude::*;
use rand::rngs::OsRng;
use tempfile::tempdir;
use tribles::prelude::BranchStore;
use tribles::prelude::{BlobStore, BlobStoreList};
use tribles::repo::{pile::Pile, Repository};

#[test]
fn idgen_outputs_id() {
    Command::cargo_bin("trible")
        .unwrap()
        .arg("id-gen")
        .assert()
        .success()
        .stdout(predicate::str::is_match("^[A-F0-9]{32}\n$").unwrap());
}

#[test]
fn list_branches_outputs_branch_id() {
    const MAX_SIZE: usize = 1 << 20; // small pile for tests
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.pile");

    {
        let pile: Pile<MAX_SIZE> = Pile::open(&path).unwrap();
        let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));
        repo.branch("main").expect("create branch");
        // drop repo to flush changes
    }

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "branch", "list", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match("^[A-F0-9]{32}\n$").unwrap());
}

#[test]
fn create_initializes_empty_pile() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("create_test.pile");

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "create", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());

    const MAX_SIZE: usize = 1 << 20; // small pile for tests
    let pile: Pile<MAX_SIZE> = Pile::open(&path).unwrap();
    assert!(pile.branches().next().is_none());
}

#[test]
fn put_ingests_file() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("put_test.pile");
    let input_path = dir.path().join("input.bin");
    std::fs::write(&input_path, b"hello world").unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            input_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());

    const MAX_SIZE: usize = 1 << 20; // small pile for tests
    let mut pile: Pile<MAX_SIZE> = Pile::open(&pile_path).unwrap();
    let reader = pile.reader();
    assert!(reader.blobs().next().is_some());
}

#[test]
fn get_restores_blob() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("get_test.pile");
    let input_path = dir.path().join("input.bin");
    let output_path = dir.path().join("output.bin");
    let contents = b"fetch me";
    std::fs::write(&input_path, contents).unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            input_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "get",
            pile_path.to_str().unwrap(),
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
fn list_blobs_outputs_handle() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("list_blobs.pile");
    let input_path = dir.path().join("input.bin");
    std::fs::write(&input_path, b"hello").unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            input_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "blob", "list", pile_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match("^blake3:[a-f0-9]{64}\n$").unwrap());
}

#[test]
fn diagnose_reports_healthy() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("diag.pile");

    // create an empty pile file
    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "create", pile_path.to_str().unwrap()])
        .assert()
        .success();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "diagnose", pile_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("healthy"));
}

#[test]
fn diagnose_reports_invalid_hash() {
    use std::io::{Seek, Write};

    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("bad.pile");
    let blob_path = dir.path().join("blob.bin");
    std::fs::write(&blob_path, b"good data").unwrap();

    // put a blob into the pile
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            blob_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    // corrupt the blob bytes directly
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&pile_path)
        .unwrap();
    // first blob starts after the 64 byte header
    file.seek(std::io::SeekFrom::Start(64)).unwrap();
    file.write_all(b"X").unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "diagnose", pile_path.to_str().unwrap()])
        .assert()
        .failure()
        .stdout(predicate::str::contains("incorrect hashes"));
}

#[test]
fn inspect_outputs_tribles() {
    use tribles::examples;
    use tribles::prelude::blobschemas::SimpleArchive;
    use tribles::prelude::*;
    use tribles::value::schemas::hash::Handle;

    const MAX_SIZE: usize = 1 << 20;
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("inspect.pile");

    let dataset = examples::dataset();
    let blob = dataset.to_blob();

    {
        let mut pile: Pile<MAX_SIZE> = Pile::open(&pile_path).unwrap();
        let handle = pile.put::<SimpleArchive, _>(blob).unwrap();
        pile.flush().unwrap();

        let hash = Handle::to_hash(handle);
        let handle_str: String = hash.from_value();

        Command::cargo_bin("trible")
            .unwrap()
            .args([
                "pile",
                "blob",
                "inspect",
                pile_path.to_str().unwrap(),
                &handle_str,
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("Length:"));
    }
}

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

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "put", &url, file_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());

    let digest = blake3::hash(contents).to_hex().to_string();
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

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "put", &url, file_path.to_str().unwrap()])
        .assert()
        .success();

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");

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
fn pile_branch_create_outputs_id() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("create_branch.pile");

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "create",
            pile_path.to_str().unwrap(),
            "main",
        ])
        .assert()
        .success()
        .stdout(predicate::str::is_match("^[A-F0-9]{32}\n$").unwrap());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "branch", "list", pile_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match("^[A-F0-9]{32}\n$").unwrap());
}

#[test]
fn branch_push_pull_transfers_branch() {
    const MAX_SIZE: usize = 1 << 20;
    let dir = tempdir().unwrap();
    let local = dir.path().join("local.pile");
    let remote_dir = dir.path().join("remote");
    std::fs::create_dir_all(remote_dir.join("branches")).unwrap();
    std::fs::create_dir_all(remote_dir.join("blobs")).unwrap();
    let url = format!("file://{}", remote_dir.display());

    let branch_id = {
        let pile: Pile<MAX_SIZE> = Pile::open(&local).unwrap();
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
