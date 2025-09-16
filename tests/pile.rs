use assert_cmd::Command;
use ed25519_dalek::SigningKey;
use predicates::prelude::*;
use rand::rngs::OsRng;
use tempfile::tempdir;
use tribles::prelude::BlobStore;
use tribles::prelude::BlobStoreList;
use tribles::prelude::BranchStore;
use tribles::repo::pile::Pile;
use tribles::repo::Repository;
use tribles::value::schemas::hash::Blake3;

#[test]
fn list_branches_outputs_branch_id() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.pile");

    {
        let pile: Pile<Blake3> = Pile::open(&path).unwrap();
        let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));
        repo.create_branch("main", None).expect("create branch");
        // drop repo to flush changes
    }

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "branch", "list", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match("^[A-F0-9]{32}\\n$").unwrap());
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

    let mut pile: Pile<Blake3> = Pile::open(&path).unwrap();
    // Explicitly refresh after open to populate in-memory indices.
    pile.refresh().unwrap();
    let mut iter = pile.branches().unwrap();
    assert!(iter.next().is_none());
}

#[test]
fn create_creates_parent_directories() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("nested")
        .join("dirs")
        .join("create_test.pile");

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "create", path.to_str().unwrap()])
        .assert()
        .success();

    assert!(path.exists());
    assert!(path.parent().unwrap().exists());
}

#[test]
fn put_ingests_file() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("put_test.pile");
    let input_path = dir.path().join("input.bin");
    std::fs::write(&input_path, b"hello world").unwrap();

    let digest = blake3::hash(b"hello world").to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!("^{handle}\\n$");

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
        .stdout(predicate::str::is_match(pattern).unwrap());

    let mut pile: Pile<Blake3> = Pile::open(&pile_path).unwrap();
    let reader = pile.reader().unwrap();
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
fn list_blobs_outputs_expected_handle() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("list_blobs.pile");
    let input_path = dir.path().join("input.bin");
    let contents = b"hello";
    std::fs::write(&input_path, contents).unwrap();

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!("^{handle}\\n$");

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
        .stdout(predicate::str::is_match(&pattern).unwrap());
}

#[test]
fn list_blobs_with_metadata_outputs_details() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("list_blobs_meta.pile");
    let input_path = dir.path().join("input.bin");
    let contents = b"hello";
    std::fs::write(&input_path, contents).unwrap();

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!(r"^{}\t\S+\t{}\n$", handle, contents.len());

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
        .args([
            "pile",
            "blob",
            "list",
            "--metadata",
            pile_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::is_match(&pattern).unwrap());
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
    use std::io::Seek;
    use std::io::Write;

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

    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("inspect.pile");

    let dataset = examples::dataset();
    let blob = dataset.to_blob();

    {
        let mut pile: Pile<Blake3> = Pile::open(&pile_path).unwrap();
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
        .stdout(predicate::str::is_match("^[A-F0-9]{32}\\n$").unwrap());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "branch", "list", pile_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match("^[A-F0-9]{32}\\n$").unwrap());
}
