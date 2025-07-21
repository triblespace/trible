use assert_cmd::Command;
use ed25519_dalek::SigningKey;
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
        .args(["pile", "list-branches", path.to_str().unwrap()])
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
