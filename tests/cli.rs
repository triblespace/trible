use assert_cmd::Command;
use ed25519_dalek::SigningKey;
use predicates::prelude::*;
use rand::rngs::OsRng;
use tempfile::tempdir;
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
