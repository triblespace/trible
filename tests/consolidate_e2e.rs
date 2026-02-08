use assert_cmd::Command;
use ed25519_dalek::SigningKey;
use std::collections::HashSet;
use std::convert::TryInto;
use tempfile::tempdir;
use triblespace::prelude::blobschemas::SimpleArchive;
use triblespace::prelude::*;
use triblespace_core::id::id_hex;
use triblespace_core::metadata;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::Repository;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::Blake3;
use triblespace_core::value::schemas::hash::Handle;
use triblespace_core::value::Value;

fn random_signing_key() -> SigningKey {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed).expect("getrandom");
    SigningKey::from_bytes(&seed)
}

/// End-to-end test: create multiple branches with the same name, run the
/// consolidate command and verify the resulting merge commit parents match
/// the original branch heads.
#[test]
fn consolidate_merges_branch_heads() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("test-consolidate.pile");

    // Create a repository and three branches with the same name.
    let mut original_heads: Vec<String> = Vec::new();
    let mut branch_ids: Vec<String> = Vec::new();
    {
        let pile: Pile<Blake3> = Pile::open(&pile_path).unwrap();
        let mut repo = Repository::new(pile, random_signing_key());

        for i in 0..3 {
            let branch_id = repo.create_branch("mem", None).expect("create branch");
            branch_ids.push(format!("{:X}", *branch_id));
            let mut ws = repo.pull(*branch_id).expect("pull");
            let e = ufoid();
            let mut content = TribleSet::new();
            let label = ws.put::<blobschemas::LongString, _>(format!("branch-{i}"));
            content += entity! { &e @ metadata::name: label };
            ws.commit(content, None, Some(&format!("commit-{i}")));

            // Push and assert no conflict
            let res = repo.try_push(&mut ws).expect("push");
            assert!(res.is_none(), "unexpected push conflict");

            let head = ws.head().expect("head present");
            let hh: Value<triblespace_core::value::schemas::hash::Hash<Blake3>> =
                Handle::to_hash(head);
            original_heads.push(hh.from_value());
        }
        // repo drops here and flushes
    }

    // Write a signing key file (hex) used by the trible CLI when creating the merge commit.
    let sk = random_signing_key();
    let sk_hex = hex::encode(sk.to_bytes());
    let key_path = dir.path().join("signing.key");
    std::fs::write(&key_path, sk_hex).unwrap();

    // Run the CLI consolidate command
    let mut args: Vec<String> = vec![
        "pile".to_string(),
        "branch".to_string(),
        "consolidate".to_string(),
        pile_path.to_str().unwrap().to_string(),
    ];
    args.extend(branch_ids);
    args.extend([
        "--out-name".to_string(),
        "mem-out".to_string(),
        "--signing-key".to_string(),
        key_path.to_str().unwrap().to_string(),
    ]);

    let out = Command::cargo_bin("trible")
        .unwrap()
        .args(args)
        .output()
        .expect("run trible");

    assert!(
        out.status.success(),
        "consolidate failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);

    // Parse new branch id (32 hex chars)
    let id_hex = stdout
        .split_whitespace()
        .rev()
        .find(|tok| tok.len() == 32 && tok.chars().all(|c| c.is_ascii_hexdigit()))
        .expect("new branch id in output");

    // Open the pile and read the resulting branch metadata and commit
    let mut pile: Pile<Blake3> = Pile::open(&pile_path).unwrap();
    pile.refresh().unwrap();
    let raw = hex::decode(id_hex).unwrap();
    let raw16: [u8; 16] = raw.as_slice().try_into().unwrap();
    let bid = triblespace_core::id::Id::new(raw16).unwrap();

    let reader = pile.reader().unwrap();
    let meta_handle = pile.head(bid).unwrap().expect("new branch metadata");
    let meta: TribleSet = reader.get(meta_handle).unwrap();

    // repo head attribute id
    let repo_head_attr: triblespace_core::id::Id = id_hex!("272FBC56108F336C4D2E17289468C35F");
    let repo_parent_attr: triblespace_core::id::Id = id_hex!("317044B612C690000D798CA660ECFD2A");

    // extract the commit handle for the branch head
    let mut head_handle_opt: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
    for t in meta.iter() {
        if t.a() == &repo_head_attr {
            head_handle_opt = Some(*t.v::<Handle<Blake3, SimpleArchive>>());
            break;
        }
    }
    let head_handle = head_handle_opt.expect("branch head set");

    // read commit metadata
    let commit_meta: TribleSet = reader.get(head_handle).unwrap();

    // collect parent commits
    let mut parents: HashSet<String> = HashSet::new();
    for t in commit_meta.iter() {
        if t.a() == &repo_parent_attr {
            let p = *t.v::<Handle<Blake3, SimpleArchive>>();
            let hh: Value<triblespace_core::value::schemas::hash::Hash<Blake3>> =
                Handle::to_hash(p);
            parents.insert(hh.from_value());
        }
    }

    // original_heads may contain duplicates if some branches had no head; use set
    let orig_set: HashSet<String> = original_heads.into_iter().collect();
    assert_eq!(
        parents, orig_set,
        "parents of merge commit do not match original heads"
    );
}
