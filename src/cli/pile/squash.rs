use anyhow::{anyhow, Result};
use std::path::PathBuf;

use triblespace::prelude::*;
use triblespace_core::blob::schemas::UnknownBlob;
use triblespace_core::blob::Blob;
use triblespace_core::repo;
use triblespace_core::repo::pile::Pile;
use triblespace_core::value::schemas::hash::Blake3;
use triblespace_core::value::schemas::hash::Handle;
use triblespace_core::value::Value;

use super::signing::load_signing_key;

pub fn run(source: PathBuf, dest: PathBuf, signing_key: Option<PathBuf>) -> Result<()> {
    let key = load_signing_key(&signing_key)?;

    // Open source pile.
    let mut src_pile: Pile<Blake3> = Pile::open(&source)?;
    src_pile.restore().map_err(|e| anyhow!("restore source: {e:?}"))?;

    // Enumerate branches from source.
    let branch_ids: Vec<Id> = src_pile
        .branches()
        .map_err(|e| anyhow!("branches: {e:?}"))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| anyhow!("branch iter: {e:?}"))?;

    // Create source repository for checkout.
    let mut src_repo = Repository::new(src_pile, key.clone(), TribleSet::new())
        .map_err(|e| anyhow!("source repo: {e:?}"))?;

    // Resolve branch names and checkout data from source.
    struct BranchData {
        name: String,
        data: TribleSet,
        metadata: TribleSet,
    }

    let mut branches: Vec<BranchData> = Vec::new();

    for &bid in &branch_ids {
        let mut ws = match src_repo.pull(bid) {
            Ok(ws) => ws,
            Err(e) => {
                eprintln!("skip {bid:X}: pull failed: {e:?}");
                continue;
            }
        };

        // Get branch name from branch metadata.
        let name = (|| -> Option<String> {
            let meta_handle = src_repo.storage_mut().head(bid).ok()??;
            let reader = src_repo.storage_mut().reader().ok()?;
            let meta: TribleSet = reader.get(meta_handle).ok()?;
            let name_attr = triblespace_core::metadata::name.id();
            for t in meta.iter() {
                if *t.a() == name_attr {
                    let handle: Value<Handle<Blake3, triblespace_core::blob::schemas::longstring::LongString>> =
                        Value::new(t.data[32..64].try_into().unwrap());
                    let name_view: View<str> = reader.get(handle).ok()?;
                    return Some(name_view.to_string());
                }
            }
            None
        })()
        .unwrap_or_else(|| format!("{bid:x}"));

        // Checkout data + metadata.
        let (data, metadata) = match ws.checkout_with_metadata(..) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("skip {name}: checkout failed: {e:?}");
                continue;
            }
        };

        if data.is_empty() {
            println!("skip {name}: empty");
            continue;
        }

        println!(
            "read {name}: {} data tribles, {} metadata tribles",
            data.len(),
            metadata.len()
        );
        branches.push(BranchData {
            name,
            data,
            metadata,
        });
    }

    // Get source reader for blob transfer.
    let src_reader = src_repo
        .storage_mut()
        .reader()
        .map_err(|e| anyhow!("source reader: {e:?}"))?;

    // Create destination pile (refuse to overwrite).
    if dest.exists() && std::fs::metadata(&dest)?.len() > 0 {
        return Err(anyhow!(
            "destination {} already exists — refusing to overwrite",
            dest.display()
        ));
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::File::create(&dest)?;
    let mut dst_pile: Pile<Blake3> = Pile::open(&dest)?;

    // For each branch: transfer referenced blobs, create squashed commit.
    let mut total_blobs = 0usize;
    let mut total_branches = 0usize;

    for branch in &branches {
        // Collect all potential blob handles from data + metadata values.
        let mut roots: Vec<Value<Handle<Blake3, UnknownBlob>>> = Vec::new();
        for trible in branch.data.iter() {
            let raw: [u8; 32] = trible.data[32..64].try_into().unwrap();
            roots.push(Value::<Handle<Blake3, UnknownBlob>>::new(raw));
        }
        for trible in branch.metadata.iter() {
            let raw: [u8; 32] = trible.data[32..64].try_into().unwrap();
            roots.push(Value::<Handle<Blake3, UnknownBlob>>::new(raw));
        }

        // Transfer reachable blobs from source to dest.
        let reachable = repo::reachable(&src_reader, roots);
        let mut branch_blobs = 0usize;
        for r in repo::transfer(&src_reader, &mut dst_pile, reachable) {
            match r {
                Ok(_) => branch_blobs += 1,
                Err(_) => {} // Non-blob values tried as handles — expected, ignore.
            }
        }

        // Create repository on dest for this branch.
        // We need a fresh repo each time because create_branch needs it.
        let mut dst_repo = Repository::new(dst_pile, key.clone(), TribleSet::new())
            .map_err(|e| anyhow!("dest repo: {e:?}"))?;

        let branch_id = dst_repo
            .create_branch(&branch.name, None)
            .map_err(|e| anyhow!("create branch '{}': {e:?}", branch.name))?;
        let mut ws = dst_repo
            .pull(*branch_id)
            .map_err(|e| anyhow!("pull dest branch: {e:?}"))?;

        if branch.metadata.is_empty() {
            ws.commit(branch.data.clone(), &format!("squashed {}", branch.name));
        } else {
            let meta_handle = ws.put(branch.metadata.clone().to_blob());
            ws.commit_with_metadata(
                branch.data.clone(),
                meta_handle,
                &format!("squashed {}", branch.name),
            );
        }

        dst_repo
            .push(&mut ws)
            .map_err(|e| anyhow!("push '{}': {e:?}", branch.name))?;

        println!(
            "wrote {}: {} data, {} metadata, {} blobs",
            branch.name,
            branch.data.len(),
            branch.metadata.len(),
            branch_blobs,
        );

        total_blobs += branch_blobs;
        total_branches += 1;

        // Take the pile back out of the repo for the next iteration.
        dst_pile = dst_repo.into_storage();
    }

    dst_pile.close().map_err(|e| anyhow!("close dest: {e:?}"))?;
    src_repo.close().map_err(|e| anyhow!("close source: {e:?}"))?;

    let src_size = std::fs::metadata(&source)?.len();
    let dst_size = std::fs::metadata(&dest)?.len();
    println!(
        "\nSquashed {} branches, {} blobs",
        total_branches, total_blobs,
    );
    println!(
        "Size: {} → {} ({:.1}%)",
        format_size(src_size),
        format_size(dst_size),
        (dst_size as f64 / src_size as f64) * 100.0,
    );

    Ok(())
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
