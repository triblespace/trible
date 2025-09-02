use anyhow::Result;
use clap::Parser;
use std::fs;
use std::path::PathBuf;

// DEFAULT_MAX_PILE_SIZE no longer required for the updated Pile API

use tribles::prelude::BlobStore;
use tribles::prelude::BlobStoreGet;
use tribles::prelude::BranchStore;

pub mod blob;
pub mod branch;

#[derive(Parser)]
pub enum PileCommand {
    /// Operations on branches stored in a pile file.
    Branch {
        #[command(subcommand)]
        cmd: branch::Command,
    },
    /// Operations on blobs stored in a pile file.
    Blob {
        #[command(subcommand)]
        cmd: blob::Command,
    },
    /// Create a new empty pile file.
    ///
    /// This is mainly a cross-platform convenience; a plain `touch` on
    /// Unix-like systems achieves the same result.
    Create {
        /// Path to the pile file to create
        path: PathBuf,
    },
    /// Run diagnostics and repair checks on a pile file.
    Diagnose {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Exit non-zero at the first detected issue
        #[arg(long)]
        fail_fast: bool,
    },
}

pub fn run(cmd: PileCommand) -> Result<()> {
    match cmd {
        PileCommand::Branch { cmd } => branch::run(cmd),
        PileCommand::Blob { cmd } => blob::run(cmd),
        PileCommand::Create { path } => {
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;

            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }

            let mut pile: Pile<Blake3> = Pile::open(&path)?;
            pile.flush().map_err(|e| anyhow::anyhow!("{e:?}"))?;
            Ok(())
        }
        PileCommand::Diagnose { pile, fail_fast } => {
            use tribles::prelude::blobschemas::SimpleArchive;

            use tribles::repo::pile::Pile;
            use tribles::repo::pile::ReadError;
            use tribles::trible::TribleSet;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use tribles::value::schemas::hash::Hash;
            use tribles::value::Value;

            match Pile::<Blake3>::open(&pile) {
                Ok(mut pile) => {
                    let res = (|| -> Result<(), anyhow::Error> {
                        let mut any_error = false;
                        let reader = pile
                            .reader()
                            .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                        let mut invalid = 0usize;
                        let mut total = 0usize;
                        for item in reader.iter() {
                            match item {
                                Ok((handle, blob)) => {
                                    total += 1;
                                    let expected: tribles::value::Value<Hash<Blake3>> =
                                        Handle::to_hash(handle);
                                    let computed = Hash::<Blake3>::digest(&blob.bytes);
                                    if expected != computed {
                                        invalid += 1;
                                    }
                                }
                                Err(_) => {
                                    // Treat iterator errors (validation, missing index) as invalid blobs.
                                    total += 1;
                                    invalid += 1;
                                }
                            }
                        }

                        if invalid == 0 {
                            println!("Pile appears healthy");
                        } else {
                            println!(
                                "Pile corrupt: {invalid} of {total} blobs have incorrect hashes"
                            );
                            if fail_fast {
                                anyhow::bail!("invalid blob hashes detected");
                            }
                            any_error = true;
                        }

                        // Branch integrity diagnostics
                        println!("\nBranches:");
                        let _repo_branch_attr: tribles::id::Id =
                            tribles::id_hex!("8694CC73AF96A5E1C7635C677D1B928A");
                        let repo_head_attr: tribles::id::Id =
                            tribles::id_hex!("272FBC56108F336C4D2E17289468C35F");
                        let repo_parent_attr: tribles::id::Id =
                            tribles::id_hex!("317044B612C690000D798CA660ECFD2A");
                        let repo_content_attr: tribles::id::Id =
                            tribles::id_hex!("4DD4DDD05CC31734B03ABB4E43188B1F");

                        fn verify_chain(
                            reader: &tribles::repo::pile::PileReader<Blake3>,
                            start: Value<Handle<Blake3, SimpleArchive>>,
                            repo_parent_attr: tribles::id::Id,
                            repo_content_attr: tribles::id::Id,
                        ) -> (usize, Option<String>) {
                            use std::collections::BTreeSet;
                            let mut visited: BTreeSet<String> = BTreeSet::new();
                            let mut stack: Vec<Value<Handle<Blake3, SimpleArchive>>> = vec![start];
                            let mut count = 0usize;
                            while let Some(h) = stack.pop() {
                                let hh: Value<Hash<Blake3>> = Handle::to_hash(h);
                                let hex: String = hh.from_value();
                                if !visited.insert(hex.clone()) {
                                    continue;
                                }
                                if reader.metadata(h).is_none() {
                                    return (count, Some(format!("commit blake3:{hex} missing")));
                                }
                                let meta: TribleSet =
                                    match reader.get::<TribleSet, SimpleArchive>(h) {
                                        Ok(m) => m,
                                        Err(e) => {
                                            return (
                                                count,
                                                Some(format!(
                                                    "commit blake3:{hex} decode failed: {e:?}"
                                                )),
                                            )
                                        }
                                    };
                                let mut content_ok = false;
                                let mut parents: Vec<Value<Handle<Blake3, SimpleArchive>>> =
                                    Vec::new();
                                for t in meta.iter() {
                                    if t.a() == &repo_content_attr {
                                        let c = *t.v::<Handle<Blake3, SimpleArchive>>();
                                        if reader.metadata(c).is_some() {
                                            content_ok = true;
                                        }
                                    } else if t.a() == &repo_parent_attr {
                                        parents.push(*t.v::<Handle<Blake3, SimpleArchive>>());
                                    }
                                }
                                if !content_ok {
                                    return (
                                        count,
                                        Some(format!("commit blake3:{hex} content blob missing")),
                                    );
                                }
                                for p in parents {
                                    stack.push(p);
                                }
                                count += 1;
                            }
                            (count, None)
                        }

                        // Ensure in-memory indices are loaded before enumerating branches.
                        pile.refresh()?;
                        let mut iter = pile.branches()?;
                        while let Some(r) = iter.next() {
                            let bid = r?;
                            let meta_handle_opt = pile.head(bid)?;
                            let id_hex = format!("{bid:X}");
                            match meta_handle_opt {
                                None => {
                                    println!("- {id_hex}: no branch metadata head set");
                                    if fail_fast {
                                        anyhow::bail!("no branch metadata head set for {id_hex}");
                                    }
                                    any_error = true;
                                }
                                Some(meta_handle) => {
                                    let meta_present = reader.metadata(meta_handle).is_some();
                                    let mut name_val: Option<String> = None;
                                    let mut head_val: Option<Value<Handle<Blake3, SimpleArchive>>> =
                                        None;
                                    let mut meta_err: Option<String> = None;
                                    if meta_present {
                                        match reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                                            Ok(meta) => {
                                                for t in meta.iter() {
                                                    if t.a() == &tribles::metadata::ATTR_NAME {
                                                        let n: Value<
                                                            tribles::value::schemas::shortstring::ShortString,
                                                        > = *t.v();
                                                        name_val = Some(n.from_value());
                                                    } else if t.a() == &repo_head_attr {
                                                        head_val = Some(*t.v::<Handle<
                                                            Blake3,
                                                            SimpleArchive,
                                                        >>(
                                                        ));
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                meta_err = Some(format!("decode failed: {e:?}"));
                                            }
                                        }
                                    }
                                    let meta_hash: Value<Hash<Blake3>> =
                                        Handle::to_hash(meta_handle);
                                    let meta_hex: String = meta_hash.from_value();
                                    if let Some(n) = name_val.as_ref() {
                                        println!(
                                            "- {id_hex} ({n}): meta blake3:{meta_hex} [{}]{}",
                                            if meta_present { "present" } else { "missing" },
                                            meta_err
                                                .as_deref()
                                                .map(|e| format!(" ({e})"))
                                                .unwrap_or_default()
                                        );
                                    } else {
                                        println!(
                                            "- {id_hex}: meta blake3:{meta_hex} [{}]{}",
                                            if meta_present { "present" } else { "missing" },
                                            meta_err
                                                .as_deref()
                                                .map(|e| format!(" ({e})"))
                                                .unwrap_or_default()
                                        );
                                    }
                                    if let Some(head) = head_val {
                                        let (count, err) = verify_chain(
                                            &reader,
                                            head,
                                            repo_parent_attr,
                                            repo_content_attr,
                                        );
                                        if let Some(e) = err {
                                            println!("  commit chain error: {e}");
                                            if fail_fast {
                                                anyhow::bail!(e);
                                            }
                                            any_error = true;
                                        } else {
                                            println!("  commit chain: {count} commits");
                                        }
                                    } else {
                                        println!("  no head set");
                                        if fail_fast {
                                            anyhow::bail!("no head set for {id_hex}");
                                        }
                                        any_error = true;
                                    }
                                }
                            }
                        }

                        if any_error {
                            anyhow::bail!("diagnostics reported issues");
                        }

                        Ok(())
                    })();

                    let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
                    res.and(close_res)?;
                }
                Err(ReadError::IoError(err)) if err.kind() == std::io::ErrorKind::NotFound => {
                    anyhow::bail!("pile not found");
                }
                Err(e) => return Err(e.into()),
            }
            Ok(())
        }
    }
}
