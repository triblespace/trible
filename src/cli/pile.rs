use anyhow::Result;
use clap::Parser;
use rand::rngs::OsRng;
use std::fs::File;
use std::path::PathBuf;

use super::util::parse_blob_handle;
use crate::DEFAULT_MAX_PILE_SIZE;
use tribles::prelude::BlobStore;
use tribles::prelude::BlobStoreGet;
use tribles::prelude::BlobStoreList;
use tribles::prelude::BlobStorePut;
use tribles::prelude::BranchStore;

#[derive(Parser)]
pub enum PileCommand {
    /// Operations on branches stored in a pile file.
    Branch {
        #[command(subcommand)]
        cmd: PileBranchCommand,
    },
    /// Operations on blobs stored in a pile file.
    Blob {
        #[command(subcommand)]
        cmd: BlobCommand,
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

#[derive(Parser)]
pub enum PileBranchCommand {
    /// List all branch identifiers in a pile file.
    List {
        /// Path to the pile file to inspect
        path: PathBuf,
    },
    /// Create a new branch in a pile file.
    Create {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Name of the branch to create
        name: String,
    },
}

#[derive(Parser)]
pub enum BlobCommand {
    /// List all blob handles stored in a pile file.
    List {
        /// Path to the pile file to inspect
        path: PathBuf,
        /// Show creation time and size for each blob
        #[arg(long)]
        metadata: bool,
    },
    /// Ingest a file into a pile, creating the pile if necessary.
    Put {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// File whose contents should be stored in the pile
        file: PathBuf,
    },
    /// Extract a blob from a pile by its handle.
    Get {
        /// Path to the pile file to read
        pile: PathBuf,
        /// Handle of the blob to retrieve (e.g. "blake3:HEX...")
        handle: String,
        /// Destination file path for the extracted blob
        output: PathBuf,
    },
    /// Inspect a blob and print basic metadata.
    Inspect {
        /// Path to the pile file to read
        pile: PathBuf,
        /// Handle of the blob to inspect (e.g. "blake3:HEX...")
        handle: String,
    },
}

pub fn run(cmd: PileCommand) -> Result<()> {
    match cmd {
        PileCommand::Branch { cmd } => match cmd {
            PileBranchCommand::List { path } => {
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::Blake3;

                let pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&path)?;

                for branch in pile.branches() {
                    let id = branch?;
                    println!("{id:X}");
                }
            }
            PileBranchCommand::Create { pile, name } => {
                use ed25519_dalek::SigningKey;
                use tribles::repo::pile::Pile;
                use tribles::repo::Repository;
                use tribles::value::schemas::hash::Blake3;

                let pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;
                let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));
                let ws = repo.branch(&name).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                println!("{:#X}", ws.branch_id());
            }
        },
        PileCommand::Blob { cmd } => match cmd {
            BlobCommand::List { path, metadata } => {
                use chrono::DateTime;
                use chrono::Utc;
                use std::time::Duration;
                use std::time::UNIX_EPOCH;

                use tribles::blob::schemas::UnknownBlob;
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::Blake3;
                use tribles::value::schemas::hash::Handle;
                use tribles::value::schemas::hash::Hash;

                let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&path)?;
                let reader = pile.reader();
                for handle in reader.blobs() {
                    let handle: tribles::value::Value<Handle<Blake3, UnknownBlob>> = handle?;
                    let hash: tribles::value::Value<Hash<Blake3>> = Handle::to_hash(handle);
                    let string: String = hash.from_value();
                    if metadata {
                        if let Some(meta) = reader.metadata(handle) {
                            let dt = UNIX_EPOCH + Duration::from_millis(meta.timestamp);
                            let time: DateTime<Utc> = DateTime::<Utc>::from(dt);
                            println!("{}\t{}\t{}", string, time.to_rfc3339(), meta.length);
                        } else {
                            println!("{string}");
                        }
                    } else {
                        println!("{string}");
                    }
                }
            }
            BlobCommand::Put { pile, file } => {
                use tribles::blob::schemas::UnknownBlob;
                use tribles::blob::Bytes;
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::Blake3;
                use tribles::value::schemas::hash::Handle;
                use tribles::value::schemas::hash::Hash;

                let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;
                let file_handle = File::open(&file)?;
                let bytes = unsafe { Bytes::map_file(&file_handle)? };
                let handle = pile.put::<UnknownBlob, _>(bytes)?;
                let hash: tribles::value::Value<Hash<Blake3>> = Handle::to_hash(handle);
                let string: String = hash.from_value();
                println!("{string}");
                pile.flush().map_err(|e| anyhow::anyhow!("{e:?}"))?;
            }
            BlobCommand::Get {
                pile,
                handle,
                output,
            } => {
                use std::io::Write;

                use tribles::blob::schemas::UnknownBlob;
                use tribles::blob::Bytes;
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::Blake3;
                use tribles::value::schemas::hash::Handle;

                let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;
                let hash_val = parse_blob_handle(&handle)?;
                let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> =
                    hash_val.into();
                let reader = pile.reader();
                let bytes: Bytes = reader.get(handle_val)?;
                let mut file = File::create(&output)?;
                file.write_all(&bytes)?;
            }
            BlobCommand::Inspect { pile, handle } => {
                use chrono::DateTime;
                use chrono::Utc;
                use file_type::FileType;
                use std::time::Duration;
                use std::time::UNIX_EPOCH;

                use tribles::blob::schemas::UnknownBlob;
                use tribles::blob::Blob;
                use tribles::repo::pile::BlobMetadata;
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::Blake3;
                use tribles::value::schemas::hash::Handle;

                let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;
                let hash_val = parse_blob_handle(&handle)?;
                let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> =
                    hash_val.into();
                let reader = pile.reader();
                let blob: Blob<UnknownBlob> = reader.get(handle_val)?;
                let metadata: BlobMetadata = reader
                    .metadata(handle_val)
                    .ok_or_else(|| anyhow::anyhow!("blob not found"))?;

                let dt = UNIX_EPOCH + Duration::from_millis(metadata.timestamp);
                let time: DateTime<Utc> = DateTime::<Utc>::from(dt);

                let ftype = FileType::from_bytes(&blob.bytes);
                let name = ftype.name();

                let handle_str: String = hash_val.from_value();
                println!(
                    "Hash: {handle_str}\nTime: {}\nLength: {} bytes\nType: {}",
                    time.to_rfc3339(),
                    metadata.length,
                    name
                );
            }
        },
        PileCommand::Create { path } => {
            use std::fs;
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;

            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }

            let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&path)?;
            pile.flush().map_err(|e| anyhow::anyhow!("{e:?}"))?;
        }
        PileCommand::Diagnose { pile, fail_fast } => {
            use tribles::prelude::blobschemas::SimpleArchive;
            use tribles::prelude::Id;
            use tribles::repo::pile::OpenError;
            use tribles::repo::pile::Pile;
            use tribles::trible::TribleSet;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use tribles::value::schemas::hash::Hash;
            use tribles::value::Value;

            match Pile::<DEFAULT_MAX_PILE_SIZE, Blake3>::try_open(&pile) {
                Ok(mut pile) => {
                    let mut any_error = false;
                    let reader = pile.reader();
                    let mut invalid = 0usize;
                    let mut total = 0usize;
                    for (handle, blob) in reader.iter() {
                        total += 1;
                        let expected: tribles::value::Value<Hash<Blake3>> = Handle::to_hash(handle);
                        let computed = Hash::<Blake3>::digest(&blob.bytes);
                        if expected != computed {
                            invalid += 1;
                        }
                    }

                    if invalid == 0 {
                        println!("Pile appears healthy");
                    } else {
                        println!("Pile corrupt: {invalid} of {total} blobs have incorrect hashes");
                        if fail_fast {
                            anyhow::bail!("invalid blob hashes detected");
                        }
                        any_error = true;
                    }

                    // Branch integrity diagnostics
                    println!("\nBranches:");
                    let repo_branch_attr: tribles::id::Id =
                        tribles::id_hex!("8694CC73AF96A5E1C7635C677D1B928A");
                    let repo_head_attr: tribles::id::Id =
                        tribles::id_hex!("272FBC56108F336C4D2E17289468C35F");
                    let repo_parent_attr: tribles::id::Id =
                        tribles::id_hex!("317044B612C690000D798CA660ECFD2A");
                    let repo_content_attr: tribles::id::Id =
                        tribles::id_hex!("4DD4DDD05CC31734B03ABB4E43188B1F");

                    // Helper: verify the entire commit DAG reachable from head.
                    // Returns (visited_commits, error). If error is Some, it contains
                    // the first failure encountered.
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
                                continue; // already verified
                            }
                            // Commit blob present?
                            if reader.metadata(h).is_none() {
                                return (count, Some(format!("commit blake3:{hex} missing")));
                            }
                            // Decode commit metadata
                            let meta: TribleSet = match reader.get::<TribleSet, SimpleArchive>(h) {
                                Ok(m) => m,
                                Err(e) => {
                                    return (
                                        count,
                                        Some(format!("commit blake3:{hex} decode failed: {e:?}")),
                                    )
                                }
                            };
                            // Check content exists; collect all parents
                            let mut content_ok = false;
                            let mut parents: Vec<Value<Handle<Blake3, SimpleArchive>>> = Vec::new();
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
                            // enqueue parents (DFS)
                            for p in parents {
                                stack.push(p);
                            }
                            count += 1;
                        }
                        (count, None)
                    }

                    for r in pile.branches() {
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
                                                    let n: Value<tribles::value::schemas::shortstring::ShortString> = *t.v();
                                                    name_val = Some(n.from_value());
                                                } else if t.a() == &repo_head_attr {
                                                    head_val =
                                                        Some(
                                                            *t.v::<Handle<Blake3, SimpleArchive>>(),
                                                        );
                                                }
                                            }
                                        }
                                        Err(e) => meta_err = Some(format!("decode failed: {e:?}")),
                                    }
                                }
                                let meta_hash: Value<Hash<Blake3>> = Handle::to_hash(meta_handle);
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

                                if !meta_present || meta_err.is_some() {
                                    if fail_fast {
                                        anyhow::bail!(
                                            "branch {id_hex}: metadata missing or undecodable"
                                        );
                                    }
                                    any_error = true;
                                }

                                // If head missing or undecodable, try to find a last recoverable head from history
                                let mut head_present = false;
                                if let Some(h) = head_val.clone() {
                                    head_present = reader.metadata(h).is_some();
                                }
                                let mut proposed_recoverable: Option<(
                                    Value<Handle<Blake3, SimpleArchive>>,
                                    u64,
                                    String,
                                    usize,
                                )> = None;
                                if !head_present && !fail_fast {
                                    // Build list of candidate (timestamp, meta_handle, head) for this branch
                                    let mut candidates: Vec<(
                                        u64,
                                        Value<Handle<Blake3, SimpleArchive>>,
                                        Value<Handle<Blake3, SimpleArchive>>,
                                    )> = Vec::new();
                                    for (h, _blob) in reader.iter() {
                                        let sah: Value<Handle<Blake3, SimpleArchive>> =
                                            h.transmute();
                                        let Some(md) = reader.metadata(sah) else {
                                            continue;
                                        };
                                        let Ok(meta): Result<TribleSet, _> =
                                            reader.get::<TribleSet, SimpleArchive>(sah)
                                        else {
                                            continue;
                                        };
                                        let mut is_for_branch = false;
                                        let mut hhead: Option<
                                            Value<Handle<Blake3, SimpleArchive>>,
                                        > = None;
                                        for t in meta.iter() {
                                            if t.a() == &repo_branch_attr {
                                                let v: Value<
                                                    tribles::prelude::valueschemas::GenId,
                                                > = *t.v();
                                                if let Ok(id) =
                                                    v.try_from_value::<tribles::id::Id>()
                                                {
                                                    if id == bid {
                                                        is_for_branch = true;
                                                    }
                                                }
                                            } else if t.a() == &repo_head_attr {
                                                hhead =
                                                    Some(*t.v::<Handle<Blake3, SimpleArchive>>());
                                            }
                                        }
                                        if !is_for_branch {
                                            continue;
                                        }
                                        if let Some(hh) = hhead {
                                            if reader.metadata(hh).is_some() {
                                                candidates.push((md.timestamp, sah, hh));
                                            }
                                        }
                                    }
                                    // Newest first
                                    candidates.sort_by(|a, b| b.0.cmp(&a.0));
                                    // Verify each candidate head until we find one with an intact DAG
                                    for (ts, meta_sah, hh) in candidates.into_iter() {
                                        let (depth, err) = verify_chain(
                                            &reader,
                                            hh,
                                            repo_parent_attr,
                                            repo_content_attr,
                                        );
                                        if err.is_none() {
                                            let meta_hash: Value<Hash<Blake3>> =
                                                Handle::to_hash(meta_sah);
                                            let meta_hex: String = meta_hash.from_value();
                                            proposed_recoverable = Some((hh, ts, meta_hex, depth));
                                            break;
                                        }
                                    }
                                }

                                if let Some(h) = head_val.clone().or_else(|| {
                                    proposed_recoverable.as_ref().map(|(h, _, _, _)| *h)
                                }) {
                                    // Verify entire commit DAG reachable from head
                                    let (depth, err) = verify_chain(
                                        &reader,
                                        h,
                                        repo_parent_attr,
                                        repo_content_attr,
                                    );
                                    if let Some(err) = err {
                                        println!("  chain: broken after {depth} commits: {err}");
                                        if fail_fast {
                                            anyhow::bail!("branch {id_hex}: {err}");
                                        }
                                        any_error = true;
                                    } else {
                                        println!("  chain: ok ({depth} commits)");
                                    }
                                }

                                if head_present {
                                    let head_hash: Value<Hash<Blake3>> =
                                        Handle::to_hash(head_val.unwrap());
                                    let head_hex: String = head_hash.from_value();
                                    println!("  head: blake3:{head_hex} [present]");
                                } else if let Some((h, _ts, meta_hex2, depth_ok)) =
                                    proposed_recoverable
                                {
                                    let head_hash: Value<Hash<Blake3>> = Handle::to_hash(h);
                                    let head_hex: String = head_hash.from_value();
                                    println!("  proposed_recoverable_head: meta blake3:{meta_hex2} -> head blake3:{head_hex} (chain ok: {depth_ok} commits)");
                                    any_error = true;
                                } else {
                                    println!("  head: missing and no recoverable history found");
                                    any_error = true;
                                }
                            }
                        }
                    }

                    if any_error {
                        anyhow::bail!("diagnose encountered issues");
                    }
                }
                Err(OpenError::CorruptPile { valid_length }) => {
                    println!("Pile corrupt, valid portion: {valid_length} bytes");
                    anyhow::bail!("pile corruption detected");
                }
                Err(err) => return Err(anyhow::anyhow!("{err:?}")),
            }
        }
    }
    Ok(())
}
