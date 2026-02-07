use anyhow::Result;
use clap::Parser;
use std::convert::TryInto;
use std::path::PathBuf;

// DEFAULT_MAX_PILE_SIZE removed; the new Pile API no longer uses a size const generic

use triblespace::prelude::BlobStore;
use triblespace::prelude::BlobStoreGet;
use triblespace::prelude::BranchStore;
use triblespace::prelude::View;
use triblespace_core::blob::schemas::longstring::LongString;
use triblespace_core::blob::schemas::simplearchive::SimpleArchive;
use triblespace_core::blob::ToBlob;
use triblespace_core::id::id_hex;
use triblespace_core::id::Id;
use triblespace_core::repo::pile::{Pile, PileReader};
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::{Blake3, Handle};
use triblespace_core::value::Value;

use super::signing::load_signing_key;
use triblespace_core::repo::BlobStoreMeta;

type BranchNameHandle = Value<Handle<Blake3, LongString>>;

#[derive(Parser)]
pub enum Command {
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
        /// Optional signing key path. The file should contain a 64-char hex seed.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Inspect a branch in a pile and print its id, name, and current head handle.
    Inspect {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier to inspect (hex encoded). Mutually exclusive with --name
        #[arg(long, conflicts_with = "name")]
        id: Option<String>,
        /// Branch name to inspect. Mutually exclusive with --id
        #[arg(long, conflicts_with = "id")]
        name: Option<String>,
    },
    /// Scan the pile for historical branch metadata entries for this branch.
    /// This lists candidate metadata blobs that reference the branch id.
    History {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier to inspect (hex encoded). Mutually exclusive with --name
        #[arg(long, conflicts_with = "name")]
        id: Option<String>,
        /// Branch name to inspect. Mutually exclusive with --id
        #[arg(long, conflicts_with = "id")]
        name: Option<String>,
        /// Maximum results to print
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Export a branch from one pile into another, copying reachable blobs.
    ///
    /// This transfers all blobs reachable from the source branch metadata into
    /// the destination pile and sets the destination branch head to the same
    /// branch metadata handle (preserving the branch id).
    Export {
        /// Path to the source pile file
        #[arg(long)]
        from_pile: PathBuf,
        /// Branch identifier to export (hex encoded)
        #[arg(long)]
        branch: String,
        /// Path to the destination pile file
        #[arg(long)]
        to_pile: PathBuf,
        /// Optional signing key path for destination repo operations.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Show statistics for a branch: commits, triples, entities, attributes.
    Stats {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier to inspect (hex encoded). Mutually exclusive with --name
        #[arg(long, conflicts_with = "name")]
        id: Option<String>,
        /// Branch name to inspect. Mutually exclusive with --id
        #[arg(long, conflicts_with = "id")]
        name: Option<String>,
    },
    /// Import reachable blobs from a source branch into a target pile and
    /// attach them to the target branch via a single merge commit.
    MergeImport {
        /// Path to the source pile file
        #[arg(long)]
        from_pile: PathBuf,
        /// Source branch identifier (hex). Mutually exclusive with --from-name
        #[arg(long, conflicts_with = "from_name")]
        from_id: Option<String>,
        /// Source branch name. Mutually exclusive with --from-id
        #[arg(long, conflicts_with = "from_id")]
        from_name: Option<String>,

        /// Path to the destination pile file
        #[arg(long)]
        to_pile: PathBuf,
        /// Destination branch identifier (hex). Mutually exclusive with --to-name
        #[arg(long, conflicts_with = "to_name")]
        to_id: Option<String>,
        /// Destination branch name. Mutually exclusive with --to-id
        #[arg(long, conflicts_with = "to_id")]
        to_name: Option<String>,
        /// Optional signing key path. The file should contain a 64-char hex seed.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Consolidate multiple branches with the same name into a single new branch.
    Consolidate {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Branch name to consolidate
        name: String,
        /// Optional name for the newly created consolidated branch
        #[arg(long)]
        out_name: Option<String>,
        /// Dry run: show what would be done without making changes
        #[arg(long)]
        dry_run: bool,
        /// Optional signing key path. The file should contain a 64-char hex seed.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::List { path } => {
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::value::schemas::hash::Blake3;

            let mut pile: Pile<Blake3> = Pile::open(&path)?;
            let res = (|| -> Result<(), anyhow::Error> {
                // Refresh in-memory indices from the file so branches() reflects current state.
                pile.refresh()?;

                let iter = pile.branches()?;
                for branch in iter {
                    let id = branch?;
                    println!("{id:X}");
                }
                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Create {
            pile,
            name,
            signing_key,
        } => {
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::repo::Repository;
            use triblespace_core::value::schemas::hash::Blake3;
            let pile: Pile<Blake3> = Pile::open(&pile)?;
            let key = load_signing_key(&signing_key)?;
            let mut repo = Repository::new(pile, key);
            let branch_id = repo
                .create_branch(&name, None)
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            println!("{:#X}", *branch_id);
            // Ensure the underlying pile is closed and errors are surfaced.
            repo.into_storage()
                .close()
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        }
        Command::Inspect { pile, id, name } => {
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace::prelude::valueschemas::Handle;
            use triblespace_core::id::Id;

            use triblespace_core::repo::pile::Pile;
            use triblespace_core::trible::TribleSet;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::schemas::hash::Hash;
            use triblespace_core::value::Value;

            let mut pile: Pile<Blake3> = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                let branch_id: Id = if let Some(id_hex) = id {
                    let raw = hex::decode(id_hex)?;
                    let raw: [u8; 16] = raw.as_slice().try_into()?;
                    Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?
                } else if let Some(name) = name {
                    let reader = pile
                        .reader()
                        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                    find_branch_by_name(&mut pile, &reader, &name)?
                } else {
                    anyhow::bail!("provide either --id HEX or --name NAME");
                };

                let meta_handle = pile
                    .head(branch_id)?
                    .ok_or_else(|| anyhow::anyhow!("branch not found"))?;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                let meta_present = reader.metadata(meta_handle)?.is_some();
                let (name_val, head_val, head_err): (
                    Option<String>,
                    Option<Value<Handle<Blake3, SimpleArchive>>>,
                    Option<String>,
                ) = if meta_present {
                    match reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                        Ok(meta) => {
                            let mut head_val: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
                            let repo_head_attr: triblespace_core::id::Id =
                                id_hex!("272FBC56108F336C4D2E17289468C35F");
                            for t in meta.iter() {
                                if t.a() == &repo_head_attr {
                                    let h = *t.v::<Handle<Blake3, SimpleArchive>>();
                                    head_val = Some(h);
                                }
                            }
                            let name_val = load_branch_name(&reader, &meta)?;
                            (name_val, head_val, None)
                        }
                        Err(e) => (None, None, Some(format!("decode failed: {e:?}"))),
                    }
                } else {
                    (None, None, None)
                };

                let id_hex = format!("{branch_id:X}");
                let meta_hash: Value<Hash<Blake3>> = Handle::to_hash(meta_handle);
                let meta_hex: String = meta_hash.from_value();

                println!("Id:        {id_hex}");
                if let Some(nstr) = name_val.clone() {
                    println!("Name:      {nstr}");
                }
                println!(
                    "Meta:      blake3:{meta_hex} [{}]{}",
                    if meta_present { "present" } else { "missing" },
                    head_err
                        .as_deref()
                        .map(|e| format!(" ({e})"))
                        .unwrap_or_default()
                );
                if let Some(h) = head_val {
                    let head_hash: Value<Hash<Blake3>> = Handle::to_hash(h);
                    let head_hex: String = head_hash.from_value();
                    let present = reader.metadata(h)?.is_some();
                    println!(
                        "Head:      blake3:{head_hex} [{}]",
                        if present { "present" } else { "missing" }
                    );
                }
                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::History {
            pile,
            id,
            name,
            limit,
        } => {
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace::prelude::valueschemas::Handle;
            use triblespace_core::blob::schemas::UnknownBlob;
            use triblespace_core::id::Id;

            use triblespace_core::repo::pile::Pile;
            use triblespace_core::trible::TribleSet;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::schemas::hash::Hash;
            use triblespace_core::value::Value;

            let mut pile: Pile<Blake3> = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                // Ensure indices are loaded before scanning
                pile.refresh()?;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                let branch_id: Id = if let Some(id_hex) = id {
                    let raw = hex::decode(id_hex)?;
                    let raw: [u8; 16] = raw.as_slice().try_into()?;
                    Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?
                } else if let Some(name) = name {
                    find_branch_by_name(&mut pile, &reader, &name)?
                } else {
                    anyhow::bail!("provide either --id HEX or --name NAME");
                };

                let repo_branch_attr: triblespace_core::id::Id =
                    id_hex!("8694CC73AF96A5E1C7635C677D1B928A");
                let repo_head_attr: triblespace_core::id::Id =
                    id_hex!("272FBC56108F336C4D2E17289468C35F");

                let mut printed = 0usize;
                for item in reader.iter() {
                    let (handle, _blob) = item.expect("infallible iteration");
                    let handle: Value<Handle<Blake3, UnknownBlob>> = handle;
                    let sah: Value<Handle<Blake3, SimpleArchive>> = handle.transmute();
                    let Ok(meta): Result<TribleSet, _> =
                        reader.get::<TribleSet, SimpleArchive>(sah)
                    else {
                        continue;
                    };
                    let mut is_meta_for_branch = false;
                    let mut head_val: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
                    for t in meta.iter() {
                        if t.a() == &repo_branch_attr {
                            let v: Value<triblespace::prelude::valueschemas::GenId> = *t.v();
                            if let Ok(id) = v.try_from_value::<triblespace_core::id::Id>() {
                                if id == branch_id {
                                    is_meta_for_branch = true;
                                }
                            }
                        } else if t.a() == &repo_head_attr {
                            head_val = Some(*t.v::<Handle<Blake3, SimpleArchive>>());
                        }
                    }
                    if !is_meta_for_branch {
                        continue;
                    }
                    let meta_hash: Value<Hash<Blake3>> = Handle::to_hash(sah);
                    let meta_hex: String = meta_hash.from_value();
                    if let Some(h) = head_val {
                        let head_hash: Value<Hash<Blake3>> = Handle::to_hash(h);
                        let head_hex: String = head_hash.from_value();
                        let present = reader.metadata(h)?.is_some();
                        println!(
                            "Meta blake3:{meta_hex}  Head blake3:{head_hex}  [{}]",
                            if present { "present" } else { "missing" }
                        );
                    } else {
                        println!("Meta blake3:{meta_hex}  Head: (unset)");
                    }
                    printed += 1;
                    if printed >= limit {
                        break;
                    }
                }
                if printed == 0 {
                    println!("No metadata entries found for this branch in pile blobs.");
                }
                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Export {
            from_pile,
            branch,
            to_pile,
            signing_key,
        } => {
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace_core::repo;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::schemas::hash::Handle;
            use triblespace_core::value::Value;

            // Parse branch id hex
            let raw = hex::decode(branch)?;
            let raw: [u8; 16] = raw.as_slice().try_into()?;
            let bid =
                triblespace_core::id::Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?;

            let mut src: Pile<Blake3> = Pile::open(&from_pile)?;
            let mut dst: Pile<Blake3> = Pile::open(&to_pile)?;

            // Obtain the source branch metadata handle (root) and ensure it exists.
            let src_meta = src
                .head(bid)?
                .ok_or_else(|| anyhow::anyhow!("source branch head not found"))?;

            // Prepare a mapping from source handle raw -> destination handle for later lookup.
            use std::collections::HashMap;
            use triblespace_core::value::VALUE_LEN;
            let mut mapping: HashMap<[u8; VALUE_LEN], Value<Handle<Blake3, _>>> = HashMap::new();

            let src_reader = src
                .reader()
                .map_err(|e| anyhow::anyhow!("src pile reader error: {e:?}"))?;
            let handles = repo::reachable(&src_reader, std::iter::once(src_meta.transmute()));

            let mut visited: usize = 0;
            let mut stored: usize = 0;
            for r in repo::transfer(&src_reader, &mut dst, handles) {
                match r {
                    Ok((src_h, dst_h)) => {
                        visited += 1;
                        stored += 1;
                        mapping.insert(src_h.raw, dst_h);
                    }
                    Err(e) => return Err(anyhow::anyhow!("transfer failed: {e}")),
                }
            }

            // Find the destination handle corresponding to the source branch meta.
            let dst_meta = mapping
                .get(&src_meta.raw)
                .ok_or_else(|| anyhow::anyhow!("destination meta handle not found after transfer"))?
                .clone();

            // Update the destination pile branch pointer to the copied meta handle.
            let old = dst.head(bid)?;
            let res = dst
                .update(bid, old, dst_meta.transmute())
                .map_err(|e| anyhow::anyhow!("destination branch update failed: {e:?}"))?;
            match res {
                triblespace_core::repo::PushResult::Success() => {
                    println!(
                        "export: copied visited={} stored={} and set branch {:#X}",
                        visited, stored, bid
                    );
                }
                triblespace_core::repo::PushResult::Conflict(existing) => {
                    println!("export: copied visited={} stored={} but branch update conflicted: existing={:?}", visited, stored, existing);
                }
            }

            // Close piles explicitly.
            src.close().map_err(|e| anyhow::anyhow!("{e:?}"))?;
            dst.close().map_err(|e| anyhow::anyhow!("{e:?}"))?;
        }
        Command::Stats { pile, id, name } => {
            use std::collections::{BTreeSet, HashSet};
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace::prelude::valueschemas::Handle;
            use triblespace_core::id::Id;

            use triblespace_core::repo::pile::Pile;
            use triblespace_core::trible::TribleSet;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::schemas::hash::Hash;
            use triblespace_core::value::Value;

            let mut pile: Pile<Blake3> = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                // Ensure indices are loaded before scanning
                pile.refresh()?;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                let branch_id: Id = if let Some(id_hex) = id {
                    let raw = hex::decode(id_hex)?;
                    let raw: [u8; 16] = raw.as_slice().try_into()?;
                    Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?
                } else if let Some(name) = name {
                    find_branch_by_name(&mut pile, &reader, &name)?
                } else {
                    anyhow::bail!("provide either --id HEX or --name NAME");
                };

                // Traversal attributes
                let repo_parent_attr: triblespace_core::id::Id =
                    id_hex!("317044B612C690000D798CA660ECFD2A");
                let repo_content_attr: triblespace_core::id::Id =
                    id_hex!("4DD4DDD05CC31734B03ABB4E43188B1F");

                // Resolve branch head
                let meta_handle = pile
                    .head(branch_id)?
                    .ok_or_else(|| anyhow::anyhow!("branch not found"))?;

                let mut head_opt: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
                if reader.metadata(meta_handle)?.is_some() {
                    if let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                        let repo_head_attr: triblespace_core::id::Id =
                            id_hex!("272FBC56108F336C4D2E17289468C35F");
                        for t in meta.iter() {
                            if t.a() == &repo_head_attr {
                                head_opt = Some(*t.v::<Handle<Blake3, SimpleArchive>>());
                                break;
                            }
                        }
                    }
                }

                let head = head_opt.ok_or_else(|| anyhow::anyhow!("branch has no head set"))?;

                // Traverse commit graph, union content tribles
                let mut visited: BTreeSet<String> = BTreeSet::new();
                let mut stack: Vec<Value<Handle<Blake3, SimpleArchive>>> = vec![head];
                let mut commit_count: usize = 0;
                let mut total_triples_accum: usize = 0;
                let mut unioned = TribleSet::new();

                while let Some(h) = stack.pop() {
                    let hh: Value<Hash<Blake3>> = Handle::to_hash(h);
                    let hex: String = hh.from_value();
                    if !visited.insert(hex.clone()) {
                        continue;
                    }
                    commit_count += 1;

                    if reader.metadata(h)?.is_none() {
                        continue;
                    }

                    let meta: TribleSet = match reader.get::<TribleSet, SimpleArchive>(h) {
                        Ok(m) => m,
                        Err(_) => continue,
                    };

                    let mut parents: Vec<Value<Handle<Blake3, SimpleArchive>>> = Vec::new();
                    let mut content_handles: Vec<Value<Handle<Blake3, SimpleArchive>>> = Vec::new();
                    for t in meta.iter() {
                        if t.a() == &repo_content_attr {
                            let c = *t.v::<Handle<Blake3, SimpleArchive>>();
                            content_handles.push(c);
                        } else if t.a() == &repo_parent_attr {
                            parents.push(*t.v::<Handle<Blake3, SimpleArchive>>());
                        }
                    }

                    for c in content_handles {
                        if reader.metadata(c)?.is_none() {
                            continue;
                        }
                        let content: TribleSet = match reader.get::<TribleSet, SimpleArchive>(c) {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        total_triples_accum += content.len();
                        unioned.union(content);
                    }

                    for p in parents {
                        stack.push(p);
                    }
                }

                // Count unique triples, entities, attributes
                let unique_triples = unioned.len();
                let mut entities: HashSet<Id> = HashSet::new();
                let mut attributes: HashSet<Id> = HashSet::new();
                for t in unioned.iter() {
                    entities.insert(*t.e());
                    attributes.insert(*t.a());
                }

                println!("Branch: {branch_id:X}");
                println!("Commits: {commit_count}");
                println!("Triples (unique): {unique_triples}");
                println!("Triples (accum): {total_triples_accum}");
                println!("Entities: {}", entities.len());
                println!("Attributes: {}", attributes.len());

                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::MergeImport {
            from_pile,
            from_id,
            from_name,
            to_pile,
            to_id,
            to_name,
            signing_key,
        } => {
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace_core::repo;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::repo::Repository;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::schemas::hash::Handle;
            use triblespace_core::value::Value;

            fn resolve_branch_id(
                pile: &mut Pile<Blake3>,
                id_hex: &Option<String>,
                name_opt: &Option<String>,
            ) -> anyhow::Result<triblespace_core::id::Id> {
                use triblespace_core::trible::TribleSet;
                if let Some(h) = id_hex {
                    let raw = hex::decode(h)?;
                    let raw: [u8; 16] = raw.as_slice().try_into()?;
                    return triblespace_core::id::Id::new(raw)
                        .ok_or_else(|| anyhow::anyhow!("bad id"));
                }
                let name = name_opt
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("provide --id or --name"))?;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                find_branch_by_name(pile, &reader, &name)
            }

            use triblespace_core::id::Id;

            let mut src: Pile<Blake3> = Pile::open(&from_pile)?;
            let mut dst: Pile<Blake3> = Pile::open(&to_pile)?;

            // Simple stats struct to report how many handles were visited and
            // how many were actually stored on the destination. This replaces
            // the old `repo::copy_reachable` helper which was removed when the
            // transfer API was made more modular.
            struct CopyStats {
                visited: usize,
                stored: usize,
            }

            // We'll perform the potentially-failing copy step inside a closure
            // and capture the results in locals so we can ensure both piles are
            // explicitly closed whether the operation succeeds or fails.
            let mut src_bid_opt: Option<Id> = None;
            let mut dst_bid_opt: Option<Id> = None;
            let mut src_head_opt: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
            let mut stats_opt: Option<_> = None;

            let pre_res = (|| -> Result<(), anyhow::Error> {
                let src_bid = resolve_branch_id(&mut src, &from_id, &from_name)?;
                let dst_bid = resolve_branch_id(&mut dst, &to_id, &to_name)?;

                let src_head: Value<Handle<Blake3, SimpleArchive>> = src
                    .head(src_bid)?
                    .ok_or_else(|| anyhow::anyhow!("source branch head not found"))?;

                let src_reader = src
                    .reader()
                    .map_err(|e| anyhow::anyhow!("src pile reader error: {e:?}"))?;

                // Walk reachable handles starting from the source head and
                // transfer them into the destination pile. Aggregate simple
                // stats along the way so the CLI can report progress.
                let mut visited: usize = 0;
                let mut stored: usize = 0;
                let handles = repo::reachable(&src_reader, std::iter::once(src_head.transmute()));
                for r in repo::transfer(&src_reader, &mut dst, handles) {
                    match r {
                        Ok((_src, _dst)) => {
                            visited += 1;
                            stored += 1;
                        }
                        Err(e) => return Err(anyhow::anyhow!("transfer failed: {e}")),
                    }
                }
                let stats = CopyStats { visited, stored };

                src_bid_opt = Some(src_bid);
                dst_bid_opt = Some(dst_bid);
                src_head_opt = Some(src_head);
                stats_opt = Some(stats);
                Ok(())
            })();

            if let Err(e) = pre_res {
                // Best-effort close of opened piles in case of early failure.
                let _ = src.close();
                let _ = dst.close();
                return Err(e);
            }

            // Safe to unwrap because pre_res succeeded.
            let _src_bid = src_bid_opt.unwrap();
            let dst_bid = dst_bid_opt.unwrap();
            let src_head = src_head_opt.unwrap();
            let stats = stats_opt.unwrap();

            // Close the source pile now that we've finished reading from it.
            src.close().map_err(|e| anyhow::anyhow!("{e:?}"))?;

            // `dst` has been moved into the repository below; create the repo and
            // run merging operations, then ensure the destination storage is
            // explicitly closed via `into_storage().close()`.
            // Load signing key for destination repo (cli flag > env var > generated)
            let key = load_signing_key(&signing_key)?;
            let mut repo = Repository::new(dst, key);
            let mut ws = repo
                .pull(dst_bid)
                .map_err(|e| anyhow::anyhow!("failed to open destination branch: {e:?}"))?;
            ws.merge_commit(src_head)
                .map_err(|e| anyhow::anyhow!("merge failed: {e:?}"))?;

            while let Some(mut incoming) = repo
                .try_push(&mut ws)
                .map_err(|e| anyhow::anyhow!("push failed: {e:?}"))?
            {
                incoming
                    .merge(&mut ws)
                    .map_err(|e| anyhow::anyhow!("merge conflict: {e:?}"))?;
                ws = incoming;
            }

            // Ensure the destination pile (now owned by `repo`) is closed.
            repo.into_storage()
                .close()
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;

            println!(
                "merge-import: copied visited={} stored={} and attached source head to destination branch",
                stats.visited, stats.stored
            );
        }
        Command::Consolidate {
            pile,
            name,
            out_name,
            dry_run,
            signing_key,
        } => {
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::repo::Repository;
            use triblespace_core::trible::TribleSet;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::schemas::hash::Handle;
            use triblespace_core::value::Value;
            // Trait imports required for method resolution
            use triblespace::prelude::BlobStorePut;
            use triblespace_core::blob::ToBlob;

            let mut pile: Pile<Blake3> = Pile::open(&pile)?;

            // Ensure in-memory indices are populated.
            pile.refresh()?;

            let reader = pile
                .reader()
                .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

            // Attribute ids used in branch metadata
            let name_attr = triblespace_core::metadata::name.id();
            let repo_head_attr: triblespace_core::id::Id =
                id_hex!("272FBC56108F336C4D2E17289468C35F");
            let wanted_name = branch_name_handle(&name);

            // Collect all branch ids whose metadata name matches `name`.
            let mut candidates: Vec<(
                triblespace_core::id::Id,
                Option<Value<Handle<Blake3, SimpleArchive>>>,
            )> = Vec::new();
            for r in pile.branches()? {
                let bid = r?;
                if let Some(meta_handle) = pile.head(bid)? {
                    if reader.metadata(meta_handle)?.is_some() {
                        match reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                            Ok(meta) => {
                                let mut head_val: Option<Value<Handle<Blake3, SimpleArchive>>> =
                                    None;
                                let mut matches = false;
                                for t in meta.iter() {
                                    if t.a() == &name_attr {
                                        let h: BranchNameHandle = *t.v();
                                        if h == wanted_name {
                                            matches = true;
                                        }
                                    } else if t.a() == &repo_head_attr {
                                        head_val = Some(*t.v::<Handle<Blake3, SimpleArchive>>());
                                    }
                                }
                                if matches {
                                    candidates.push((bid, head_val));
                                }
                            }
                            Err(_) => {
                                // Ignore malformed metadata blobs for now.
                            }
                        }
                    }
                }
            }

            if candidates.is_empty() {
                println!("no branches with name '{name}' found");
                let _ = pile.close();
                return Ok(());
            }

            println!("found {} branch(es) named '{name}'", candidates.len());
            for (bid, head) in &candidates {
                let id_hex = format!("{bid:X}");
                if let Some(h) = head {
                    let hh: Value<triblespace_core::value::schemas::hash::Hash<Blake3>> =
                        Handle::to_hash(*h);
                    let hex: String = hh.from_value();
                    println!("- {id_hex} -> commit blake3:{hex}");
                } else {
                    println!("- {id_hex} -> <no head>");
                }
            }

            if dry_run {
                println!("dry-run: no changes will be made");
                let _ = pile.close();
                return Ok(());
            }

            if candidates.len() == 1 {
                println!("only one branch present; nothing to consolidate");
                let _ = pile.close();
                return Ok(());
            }

            // Collect parent commit handles (skip branches without a head)
            let parents: Vec<Value<Handle<Blake3, SimpleArchive>>> =
                candidates.iter().filter_map(|(_, h)| *h).collect();

            if parents.is_empty() {
                let _ = pile.close();
                anyhow::bail!("no branch heads available to attach");
            }

            // Create a single merge commit that has all branch heads as parents.
            let signing_key = load_signing_key(&signing_key)?;
            let commit_set = triblespace_core::repo::commit::commit_metadata(
                &signing_key,
                parents.clone(),
                None,
                None,
                None,
            );
            let commit_blob = commit_set.to_blob();

            // Decide output branch name
            let out = out_name.unwrap_or_else(|| format!("{name}-consolidated"));

            // Store the commit blob in the pile before creating the branch.
            let commit_handle = pile
                .put(commit_blob)
                .map_err(|e| anyhow::anyhow!("failed to put commit blob: {e:?}"))?;

            // Move the pile into a Repository so we can atomically create the branch.
            let mut repo = Repository::new(pile, signing_key.clone());
            let new_id = *repo
                .create_branch_with_key(&out, Some(commit_handle), signing_key)
                .map_err(|e| anyhow::anyhow!("failed to create consolidated branch: {e:?}"))?;

            repo.into_storage()
                .close()
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;

            println!("created consolidated branch '{out}' with id {new_id:X}");
        }
    }
    Ok(())
}

fn branch_name_handle(name: &str) -> BranchNameHandle {
    name.to_owned().to_blob().get_handle::<Blake3>()
}

fn find_branch_by_name(
    pile: &mut Pile<Blake3>,
    reader: &PileReader<Blake3>,
    name: &str,
) -> Result<Id> {
    let wanted = branch_name_handle(name);
    let name_attr = triblespace_core::metadata::name.id();
    for r in pile.branches()? {
        let bid = r?;
        let Some(meta_handle) = pile.head(bid)? else {
            continue;
        };
        let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(meta_handle) else {
            continue;
        };
        for t in meta.iter() {
            if t.a() == &name_attr {
                let h: BranchNameHandle = *t.v();
                if h == wanted {
                    return Ok(bid);
                }
            }
        }
    }
    anyhow::bail!("branch not found: {name}")
}

fn load_branch_name(
    reader: &impl BlobStoreGet<Blake3>,
    meta: &TribleSet,
) -> Result<Option<String>> {
    let name_attr = triblespace_core::metadata::name.id();
    let mut handle_opt: Option<BranchNameHandle> = None;
    for t in meta.iter() {
        if t.a() == &name_attr {
            let h: BranchNameHandle = *t.v();
            if handle_opt.replace(h).is_some() {
                return Ok(None);
            }
        }
    }

    let Some(handle) = handle_opt else {
        return Ok(None);
    };

    let view: View<str> = reader
        .get(handle)
        .map_err(|err| anyhow::anyhow!("read branch name blob: {err:?}"))?;
    Ok(Some(view.as_ref().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn parse_signing_key_hex_and_file() {
        // File containing hex
        let mut seed = [0u8; 32];
        for i in 0..32 {
            seed[i] = i as u8;
        }
        let hex = hex::encode(seed);
        let mut f = NamedTempFile::new().expect("tmpfile");
        writeln!(f, "{}", hex).expect("write");
        let path = f.path().to_path_buf();
        let key = load_signing_key(&Some(path)).expect("parse file");
        let expected = ed25519_dalek::SigningKey::from_bytes(&seed);
        assert_eq!(key.to_bytes(), expected.to_bytes());
    }
}
