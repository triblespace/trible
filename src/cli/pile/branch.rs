use anyhow::Result;
use clap::Parser;
use rand::rngs::OsRng;
use std::convert::TryInto;
use std::path::PathBuf;

// DEFAULT_MAX_PILE_SIZE removed; the new Pile API no longer uses a size const generic

use tribles::prelude::BlobStore;
use tribles::prelude::BlobStoreGet;
use tribles::prelude::BranchStore;

use ed25519_dalek::SigningKey;
use std::env;
use std::fs;

fn load_signing_key(path_opt: &Option<PathBuf>) -> Result<SigningKey, anyhow::Error> {
    // Accept only a path to a file (via CLI flag or TRIBLES_SIGNING_KEY env var)
    // containing a 64-char hex seed. If the path is absent, generate an
    // ephemeral signing key.
    let key_path_opt: Option<PathBuf> = if let Some(p) = path_opt {
        Some(p.clone())
    } else if let Ok(s) = env::var("TRIBLES_SIGNING_KEY") {
        Some(PathBuf::from(s))
    } else {
        None
    };

    if let Some(p) = key_path_opt {
        let content = fs::read_to_string(&p)
            .map_err(|e| anyhow::anyhow!("failed to read signing key: {e}"))?;
        let hexstr = content.trim();
        if hexstr.len() != 64 || !hexstr.chars().all(|c| c.is_ascii_hexdigit()) {
            anyhow::bail!(
                "signing key file {} does not contain valid 64-char hex",
                p.display()
            );
        }
        let bytes = hex::decode(hexstr)
            .map_err(|e| anyhow::anyhow!("invalid hex in signing key file: {e}"))?;
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        return Ok(SigningKey::from_bytes(&arr));
    }

    Ok(SigningKey::generate(&mut OsRng))
}

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
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;

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
            use tribles::repo::pile::Pile;
            use tribles::repo::Repository;
            use tribles::value::schemas::hash::Blake3;
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
            use tribles::id::Id;
            use tribles::prelude::blobschemas::SimpleArchive;
            use tribles::prelude::valueschemas::Handle;

            use tribles::repo::pile::Pile;
            use tribles::trible::TribleSet;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Hash;
            use tribles::value::Value;

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
                    let mut found: Option<Id> = None;
                    let iter = pile.branches()?;
                    for r in iter {
                        let bid = r?;
                        if let Some(meta_handle) = pile.head(bid)? {
                            let meta: TribleSet = reader
                                .get::<TribleSet, SimpleArchive>(meta_handle)
                                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                            for t in meta.iter() {
                                if t.a() == &tribles::metadata::ATTR_NAME {
                                    let n: Value<
                                        tribles::value::schemas::shortstring::ShortString,
                                    > = *t.v();
                                    let nstr: String = n.from_value();
                                    if nstr == name {
                                        found = Some(bid);
                                        break;
                                    }
                                }
                            }
                            if found.is_some() {
                                break;
                            }
                        }
                    }
                    found.ok_or_else(|| anyhow::anyhow!("branch named not found"))?
                } else {
                    anyhow::bail!("provide either --id HEX or --name NAME");
                };

                let meta_handle = pile
                    .head(branch_id)?
                    .ok_or_else(|| anyhow::anyhow!("branch not found"))?;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                let meta_present = reader.metadata(meta_handle).is_some();
                let (name_val, head_val, head_err): (
                    Option<String>,
                    Option<Value<Handle<Blake3, SimpleArchive>>>,
                    Option<String>,
                ) = if meta_present {
                    match reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                        Ok(meta) => {
                            let mut name_val: Option<String> = None;
                            let mut head_val: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
                            let repo_head_attr: tribles::id::Id =
                                tribles::id_hex!("272FBC56108F336C4D2E17289468C35F");
                            for t in meta.iter() {
                                if t.a() == &tribles::metadata::ATTR_NAME {
                                    let n: Value<
                                        tribles::value::schemas::shortstring::ShortString,
                                    > = *t.v();
                                    name_val = Some(n.from_value());
                                } else if t.a() == &repo_head_attr {
                                    let h = *t.v::<Handle<Blake3, SimpleArchive>>();
                                    head_val = Some(h);
                                }
                            }
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
                    let present = reader.metadata(h).is_some();
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
            use tribles::blob::schemas::UnknownBlob;
            use tribles::id::Id;
            use tribles::prelude::blobschemas::SimpleArchive;
            use tribles::prelude::valueschemas::Handle;

            use tribles::repo::pile::Pile;
            use tribles::trible::TribleSet;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Hash;
            use tribles::value::Value;

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
                    let mut found: Option<Id> = None;
                    let iter = pile.branches()?;
                    for r in iter {
                        let bid = r?;
                        if let Some(meta_handle) = pile.head(bid)? {
                            let meta: TribleSet = reader
                                .get::<TribleSet, SimpleArchive>(meta_handle)
                                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                            for t in meta.iter() {
                                if t.a() == &tribles::metadata::ATTR_NAME {
                                    let n: Value<
                                        tribles::value::schemas::shortstring::ShortString,
                                    > = *t.v();
                                    let nstr: String = n.from_value();
                                    if nstr == name {
                                        found = Some(bid);
                                        break;
                                    }
                                }
                            }
                            if found.is_some() {
                                break;
                            }
                        }
                    }
                    found.ok_or_else(|| anyhow::anyhow!("branch named not found"))?
                } else {
                    anyhow::bail!("provide either --id HEX or --name NAME");
                };

                let repo_branch_attr: tribles::id::Id =
                    tribles::id_hex!("8694CC73AF96A5E1C7635C677D1B928A");
                let repo_head_attr: tribles::id::Id =
                    tribles::id_hex!("272FBC56108F336C4D2E17289468C35F");

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
                            let v: Value<tribles::prelude::valueschemas::GenId> = *t.v();
                            if let Ok(id) = v.try_from_value::<tribles::id::Id>() {
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
                        let present = reader.metadata(h).is_some();
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
        Command::MergeImport {
            from_pile,
            from_id,
            from_name,
            to_pile,
            to_id,
            to_name,
            signing_key,
        } => {
            use tribles::prelude::blobschemas::SimpleArchive;
            use tribles::repo;
            use tribles::repo::pile::Pile;
            use tribles::repo::Repository;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use tribles::value::Value;

            fn resolve_branch_id(
                pile: &mut Pile<Blake3>,
                id_hex: &Option<String>,
                name_opt: &Option<String>,
            ) -> anyhow::Result<tribles::id::Id> {
                use tribles::trible::TribleSet;
                if let Some(h) = id_hex {
                    let raw = hex::decode(h)?;
                    let raw: [u8; 16] = raw.as_slice().try_into()?;
                    return tribles::id::Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"));
                }
                let name = name_opt
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("provide --id or --name"))?;
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                let iter = pile.branches()?;
                for r in iter {
                    let bid = r?;
                    if let Some(meta_handle) = pile.head(bid)? {
                        if let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                            for t in meta.iter() {
                                if t.a() == &tribles::metadata::ATTR_NAME {
                                    let n: tribles::value::Value<
                                        tribles::value::schemas::shortstring::ShortString,
                                    > = *t.v();
                                    if n.from_value::<String>() == name {
                                        return Ok(bid);
                                    }
                                }
                            }
                        }
                    }
                }
                anyhow::bail!("branch not found: {name}")
            }

            use tribles::id::Id;

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
                .push(&mut ws)
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
            use tribles::prelude::blobschemas::SimpleArchive;
            use tribles::repo::pile::Pile;
            use tribles::repo::Repository;
            use tribles::trible::TribleSet;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use tribles::value::Value;
            // Trait imports required for method resolution
            use tribles::blob::ToBlob;
            use tribles::prelude::BlobStorePut;

            let mut pile: Pile<Blake3> = Pile::open(&pile)?;

            // Ensure in-memory indices are populated.
            pile.refresh()?;

            let reader = pile
                .reader()
                .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

            // Attribute ids used in branch metadata
            let name_attr = tribles::metadata::ATTR_NAME;
            let repo_head_attr: tribles::id::Id =
                tribles::id_hex!("272FBC56108F336C4D2E17289468C35F");

            // Collect all branch ids whose metadata name matches `name`.
            let mut candidates: Vec<(
                tribles::id::Id,
                Option<Value<Handle<Blake3, SimpleArchive>>>,
            )> = Vec::new();
            for r in pile.branches()? {
                let bid = r?;
                if let Some(meta_handle) = pile.head(bid)? {
                    if reader.metadata(meta_handle).is_some() {
                        match reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                            Ok(meta) => {
                                let mut branch_name: Option<String> = None;
                                let mut head_val: Option<Value<Handle<Blake3, SimpleArchive>>> =
                                    None;
                                for t in meta.iter() {
                                    if t.a() == &name_attr {
                                        let n: Value<
                                            tribles::value::schemas::shortstring::ShortString,
                                        > = *t.v();
                                        branch_name = Some(n.from_value());
                                    } else if t.a() == &repo_head_attr {
                                        head_val = Some(*t.v::<Handle<Blake3, SimpleArchive>>());
                                    }
                                }
                                if let Some(n) = branch_name {
                                    if n == name {
                                        candidates.push((bid, head_val));
                                    }
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
                    let hh: Value<tribles::value::schemas::hash::Hash<Blake3>> =
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
            let commit_set =
                tribles::repo::commit::commit_metadata(&signing_key, parents.clone(), None, None);
            let commit_blob = commit_set.to_blob();

            // Store the commit blob in the pile before creating the branch.
            let commit_handle = pile
                .put(commit_blob)
                .map_err(|e| anyhow::anyhow!("failed to put commit blob: {e:?}"))?;

            // Decide output branch name
            let out = out_name.unwrap_or_else(|| format!("{name}-consolidated"));

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
