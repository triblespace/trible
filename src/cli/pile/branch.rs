use anyhow::Result;
use clap::Parser;
use rand::rngs::OsRng;
use std::convert::TryInto;
use std::path::PathBuf;

use crate::DEFAULT_MAX_PILE_SIZE;

use tribles::prelude::{BlobStore, BlobStoreGet, BranchStore};

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
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::List { path } => {
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;

            let pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&path)?;

            for branch in pile.branches() {
                let id = branch?;
                println!("{id:X}");
            }
        }
        Command::Create { pile, name } => {
            use ed25519_dalek::SigningKey;
            use tribles::repo::pile::Pile;
            use tribles::repo::Repository;
            use tribles::value::schemas::hash::Blake3;

            let pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;
            let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));
            let ws = repo.branch(&name).map_err(|e| anyhow::anyhow!("{e:?}"))?;
            println!("{:#X}", ws.branch_id());
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

            let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;

            let branch_id: Id = if let Some(id_hex) = id {
                let raw = hex::decode(id_hex)?;
                let raw: [u8; 16] = raw.as_slice().try_into()?;
                Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?
            } else if let Some(name) = name {
                let reader = pile.reader();
                let mut found: Option<Id> = None;
                for r in pile.branches() {
                    let bid = r?;
                    if let Some(meta_handle) = pile.head(bid)? {
                        let meta: TribleSet = reader
                            .get::<TribleSet, SimpleArchive>(meta_handle)
                            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                        for t in meta.iter() {
                            if t.a() == &tribles::metadata::ATTR_NAME {
                                let n: Value<tribles::value::schemas::shortstring::ShortString> =
                                    *t.v();
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
            let reader = pile.reader();
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
                                let n: Value<tribles::value::schemas::shortstring::ShortString> =
                                    *t.v();
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

            let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;
            let reader = pile.reader();

            let branch_id: Id = if let Some(id_hex) = id {
                let raw = hex::decode(id_hex)?;
                let raw: [u8; 16] = raw.as_slice().try_into()?;
                Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?
            } else if let Some(name) = name {
                let mut found: Option<Id> = None;
                for r in pile.branches() {
                    let bid = r?;
                    if let Some(meta_handle) = pile.head(bid)? {
                        let meta: TribleSet = reader
                            .get::<TribleSet, SimpleArchive>(meta_handle)
                            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                        for t in meta.iter() {
                            if t.a() == &tribles::metadata::ATTR_NAME {
                                let n: Value<tribles::value::schemas::shortstring::ShortString> =
                                    *t.v();
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
            for (handle, blob) in reader.iter() {
                let handle: Value<Handle<Blake3, UnknownBlob>> = handle;
                let sah: Value<Handle<Blake3, SimpleArchive>> = handle.transmute();
                let Ok(meta): Result<TribleSet, _> = reader.get::<TribleSet, SimpleArchive>(sah)
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
        }
        Command::MergeImport {
            from_pile,
            from_id,
            from_name,
            to_pile,
            to_id,
            to_name,
        } => {
            use ed25519_dalek::SigningKey;
            use tribles::blob::schemas::UnknownBlob;
            use tribles::prelude::blobschemas::SimpleArchive;
            use tribles::repo;
            use tribles::repo::pile::Pile;
            use tribles::repo::Repository;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use tribles::value::Value;

            fn resolve_branch_id(
                pile: &mut Pile<DEFAULT_MAX_PILE_SIZE, Blake3>,
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
                let reader = pile.reader();
                for r in pile.branches() {
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

            let mut src: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&from_pile)?;
            let mut dst: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&to_pile)?;

            let src_bid = resolve_branch_id(&mut src, &from_id, &from_name)?;
            let dst_bid = resolve_branch_id(&mut dst, &to_id, &to_name)?;

            let src_head: Value<Handle<Blake3, SimpleArchive>> = src
                .head(src_bid)?
                .ok_or_else(|| anyhow::anyhow!("source branch head not found"))?;

            let stats = repo::copy_reachable(&src.reader(), &mut dst, [src_head.transmute()])
                .map_err(|e| anyhow::anyhow!("copy_reachable failed: {e}"))?;

            let mut repo = Repository::new(dst, SigningKey::generate(&mut OsRng));
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

            println!(
                "merge-import: copied visited={} stored={} and attached source head to destination branch",
                stats.visited, stats.stored
            );
        }
    }
    Ok(())
}
