use anyhow::Result;
use clap::Parser;
use std::convert::TryInto;
use std::path::PathBuf;

use crate::DEFAULT_MAX_PILE_SIZE;
use tribles::prelude::BlobStore;
use tribles::prelude::BlobStoreGet;
use tribles::prelude::blobschemas::SimpleArchive;
use tribles::trible::TribleSet;
use tribles::prelude::Id;
use tribles::prelude::BranchStore;

#[derive(Parser)]
pub enum BranchCommand {
    /// Push a branch from a pile to a remote object store.
    Push {
        /// URL of the target object store
        url: String,
        /// Path to the source pile file
        pile: PathBuf,
        /// Branch identifier to push (hex encoded)
        branch: String,
    },
    /// Pull a branch from a remote object store into a pile.
    Pull {
        /// URL of the source object store
        url: String,
        /// Path to the destination pile file
        pile: PathBuf,
        /// Branch identifier to pull (hex encoded)
        branch: String,
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
}

pub fn run(cmd: BranchCommand) -> Result<()> {
    match cmd {
        BranchCommand::Push { url, pile, branch } => {
            use tribles::id::Id;
            use tribles::repo;
            use tribles::repo::objectstore::ObjectStoreRemote;
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;
            use url::Url;

            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;

            let reader = pile.reader();
            for r in repo::transfer::<_, _, Blake3, Blake3, tribles::blob::schemas::UnknownBlob>(
                &reader,
                &mut remote,
            ) {
                r?;
            }

            let raw = hex::decode(branch)?;
            let raw: [u8; 16] = raw.as_slice().try_into()?;
            let id = Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?;

            let handle = pile
                .head(id)?
                .ok_or_else(|| anyhow::anyhow!("branch not found"))?;
            let old = remote.head(id)?;
            remote.update(id, old, handle)?;
        }
        BranchCommand::Pull { url, pile, branch } => {
            use tribles::id::Id;
            use tribles::repo;
            use tribles::repo::objectstore::ObjectStoreRemote;
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;
            use url::Url;

            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;

            let reader = remote.reader();
            for r in repo::transfer::<_, _, Blake3, Blake3, tribles::blob::schemas::UnknownBlob>(
                &reader, &mut pile,
            ) {
                r?;
            }

            let raw = hex::decode(branch)?;
            let raw: [u8; 16] = raw.as_slice().try_into()?;
            let id = Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?;

            let handle = remote
                .head(id)?
                .ok_or_else(|| anyhow::anyhow!("branch not found"))?;
            let old = pile.head(id)?;
            pile.update(id, old, handle)?;
            pile.flush().map_err(|e| anyhow::anyhow!("{e:?}"))?;
        }
        BranchCommand::Inspect { pile, id, name } => {
            use tribles::id::Id;
            use tribles::prelude::blobschemas::SimpleArchive;
            use tribles::prelude::valueschemas::Handle;
            use tribles::repo;
            use tribles::repo::pile::Pile;
            use tribles::trible::TribleSet;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Hash;
            use tribles::value::Value;

            let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;

            // Resolve branch id either from --id or by searching for --name
            let branch_id: Id = if let Some(id_hex) = id {
                let raw = hex::decode(id_hex)?;
                let raw: [u8; 16] = raw.as_slice().try_into()?;
                Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?
            } else if let Some(name) = name {
                // Enumerate branches and find the one matching name in metadata
                let reader = pile.reader();
                let mut found: Option<Id> = None;
                for r in pile.branches() {
                    let bid = r?;
                    if let Some(meta_handle) = pile.head(bid)? {
                        let meta: TribleSet = reader
                            .get::<TribleSet, SimpleArchive>(meta_handle)
                            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                        // scan for metadata::name
                        for t in meta.iter() {
                            if t.a() == &tribles::metadata::ATTR_NAME {
                                let n: Value<tribles::value::schemas::shortstring::ShortString> = *t.v();
                                let nstr: String = n.from_value();
                                if nstr == name {
                                    found = Some(bid);
                                    break;
                                }
                            }
                        }
                        if found.is_some() { break; }
                    }
                }
                found.ok_or_else(|| anyhow::anyhow!("branch named not found"))?
            } else {
                anyhow::bail!("provide either --id HEX or --name NAME");
            };

            // Load branch metadata blob handle
            let meta_handle = pile
                .head(branch_id)?
                .ok_or_else(|| anyhow::anyhow!("branch not found"))?;
            let reader = pile.reader();
            let meta_present = reader.metadata(meta_handle).is_some();
            // Try to decode metadata, but continue gracefully if it fails
            let (name_val, head_val, head_err): (Option<String>, Option<Value<Handle<Blake3, SimpleArchive>>>, Option<String>) =
                if meta_present {
                    match reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                        Ok(meta) => {
                            let mut name_val: Option<String> = None;
                            let mut head_val: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
                            // repo::head attr id from NS! in tribles-rust/src/repo.rs
                            let repo_head_attr: tribles::id::Id = tribles::id_hex!("272FBC56108F336C4D2E17289468C35F");
                            for t in meta.iter() {
                                if t.a() == &tribles::metadata::ATTR_NAME {
                                    let n: Value<tribles::value::schemas::shortstring::ShortString> = *t.v();
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
            // head hash computed later only if we have a decoded head handle

            println!("Id:        {id_hex}");
            if let Some(nstr) = name_val.clone() { println!("Name:      {nstr}"); }
            println!("Meta:      blake3:{meta_hex}");
            println!("Meta blob: {}{}",
                if meta_present { "present" } else { "missing" },
                head_err.as_deref().map(|e| format!(" ({e})")).unwrap_or_default()
            );
            if let Some(h) = head_val.clone() {
                let head_hash: Value<Hash<Blake3>> = Handle::to_hash(h);
                let head_hex: String = head_hash.from_value();
                println!("Head:      blake3:{head_hex}");
                let present = reader.metadata(h).is_some();
                println!("Head blob: {}", if present { "present" } else { "missing" });
            } else {
                println!("Head:      (unknown: metadata missing or undecodable)");
            }
        }
        BranchCommand::History { pile, id, name, limit } => {
            use tribles::blob::schemas::UnknownBlob;
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use tribles::value::schemas::hash::Hash;
            use tribles::value::Value;

            // Resolve branch id (reuse Inspect resolution logic)
            let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;
            let reader = pile.reader();

            let branch_id: Id = if let Some(id_hex) = id {
                let raw = hex::decode(id_hex)?;
                let raw: [u8; 16] = raw.as_slice().try_into()?;
                Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?
            } else if let Some(name) = name {
                // Find by name as in Inspect
                let mut found: Option<Id> = None;
                for r in pile.branches() {
                    let bid = r?;
                    if let Some(meta_handle) = pile.head(bid)? {
                        let meta: TribleSet = reader
                            .get::<TribleSet, SimpleArchive>(meta_handle)
                            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                        for t in meta.iter() {
                            if t.a() == &tribles::metadata::ATTR_NAME {
                                let n: Value<tribles::value::schemas::shortstring::ShortString> = *t.v();
                                let nstr: String = n.from_value();
                                if nstr == name { found = Some(bid); break; }
                            }
                        }
                        if found.is_some() { break; }
                    }
                }
                found.ok_or_else(|| anyhow::anyhow!("branch named not found"))?
            } else {
                anyhow::bail!("provide either --id HEX or --name NAME");
            };

            // Attribute ids we care about
            let repo_branch_attr: tribles::id::Id = tribles::id_hex!("8694CC73AF96A5E1C7635C677D1B928A");
            let repo_head_attr: tribles::id::Id = tribles::id_hex!("272FBC56108F336C4D2E17289468C35F");

            // Scan all blobs, filtering to branch metadata sets for this id
            let mut printed = 0usize;
            for (handle, blob) in reader.iter() {
                let handle: Value<Handle<Blake3, UnknownBlob>> = handle;
                // Try to read as SimpleArchive -> TribleSet via the reader to benefit from validation
                let sah: Value<Handle<Blake3, SimpleArchive>> = handle.transmute();
                let Ok(meta): Result<TribleSet, _> = reader.get::<TribleSet, SimpleArchive>(sah) else {
                    continue;
                };
                // Check if this set declares repo::branch = branch_id
                let mut is_meta_for_branch = false;
                let mut head_val: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
                for t in meta.iter() {
                    if t.a() == &repo_branch_attr {
                        let v: Value<tribles::prelude::valueschemas::GenId> = *t.v();
                        if let Ok(id) = v.try_from_value::<tribles::id::Id>() {
                            if id == branch_id { is_meta_for_branch = true; }
                        }
                    } else if t.a() == &repo_head_attr {
                        head_val = Some(*t.v::<Handle<Blake3, SimpleArchive>>());
                    }
                }
                if !is_meta_for_branch { continue; }
                let meta_hash: Value<Hash<Blake3>> = Handle::to_hash(sah);
                let meta_hex: String = meta_hash.from_value();
                if let Some(h) = head_val {
                    let head_hash: Value<Hash<Blake3>> = Handle::to_hash(h);
                    let head_hex: String = head_hash.from_value();
                    let present = reader.metadata(h).is_some();
                    println!("Meta blake3:{meta_hex}  Head blake3:{head_hex}  [{}]", if present {"present"} else {"missing"});
                } else {
                    println!("Meta blake3:{meta_hex}  Head: (unset)");
                }
                printed += 1;
                if printed >= limit { break; }
            }
            if printed == 0 { println!("No metadata entries found for this branch in pile blobs."); }
        }
    }
    Ok(())
}
