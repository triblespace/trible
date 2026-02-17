use anyhow::Result;
use clap::Parser;
use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

// DEFAULT_MAX_PILE_SIZE removed; the new Pile API no longer uses a size const generic

use triblespace::prelude::blobschemas::SimpleArchive;
use triblespace::prelude::BlobStore;
use triblespace::prelude::BlobStoreGet;
use triblespace::prelude::BranchStore;
use triblespace::prelude::View;
use triblespace_core::blob::schemas::longstring::LongString;
use triblespace_core::blob::ToBlob;
use triblespace_core::id::id_hex;
use triblespace_core::id::Id;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::{Blake3, Handle};
use triblespace_core::value::Value;

use super::signing::load_signing_key;
use triblespace_core::repo::BlobStoreMeta;

type BranchNameHandle = Value<Handle<Blake3, LongString>>;

// These markers are part of the stable on-disk pile format (see
// triblespace-rs/book/src/pile-format.md). Copy them exactly; do not invent.
#[allow(non_upper_case_globals)]
const MAGIC_MARKER_BLOB: Id = id_hex!("1E08B022FF2F47B6EBACF1D68EB35D96");
#[allow(non_upper_case_globals)]
const MAGIC_MARKER_BRANCH: Id = id_hex!("2BC991A7F5D5D2A3A468C53B0AA03504");
#[allow(non_upper_case_globals)]
const MAGIC_MARKER_BRANCH_TOMBSTONE: Id = id_hex!("E888CC787202D2AE4C654BFE9699C430");

const RECORD_LEN: u64 = 64;

#[derive(Parser)]
pub enum Command {
    /// List branches in a pile file (id + head + name).
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
        /// Branch identifier to inspect (hex encoded)
        branch: String,
    },
    /// Delete a branch in a pile (writes a tombstone).
    Delete {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Branch identifier to delete (hex encoded)
        branch: String,
    },
    /// Set the branch metadata handle for a branch in a pile (CAS update).
    ///
    /// This updates the branch store head to point at the provided branch
    /// metadata blob handle. The pile does not verify that the referenced blob
    /// exists (head-only piles are allowed).
    Set {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Branch identifier to set (hex encoded)
        branch: String,
        /// Branch metadata blob handle (64 hex chars, optionally prefixed with `blake3:`)
        meta: String,
        /// Expected current branch metadata blob handle (CAS). Uses current head when omitted.
        #[arg(long)]
        expected: Option<String>,
    },
    /// Scan the pile for historical branch metadata entries for this branch.
    /// This lists candidate metadata blobs that reference the branch id.
    History {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier to inspect (hex encoded)
        branch: String,
        /// Maximum results to print
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Show a reflog-like history of branch head updates stored in the pile.
    ///
    /// This scans the pile file for branch update and tombstone records and
    /// prints the most recent entries for a branch (latest first).
    Reflog {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier to inspect (hex encoded)
        branch: String,
        /// Maximum results to print
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// List branch identifiers seen in the pile file, including deleted branches.
    ///
    /// This scans the pile file for branch update/tombstone records and reports
    /// the most recent entry per branch id. Use `--deleted` to only show
    /// currently tombstoned branch ids.
    Journal {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Maximum results to print
        #[arg(long, default_value_t = 200)]
        limit: usize,
        /// Only show branches that are currently deleted (tombstoned)
        #[arg(long)]
        deleted: bool,
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
    },
    /// Show statistics for a branch: commits, triples, entities, attributes.
    Stats {
        /// Path to the pile file to inspect
        pile: PathBuf,
        /// Branch identifier to inspect (hex encoded)
        branch: String,
    },
    /// Import reachable blobs from a source branch into a target pile and
    /// attach them to the target branch via a single merge commit.
    MergeImport {
        /// Path to the source pile file
        #[arg(long)]
        from_pile: PathBuf,
        /// Source branch identifier (hex)
        #[arg(long)]
        from_id: String,

        /// Path to the destination pile file
        #[arg(long)]
        to_pile: PathBuf,
        /// Destination branch identifier (hex)
        #[arg(long)]
        to_id: String,
        /// Optional signing key path. The file should contain a 64-char hex seed.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Consolidate multiple branches into a single new branch.
    Consolidate {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Branch identifier(s) to consolidate (hex encoded)
        #[arg(num_args = 1..)]
        branches: Vec<String>,
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

                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
                let iter = pile.branches()?;
                let head_attr = triblespace_core::repo::head.id();
                let mut rows: Vec<(String, Id, String)> = Vec::new();
                for branch in iter {
                    let id = branch?;
                    let meta_handle = match pile.head(id)? {
                        Some(handle) => handle,
                        None => {
                            rows.push(("<deleted>".to_string(), id, "-".to_string()));
                            continue;
                        }
                    };

                    let (name, head) = match reader.get::<TribleSet, _>(meta_handle) {
                        Ok(meta) => {
                            let name_attr = triblespace_core::metadata::name.id();
                            let mut name_handle: Option<BranchNameHandle> = None;
                            let mut head_handle: Option<Value<Handle<Blake3, SimpleArchive>>> =
                                None;
                            for t in meta.iter() {
                                if t.a() == &name_attr {
                                    let h: BranchNameHandle = *t.v();
                                    if name_handle.replace(h).is_some() {
                                        // Multiple names -> treat as unnamed.
                                        name_handle = None;
                                        break;
                                    }
                                } else if t.a() == &head_attr {
                                    let h: Value<Handle<Blake3, SimpleArchive>> = *t.v();
                                    if head_handle.replace(h).is_some() {
                                        // Multiple heads -> treat as missing.
                                        head_handle = None;
                                    }
                                }
                            }

                            let name = match name_handle {
                                None => "<unnamed>".to_string(),
                                Some(handle) => match reader.get::<View<str>, _>(handle) {
                                    Ok(view) => view.as_ref().to_string(),
                                    Err(_) => format!(
                                        "<name blob missing ({})>",
                                        hex::encode_upper(&handle.raw[..4])
                                    ),
                                },
                            };

                            let head = match head_handle {
                                None => "-".to_string(),
                                Some(handle) => format!("blake3:{}", hex::encode(handle.raw)),
                            };

                            (name, head)
                        }
                        Err(_) => (
                            format!(
                                "<metadata blob missing ({})>",
                                hex::encode_upper(&meta_handle.raw[..4])
                            ),
                            "-".to_string(),
                        ),
                    };

                    rows.push((name, id, head));
                }

                rows.sort_by(|(a_name, a_id, _), (b_name, b_id, _)| {
                    a_name.cmp(b_name).then_with(|| a_id.cmp(b_id))
                });

                for (name, id, head) in rows {
                    println!("{id:X}\t{head}\t{name}");
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

            let res = (|| -> Result<(), anyhow::Error> {
                let branch_id = repo
                    .create_branch(&name, None)
                    .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                println!("{:#X}", *branch_id);
                Ok(())
            })();

            // Ensure the underlying pile is closed whether the command succeeds or fails.
            let close_res = repo
                .into_storage()
                .close()
                .map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Inspect { pile, branch } => {
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace::prelude::valueschemas::Handle;

            use triblespace_core::repo::pile::Pile;
            use triblespace_core::trible::TribleSet;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::schemas::hash::Hash;
            use triblespace_core::value::Value;

            let mut pile: Pile<Blake3> = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                let branch_id = parse_branch_id_hex(&branch)?;

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
        Command::Delete { pile, branch } => {
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::value::schemas::hash::Blake3;

            let mut pile: Pile<Blake3> = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                let branch_id = parse_branch_id_hex(&branch)?;

                let old = pile
                    .head(branch_id)?
                    .ok_or_else(|| anyhow::anyhow!("branch not found"))?;

                match pile.update(branch_id, Some(old), None)? {
                    triblespace_core::repo::PushResult::Success() => {
                        println!("deleted branch {branch_id:X}");
                        Ok(())
                    }
                    triblespace_core::repo::PushResult::Conflict(_) => {
                        anyhow::bail!("branch {branch_id:X} advanced concurrently; rerun delete")
                    }
                }
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::Set {
            pile,
            branch,
            meta,
            expected,
        } => {
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace::prelude::valueschemas::Handle;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::Value;

            let mut pile: Pile<Blake3> = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                let branch_id = parse_branch_id_hex(&branch)?;
                let new_meta: Value<Handle<Blake3, SimpleArchive>> = parse_blake3_handle(&meta)?;

                let expected_old: Option<Value<Handle<Blake3, SimpleArchive>>> = match expected {
                    Some(s) => parse_blake3_handle_opt(&s)?,
                    None => pile.head(branch_id)?,
                };

                match pile.update(branch_id, expected_old, Some(new_meta))? {
                    triblespace_core::repo::PushResult::Success() => {
                        println!(
                            "set branch {bid:X} meta blake3:{meta}",
                            bid = branch_id,
                            meta = hex::encode(new_meta.raw)
                        );
                        Ok(())
                    }
                    triblespace_core::repo::PushResult::Conflict(existing) => {
                        let got = existing
                            .map(|h| format!("blake3:{}", hex::encode(h.raw)))
                            .unwrap_or_else(|| "-".to_string());
                        anyhow::bail!("branch head changed concurrently; current={got}")
                    }
                }
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        Command::History {
            pile,
            branch,
            limit,
        } => {
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace::prelude::valueschemas::Handle;
            use triblespace_core::blob::schemas::UnknownBlob;

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

                let branch_id = parse_branch_id_hex(&branch)?;

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
        Command::Reflog {
            pile,
            branch,
            limit,
        } => {
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace::prelude::valueschemas::Handle;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::trible::TribleSet;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::Value;

            let branch_id = parse_branch_id_hex(&branch)?;

            let mut pile_reader: Pile<Blake3> = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                // Ensure indices are loaded; metadata lookups below rely on it.
                pile_reader.refresh()?;
                let reader = pile_reader
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                let mut file = std::fs::File::open(&pile)?;
                let file_len = file.metadata()?.len();

                #[derive(Clone, Debug)]
                enum Kind {
                    Set,
                    Delete,
                }

                #[derive(Clone, Debug)]
                struct Entry {
                    offset: u64,
                    kind: Kind,
                    // Branch metadata handle (the branch store value), if this is a Set.
                    meta: Option<Value<Handle<Blake3, SimpleArchive>>>,
                    meta_present: bool,
                    // Commit head stored in the branch metadata (repo::head), if readable.
                    head: Option<Value<Handle<Blake3, SimpleArchive>>>,
                    head_present: Option<bool>,
                    name: Option<String>,
                }

                let mut entries: VecDeque<Entry> = VecDeque::with_capacity(limit.max(1));

                let mut offset: u64 = 0;
                let mut buf = [0u8; RECORD_LEN as usize];
                while offset + RECORD_LEN <= file_len {
                    file.seek(SeekFrom::Start(offset))?;
                    if let Err(_) = file.read_exact(&mut buf) {
                        break;
                    }

                    let magic: [u8; 16] = buf[0..16].try_into().unwrap();
                    if magic == MAGIC_MARKER_BLOB.raw() {
                        // Skip blob payload without reading it.
                        let len = u64::from_ne_bytes(buf[24..32].try_into().unwrap());
                        let pad = blob_padding(len);
                        offset = offset
                            .checked_add(RECORD_LEN)
                            .and_then(|o| o.checked_add(len))
                            .and_then(|o| o.checked_add(pad))
                            .ok_or_else(|| anyhow::anyhow!("pile too large"))?;
                        continue;
                    }

                    if magic == MAGIC_MARKER_BRANCH.raw() {
                        let raw_id: [u8; 16] = buf[16..32].try_into().unwrap();
                        let Some(id) = Id::new(raw_id) else {
                            // Nil/invalid branch id => corrupt record; stop.
                            break;
                        };

                        if id == branch_id {
                            let raw_handle: [u8; 32] = buf[32..64].try_into().unwrap();
                            let meta: Value<Handle<Blake3, SimpleArchive>> = Value::new(raw_handle);
                            let meta_present = reader.metadata(meta)?.is_some();

                            let mut head: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
                            let mut head_present: Option<bool> = None;
                            let mut name: Option<String> = None;
                            if meta_present {
                                if let Ok(meta_set) = reader.get::<TribleSet, SimpleArchive>(meta) {
                                    name = load_branch_name(&reader, &meta_set).ok().flatten();
                                    head = extract_repo_head(&meta_set);
                                    if let Some(h) = head {
                                        head_present = Some(reader.metadata(h)?.is_some());
                                    }
                                }
                            }

                            if entries.len() == limit {
                                entries.pop_front();
                            }
                            entries.push_back(Entry {
                                offset,
                                kind: Kind::Set,
                                meta: Some(meta),
                                meta_present,
                                head,
                                head_present,
                                name,
                            });
                        }

                        offset += RECORD_LEN;
                        continue;
                    }

                    if magic == MAGIC_MARKER_BRANCH_TOMBSTONE.raw() {
                        let raw_id: [u8; 16] = buf[16..32].try_into().unwrap();
                        let Some(id) = Id::new(raw_id) else {
                            break;
                        };

                        if id == branch_id {
                            if entries.len() == limit {
                                entries.pop_front();
                            }
                            entries.push_back(Entry {
                                offset,
                                kind: Kind::Delete,
                                meta: None,
                                meta_present: false,
                                head: None,
                                head_present: None,
                                name: None,
                            });
                        }

                        offset += RECORD_LEN;
                        continue;
                    }

                    // Unknown marker or torn tail.
                    break;
                }

                // Print latest first, like git's reflog.
                for (idx, entry) in entries.iter().rev().enumerate() {
                    let offset = entry.offset;
                    let kind = match entry.kind {
                        Kind::Set => "set",
                        Kind::Delete => "delete",
                    };

                    let meta = match entry.meta {
                        None => "-".to_string(),
                        Some(h) => format!("blake3:{}", hex::encode(h.raw)),
                    };
                    let head = match entry.head {
                        None => "-".to_string(),
                        Some(h) => format!("blake3:{}", hex::encode(h.raw)),
                    };
                    let name = entry.name.as_deref().unwrap_or("-");
                    let meta_state = if entry.meta.is_none() {
                        "-"
                    } else if entry.meta_present {
                        "present"
                    } else {
                        "missing"
                    };
                    let head_state = match entry.head_present {
                        None => "-",
                        Some(true) => "present",
                        Some(false) => "missing",
                    };

                    println!(
                        "{idx}\toffset={offset}\t{kind}\tmeta={meta}\tmeta[{meta_state}]\thead={head}\thead[{head_state}]\tname={name}"
                    );
                }
                Ok(())
            })();

            let close_res = pile_reader
                .close()
                .map_err(|e| anyhow::anyhow!("close pile: {e:?}"));
            res.and(close_res)?;
        }
        Command::Journal {
            pile,
            limit,
            deleted,
        } => {
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace::prelude::valueschemas::Handle;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::trible::TribleSet;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::Value;

            let mut pile_reader: Pile<Blake3> = Pile::open(&pile)?;
            let res = (|| -> Result<(), anyhow::Error> {
                // Ensure indices are loaded; metadata lookups below rely on it.
                pile_reader.refresh()?;
                let reader = pile_reader
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                let mut file = std::fs::File::open(&pile)?;
                let file_len = file.metadata()?.len();

            #[derive(Clone, Copy, Debug, PartialEq, Eq)]
            enum Kind {
                Set,
                Delete,
            }

            #[derive(Clone, Debug)]
            struct State {
                offset: u64,
                kind: Kind,
                // Most recent metadata handle (only when kind == Set).
                meta: Option<Value<Handle<Blake3, SimpleArchive>>>,
                // Most recent Set metadata handle (kept even after tombstone).
                last_set: Option<Value<Handle<Blake3, SimpleArchive>>>,
            }

            let mut states: HashMap<Id, State> = HashMap::new();

            let mut offset: u64 = 0;
            let mut buf = [0u8; RECORD_LEN as usize];
            while offset + RECORD_LEN <= file_len {
                file.seek(SeekFrom::Start(offset))?;
                if file.read_exact(&mut buf).is_err() {
                    break;
                }

                let magic: [u8; 16] = buf[0..16].try_into().unwrap();
                if magic == MAGIC_MARKER_BLOB.raw() {
                    let len = u64::from_ne_bytes(buf[24..32].try_into().unwrap());
                    let pad = blob_padding(len);
                    offset = offset
                        .checked_add(RECORD_LEN)
                        .and_then(|o| o.checked_add(len))
                        .and_then(|o| o.checked_add(pad))
                        .ok_or_else(|| anyhow::anyhow!("pile too large"))?;
                    continue;
                }

                if magic == MAGIC_MARKER_BRANCH.raw() {
                    let raw_id: [u8; 16] = buf[16..32].try_into().unwrap();
                    let Some(id) = Id::new(raw_id) else {
                        break;
                    };
                    let raw_handle: [u8; 32] = buf[32..64].try_into().unwrap();
                    let meta: Value<Handle<Blake3, SimpleArchive>> = Value::new(raw_handle);

                    let entry = states.entry(id).or_insert(State {
                        offset,
                        kind: Kind::Set,
                        meta: Some(meta),
                        last_set: Some(meta),
                    });
                    entry.offset = offset;
                    entry.kind = Kind::Set;
                    entry.meta = Some(meta);
                    entry.last_set = Some(meta);

                    offset += RECORD_LEN;
                    continue;
                }

                if magic == MAGIC_MARKER_BRANCH_TOMBSTONE.raw() {
                    let raw_id: [u8; 16] = buf[16..32].try_into().unwrap();
                    let Some(id) = Id::new(raw_id) else {
                        break;
                    };

                    let entry = states.entry(id).or_insert(State {
                        offset,
                        kind: Kind::Delete,
                        meta: None,
                        last_set: None,
                    });
                    entry.offset = offset;
                    entry.kind = Kind::Delete;
                    entry.meta = None;

                    offset += RECORD_LEN;
                    continue;
                }

                break;
            }

            let mut rows: Vec<(Id, State)> = states.into_iter().collect();
            rows.sort_by(|(_a_id, a), (_b_id, b)| b.offset.cmp(&a.offset));

                let mut printed: usize = 0;
                for (id, state) in rows {
                    if deleted && state.kind != Kind::Delete {
                        continue;
                    }

                let meta_handle: Option<Value<Handle<Blake3, SimpleArchive>>> = match state.kind {
                    Kind::Set => state.meta,
                    Kind::Delete => state.last_set,
                };

                let mut meta_present: bool = false;
                let mut head: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
                let mut head_present: Option<bool> = None;
                let mut name: Option<String> = None;

                let meta = match meta_handle {
                    None => "-".to_string(),
                    Some(h) => {
                        meta_present = reader.metadata(h)?.is_some();
                        if meta_present {
                            if let Ok(meta_set) = reader.get::<TribleSet, SimpleArchive>(h) {
                                name = load_branch_name(&reader, &meta_set).ok().flatten();
                                head = extract_repo_head(&meta_set);
                                if let Some(ch) = head {
                                    head_present = Some(reader.metadata(ch)?.is_some());
                                }
                            }
                        }
                        format!("blake3:{}", hex::encode(h.raw))
                    }
                };

                let kind = match state.kind {
                    Kind::Set => "set",
                    Kind::Delete => "delete",
                };

                let meta_state = if meta == "-" {
                    "-"
                } else if meta_present {
                    "present"
                } else {
                    "missing"
                };

                let head_state = match head_present {
                    None => "-",
                    Some(true) => "present",
                    Some(false) => "missing",
                };

                let head = match head {
                    None => "-".to_string(),
                    Some(h) => format!("blake3:{}", hex::encode(h.raw)),
                };

                let name = name.unwrap_or_else(|| "-".to_string());

                println!("{id:X}\t{kind}\t{meta}\t{meta_state}\t{head}\t{head_state}\t{name}",);

                    printed += 1;
                    if printed >= limit {
                        break;
                    }
                }
                Ok(())
            })();

            let close_res = pile_reader
                .close()
                .map_err(|e| anyhow::anyhow!("close pile: {e:?}"));
            res.and(close_res)?;
        }
        Command::Export {
            from_pile,
            branch,
            to_pile,
        } => {
            use triblespace_core::repo;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::schemas::hash::Handle;
            use triblespace_core::value::Value;

            let bid = parse_branch_id_hex(&branch)?;

            let mut src: Pile<Blake3> = Pile::open(&from_pile)?;
            let mut dst: Pile<Blake3> = match Pile::open(&to_pile) {
                Ok(pile) => pile,
                Err(err) => {
                    let _ = src.close();
                    return Err(err.into());
                }
            };

            let res = (|| -> Result<(), anyhow::Error> {
                // Obtain the source branch metadata handle (root) and ensure it exists.
                let src_meta = src
                    .head(bid)?
                    .ok_or_else(|| anyhow::anyhow!("source branch head not found"))?;

                // Prepare a mapping from source handle raw -> destination handle for later lookup.
                use std::collections::HashMap;
                use triblespace_core::value::VALUE_LEN;
                let mut mapping: HashMap<[u8; VALUE_LEN], Value<Handle<Blake3, _>>> =
                    HashMap::new();

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
                    .ok_or_else(|| {
                        anyhow::anyhow!("destination meta handle not found after transfer")
                    })?
                    .clone();

                // Update the destination pile branch pointer to the copied meta handle.
                let old = dst.head(bid)?;
                let res = dst
                    .update(bid, old, Some(dst_meta.transmute()))
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
                Ok(())
            })();

            let close_src = src.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            let close_dst = dst.close().map_err(|e| anyhow::anyhow!("{e:?}"));

            match res {
                Ok(()) => {
                    close_src?;
                    close_dst?;
                    Ok(())
                }
                Err(err) => {
                    if let Err(close_err) = close_src {
                        eprintln!("warning: failed to close source pile cleanly: {close_err:#}");
                    }
                    if let Err(close_err) = close_dst {
                        eprintln!(
                            "warning: failed to close destination pile cleanly: {close_err:#}"
                        );
                    }
                    Err(err)
                }
            }?;
        }
        Command::Stats { pile, branch } => {
            use std::collections::{BTreeSet, HashSet};
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace::prelude::valueschemas::Handle;

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

                let branch_id = parse_branch_id_hex(&branch)?;

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
                        unioned += content;
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
            to_pile,
            to_id,
            signing_key,
        } => {
            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace_core::repo;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::repo::Repository;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::schemas::hash::Handle;
            use triblespace_core::value::Value;

            struct CopyStats {
                visited: usize,
                stored: usize,
            }

            let src_bid = parse_branch_id_hex(&from_id)?;
            let dst_bid = parse_branch_id_hex(&to_id)?;
            let key = load_signing_key(&signing_key)?;

            let mut src: Pile<Blake3> = Pile::open(&from_pile)?;
            let dst_pile: Pile<Blake3> = match Pile::open(&to_pile) {
                Ok(pile) => pile,
                Err(err) => {
                    let _ = src.close();
                    return Err(err.into());
                }
            };

            let mut repo = Repository::new(dst_pile, key);
            let result = (|| -> Result<CopyStats, anyhow::Error> {
                let src_head: Value<Handle<Blake3, SimpleArchive>> = src
                    .head(src_bid)?
                    .ok_or_else(|| anyhow::anyhow!("source branch head not found"))?;

                let src_reader = src
                    .reader()
                    .map_err(|e| anyhow::anyhow!("src pile reader error: {e:?}"))?;

                let handles = repo::reachable(&src_reader, std::iter::once(src_head.transmute()));
                let mut visited: usize = 0;
                let mut stored: usize = 0;
                for r in repo::transfer(&src_reader, repo.storage_mut(), handles) {
                    match r {
                        Ok((_src_h, _dst_h)) => {
                            visited += 1;
                            stored += 1;
                        }
                        Err(e) => return Err(anyhow::anyhow!("transfer failed: {e}")),
                    }
                }

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

                Ok(CopyStats { visited, stored })
            })();

            let close_src = src.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            let close_dst = repo
                .into_storage()
                .close()
                .map_err(|e| anyhow::anyhow!("{e:?}"));

            match result {
                Ok(stats) => {
                    close_src?;
                    close_dst?;
                    println!(
                        "merge-import: copied visited={} stored={} and attached source head to destination branch",
                        stats.visited, stats.stored
                    );
                    Ok(())
                }
                Err(err) => {
                    if let Err(close_err) = close_src {
                        eprintln!("warning: failed to close source pile cleanly: {close_err:#}");
                    }
                    if let Err(close_err) = close_dst {
                        eprintln!(
                            "warning: failed to close destination pile cleanly: {close_err:#}"
                        );
                    }
                    Err(err)
                }
            }?;
        }
        Command::Consolidate {
            pile,
            branches,
            out_name,
            dry_run,
            signing_key,
        } => {
            use std::collections::HashSet;

            use triblespace::prelude::blobschemas::SimpleArchive;
            use triblespace::prelude::BlobStorePut;
            use triblespace_core::repo::pile::Pile;
            use triblespace_core::repo::Repository;
            use triblespace_core::trible::TribleSet;
            use triblespace_core::value::schemas::hash::Blake3;
            use triblespace_core::value::schemas::hash::Hash;
            use triblespace_core::value::Value;

            // Parse branch ids before opening the pile so CLI errors don't leave files open.
            let mut seen: HashSet<Id> = HashSet::new();
            let mut branch_ids: Vec<Id> = Vec::new();
            for raw in branches {
                let bid = parse_branch_id_hex(&raw)?;
                if seen.insert(bid) {
                    branch_ids.push(bid);
                }
            }

            let key = load_signing_key(&signing_key)?;
            let pile: Pile<Blake3> = Pile::open(&pile)?;
            let mut repo = Repository::new(pile, key.clone());

            let res = (|| -> Result<(), anyhow::Error> {
                // Ensure in-memory indices are populated.
                repo.storage_mut().refresh()?;
                let reader = repo
                    .storage_mut()
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                // Attribute ids used in branch metadata.
                let repo_head_attr: triblespace_core::id::Id =
                    id_hex!("272FBC56108F336C4D2E17289468C35F");

                // Collect all branch ids and their current heads.
                let mut candidates: Vec<(Id, Option<Value<Handle<Blake3, SimpleArchive>>>)> =
                    Vec::new();
                for bid in branch_ids {
                    let meta_handle = repo
                        .storage_mut()
                        .head(bid)?
                        .ok_or_else(|| anyhow::anyhow!("branch not found: {bid:X}"))?;

                    let mut head_val: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
                    if reader.metadata(meta_handle)?.is_some() {
                        if let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(meta_handle) {
                            for t in meta.iter() {
                                if t.a() == &repo_head_attr {
                                    head_val = Some(*t.v::<Handle<Blake3, SimpleArchive>>());
                                    break;
                                }
                            }
                        }
                    }

                    candidates.push((bid, head_val));
                }

                println!("found {} branch(es)", candidates.len());
                for (bid, head) in &candidates {
                    let id_hex = format!("{bid:X}");
                    if let Some(h) = head {
                        let hh: Value<Hash<Blake3>> = Handle::to_hash(*h);
                        let hex: String = hh.from_value();
                        println!("- {id_hex} -> commit blake3:{hex}");
                    } else {
                        println!("- {id_hex} -> <no head>");
                    }
                }

                if dry_run {
                    println!("dry-run: no changes will be made");
                    return Ok(());
                }

                if candidates.len() == 1 {
                    println!("only one branch present; nothing to consolidate");
                    return Ok(());
                }

                // Collect parent commit handles (skip branches without a head).
                let parents: Vec<Value<Handle<Blake3, SimpleArchive>>> =
                    candidates.iter().filter_map(|(_, h)| *h).collect();
                if parents.is_empty() {
                    anyhow::bail!("no branch heads available to attach");
                }

                // Create a single merge commit that has all branch heads as parents.
                let commit_set = triblespace_core::repo::commit::commit_metadata(
                    &key,
                    parents.clone(),
                    None,
                    None,
                    None,
                );
                let commit_handle = repo
                    .storage_mut()
                    .put(commit_set.to_blob())
                    .map_err(|e| anyhow::anyhow!("failed to put commit blob: {e:?}"))?;

                // Decide output branch name.
                let out = out_name.unwrap_or_else(|| "consolidated".to_string());

                let new_id = *repo
                    .create_branch_with_key(&out, Some(commit_handle), key.clone())
                    .map_err(|e| anyhow::anyhow!("failed to create consolidated branch: {e:?}"))?;
                println!("created consolidated branch '{out}' with id {new_id:X}");
                Ok(())
            })();

            let close_res = repo
                .into_storage()
                .close()
                .map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
    }
    Ok(())
}

fn blob_padding(len: u64) -> u64 {
    // The pile stores blobs padded so the next record begins on a 64-byte boundary.
    let rem = len % RECORD_LEN;
    if rem == 0 {
        0
    } else {
        RECORD_LEN - rem
    }
}

fn extract_repo_head(meta: &TribleSet) -> Option<Value<Handle<Blake3, SimpleArchive>>> {
    use triblespace::prelude::blobschemas::SimpleArchive;
    use triblespace::prelude::valueschemas::Handle;
    use triblespace_core::repo;
    use triblespace_core::value::schemas::hash::Blake3;
    use triblespace_core::value::Value;

    let head_attr = repo::head.id();
    let mut head_handle: Option<Value<Handle<Blake3, SimpleArchive>>> = None;
    for t in meta.iter() {
        if t.a() == &head_attr {
            let h: Value<Handle<Blake3, SimpleArchive>> = *t.v();
            if head_handle.replace(h).is_some() {
                // Multiple heads -> ambiguous.
                return None;
            }
        }
    }
    head_handle
}

fn parse_branch_id_hex(s: &str) -> Result<Id> {
    let raw = hex::decode(s).map_err(|e| anyhow::anyhow!("branch id hex decode failed: {e}"))?;
    let raw: [u8; 16] = raw
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("branch id must be 16 bytes (32 hex chars)"))?;
    Id::new(raw).ok_or_else(|| anyhow::anyhow!("branch id cannot be nil"))
}

fn parse_blake3_handle(s: &str) -> Result<Value<Handle<Blake3, SimpleArchive>>> {
    let s = s.trim();
    let hex = match s.split_once(':') {
        Some((proto, rest)) => {
            if proto.eq_ignore_ascii_case("blake3") {
                rest
            } else {
                return Err(anyhow::anyhow!("unsupported handle protocol: {proto}"));
            }
        }
        None => s,
    };

    let raw = hex::decode(hex).map_err(|e| anyhow::anyhow!("handle hex decode failed: {e}"))?;
    let raw: [u8; 32] = raw
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("handle must be 32 bytes (64 hex chars)"))?;
    Ok(Value::new(raw))
}

fn parse_blake3_handle_opt(s: &str) -> Result<Option<Value<Handle<Blake3, SimpleArchive>>>> {
    let s = s.trim();
    if s == "-" || s.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    Ok(Some(parse_blake3_handle(s)?))
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
