use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::path::PathBuf;

use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::BlobStore;
use triblespace::prelude::BlobStoreGet;
use triblespace::prelude::BranchStore;
use triblespace::prelude::View;
use triblespace_core::id::Id;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::Repository;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::Blake3;
use triblespace_core::value::schemas::hash::Handle;
use triblespace_core::value::schemas::hash::Hash;
use triblespace_core::value::Value;

use super::signing::load_signing_key;

type CommitHandle = Value<Handle<Blake3, triblespace::prelude::blobschemas::SimpleArchive>>;

#[derive(Debug, Clone)]
struct BranchInfo {
    id: Id,
    name: Option<String>,
    head: Option<CommitHandle>,
}

struct BranchIndex {
    by_id: HashMap<Id, BranchInfo>,
    by_name: HashMap<String, Id>,
    ambiguous_names: HashSet<String>,
}

impl BranchIndex {
    fn new(infos: Vec<BranchInfo>) -> Self {
        let mut index = Self {
            by_id: HashMap::new(),
            by_name: HashMap::new(),
            ambiguous_names: HashSet::new(),
        };

        for info in infos {
            index.insert(info);
        }

        index
    }

    fn insert(&mut self, info: BranchInfo) {
        if let Some(name) = info.name.clone() {
            if self.by_name.contains_key(&name) {
                self.by_name.remove(&name);
                self.ambiguous_names.insert(name);
            } else if !self.ambiguous_names.contains(&name) {
                self.by_name.insert(name, info.id);
            }
        }
        self.by_id.insert(info.id, info);
    }

    fn info(&self, id: Id) -> Option<&BranchInfo> {
        self.by_id.get(&id)
    }
}

#[derive(Debug, Clone)]
enum BranchRef {
    Id(Id),
    Name(String),
}

fn parse_branch_ref(raw: &str) -> Result<BranchRef> {
    let raw = raw.trim();
    if raw.len() == 32 && raw.chars().all(|c| c.is_ascii_hexdigit()) {
        let bytes = hex::decode(raw)?;
        let arr: [u8; 16] = bytes.as_slice().try_into()?;
        let id = Id::new(arr).ok_or_else(|| anyhow::anyhow!("bad id"))?;
        Ok(BranchRef::Id(id))
    } else {
        Ok(BranchRef::Name(raw.to_string()))
    }
}

fn load_branch_info(pile: &mut Pile<Blake3>) -> Result<Vec<BranchInfo>> {
    use triblespace::prelude::blobschemas::SimpleArchive;

    let reader = pile
        .reader()
        .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
    let iter = pile
        .branches()
        .map_err(|e| anyhow::anyhow!("list branches: {e:?}"))?;

    let name_attr = triblespace_core::metadata::name.id();
    let head_attr = triblespace_core::repo::head.id();

    let mut out = Vec::new();
    for branch in iter {
        let branch_id = branch.map_err(|e| anyhow::anyhow!("branch id: {e:?}"))?;
        let Some(meta_handle) = pile
            .head(branch_id)
            .map_err(|e| anyhow::anyhow!("branch head: {e:?}"))?
        else {
            out.push(BranchInfo {
                id: branch_id,
                name: None,
                head: None,
            });
            continue;
        };

        let meta: TribleSet = reader
            .get::<TribleSet, SimpleArchive>(meta_handle)
            .map_err(|e| anyhow::anyhow!("branch metadata: {e:?}"))?;

        let mut name: Option<String> = None;
        let mut head: Option<CommitHandle> = None;

        for t in meta.iter() {
            if t.a() == &name_attr {
                if name.is_some() {
                    bail!("branch {branch_id:X} has multiple name values");
                }
                let handle: Value<Handle<Blake3, LongString>> = *t.v();
                let view: View<str> = reader
                    .get(handle)
                    .map_err(|e| anyhow::anyhow!("branch name blob: {e:?}"))?;
                name = Some(view.to_string());
            } else if t.a() == &head_attr {
                if head.is_some() {
                    bail!("branch {branch_id:X} has multiple heads");
                }
                head = Some(*t.v::<Handle<Blake3, SimpleArchive>>());
            }
        }

        out.push(BranchInfo {
            id: branch_id,
            name,
            head,
        });
    }

    Ok(out)
}

fn commit_hex(handle: CommitHandle) -> String {
    let hash: Value<Hash<Blake3>> = Handle::to_hash(handle);
    hash.from_value()
}

struct ResolvedSource {
    label: String,
    head: Option<CommitHandle>,
}

pub fn run(
    pile_path: PathBuf,
    target: String,
    sources: Vec<String>,
    signing_key: Option<PathBuf>,
) -> Result<()> {
    let key = load_signing_key(&signing_key)?;
    let pile: Pile<Blake3> = Pile::open(&pile_path)?;
    let mut repo = Repository::new(pile, key);

    let res = (|| -> Result<(), anyhow::Error> {
        repo.storage_mut()
            .refresh()
            .map_err(|e| anyhow::anyhow!("refresh pile: {e:?}"))?;

        let mut index = BranchIndex::new(load_branch_info(repo.storage_mut())?);

        let target_ref = parse_branch_ref(&target)?;
        let (mut target_id_opt, mut target_name_opt) = match target_ref.clone() {
            BranchRef::Id(id) => {
                if index.info(id).is_none() {
                    bail!("target branch not found: {id:X}");
                }
                (Some(id), None)
            }
            BranchRef::Name(name) => {
                if index.ambiguous_names.contains(&name) {
                    bail!("branch name '{name}' is ambiguous; use the branch id instead");
                }
                match index.by_name.get(&name).copied() {
                    Some(id) => (Some(id), None),
                    None => (None, Some(name)),
                }
            }
        };

        let target_display = match target_ref {
            BranchRef::Id(id) => format!("{id:X}"),
            BranchRef::Name(name) => name,
        };

        let mut resolved_sources = Vec::new();
        let mut missing = Vec::new();
        let mut ambiguous = Vec::new();
        let mut seen = HashSet::new();

        for raw in sources {
            let source_ref = parse_branch_ref(&raw)?;
            let (id, label) = match source_ref {
                BranchRef::Id(id) => {
                    let Some(info) = index.info(id) else {
                        missing.push(format!("{id:X}"));
                        continue;
                    };
                    let label = info
                        .name
                        .clone()
                        .map(|name| format!("{name} ({id:X})"))
                        .unwrap_or_else(|| format!("{id:X}"));
                    (id, label)
                }
                BranchRef::Name(name) => {
                    if index.ambiguous_names.contains(&name) {
                        ambiguous.push(name);
                        continue;
                    }
                    match index.by_name.get(&name).copied() {
                        Some(id) => (id, name),
                        None => {
                            missing.push(name);
                            continue;
                        }
                    }
                }
            };

            if let Some(target_id) = target_id_opt {
                if id == target_id {
                    bail!("source branch matches target branch");
                }
            }

            if !seen.insert(id) {
                continue;
            }

            let info = index
                .info(id)
                .ok_or_else(|| anyhow::anyhow!("branch not found: {id:X}"))?;

            resolved_sources.push(ResolvedSource {
                label,
                head: info.head,
            });
        }

        if !ambiguous.is_empty() {
            ambiguous.sort();
            bail!("ambiguous branch name(s): {}", ambiguous.join(", "));
        }

        if !missing.is_empty() {
            missing.sort();
            bail!("unknown branch(es): {}", missing.join(", "));
        }

        let target_head = target_id_opt.and_then(|id| index.info(id).and_then(|info| info.head));
        let mut merged_branches = Vec::new();
        let mut empty_branches = Vec::new();
        let mut unique_heads = Vec::new();
        let mut seen_heads = HashSet::new();

        for source in resolved_sources {
            let Some(head) = source.head else {
                empty_branches.push(source.label);
                continue;
            };

            if Some(head) == target_head {
                continue;
            }

            merged_branches.push((source.label, head));
            if seen_heads.insert(head) {
                unique_heads.push(head);
            }
        }

        if unique_heads.is_empty() {
            println!("No source heads to merge (all selected branches are empty).");
            return Ok(());
        }

        let target_id = match target_id_opt {
            Some(id) => id,
            None => {
                let name = target_name_opt
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("target branch name missing"))?;
                let branch_id = repo
                    .create_branch(&name, None)
                    .map_err(|e| anyhow::anyhow!("create target branch: {e:?}"))?
                    .release();
                index.insert(BranchInfo {
                    id: branch_id,
                    name: Some(name),
                    head: None,
                });
                branch_id
            }
        };

        let unique_count = unique_heads.len();
        let mut ws = repo
            .pull(target_id)
            .map_err(|e| anyhow::anyhow!("pull target branch: {e:?}"))?;

        for head in unique_heads {
            ws.merge_commit(head)
                .map_err(|e| anyhow::anyhow!("merge failed: {e:?}"))?;
        }

        repo.push(&mut ws)
            .map_err(|e| anyhow::anyhow!("push failed: {e:?}"))?;

        println!(
            "Updated {}:{} with {} merged head(s) from {} branch(es)",
            pile_path.display(),
            target_display,
            unique_count,
            merged_branches.len()
        );

        for (label, head) in merged_branches {
            println!("- {label} head=blake3:{}", commit_hex(head));
        }

        if !empty_branches.is_empty() {
            empty_branches.sort();
            println!(
                "Skipped {} branch(es) with no head: {}",
                empty_branches.len(),
                empty_branches.join(", ")
            );
        }

        Ok(())
    })();

    let close_res = repo
        .into_storage()
        .close()
        .map_err(|e| anyhow::anyhow!("{e:?}"));

    res.and(close_res)?;
    Ok(())
}
