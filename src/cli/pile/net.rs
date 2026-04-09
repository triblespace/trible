//! CLI commands for distributed pile sync.

use std::path::PathBuf;
use std::collections::HashMap;

use anyhow::{Result, anyhow};
use clap::Parser;
use iroh_base::EndpointId;

use triblespace_net::host::{self, HostConfig};
use triblespace_net::leader::Leader;
use triblespace_net::follower::Follower;
use triblespace_net::identity::load_or_create_key;

type Pile = triblespace_core::repo::pile::Pile<triblespace_core::value::schemas::hash::Blake3>;

fn open_pile(path: &PathBuf) -> Result<Pile> {
    Pile::open(path).map_err(|e| anyhow!("open pile: {e:?}"))
}

fn parse_peers(strs: &[String]) -> Vec<EndpointId> {
    strs.iter()
        .filter_map(|s| s.parse::<iroh_base::PublicKey>().ok().map(EndpointId::from))
        .collect()
}

fn key_dir(pile_path: &PathBuf) -> &std::path::Path {
    pile_path.parent().unwrap_or(pile_path.as_ref())
}

/// Extract branch name + commit handle from a branch metadata blob.
fn read_branch_meta(
    store: &mut impl triblespace_core::repo::BlobStore<triblespace_core::value::schemas::hash::Blake3>,
    head_hash: &[u8; 32],
) -> Option<(
    String,
    triblespace_core::value::Value<triblespace_core::value::schemas::hash::Handle<
        triblespace_core::value::schemas::hash::Blake3,
        triblespace_core::blob::schemas::simplearchive::SimpleArchive,
    >>,
)> {
    use triblespace_core::repo::BlobStoreGet;
    use triblespace_core::value::Value;
    type SA = triblespace_core::blob::schemas::simplearchive::SimpleArchive;
    type LS = triblespace_core::blob::schemas::longstring::LongString;
    type B3 = triblespace_core::value::schemas::hash::Blake3;
    type H<S> = triblespace_core::value::schemas::hash::Handle<B3, S>;

    let reader = store.reader().ok()?;
    let meta: triblespace_core::trible::TribleSet = reader.get(Value::<H<SA>>::new(*head_hash)).ok()?;

    use triblespace_core::macros::{find, pattern};
    let nh: Value<H<LS>> = find!(h: Value<H<LS>>, pattern!(&meta, [{ _?e @ triblespace_core::metadata::name: ?h }])).next()?;
    let name: anybytes::View<str> = reader.get(nh).ok()?;
    let commit: Value<H<SA>> = find!(h: Value<H<SA>>, pattern!(&meta, [{ _?e @ triblespace_core::repo::head: ?h }])).next()?;

    Some((name.as_ref().to_string(), commit))
}

// ── CLI ──────────────────────────────────────────────────────────────

#[derive(Parser)]
pub enum Command {
    /// Show this node's network identity.
    Identity {
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// Sync with peers. No topic = serve only. With topic = live bidirectional sync.
    Sync {
        pile: PathBuf,
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// One-shot pull a branch from a remote peer.
    Pull {
        pile: PathBuf,
        remote: String,
        #[arg(long)]
        branch: String,
        #[arg(long)]
        key: Option<PathBuf>,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::Identity { key } => run_identity(key),
        Command::Sync { pile, peers, topic, key } => {
            run_sync(pile, peers, topic, key)
        }
        Command::Pull { pile, remote, branch, key } => {
            run_pull(pile, remote, branch, key)
        }
    }
}

// ── Identity ─────────────────────────────────────────────────────────

fn run_identity(sk: Option<PathBuf>) -> Result<()> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let key = load_or_create_key(&sk, &cwd)?;
    let public = triblespace_net::identity::iroh_secret(&key).public();
    println!("node: {public}");
    Ok(())
}

// ── Sync ─────────────────────────────────────────────────────────────

fn run_sync(pile_path: PathBuf, peer_strs: Vec<String>, topic: Option<String>, key_path: Option<PathBuf>) -> Result<()> {
    use triblespace_core::repo::Repository;

    let key = load_or_create_key(&key_path, key_dir(&pile_path))?;
    let peers = parse_peers(&peer_strs);
    let pile = open_pile(&pile_path)?;

    let (sender, receiver) = host::spawn(key.clone(), HostConfig {
        peers,
        gossip_topic: topic.clone(),
    });
    let leader = Leader::new(pile, sender.clone());
    let mut follower = Follower::new(leader, receiver);

    // Seed snapshot for serving.
    if let Some(snap) = triblespace_net::host::StoreSnapshot::from_store(&mut follower) {
        for (branch, head) in &snap.branches {
            sender.gossip(*branch, *head);
        }
        sender.update_snapshot(snap);
    }

    eprintln!("node: {}", sender.id());
    if let Some(ref t) = topic {
        eprintln!("topic: {t}");
        eprintln!("live sync active. (Ctrl-C to stop)\n");
    } else {
        eprintln!("serving. (Ctrl-C to stop)");
    }

    let mut merged_heads: HashMap<[u8; 16], [u8; 32]> = HashMap::new();
    let mut last_announce = std::time::Instant::now();

    loop {
        let n = follower.poll();

        // Re-announce heads periodically.
        if topic.is_some() && last_announce.elapsed() > std::time::Duration::from_secs(10) {
            if let Some(snap) = triblespace_net::host::StoreSnapshot::from_store(&mut follower) {
                for (branch, head) in &snap.branches {
                    sender.gossip(*branch, *head);
                }
                sender.update_snapshot(snap);
            }
            last_announce = std::time::Instant::now();
        }

        // Auto-merge incoming remote heads.
        if n > 0 {
            let heads: Vec<_> = follower.remote_heads().iter().map(|(b, h)| (*b, *h)).collect();
            for (branch_bytes, head_hash) in heads {
                if merged_heads.get(&branch_bytes) == Some(&head_hash) { continue; }
                let Some(remote_branch_id) = triblespace_core::id::Id::new(branch_bytes) else { continue; };
                let Some((name, _)) = read_branch_meta(follower.store_mut(), &head_hash) else { continue; };

                let merge_result = (|| -> Result<()> {
                    let pile = open_pile(&pile_path)?;
                    let mut repo = Repository::new(pile, key.clone(), triblespace_core::trible::TribleSet::new())
                        .map_err(|e| anyhow!("repo: {e:?}"))?;
                    let local_id = repo.ensure_branch(&name, None).map_err(|_| anyhow!("ensure branch"))?;

                    let remote_ws = repo.pull(remote_branch_id).map_err(|e| anyhow!("pull remote: {e:?}"))?;
                    let Some(remote_commit) = remote_ws.head() else { return Ok(()); };

                    let mut local_ws = repo.pull(local_id).map_err(|e| anyhow!("pull local: {e:?}"))?;

                    // Skip if remote commit is already our head or an ancestor.
                    if let Some(local_head) = local_ws.head() {
                        if local_head == remote_commit {
                            return Ok(()); // Same commit — nothing to merge.
                        }
                        // Check if remote is an ancestor of local.
                        use triblespace_core::repo::{ancestors, CommitSelector};
                        let all_ancestors = ancestors(local_head)
                            .select(&mut local_ws)
                            .map_err(|e| anyhow!("ancestors: {e:?}"))?;
                        if all_ancestors.get(&remote_commit.raw).is_some() {
                            return Ok(()); // Remote is already an ancestor — skip.
                        }
                    }

                    local_ws.merge_commit(remote_commit).map_err(|e| anyhow!("merge: {e:?}"))?;
                    repo.push(&mut local_ws).map_err(|_| anyhow!("push"))?;
                    let _ = repo.into_storage().close();
                    Ok(())
                })();

                match merge_result {
                    Ok(()) => {
                        merged_heads.insert(branch_bytes, head_hash);
                        eprintln!("  merged '{name}'");
                        if let Some(snap) = triblespace_net::host::StoreSnapshot::from_store(&mut follower) {
                            sender.update_snapshot(snap);
                        }
                    }
                    Err(e) => eprintln!("  merge error '{name}': {e}"),
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

// ── Pull ─────────────────────────────────────────────────────────────

fn run_pull(pile_path: PathBuf, remote: String, branch: String, key_path: Option<PathBuf>) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let key = load_or_create_key(&key_path, key_dir(&pile_path))?;
        let ep = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
            .secret_key(triblespace_net::identity::iroh_secret(&key))
            .bind().await.map_err(|e| anyhow!("bind: {e}"))?;

        let remote_key: iroh_base::PublicKey = remote.parse()
            .map_err(|e| anyhow!("bad node ID: {e}"))?;

        eprintln!("connecting to {}...", remote_key.fmt_short());
        let conn = ep.connect(remote_key, triblespace_net::protocol::PILE_SYNC_ALPN).await
            .map_err(|e| anyhow!("connect: {e}"))?;

        let (remote_id, _) = triblespace_net::protocol::resolve_branch_name(&conn, &branch).await?
            .ok_or_else(|| anyhow!("branch '{branch}' not found"))?;
        let branch_id_bytes: [u8; 16] = remote_id.into();

        // Fetch via Host (gets DHT for free).
        let (sender, receiver) = host::spawn(key.clone(), HostConfig::default());
        let leader = Leader::new(open_pile(&pile_path)?, sender.clone());
        let mut follower = Follower::new(leader, receiver);

        sender.fetch(remote_key.into(), branch_id_bytes);
        eprintln!("syncing...");

        // Poll until HEAD arrives.
        loop {
            follower.poll();
            if follower.remote_head_raw(&branch_id_bytes).is_some() { break; }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        let head_hash = follower.remote_head_raw(&branch_id_bytes).unwrap();
        let Some((name, _)) = read_branch_meta(follower.store_mut(), &head_hash) else {
            return Err(anyhow!("can't read branch metadata"));
        };

        // Merge.
        use triblespace_core::repo::Repository;
        let pile = open_pile(&pile_path)?;
        let mut repo = Repository::new(pile, key.clone(), triblespace_core::trible::TribleSet::new())
            .map_err(|e| anyhow!("repo: {e:?}"))?;
        let local_id = repo.ensure_branch(&name, None).map_err(|_| anyhow!("ensure branch"))?;
        let remote_ws = repo.pull(remote_id).map_err(|e| anyhow!("pull remote: {e:?}"))?;
        let Some(remote_commit) = remote_ws.head() else {
            return Err(anyhow!("remote has no commit"));
        };
        let mut local_ws = repo.pull(local_id).map_err(|e| anyhow!("pull local: {e:?}"))?;
        local_ws.merge_commit(remote_commit).map_err(|e| anyhow!("merge: {e:?}"))?;
        repo.push(&mut local_ws).map_err(|_| anyhow!("push"))?;
        let _ = repo.into_storage().close();

        eprintln!("merged '{name}'");
        conn.close(0u32.into(), b"done");
        ep.close().await;
        Ok(())
    })
}
