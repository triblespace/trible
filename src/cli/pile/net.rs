//! CLI commands for distributed pile sync.

use std::path::PathBuf;

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

/// Snapshot the store's non-tracking branches and gossip them.
///
/// Tracking branches are local mirror state — they're created by the
/// Follower from incoming gossip and shouldn't be re-announced or they'd
/// echo back to the publisher and create extra tracking branches.
fn publish_non_tracking_branches<S>(store: &mut S, sender: &triblespace_net::host::HostSender)
where
    S: triblespace_core::repo::BlobStore<triblespace_core::value::schemas::hash::Blake3>
        + triblespace_core::repo::BranchStore<triblespace_core::value::schemas::hash::Blake3>,
{
    let Some(snap) = triblespace_net::host::StoreSnapshot::from_store(store) else { return };
    for (branch, head) in &snap.branches {
        if let Some(bid) = triblespace_core::id::Id::new(*branch) {
            if triblespace_net::tracking::is_tracking_branch(store, bid) {
                continue;
            }
        }
        sender.gossip(*branch, *head);
    }
    sender.update_snapshot(snap);
}

fn run_sync(pile_path: PathBuf, peer_strs: Vec<String>, topic: Option<String>, key_path: Option<PathBuf>) -> Result<()> {
    use triblespace_core::repo::Repository;

    let key = load_or_create_key(&key_path, key_dir(&pile_path))?;
    let peers = parse_peers(&peer_strs);

    let (sender, receiver) = host::spawn(key.clone(), HostConfig {
        peers,
        gossip_topic: topic.clone(),
    });
    // Single pile handle, layered:
    //   Repository<Follower<Leader<Pile>>>
    // Leader gossips writes; Follower drains incoming gossip events into the
    // SAME pile via `Poll`; Repository is what the merge code drives.
    // `repo.poll()` cascades through every layer in one call.
    let pile = open_pile(&pile_path)?;
    let leader = Leader::new(pile, sender.clone());
    let follower = Follower::new(leader, receiver);
    let mut repo = Repository::new(follower, key.clone(), triblespace_core::trible::TribleSet::new())
        .map_err(|e| anyhow!("repo: {e:?}"))?;

    publish_non_tracking_branches(repo.storage_mut(), &sender);

    eprintln!("node: {}", sender.id());
    if let Some(ref t) = topic {
        eprintln!("topic: {t}");
        eprintln!("live sync active. (Ctrl-C to stop)\n");
    } else {
        eprintln!("serving. (Ctrl-C to stop)");
    }

    let mut last_announce = std::time::Instant::now();

    loop {
        // Re-announce heads periodically.
        if topic.is_some() && last_announce.elapsed() > std::time::Duration::from_secs(10) {
            publish_non_tracking_branches(repo.storage_mut(), &sender);
            last_announce = std::time::Instant::now();
        }

        // Auto-merge: walk the tracking branches in the pile and merge each
        // into its same-named local branch. The Follower auto-drains the
        // gossip channel on every read (just like Pile auto-refreshes from
        // disk), so list_tracking_branches always sees the latest state.
        // The ancestor checks inside merge_commit handle dedup.
        let tracks = triblespace_net::tracking::list_tracking_branches(repo.storage_mut());
        let mut any_merged = false;
        for info in tracks {
            let triblespace_net::tracking::TrackingBranchInfo {
                local_id: tracking_id,
                remote_name: name,
                ..
            } = info;

            let merge_result = (|| -> Result<bool> {
                let local_id = repo.ensure_branch(&name, None).map_err(|_| anyhow!("ensure branch"))?;
                let remote_ws = repo.pull(tracking_id).map_err(|e| anyhow!("pull tracking: {e:?}"))?;
                let Some(remote_commit) = remote_ws.head() else { return Ok(false); };

                let mut local_ws = repo.pull(local_id).map_err(|e| anyhow!("pull local: {e:?}"))?;
                let prev_head = local_ws.head();
                let new_head = local_ws.merge_commit(remote_commit)
                    .map_err(|e| anyhow!("merge: {e:?}"))?;
                if Some(new_head) == prev_head {
                    // No-op (already up to date) — nothing to push.
                    return Ok(false);
                }
                repo.push(&mut local_ws).map_err(|_| anyhow!("push"))?;
                Ok(true)
            })();

            match merge_result {
                Ok(true) => {
                    eprintln!("  merged '{name}'");
                    any_merged = true;
                }
                Ok(false) => { /* up-to-date, no-op */ }
                Err(e) => eprintln!("  merge error '{name}': {e}"),
            }
        }

        if any_merged {
            publish_non_tracking_branches(repo.storage_mut(), &sender);
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

        // Single pile handle, layered Repository<Follower<Leader<Pile>>>.
        // `repo.poll()` cascades through every layer.
        use triblespace_core::repo::Repository;
        let (sender, receiver) = host::spawn(key.clone(), HostConfig::default());
        let pile = open_pile(&pile_path)?;
        let leader = Leader::new(pile, sender.clone());
        let follower = Follower::new(leader, receiver);
        let mut repo = Repository::new(follower, key.clone(), triblespace_core::trible::TribleSet::new())
            .map_err(|e| anyhow!("repo: {e:?}"))?;

        sender.fetch(remote_key.into(), branch_id_bytes);
        eprintln!("syncing...");

        // Spin until the Follower has materialized a tracking branch for the
        // remote. find_tracking_branch iterates `branches()`, which auto-
        // drains the gossip channel via Follower's read path — no explicit
        // poll needed.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let tracking_id = loop {
            if let Some(id) = triblespace_net::tracking::find_tracking_branch(
                repo.storage_mut(), remote_id,
            ) {
                break id;
            }
            if std::time::Instant::now() > deadline {
                return Err(anyhow!("timed out waiting for remote HEAD"));
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        };

        // Read the remote name from the tracking branch metadata.
        let name = triblespace_net::tracking::list_tracking_branches(repo.storage_mut())
            .into_iter()
            .find(|info| info.local_id == tracking_id)
            .ok_or_else(|| anyhow!("tracking branch vanished"))?
            .remote_name;

        // Merge through the Repository (same pile as the Follower wrote to).
        // `merge_commit` decides between no-op / fast-forward / merge commit.
        let local_id = repo.ensure_branch(&name, None).map_err(|_| anyhow!("ensure branch"))?;
        let remote_ws = repo.pull(tracking_id).map_err(|e| anyhow!("pull tracking: {e:?}"))?;
        let Some(remote_commit) = remote_ws.head() else {
            return Err(anyhow!("remote has no commit"));
        };
        let mut local_ws = repo.pull(local_id).map_err(|e| anyhow!("pull local: {e:?}"))?;
        let prev_head = local_ws.head();
        let new_head = local_ws.merge_commit(remote_commit)
            .map_err(|e| anyhow!("merge: {e:?}"))?;
        if Some(new_head) != prev_head {
            repo.push(&mut local_ws).map_err(|_| anyhow!("push"))?;
        }
        // Repository<Follower<Leader<Pile>>> → unwrap layers and close the pile.
        let _ = repo.into_storage().into_store().into_store().close();

        eprintln!("merged '{name}'");
        conn.close(0u32.into(), b"done");
        ep.close().await;
        Ok(())
    })
}
