//! CLI commands for distributed pile sync.

use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::Parser;
use iroh_base::EndpointId;

use triblespace_net::peer::{Peer, PeerConfig};
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

fn run_sync(pile_path: PathBuf, peer_strs: Vec<String>, topic: Option<String>, key_path: Option<PathBuf>) -> Result<()> {
    use triblespace_core::repo::Repository;

    let key = load_or_create_key(&key_path, key_dir(&pile_path))?;
    let peers = parse_peers(&peer_strs);

    // Single pile handle, wrapped in a Peer (which spawns the iroh thread)
    // and then a Repository for the workspace/commit API. Reads on the Peer
    // auto-drain incoming gossip + auto-publish external writes; writes
    // auto-publish via the network thread.
    let pile = open_pile(&pile_path)?;
    let peer = Peer::new(pile, key.clone(), PeerConfig {
        peers,
        gossip_topic: topic.clone(),
    });
    let mut repo = Repository::new(peer, key.clone(), triblespace_core::trible::TribleSet::new())
        .map_err(|e| anyhow!("repo: {e:?}"))?;

    eprintln!("node: {}", repo.storage().id());
    if let Some(ref t) = topic {
        eprintln!("topic: {t}");
        eprintln!("live sync active. (Ctrl-C to stop)\n");
    } else {
        eprintln!("serving. (Ctrl-C to stop)");
    }

    // Initial broadcast so peers connecting later can learn our state.
    repo.storage_mut().republish_branches();
    let mut last_announce = std::time::Instant::now();

    loop {
        // Periodic re-broadcast: helps newly-joined gossip neighbors learn
        // about us. iroh-gossip dedupes identical messages so re-publishing
        // the same state is cheap.
        if topic.is_some() && last_announce.elapsed() > std::time::Duration::from_secs(10) {
            repo.storage_mut().republish_branches();
            last_announce = std::time::Instant::now();
        }

        // Auto-merge: walk the tracking branches in the pile and merge each
        // into its same-named local branch. The Peer auto-refreshes on every
        // read (drains gossip + diffs external writes), so list_tracking_branches
        // always sees the latest state. The ancestor checks inside merge_commit
        // handle dedup.
        let tracks = triblespace_net::tracking::list_tracking_branches(repo.storage_mut());
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
                    return Ok(false);
                }
                repo.push(&mut local_ws).map_err(|_| anyhow!("push"))?;
                Ok(true)
            })();

            match merge_result {
                Ok(true) => eprintln!("  merged '{name}'"),
                Ok(false) => { /* up-to-date, no-op */ }
                Err(e) => eprintln!("  merge error '{name}': {e}"),
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

        // Resolve the branch name on the remote via a one-shot connection.
        // This runs before we spin up the Peer's persistent network thread.
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
        conn.close(0u32.into(), b"done");
        ep.close().await;

        // Spin up the Peer for the fetch + merge. Pull-only mode
        // (gossip_topic: None) — no flood subscription, just direct fetch
        // + DHT.
        use triblespace_core::repo::Repository;
        let pile = open_pile(&pile_path)?;
        let peer = Peer::new(pile, key.clone(), PeerConfig::default());
        let mut repo = Repository::new(peer, key.clone(), triblespace_core::trible::TribleSet::new())
            .map_err(|e| anyhow!("repo: {e:?}"))?;

        repo.storage().fetch(remote_key.into(), branch_id_bytes);
        eprintln!("syncing...");

        // Spin until the Peer has materialized a tracking branch for the
        // remote. find_tracking_branch reads via branches() which
        // auto-refreshes the Peer (drains incoming gossip events).
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

        let name = triblespace_net::tracking::list_tracking_branches(repo.storage_mut())
            .into_iter()
            .find(|info| info.local_id == tracking_id)
            .ok_or_else(|| anyhow!("tracking branch vanished"))?
            .remote_name;

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
        // Repository<Peer<Pile>> → unwrap layers and close the pile.
        let _ = repo.into_storage().into_store().close();

        eprintln!("merged '{name}'");
        Ok(())
    })
}
