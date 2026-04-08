//! CLI commands for distributed pile sync over iroh.
//!
//! Thin wrappers around `triblespace_net` — argument parsing and
//! progress output only. All protocol logic lives in the library.

use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::Parser;
use iroh::endpoint::presets;
use iroh::Endpoint;
use iroh_base::EndpointId;

use triblespace_net::identity::{load_or_create_key, iroh_secret};
use triblespace_net::node::Host;
use triblespace_net::protocol::PILE_SYNC_ALPN;

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
        pile: PathBuf,
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Start as a network node, serving blob requests.
    Up {
        pile: PathBuf,
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Pull a branch from a remote peer.
    Pull {
        pile: PathBuf,
        remote: String,
        #[arg(long)]
        branch: String,
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Live gossip sync with peers.
    Live {
        pile: PathBuf,
        #[arg(long, default_value = "triblespace")]
        topic: String,
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Join the DHT and announce/discover blobs.
    Dht {
        pile: PathBuf,
        #[arg(long, value_delimiter = ',')]
        bootstrap: Vec<String>,
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::Identity { pile, signing_key } => run_identity(pile, signing_key),
        Command::Up { pile, signing_key } => run_up(pile, signing_key),
        Command::Pull { pile, remote, branch, signing_key } => {
            run_pull(pile, remote, branch, signing_key)
        }
        Command::Live { pile, topic, peers, signing_key } => {
            run_live(pile, topic, peers, signing_key)
        }
        Command::Dht { pile, bootstrap, signing_key } => {
            run_dht(pile, bootstrap, signing_key)
        }
    }
}

// ── Identity ─────────────────────────────────────────────────────────

fn run_identity(pile_path: PathBuf, sk: Option<PathBuf>) -> Result<()> {
    let key = load_or_create_key(&sk, key_dir(&pile_path))?;
    println!("node: {}", iroh_secret(&key).public());
    Ok(())
}

// ── Up ───────────────────────────────────────────────────────────────

fn run_up(pile_path: PathBuf, sk: Option<PathBuf>) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let key = load_or_create_key(&sk, key_dir(&pile_path))?;
        let pile = open_pile(&pile_path)?;

        let node = Host::builder(pile, key).build().await?;

        eprintln!("node: {}", node.id());
        eprintln!("listening... (Ctrl-C to stop)");
        node.run_until_ctrl_c().await
    })
}

// ── Pull ─────────────────────────────────────────────────────────────

fn run_pull(pile_path: PathBuf, remote: String, branch: String, sk: Option<PathBuf>) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let key = load_or_create_key(&sk, key_dir(&pile_path))?;
        let ep = Endpoint::builder(presets::N0).secret_key(iroh_secret(&key)).bind().await
            .map_err(|e| anyhow!("bind: {e}"))?;

        let remote_key: iroh_base::PublicKey = remote.parse()
            .map_err(|e| anyhow!("bad node ID: {e}"))?;

        eprintln!("connecting to {}...", remote_key.fmt_short());
        let conn = ep.connect(remote_key, PILE_SYNC_ALPN).await
            .map_err(|e| anyhow!("connect: {e}"))?;

        // Resolve branch name → id on remote.
        let (remote_id, _) = triblespace_net::sync::resolve_branch_name(&conn, &branch).await?
            .ok_or_else(|| anyhow!("branch '{}' not found on remote", branch))?;

        // Repository wraps Follower wraps Pile.
        let pile = open_pile(&pile_path)?;
        let follower = triblespace_net::follower::Follower::new(pile, conn.clone());
        let mut repo = triblespace_core::repo::Repository::new(follower, key.clone(), triblespace_core::trible::TribleSet::new())
            .map_err(|e| anyhow!("repo: {e:?}"))?;
        let local_id = repo.ensure_branch(&branch, None)
            .map_err(|_| anyhow!("ensure branch failed"))?;

        // Pull blobs from remote.
        let remote_head: [u8; 16] = remote_id.into();
        let remote_head_hash = triblespace_net::protocol::op_head(&conn, &remote_head).await?
            .ok_or_else(|| anyhow!("remote has no head for this branch"))?;
        let stats = repo.storage_mut().pull_reachable(&remote_head_hash).await?;
        eprintln!("{stats}");

        // Regular merge — repo doesn't know it's wrapping a Follower.
        use triblespace_core::repo::{BlobStore, BlobStoreGet};
        use triblespace_core::value::Value;
        let remote_commit = {
            let reader = BlobStore::<triblespace_core::value::schemas::hash::Blake3>::reader(repo.storage_mut()).map_err(|e| anyhow!("reader: {e:?}"))?;
            let branch_meta_handle = Value::<triblespace_core::value::schemas::hash::Handle<triblespace_core::value::schemas::hash::Blake3, triblespace_core::blob::schemas::simplearchive::SimpleArchive>>::new(remote_head_hash);
            let branch_meta: triblespace_core::trible::TribleSet = reader.get(branch_meta_handle)
                .map_err(|e| anyhow!("read meta: {e:?}"))?;
            use triblespace_core::macros::{find, pattern};
            find!(
                h: Value<triblespace_core::value::schemas::hash::Handle<triblespace_core::value::schemas::hash::Blake3, triblespace_core::blob::schemas::simplearchive::SimpleArchive>>,
                pattern!(&branch_meta, [{ _?e @ triblespace_core::repo::head: ?h }])
            ).next().ok_or_else(|| anyhow!("no head commit in branch metadata"))?
        };
        let mut ws = repo.pull(local_id).map_err(|e| anyhow!("pull: {e:?}"))?;
        ws.merge_commit(remote_commit).map_err(|e| anyhow!("merge: {e:?}"))?;
        repo.push(&mut ws).map_err(|_| anyhow!("push failed"))?;

        let follower = repo.into_storage();
        let pile = follower.into_store();
        pile.close().map_err(|e| anyhow!("close: {e:?}"))?;
        conn.close(0u32.into(), b"done");
        ep.close().await;
        Ok(())
    })
}

// ── Live ─────────────────────────────────────────────────────────────

fn run_live(pile_path: PathBuf, topic: String, peer_strs: Vec<String>, sk: Option<PathBuf>) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let key = load_or_create_key(&sk, key_dir(&pile_path))?;
        let peers = parse_peers(&peer_strs);
        let pile = open_pile(&pile_path)?;

        let node = Host::builder(pile, key)
            .gossip(&topic, peers)
            .build().await?;

        eprintln!("node: {}", node.id());
        eprintln!("topic: {topic}");
        eprintln!("live sync active. (Ctrl-C to stop)\n");
        node.run_until_ctrl_c().await
    })
}

// ── DHT ──────────────────────────────────────────────────────────────

fn run_dht(pile_path: PathBuf, bootstrap_strs: Vec<String>, sk: Option<PathBuf>) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let key = load_or_create_key(&sk, key_dir(&pile_path))?;
        let bootstrap = parse_peers(&bootstrap_strs);
        let pile = open_pile(&pile_path)?;

        let node = Host::builder(pile, key)
            .dht(bootstrap)
            .build().await?;

        eprintln!("node: {}", node.id());
        eprintln!("listening... (Ctrl-C to stop)");
        node.run_until_ctrl_c().await
    })
}
