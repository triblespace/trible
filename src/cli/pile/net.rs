//! CLI commands for distributed pile sync over iroh.
//!
//! Thin wrappers around Host / Leader / Follower.

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
    let public = triblespace_net::identity::iroh_secret(&key).public();
    println!("node: {public}");
    Ok(())
}

// ── Up ───────────────────────────────────────────────────────────────

fn run_up(pile_path: PathBuf, sk: Option<PathBuf>) -> Result<()> {
    let key = load_or_create_key(&sk, key_dir(&pile_path))?;
    let pile = open_pile(&pile_path)?;

    let (sender, _receiver) = host::spawn(key.clone(), HostConfig::default());
    let mut leader = Leader::new(pile, sender.clone());

    // Seed the snapshot so Host can serve immediately.
    if let Some(snap) = triblespace_net::host::StoreSnapshot::from_store(&mut leader) {
        sender.update_snapshot(snap);
    }

    eprintln!("node: {}", sender.id());
    eprintln!("listening... (Ctrl-C to stop)");
    loop { std::thread::sleep(std::time::Duration::from_secs(1)); }
}

// ── Pull ─────────────────────────────────────────────────────────────

fn run_pull(pile_path: PathBuf, remote: String, branch: String, sk: Option<PathBuf>) -> Result<()> {
    let key = load_or_create_key(&sk, key_dir(&pile_path))?;
    let pile = open_pile(&pile_path)?;

    let remote_key: iroh_base::PublicKey = remote.parse()
        .map_err(|e| anyhow!("bad node ID: {e}"))?;
    let remote_id: EndpointId = remote_key.into();

    // Resolve branch name → id via the sync module (needs async for now).
    let rt = tokio::runtime::Runtime::new()?;
    let branch_id_bytes = rt.block_on(async {
        let ep = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
            .secret_key(triblespace_net::identity::iroh_secret(&key))
            .bind().await.map_err(|e| anyhow!("bind: {e}"))?;
        let conn = ep.connect(remote_key, triblespace_net::protocol::PILE_SYNC_ALPN).await
            .map_err(|e| anyhow!("connect: {e}"))?;
        let (id, _) = triblespace_net::protocol::resolve_branch_name(&conn, &branch).await?
            .ok_or_else(|| anyhow!("branch '{branch}' not found"))?;
        let id_bytes: [u8; 16] = id.into();
        conn.close(0u32.into(), b"done");
        Ok::<_, anyhow::Error>(id_bytes)
    })?;

    // Use Host + Follower for the sync.
    let (sender, receiver) = host::spawn(key.clone(), HostConfig::default());
    let leader = Leader::new(pile, sender.clone());
    let mut follower = Follower::new(leader, receiver);

    // Request fetch.
    sender.fetch(remote_id, branch_id_bytes);

    eprintln!("connecting to {}...", remote_key.fmt_short());

    // Poll until we get the HEAD event.
    loop {
        let n = follower.poll();
        if follower.remote_head_raw(&branch_id_bytes).is_some() {
            break;
        }
        if n == 0 {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    let remote_head = follower.remote_head_raw(&branch_id_bytes).unwrap();
    eprintln!("synced blobs");

    // Merge using Repository.
    use triblespace_core::repo::{BlobStore, BlobStoreGet, Repository};
    use triblespace_core::value::Value;
    let local_branch_id = {
        let mut repo = Repository::new(follower, key.clone(), triblespace_core::trible::TribleSet::new())
            .map_err(|e| anyhow!("repo: {e:?}"))?;
        let local_id = repo.ensure_branch(&branch, None)
            .map_err(|_| anyhow!("ensure branch"))?;

        let remote_commit = {
            let reader = BlobStore::<triblespace_core::value::schemas::hash::Blake3>::reader(repo.storage_mut())
                .map_err(|e| anyhow!("reader: {e:?}"))?;
            let meta_handle = Value::<triblespace_core::value::schemas::hash::Handle<
                triblespace_core::value::schemas::hash::Blake3,
                triblespace_core::blob::schemas::simplearchive::SimpleArchive,
            >>::new(remote_head);
            let meta: triblespace_core::trible::TribleSet = reader.get(meta_handle)
                .map_err(|e| anyhow!("read meta: {e:?}"))?;
            use triblespace_core::macros::{find, pattern};
            find!(
                h: Value<triblespace_core::value::schemas::hash::Handle<
                    triblespace_core::value::schemas::hash::Blake3,
                    triblespace_core::blob::schemas::simplearchive::SimpleArchive,
                >>,
                pattern!(&meta, [{ _?e @ triblespace_core::repo::head: ?h }])
            ).next().ok_or_else(|| anyhow!("no head commit"))?
        };

        let mut ws = repo.pull(local_id).map_err(|e| anyhow!("pull: {e:?}"))?;
        ws.merge_commit(remote_commit).map_err(|e| anyhow!("merge: {e:?}"))?;
        repo.push(&mut ws).map_err(|_| anyhow!("push"))?;
        let follower = repo.into_storage();
        let leader = follower.into_store();
        let pile = leader.into_store();
        pile.close().map_err(|e| anyhow!("close: {e:?}"))?;
        local_id
    };

    eprintln!("merged '{branch}'");
    Ok(())
}

// ── Live ─────────────────────────────────────────────────────────────

fn run_live(pile_path: PathBuf, topic: String, peer_strs: Vec<String>, sk: Option<PathBuf>) -> Result<()> {
    let key = load_or_create_key(&sk, key_dir(&pile_path))?;
    let peers = parse_peers(&peer_strs);
    let pile = open_pile(&pile_path)?;

    let (sender, receiver) = host::spawn(key, HostConfig {
        gossip_topic: Some(topic.clone()),
        gossip_peers: peers,
        ..Default::default()
    });
    let leader = Leader::new(pile, sender.clone());
    let mut follower = Follower::new(leader, receiver);

    eprintln!("node: {}", sender.id());
    eprintln!("topic: {topic}");
    eprintln!("live sync active. (Ctrl-C to stop)\n");

    loop {
        let n = follower.poll();
        if n > 0 {
            for (branch, head) in follower.remote_heads() {
                eprintln!("  remote head: {} = {}", hex::encode(branch), hex::encode(&head[..8]));
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

// ── DHT ──────────────────────────────────────────────────────────────

fn run_dht(pile_path: PathBuf, bootstrap_strs: Vec<String>, sk: Option<PathBuf>) -> Result<()> {
    let key = load_or_create_key(&sk, key_dir(&pile_path))?;
    let bootstrap = parse_peers(&bootstrap_strs);
    let pile = open_pile(&pile_path)?;

    let (sender, _receiver) = host::spawn(key, HostConfig {
        dht_bootstrap: bootstrap,
        ..Default::default()
    });
    let _leader = Leader::new(pile, sender.clone());

    eprintln!("node: {}", sender.id());
    eprintln!("listening... (Ctrl-C to stop)");

    loop { std::thread::sleep(std::time::Duration::from_secs(1)); }
}
