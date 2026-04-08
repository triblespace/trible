//! CLI commands for distributed pile sync over iroh.
//!
//! Thin wrappers around `triblespace_net` — argument parsing and
//! progress output only. All protocol logic lives in the library.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use iroh::protocol::Router;
use iroh::endpoint::presets;
use iroh::Endpoint;
use iroh_base::EndpointId;
use iroh_gossip::{Gossip, TopicId};
use iroh_gossip::api::Event as GossipEvent;
use futures::TryStreamExt;

use triblespace_net::identity::{load_or_create_pile_key, iroh_secret};
use triblespace_net::protocol::PILE_SYNC_ALPN;
use triblespace_net::server::PileBlobServer;
use triblespace_net::sync::sync_branch;

// ── CLI ──────────────────────────────────────────────────────────────

#[derive(Parser)]
pub enum Command {
    /// Show this pile's network identity.
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
    let key = load_or_create_pile_key(&sk, &pile_path)?;
    let public = iroh_secret(&key).public();
    println!("node: {public}");
    Ok(())
}

// ── Up ───────────────────────────────────────────────────────────────

fn run_up(pile_path: PathBuf, sk: Option<PathBuf>) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let key = load_or_create_pile_key(&sk, &pile_path)?;
        let secret = iroh_secret(&key);
        let public = secret.public();

        let ep = Endpoint::builder(presets::N0).secret_key(secret).bind().await
            .map_err(|e| anyhow!("bind: {e}"))?;

        let router = Router::builder(ep.clone())
            .accept(PILE_SYNC_ALPN, PileBlobServer { pile_path })
            .spawn();

        eprintln!("node: {public}");
        eprintln!("listening... (Ctrl-C to stop)");
        tokio::signal::ctrl_c().await?;
        router.shutdown().await.map_err(|e| anyhow!("shutdown: {e}"))
    })
}

// ── Pull ─────────────────────────────────────────────────────────────

fn run_pull(pile_path: PathBuf, remote: String, branch: String, sk: Option<PathBuf>) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let key = load_or_create_pile_key(&sk, &pile_path)?;
        let ep = Endpoint::builder(presets::N0).secret_key(iroh_secret(&key)).bind().await
            .map_err(|e| anyhow!("bind: {e}"))?;

        let remote_key: iroh_base::PublicKey = remote.parse()
            .map_err(|e| anyhow!("bad node ID: {e}"))?;

        eprintln!("connecting to {}...", remote_key.fmt_short());
        let conn = ep.connect(remote_key, PILE_SYNC_ALPN).await
            .map_err(|e| anyhow!("connect: {e}"))?;

        let stats = sync_branch(&conn, &pile_path, &key, &branch).await?;
        eprintln!("{stats}");

        conn.close(0u32.into(), b"done");
        ep.close().await;
        Ok(())
    })
}

// ── Live ─────────────────────────────────────────────────────────────

/// Well-known branch names announced to the gossip swarm.
const ANNOUNCE_BRANCHES: &[&str] = &[
    "compass", "wiki", "local-messages", "relations", "files",
];

fn run_live(pile_path: PathBuf, topic_name: String, peer_strs: Vec<String>, sk: Option<PathBuf>) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let key = load_or_create_pile_key(&sk, &pile_path)?;
        let secret = iroh_secret(&key);
        let public = secret.public();

        let topic_id = TopicId::from_bytes(*blake3::hash(topic_name.as_bytes()).as_bytes());
        let bootstrap: Vec<EndpointId> = peer_strs.iter()
            .filter_map(|s| s.parse::<iroh_base::PublicKey>().ok().map(EndpointId::from))
            .collect();

        let ep = Endpoint::builder(presets::N0).secret_key(secret).bind().await
            .map_err(|e| anyhow!("bind: {e}"))?;
        ep.online().await;

        let gossip = Gossip::builder().spawn(ep.clone());
        let router = Router::builder(ep.clone())
            .accept(PILE_SYNC_ALPN, PileBlobServer { pile_path: pile_path.clone() })
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        // Pre-warm connections.
        for peer in &bootstrap {
            if let Ok(conn) = ep.connect(*peer, PILE_SYNC_ALPN).await {
                conn.close(0u32.into(), b"warmup");
            }
        }

        let mut topic = if bootstrap.is_empty() {
            gossip.subscribe(topic_id, bootstrap).await
        } else {
            gossip.subscribe_and_join(topic_id, bootstrap).await
        }.map_err(|e| anyhow!("subscribe: {e}"))?;

        eprintln!("node: {public}");
        eprintln!("topic: {topic_name}");
        eprintln!("live sync active. (Ctrl-C to stop)\n");

        // Periodic announcements.
        let mut timer = tokio::time::interval(std::time::Duration::from_secs(15));

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => { break; }
                _ = timer.tick() => {
                    for name in ANNOUNCE_BRANCHES {
                        let head = branch_head_hex(&pile_path, name);
                        let msg = format!("HEAD {name} {} {public}", head.unwrap_or_else(|| "NONE".into()));
                        let _ = topic.broadcast(msg.into_bytes().into()).await;
                    }
                }
                event = topic.try_next() => {
                    match event {
                        Ok(Some(GossipEvent::NeighborUp(peer))) => {
                            eprintln!("  peer joined: {}", peer.fmt_short());
                            for name in ANNOUNCE_BRANCHES {
                                let head = branch_head_hex(&pile_path, name);
                                let msg = format!("HEAD {name} {} {public}", head.unwrap_or_else(|| "NONE".into()));
                                let _ = topic.broadcast(msg.into_bytes().into()).await;
                            }
                        }
                        Ok(Some(GossipEvent::NeighborDown(peer))) => {
                            eprintln!("  peer left: {}", peer.fmt_short());
                        }
                        Ok(Some(GossipEvent::Received(msg))) => {
                            let text = String::from_utf8_lossy(&msg.content);
                            let from: EndpointId = msg.delivered_from.into();
                            if let Some(rest) = text.strip_prefix("HEAD ") {
                                let parts: Vec<&str> = rest.split_whitespace().collect();
                                if parts.len() >= 2 {
                                    let branch_name = parts[0];
                                    let remote_head = parts[1];
                                    if remote_head == "NONE" { continue; }

                                    // Check locally.
                                    let already_have = if let Ok(hash_bytes) = hex::decode(remote_head) {
                                        if hash_bytes.len() == 32 {
                                            let mut h = [0u8; 32];
                                            h.copy_from_slice(&hash_bytes);
                                            if let Ok(mut pile) = triblespace_core::repo::pile::Pile::<triblespace_core::value::schemas::hash::Blake3>::open(&pile_path).map_err(|e| anyhow!("open: {e:?}")) {
                                                let r = has_blob(&mut pile, &h);
                                                let _ = pile.close();
                                                r
                                            } else { false }
                                        } else { false }
                                    } else { false };

                                    if already_have { continue; }

                                    eprintln!("  [{}] '{}' new head {}", from.fmt_short(), branch_name, &remote_head[..12]);
                                    match ep.connect(from, PILE_SYNC_ALPN).await {
                                        Ok(conn) => {
                                            match sync_branch(&conn, &pile_path, &key, branch_name).await {
                                                Ok(stats) => eprintln!("  {stats}"),
                                                Err(e) => eprintln!("  sync error: {e}"),
                                            }
                                            conn.close(0u32.into(), b"done");
                                        }
                                        Err(e) => eprintln!("  connect error: {e}"),
                                    }
                                }
                            }
                        }
                        Ok(Some(GossipEvent::Lagged)) => eprintln!("  warning: lagged"),
                        Ok(None) => break,
                        Err(e) => eprintln!("  gossip error: {e}"),
                    }
                }
            }
        }

        router.shutdown().await.map_err(|e| anyhow!("shutdown: {e}"))
    })
}

// ── DHT ──────────────────────────────────────────────────────────────

fn run_dht(pile_path: PathBuf, bootstrap_strs: Vec<String>, sk: Option<PathBuf>) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let key = load_or_create_pile_key(&sk, &pile_path)?;
        let secret = iroh_secret(&key);
        let public = secret.public();
        let my_id: EndpointId = public.into();

        let bootstrap: Vec<EndpointId> = bootstrap_strs.iter()
            .filter_map(|s| s.parse::<iroh_base::PublicKey>().ok().map(EndpointId::from))
            .collect();

        let ep = Endpoint::builder(presets::N0).secret_key(secret).bind().await
            .map_err(|e| anyhow!("bind: {e}"))?;
        ep.online().await;

        let dht_alpn = iroh_dht::rpc::ALPN;
        let pool = iroh_blobs::util::connection_pool::ConnectionPool::new(
            ep.clone(), dht_alpn,
            iroh_blobs::util::connection_pool::Options {
                max_connections: 64,
                idle_timeout: std::time::Duration::from_secs(30),
                connect_timeout: std::time::Duration::from_secs(10),
                on_connected: None,
            },
        );
        let iroh_pool = iroh_dht::pool::IrohPool::new(ep.clone(), pool);
        let (rpc, api) = iroh_dht::create_node(my_id, iroh_pool.clone(), bootstrap, Default::default());
        iroh_pool.set_self_client(Some(rpc.downgrade()));

        let dht_sender = rpc.inner().as_local().expect("local sender");
        let router = Router::builder(ep.clone())
            .accept(PILE_SYNC_ALPN, PileBlobServer { pile_path: pile_path.clone() })
            .accept(dht_alpn, irpc_iroh::IrohProtocol::with_sender(dht_sender))
            .spawn();

        eprintln!("node: {public}");

        // Announce all local blobs.
        let mut pile = triblespace_core::repo::pile::Pile::<triblespace_core::value::schemas::hash::Blake3>::open(&pile_path).map_err(|e| anyhow!("open: {e:?}"))?;
        let reader = triblespace_core::repo::BlobStore::<triblespace_core::value::schemas::hash::Blake3>::reader(&mut pile)
            .map_err(|e| anyhow!("reader: {e:?}"))?;
        let mut announced = 0usize;
        for item in reader.iter() {
            if let Ok((handle, _blob)) = item {
                let hash = blake3::Hash::from_bytes(handle.raw);
                if let Ok(_) = api.announce_provider(hash, my_id).await {
                    announced += 1;
                }
            }
        }
        let _ = pile.close();

        eprintln!("announced {announced} blobs to DHT");
        eprintln!("listening... (Ctrl-C to stop)");
        tokio::signal::ctrl_c().await?;
        router.shutdown().await.map_err(|e| anyhow!("shutdown: {e}"))
    })
}

// ── Helpers ──────────────────────────────────────────────────────────

fn branch_head_hex(pile_path: &PathBuf, name: &str) -> Option<String> {
    use triblespace_core::repo::BranchStore;
    let pile = triblespace_core::repo::pile::Pile::<triblespace_core::value::schemas::hash::Blake3>::open(pile_path).ok()?;
    let key = ed25519_dalek::SigningKey::from_bytes(&[0u8; 32]);
    let mut repo = triblespace_core::repo::Repository::new(pile, key, triblespace_core::trible::TribleSet::new()).ok()?;
    let branch_id = repo.ensure_branch(name, None).ok()?;
    let head = repo.storage_mut().head(branch_id).ok()??;
    let hex = hex::encode(head.raw);
    let _ = repo.close();
    Some(hex)
}

fn has_blob(pile: &mut triblespace_core::repo::pile::Pile<triblespace_core::value::schemas::hash::Blake3>, hash: &[u8; 32]) -> bool {
    use triblespace_core::repo::{BlobStore, BlobStoreGet};
    use triblespace_core::blob::schemas::UnknownBlob;
    use triblespace_core::value::Value;
    use triblespace_core::value::schemas::hash::{Blake3, Handle};
    let handle = Value::<Handle<Blake3, UnknownBlob>>::new(*hash);
    let Ok(reader) = BlobStore::<Blake3>::reader(pile) else { return false; };
    reader.get::<anybytes::Bytes, UnknownBlob>(handle).is_ok()
}
