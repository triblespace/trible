//! Distributed pile sync over iroh.
//!
//! The sync protocol is blob-centric: a pile is a set of content-addressed
//! blobs, and sync is set reconciliation.  The server has two operations:
//!
//! ```text
//! HEAD <branch>       → HEAD <branch> <hash>  (or NONE)
//! GET_BLOB <hash>     → BLOB <hash> <len>\n<data>  (or MISSING)
//! ```
//!
//! The client drives a BFS over the CAS graph: fetch a blob, scan it for
//! 32-byte references, check which ones are local, fetch the missing ones.
//! The walk stops when all referenced blobs are already local — making it
//! inherently incremental without understanding commits or branches.

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use anybytes::Bytes;
use clap::Parser;
use futures::TryStreamExt;
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::endpoint::{Connection, presets};
use iroh::Endpoint;
use iroh_base::{EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::{Gossip, TopicId};
use iroh_gossip::api::Event as GossipEvent;
use triblespace_core::blob::ToBlob;
use triblespace_core::blob::TryFromBlob;
use triblespace_core::blob::Blob;
use triblespace_core::blob::schemas::UnknownBlob;
use triblespace_core::blob::schemas::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::repo::{BlobStore, BlobStoreGet, BlobStorePut, BranchStore, Repository};
use triblespace_core::repo::pile::Pile;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::Value;
use triblespace_core::value::schemas::hash::{Blake3, Handle, Hash};
use ed25519_dalek::SigningKey;

use super::signing::load_or_create_pile_key;

const PILE_SYNC_ALPN: &[u8] = b"/triblespace/pile-sync/1";
const VALUE_LEN: usize = 32;

/// Well-known faculty branch names announced to the swarm.
const ANNOUNCE_BRANCHES: &[&str] = &[
    "compass", "wiki", "local-messages", "relations", "files",
];

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
            .accept(PILE_SYNC_ALPN, BlobServer { pile_path, signing_key: key })
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
            .accept(PILE_SYNC_ALPN, BlobServer { pile_path: pile_path.clone(), signing_key: key.clone() })
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        // Pre-warm connections so gossip can find peers.
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

        if std::env::var("RUST_LOG").is_ok() {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_writer(std::io::stderr)
                .try_init();
        }

        let mut timer = tokio::time::interval(std::time::Duration::from_secs(15));

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => { break; }
                _ = timer.tick() => {
                    for name in ANNOUNCE_BRANCHES {
                        let head = branch_head_hash(&pile_path, name);
                        let msg = format!("HEAD {name} {} {public}", head.unwrap_or_else(|| "NONE".into()));
                        let _ = topic.broadcast(msg.into_bytes().into()).await;
                    }
                }
                event = topic.try_next() => {
                    match event {
                        Ok(Some(GossipEvent::NeighborUp(peer))) => {
                            eprintln!("  peer joined: {}", peer.fmt_short());
                            for name in ANNOUNCE_BRANCHES {
                                let head = branch_head_hash(&pile_path, name);
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

                            // Parse: HEAD <branch> <hash|NONE> <node_id>
                            if let Some(rest) = text.strip_prefix("HEAD ") {
                                let parts: Vec<&str> = rest.split_whitespace().collect();
                                if parts.len() >= 2 {
                                    let branch_name = parts[0];
                                    let remote_head = parts[1];

                                    if remote_head == "NONE" { continue; }

                                    // Check: do we already have this blob locally?
                                    let already_have = if let Ok(mut pile) = open_pile(&pile_path) {
                                        let has = has_blob_hex(&mut pile, remote_head);
                                        let _ = pile.close();
                                        has
                                    } else { false };

                                    if already_have { continue; } // up to date

                                    eprintln!("  [{}] '{}' has new head {}", from.fmt_short(), branch_name, &remote_head[..12]);

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

// ── Blob-centric sync logic ──────────────────────────────────────────

/// Sync a branch by recursively fetching missing blobs from a remote.
///
/// 1. Ask remote for HEAD hash of the branch
/// 2. BFS over the CAS graph: for each blob hash, check local → fetch if missing
/// 3. After all blobs are local, checkout + diff + commit
async fn sync_branch(
    conn: &Connection,
    pile_path: &PathBuf,
    signing_key: &SigningKey,
    branch_name: &str,
) -> Result<String> {
    // Ask for remote head.
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| anyhow!("open_bi: {e}"))?;
    send_line(&mut send, &format!("HEAD {branch_name}")).await?;

    let response = recv_line(&mut recv).await?;
    let remote_head_hex = if let Some(rest) = response.strip_prefix("HEAD ") {
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() >= 2 && parts[1] != "NONE" {
            parts[1].to_string()
        } else {
            return Ok(format!("'{branch_name}': remote has no head"));
        }
    } else {
        return Err(anyhow!("unexpected response: {response}"));
    };

    // BFS over the CAS graph: fetch missing blobs, scan for references, repeat.
    // Stops when all referenced blobs are already local — this is inherently
    // incremental without needing to understand commits or branches.
    let mut pile = open_pile(pile_path)?;
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut fetched = 0usize;
    let mut fetched_bytes = 0usize;

    queue.push_back(remote_head_hex.clone());

    while let Some(hash_hex) = queue.pop_front() {
        if !visited.insert(hash_hex.clone()) { continue; }
        if has_blob_hex(&mut pile, &hash_hex) { continue; }

        // Fetch the blob.
        send_line(&mut send, &format!("GET_BLOB {hash_hex}")).await?;
        let response = recv_line(&mut recv).await?;

        if let Some(rest) = response.strip_prefix("BLOB ") {
            let parts: Vec<&str> = rest.splitn(2, ' ').collect();
            if parts.len() == 2 {
                let len: usize = parts[1].parse().map_err(|e| anyhow!("bad len: {e}"))?;
                let data = recv_exact(&mut recv, len).await?;

                // Conservative reference scan: every 32-byte chunk is a candidate.
                for chunk in data.chunks(VALUE_LEN) {
                    if chunk.len() == VALUE_LEN {
                        let candidate = hex::encode(chunk);
                        if !visited.contains(&candidate) {
                            queue.push_back(candidate);
                        }
                    }
                }

                let bytes: Bytes = data.into();
                let _: Value<Handle<Blake3, UnknownBlob>> = pile.put::<UnknownBlob, Bytes>(bytes)
                    .map_err(|e| anyhow!("put blob: {e:?}"))?;
                fetched += 1;
                fetched_bytes += len;
            }
        }
        // MISSING → false positive from conservative scan, skip.
    }

    // Now all blobs are local. Read the remote's data from the commit chain.
    //
    // The remote head is a branch-meta blob (SimpleArchive). It contains a
    // `head` trible pointing to the commit blob. The commit blob contains a
    // `content` trible pointing to the data blob (the actual TribleSet).
    //
    // We use the `content` attribute from triblespace_core::repo to find
    // the data handle in the commit metadata.
    let reader = BlobStore::<Blake3>::reader(&mut pile)
        .map_err(|e| anyhow!("reader: {e:?}"))?;

    // Walk: branch-meta → head commit → content data.
    let head_hash = hex::decode(&remote_head_hex).map_err(|e| anyhow!("hex: {e}"))?;
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&head_hash);
    let branch_meta_handle = Value::<Handle<Blake3, SimpleArchive>>::new(arr);

    // Read branch meta → find `head` attribute → read commit → find `content` → read data.
    let branch_meta: TribleSet = reader.get(branch_meta_handle)
        .map_err(|e| anyhow!("read branch meta: {e:?}"))?;

    // Find the head commit handle in the branch meta.
    use triblespace_core::macros::{find, pattern};

    // The branch meta has: { branch_entity @ repo::head: <commit_handle> }
    let commit_handle: Value<Handle<Blake3, SimpleArchive>> = find!(
        h: Value<Handle<Blake3, SimpleArchive>>,
        pattern!(&branch_meta, [{ _?e @ triblespace_core::repo::head: ?h }])
    ).next().ok_or_else(|| anyhow!("no head in branch meta"))?;

    // Read commit meta → find `content` → read data TribleSet.
    let commit_meta: TribleSet = reader.get(commit_handle)
        .map_err(|e| anyhow!("read commit meta: {e:?}"))?;

    let data_handle: Value<Handle<Blake3, SimpleArchive>> = find!(
        h: Value<Handle<Blake3, SimpleArchive>>,
        pattern!(&commit_meta, [{ _?e @ triblespace_core::repo::content: ?h }])
    ).next().ok_or_else(|| anyhow!("no content in commit meta"))?;

    let remote_facts: TribleSet = reader.get(data_handle)
        .map_err(|e| anyhow!("read data: {e:?}"))?;

    drop(reader);

    // Now merge: diff remote facts against local, commit delta.
    let mut repo = Repository::new(pile, signing_key.clone(), TribleSet::new())
        .map_err(|e| anyhow!("repo: {e:?}"))?;
    let branch_id = repo.ensure_branch(branch_name, None)
        .map_err(|e| anyhow!("ensure branch: {e:?}"))?;
    let mut ws = repo.pull(branch_id).map_err(|e| anyhow!("pull: {e:?}"))?;
    let local_facts = ws.checkout(..).map_err(|e| anyhow!("checkout: {e:?}"))?.into_facts();

    let delta = remote_facts.difference(&local_facts);
    let n_new = delta.len();
    if !delta.is_empty() {
        ws.commit(delta, "sync: pull");
        repo.try_push(&mut ws).map_err(|e| anyhow!("push: {e:?}"))?;
    }
    let _ = repo.close();

    send_line(&mut send, "DONE").await?;
    send.finish().map_err(|e| anyhow!("finish: {e}"))?;

    Ok(format!("'{branch_name}': {fetched} blobs ({fetched_bytes}B), {n_new} new tribles"))
}

// ── Blob server (protocol handler) ───────────────────────────────────

#[derive(Debug, Clone)]
struct BlobServer {
    pile_path: PathBuf,
    signing_key: SigningKey,
}

impl ProtocolHandler for BlobServer {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let pile_path = self.pile_path.clone();
        let signing_key = self.signing_key.clone();

        let result: Result<()> = async {
            let (mut send, mut recv) = connection.accept_bi().await
                .map_err(|e| anyhow!("accept_bi: {e}"))?;

            let mut pile = open_pile(&pile_path)?;

            loop {
                let line = match recv_line(&mut recv).await {
                    Ok(l) if !l.is_empty() => l,
                    _ => break,
                };

                if line == "DONE" {
                    break;
                } else if let Some(branch_name) = line.strip_prefix("HEAD ") {
                    let branch_name = branch_name.trim();
                    let head = branch_head_hash(&pile_path, branch_name);
                    let response = match head {
                        Some(h) => format!("HEAD {branch_name} {h}"),
                        None => format!("HEAD {branch_name} NONE"),
                    };
                    send_line(&mut send, &response).await?;
                } else if let Some(hash_hex) = line.strip_prefix("GET_BLOB ") {
                    let hash_hex = hash_hex.trim();
                    match get_blob_bytes(&mut pile, hash_hex) {
                        Some(data) => {
                            send_line(&mut send, &format!("BLOB {hash_hex} {}", data.len())).await?;
                            send.write_all(&data).await.map_err(|e| anyhow!("write: {e}"))?;
                        }
                        None => {
                            send_line(&mut send, &format!("MISSING {hash_hex}")).await?;
                        }
                    }
                }
            }

            send.finish().map_err(|e| anyhow!("finish: {e}"))?;
            pile.close().map_err(|e| anyhow!("close: {e:?}"))?;
            Ok(())
        }.await;

        if let Err(e) = &result {
            let peer = connection.remote_id().fmt_short();
            eprintln!("  handler error [{peer}]: {e}");
        }
        connection.closed().await;
        Ok(())
    }
}

// ── Pile helpers ─────────────────────────────────────────────────────

fn open_pile(path: &PathBuf) -> Result<Pile<Blake3>> {
    let mut pile = Pile::<Blake3>::open(path).map_err(|e| anyhow!("open: {e:?}"))?;
    pile.restore().map_err(|e| anyhow!("restore: {e:?}"))?;
    Ok(pile)
}

fn has_blob_hex(pile: &mut Pile<Blake3>, hash_hex: &str) -> bool {
    let Ok(bytes) = hex::decode(hash_hex) else { return false; };
    if bytes.len() != 32 { return false; }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    let handle = Value::<Handle<Blake3, UnknownBlob>>::new(arr);
    let Ok(reader) = BlobStore::<Blake3>::reader(pile) else { return false; };
    reader.get::<Bytes, UnknownBlob>(handle).is_ok()
}

fn get_blob_bytes(pile: &mut Pile<Blake3>, hash_hex: &str) -> Option<Vec<u8>> {
    let bytes = hex::decode(hash_hex).ok()?;
    if bytes.len() != 32 { return None; }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    let handle = Value::<Handle<Blake3, UnknownBlob>>::new(arr);
    let reader = BlobStore::<Blake3>::reader(pile).ok()?;
    reader.get::<Bytes, UnknownBlob>(handle).ok().map(|b| b.to_vec())
}

/// Resolve branch name → ID using a temporary repo, then get the head hash.
fn branch_head_hash(pile_path: &PathBuf, name: &str) -> Option<String> {
    let mut pile = open_pile(pile_path).ok()?;
    let key = SigningKey::from_bytes(&[0u8; 32]);
    let mut repo = Repository::new(pile, key, TribleSet::new()).ok()?;
    let branch_id = repo.ensure_branch(name, None).ok()?;
    let head = repo.storage_mut().head(branch_id).ok()??;
    let hex = hex::encode(head.raw);
    let _ = repo.close();
    Some(hex)
}

// ── Wire helpers ─────────────────────────────────────────────────────

async fn send_line(send: &mut iroh::endpoint::SendStream, msg: &str) -> Result<()> {
    let mut buf = msg.as_bytes().to_vec();
    buf.push(b'\n');
    send.write_all(&buf).await.map_err(|e| anyhow!("send: {e}"))
}

async fn recv_line(recv: &mut iroh::endpoint::RecvStream) -> Result<String> {
    let mut buf = Vec::with_capacity(256);
    loop {
        let mut byte = [0u8; 1];
        match recv.read_exact(&mut byte).await {
            Ok(()) => {
                if byte[0] == b'\n' { return Ok(String::from_utf8_lossy(&buf).into_owned()); }
                buf.push(byte[0]);
                if buf.len() > 1024 * 1024 { return Err(anyhow!("line too long")); }
            }
            Err(_) => {
                return Ok(String::from_utf8_lossy(&buf).into_owned());
            }
        }
    }
}

async fn recv_exact(recv: &mut iroh::endpoint::RecvStream, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await.map_err(|e| anyhow!("recv({len}): {e}"))?;
    Ok(buf)
}

// ── Key helpers ──────────────────────────────────────────────────────

fn iroh_secret(key: &SigningKey) -> SecretKey { SecretKey::from(key.to_bytes()) }
