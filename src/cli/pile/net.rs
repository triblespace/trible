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
                        let head = branch_head_raw(&pile_path, name).map(|h| hex::encode(h));
                        let msg = format!("HEAD {name} {} {public}", head.unwrap_or_else(|| "NONE".into()));
                        let _ = topic.broadcast(msg.into_bytes().into()).await;
                    }
                }
                event = topic.try_next() => {
                    match event {
                        Ok(Some(GossipEvent::NeighborUp(peer))) => {
                            eprintln!("  peer joined: {}", peer.fmt_short());
                            for name in ANNOUNCE_BRANCHES {
                                let head = branch_head_raw(&pile_path, name).map(|h| hex::encode(h));
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
                                    let already_have = if let Ok(hash_bytes) = hex::decode(remote_head) {
                                        if hash_bytes.len() == 32 {
                                            let mut h = [0u8; 32];
                                            h.copy_from_slice(&hash_bytes);
                                            if let Ok(mut pile) = open_pile(&pile_path) {
                                                let r = has_blob(&mut pile, &h);
                                                let _ = pile.close();
                                                r
                                            } else { false }
                                        } else { false }
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
    // List remote branches, then GET_BLOB each head to read the name.
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| anyhow!("open_bi: {e}"))?;

    send_req_list(&mut send).await?;
    let mut remote_branches: Vec<([u8; 16], RawHash)> = Vec::new();
    loop {
        let rsp = recv_u8(&mut recv).await?;
        match rsp {
            RSP_LIST_ENTRY => {
                let id = recv_branch_id(&mut recv).await?;
                let head = recv_hash(&mut recv).await?;
                remote_branches.push((id, head));
            }
            RSP_END_LIST => break,
            _ => return Err(anyhow!("unexpected list response: {rsp}")),
        }
    }

    // Fetch each branch's metadata blob to find the name.
    let mut remote_head: Option<RawHash> = None;
    for (_id, head) in &remote_branches {
        send_req_get_blob(&mut send, head).await?;
        let rsp = recv_u8(&mut recv).await?;
        if rsp == RSP_BLOB {
            let (_hash, data) = recv_blob_data(&mut recv).await?;
            // Parse the metadata blob as a TribleSet, look for metadata::name.
            let blob: Blob<SimpleArchive> = Blob::new(data.clone().into());
            if let Ok(meta) = TribleSet::try_from_blob(blob) {
                use triblespace_core::macros::{find, pattern};
                // Check if any entity in the metadata has a name matching our branch.
                for name_handle in find!(
                    h: Value<Handle<Blake3, triblespace_core::blob::schemas::longstring::LongString>>,
                    pattern!(&meta, [{ _?e @ triblespace_core::metadata::name: ?h }])
                ) {
                    // We need to resolve the name handle to a string.
                    // The blob might be in the data we just received or needs to be fetched.
                    // For now, check if it matches by fetching it.
                    send_req_get_blob(&mut send, &name_handle.raw).await?;
                    let rsp2 = recv_u8(&mut recv).await?;
                    if rsp2 == RSP_BLOB {
                        let (_h2, name_data) = recv_blob_data(&mut recv).await?;
                        if let Ok(name) = std::str::from_utf8(&name_data) {
                            if name == branch_name {
                                remote_head = Some(*head);
                                break;
                            }
                        }
                    }
                }
            }
            if remote_head.is_some() { break; }
        }
    }
    let remote_head = match remote_head {
        Some(h) => h,
        None => return Ok(format!("'{branch_name}': not found on remote")),
    };

    // BFS over the CAS graph using SYNC batches.
    // For each blob: scan it for references, tell the server what we have,
    // receive only the blobs we're missing. One round-trip per depth level.
    let mut pile = open_pile(pile_path)?;
    let mut fetched = 0usize;
    let mut fetched_bytes = 0usize;

    // Start by fetching the head blob itself.
    if !has_blob(&mut pile, &remote_head) {
        send_req_get_blob(&mut send, &remote_head).await?;
        let rsp = recv_u8(&mut recv).await?;
        if rsp == RSP_BLOB {
            let (_hash, data) = recv_blob_data(&mut recv).await?;
            fetched += 1;
            fetched_bytes += data.len();
            let bytes: Bytes = data.into();
            let _: Value<Handle<Blake3, UnknownBlob>> = pile.put::<UnknownBlob, Bytes>(bytes)
                .map_err(|e| anyhow!("put: {e:?}"))?;
        }
    }

    // BFS: process blobs level by level using SYNC.
    let mut current_level = vec![remote_head];
    let mut seen: HashSet<RawHash> = HashSet::new();
    seen.insert(remote_head);

    while !current_level.is_empty() {
        let mut next_level: Vec<RawHash> = Vec::new();

        for parent_hash in &current_level {
            // Scan the parent blob locally for 32-byte candidates.
            // Separate into: known blobs (HAVE) and unknown candidates.
            let mut have: Vec<RawHash> = Vec::new();
            if let Some(data) = get_blob(&mut pile, parent_hash) {
                for chunk in data.chunks(VALUE_LEN) {
                    if chunk.len() == VALUE_LEN {
                        let mut candidate = [0u8; 32];
                        candidate.copy_from_slice(chunk);
                        if seen.insert(candidate) {
                            if has_blob(&mut pile, &candidate) {
                                have.push(candidate);
                            }
                        }
                    }
                }
            }

            // SYNC: send what we have, receive what we're missing.
            send_req_sync(&mut send, parent_hash, &have).await?;
            loop {
                let rsp = recv_u8(&mut recv).await?;
                match rsp {
                    RSP_BLOB => {
                        let (hash, data) = recv_blob_data(&mut recv).await?;
                        fetched += 1;
                        fetched_bytes += data.len();
                        let bytes: Bytes = data.into();
                        let _: Value<Handle<Blake3, UnknownBlob>> = pile.put::<UnknownBlob, Bytes>(bytes)
                            .map_err(|e| anyhow!("put: {e:?}"))?;
                        next_level.push(hash);
                    }
                    RSP_END_SYNC => break,
                    _ => return Err(anyhow!("unexpected sync response: {rsp}")),
                }
            }
        }

        current_level = next_level;
    }

    // All blobs are local. CAS merge the remote commit into our branch.
    let remote_branch_meta = Value::<Handle<Blake3, SimpleArchive>>::new(remote_head);

    // Read the remote's branch meta to find the actual commit handle.
    let reader = BlobStore::<Blake3>::reader(&mut pile).map_err(|e| anyhow!("reader: {e:?}"))?;
    let branch_meta: TribleSet = reader.get(remote_branch_meta)
        .map_err(|e| anyhow!("read branch meta: {e:?}"))?;

    use triblespace_core::macros::{find, pattern};
    let remote_commit: Value<Handle<Blake3, SimpleArchive>> = find!(
        h: Value<Handle<Blake3, SimpleArchive>>,
        pattern!(&branch_meta, [{ _?e @ triblespace_core::repo::head: ?h }])
    ).next().ok_or_else(|| anyhow!("no head in branch meta"))?;
    drop(reader);

    let mut repo = Repository::new(pile, signing_key.clone(), TribleSet::new())
        .map_err(|e| anyhow!("repo: {e:?}"))?;
    let branch_id = repo.ensure_branch(branch_name, None)
        .map_err(|e| anyhow!("ensure branch: {e:?}"))?;
    let mut ws = repo.pull(branch_id).map_err(|e| anyhow!("pull ws: {e:?}"))?;

    // CAS: merge the remote commit into our branch.
    // If local is empty, this adopts the remote chain as-is.
    // If local has commits, this creates a merge commit with both as parents.
    ws.merge_commit(remote_commit).map_err(|e| anyhow!("merge: {e:?}"))?;
    repo.push(&mut ws).map_err(|e| anyhow!("push: {e:?}"))?;

    let _ = repo.close();

    send_req_done(&mut send).await?;
    send.finish().map_err(|e| anyhow!("finish: {e}"))?;

    Ok(format!("'{branch_name}': {fetched} blobs ({fetched_bytes}B), merged"))
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
                let msg_type = match recv_u8(&mut recv).await {
                    Ok(t) => t,
                    Err(_) => break,
                };

                match msg_type {
                    REQ_DONE => break,
                    REQ_LIST => {
                        let iter = pile.branches()
                            .map_err(|e| anyhow!("branches: {e:?}"))?;
                        for branch_result in iter {
                            let id = branch_result.map_err(|e| anyhow!("iter: {e:?}"))?;
                            if let Ok(Some(head)) = pile.head(id) {
                                let id_bytes: [u8; 16] = id.into();
                                send_rsp_list_entry(&mut send, &id_bytes, &head.raw).await?;
                            }
                        }
                        send_rsp_end_list(&mut send).await?;
                    }
                    REQ_GET_BLOB => {
                        let hash = recv_hash(&mut recv).await?;
                        match get_blob(&mut pile, &hash) {
                            Some(data) => send_rsp_blob(&mut send, &hash, &data).await?,
                            None => send_rsp_missing(&mut send).await?,
                        }
                    }
                    REQ_SYNC => {
                        let parent_hash = recv_hash(&mut recv).await?;
                        let have_count = recv_u32_be(&mut recv).await? as usize;
                        let mut have_set: HashSet<RawHash> = HashSet::with_capacity(have_count);
                        for _ in 0..have_count {
                            have_set.insert(recv_hash(&mut recv).await?);
                        }
                        // Scan the parent blob for references, send ones
                        // that are real blobs and NOT in the client's have set.
                        if let Some(parent_data) = get_blob(&mut pile, &parent_hash) {
                            for chunk in parent_data.chunks(VALUE_LEN) {
                                if chunk.len() == VALUE_LEN {
                                    let mut candidate = [0u8; 32];
                                    candidate.copy_from_slice(chunk);
                                    if !have_set.contains(&candidate) {
                                        if let Some(data) = get_blob(&mut pile, &candidate) {
                                            send_rsp_blob(&mut send, &candidate, &data).await?;
                                        }
                                    }
                                }
                            }
                        }
                        send_rsp_end_sync(&mut send).await?;
                    }
                    _ => break,
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

fn has_blob(pile: &mut Pile<Blake3>, hash: &RawHash) -> bool {
    let handle = Value::<Handle<Blake3, UnknownBlob>>::new(*hash);
    let Ok(reader) = BlobStore::<Blake3>::reader(pile) else { return false; };
    reader.get::<Bytes, UnknownBlob>(handle).is_ok()
}

fn get_blob(pile: &mut Pile<Blake3>, hash: &RawHash) -> Option<Vec<u8>> {
    let handle = Value::<Handle<Blake3, UnknownBlob>>::new(*hash);
    let reader = BlobStore::<Blake3>::reader(pile).ok()?;
    reader.get::<Bytes, UnknownBlob>(handle).ok().map(|b| b.to_vec())
}

/// Resolve branch name → ID and return the raw head hash.
fn branch_head_raw(pile_path: &PathBuf, name: &str) -> Option<RawHash> {
    let pile = open_pile(pile_path).ok()?;
    let key = SigningKey::from_bytes(&[0u8; 32]);
    let mut repo = Repository::new(pile, key, TribleSet::new()).ok()?;
    let branch_id = repo.ensure_branch(name, None).ok()?;
    let head = repo.storage_mut().head(branch_id).ok()??;
    let hash = head.raw;
    let _ = repo.close();
    Some(hash)
}

// ── Binary wire protocol ─────────────────────────────────────────────
//
// All messages are fixed-width headers + optional payload.
// No text encoding, no newlines, no hex — raw bytes throughout.
//
// Request types (client → server):
//   0x01  HEAD       [32 bytes branch_id]
//   0x02  GET_BLOB   [32 bytes hash]
//   0x00  DONE
//
// Response types (server → client):
//   0x01  HEAD_OK    [32 bytes hash]
//   0x02  BLOB       [32 bytes hash] [4 bytes len (BE)] [data...]
//   0x03  MISSING    [32 bytes hash]
//   0x04  NONE

const REQ_DONE: u8 = 0x00;
const REQ_LIST: u8 = 0x01;    // → LIST_ENTRY* END_LIST
const REQ_GET_BLOB: u8 = 0x02;
const REQ_SYNC: u8 = 0x03;    // <parent:32> <have_count:4> <hashes...> → BLOB* END_SYNC

const RSP_LIST_ENTRY: u8 = 0x01;  // <branch_id:16> <head:32>
const RSP_END_LIST: u8 = 0x02;
const RSP_BLOB: u8 = 0x03;
const RSP_MISSING: u8 = 0x04;
const RSP_END_SYNC: u8 = 0x05;

type RawHash = [u8; 32];

async fn send_req_list(send: &mut iroh::endpoint::SendStream) -> Result<()> {
    send.write_all(&[REQ_LIST]).await.map_err(|e| anyhow!("send: {e}"))
}

async fn send_rsp_list_entry(send: &mut iroh::endpoint::SendStream, branch_id: &[u8; 16], head: &RawHash) -> Result<()> {
    send.write_all(&[RSP_LIST_ENTRY]).await.map_err(|e| anyhow!("send: {e}"))?;
    send.write_all(branch_id).await.map_err(|e| anyhow!("send: {e}"))?;
    send.write_all(head).await.map_err(|e| anyhow!("send: {e}"))
}

async fn send_rsp_end_list(send: &mut iroh::endpoint::SendStream) -> Result<()> {
    send.write_all(&[RSP_END_LIST]).await.map_err(|e| anyhow!("send: {e}"))
}

async fn recv_branch_id(recv: &mut iroh::endpoint::RecvStream) -> Result<[u8; 16]> {
    let mut buf = [0u8; 16];
    recv.read_exact(&mut buf).await.map_err(|e| anyhow!("recv: {e}"))?;
    Ok(buf)
}

async fn send_req_get_blob(send: &mut iroh::endpoint::SendStream, hash: &RawHash) -> Result<()> {
    send.write_all(&[REQ_GET_BLOB]).await.map_err(|e| anyhow!("send: {e}"))?;
    send.write_all(hash).await.map_err(|e| anyhow!("send: {e}"))
}

async fn send_req_done(send: &mut iroh::endpoint::SendStream) -> Result<()> {
    send.write_all(&[REQ_DONE]).await.map_err(|e| anyhow!("send: {e}"))
}


async fn send_rsp_blob(send: &mut iroh::endpoint::SendStream, hash: &RawHash, data: &[u8]) -> Result<()> {
    send.write_all(&[RSP_BLOB]).await.map_err(|e| anyhow!("send: {e}"))?;
    send.write_all(hash).await.map_err(|e| anyhow!("send: {e}"))?;
    send.write_all(&(data.len() as u32).to_be_bytes()).await.map_err(|e| anyhow!("send: {e}"))?;
    send.write_all(data).await.map_err(|e| anyhow!("send: {e}"))
}

async fn send_req_sync(send: &mut iroh::endpoint::SendStream, parent: &RawHash, have: &[RawHash]) -> Result<()> {
    send.write_all(&[REQ_SYNC]).await.map_err(|e| anyhow!("send: {e}"))?;
    send.write_all(parent).await.map_err(|e| anyhow!("send: {e}"))?;
    send.write_all(&(have.len() as u32).to_be_bytes()).await.map_err(|e| anyhow!("send: {e}"))?;
    for h in have {
        send.write_all(h).await.map_err(|e| anyhow!("send: {e}"))?;
    }
    Ok(())
}

async fn send_rsp_end_sync(send: &mut iroh::endpoint::SendStream) -> Result<()> {
    send.write_all(&[RSP_END_SYNC]).await.map_err(|e| anyhow!("send: {e}"))
}

async fn send_rsp_missing(send: &mut iroh::endpoint::SendStream) -> Result<()> {
    send.write_all(&[RSP_MISSING]).await.map_err(|e| anyhow!("send: {e}"))
}


async fn recv_u8(recv: &mut iroh::endpoint::RecvStream) -> Result<u8> {
    let mut buf = [0u8; 1];
    recv.read_exact(&mut buf).await.map_err(|e| anyhow!("recv: {e}"))?;
    Ok(buf[0])
}

async fn recv_hash(recv: &mut iroh::endpoint::RecvStream) -> Result<RawHash> {
    let mut buf = [0u8; 32];
    recv.read_exact(&mut buf).await.map_err(|e| anyhow!("recv: {e}"))?;
    Ok(buf)
}

async fn recv_u32_be(recv: &mut iroh::endpoint::RecvStream) -> Result<u32> {
    let mut buf = [0u8; 4];
    recv.read_exact(&mut buf).await.map_err(|e| anyhow!("recv: {e}"))?;
    Ok(u32::from_be_bytes(buf))
}

async fn recv_blob_data(recv: &mut iroh::endpoint::RecvStream) -> Result<(RawHash, Vec<u8>)> {
    let hash = recv_hash(recv).await?;
    let len = recv_u32_be(recv).await? as usize;
    let mut data = vec![0u8; len];
    recv.read_exact(&mut data).await.map_err(|e| anyhow!("recv data: {e}"))?;
    Ok((hash, data))
}

// ── Key helpers ──────────────────────────────────────────────────────

fn iroh_secret(key: &SigningKey) -> SecretKey { SecretKey::from(key.to_bytes()) }
