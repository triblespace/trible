//! Distributed pile sync over iroh.
//!
//! Uses iroh's QUIC connectivity (NAT traversal, relay, key-based dialing)
//! with a custom protocol for pile-native blob sync.  Every pile has an
//! identity derived from its ed25519 signing key — the same key that signs
//! commits also serves as the iroh node ID on the network.
//!
//! ## Wire protocol (v1 — hackathon edition)
//!
//! Newline-delimited text commands over a bidirectional QUIC stream.
//! The pull command fetches full branch snapshots — no incremental DAG
//! walking yet.  A full branch checkout is serialized as a SimpleArchive
//! blob and transferred in one shot.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::endpoint::{Connection, presets};
use iroh::Endpoint;
use iroh_base::SecretKey;
use triblespace_core::blob::ToBlob;
use triblespace_core::blob::TryFromBlob;
use triblespace_core::blob::Blob;
use triblespace_core::blob::schemas::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::repo::{BlobStore, BlobStoreGet, BlobStorePut, BranchStore, Repository};
use triblespace_core::repo::pile::Pile;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::Blake3;
use ed25519_dalek::SigningKey;

use super::signing::load_signing_key;

/// ALPN protocol identifier for triblespace pile sync.
const PILE_SYNC_ALPN: &[u8] = b"/triblespace/pile-sync/1";

// ── CLI ──────────────────────────────────────────────────────────────

#[derive(Parser)]
pub enum Command {
    /// Show this pile's network identity (ed25519 public key / iroh node ID).
    Identity {
        /// Signing key file (64-char hex seed). Falls back to TRIBLES_SIGNING_KEY env var.
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Start the pile as a network node, ready to sync with peers.
    Up {
        /// Path to the pile file
        pile: PathBuf,
        /// Signing key file
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Pull a branch snapshot from a remote peer.
    Pull {
        /// Path to the local pile file
        pile: PathBuf,
        /// Remote node ID (iroh public key, base32)
        remote: String,
        /// Branch name to pull
        #[arg(long)]
        branch: String,
        /// Signing key file
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::Identity { signing_key } => run_identity(signing_key),
        Command::Up { pile, signing_key } => run_up(pile, signing_key),
        Command::Pull { pile, remote, branch, signing_key } => {
            run_pull(pile, remote, branch, signing_key)
        }
    }
}

// ── Identity ─────────────────────────────────────────────────────────

fn run_identity(signing_key_path: Option<PathBuf>) -> Result<()> {
    let signing_key = load_signing_key(&signing_key_path)?;
    let iroh_secret = iroh_secret_from_signing_key(&signing_key);
    let public = iroh_secret.public();
    println!("node: {public}");
    Ok(())
}

// ── Up ───────────────────────────────────────────────────────────────

fn run_up(pile_path: PathBuf, signing_key_path: Option<PathBuf>) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
    rt.block_on(async {
        let signing_key = load_signing_key(&signing_key_path)?;
        let iroh_secret = iroh_secret_from_signing_key(&signing_key);
        let public = iroh_secret.public();

        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(iroh_secret)
            .bind()
            .await
            .map_err(|e| anyhow!("bind: {e}"))?;

        let handler = PileSyncHandler { pile_path, signing_key };

        let router = Router::builder(endpoint.clone())
            .accept(PILE_SYNC_ALPN, handler)
            .spawn();

        eprintln!("node: {public}");
        eprintln!("listening... (Ctrl-C to stop)");

        tokio::signal::ctrl_c().await?;
        router.shutdown().await.map_err(|e| anyhow!("shutdown: {e}"))?;
        Ok(())
    })
}

// ── Pull ─────────────────────────────────────────────────────────────

fn run_pull(
    pile_path: PathBuf,
    remote: String,
    branch_name: String,
    signing_key_path: Option<PathBuf>,
) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
    rt.block_on(async {
        let signing_key = load_signing_key(&signing_key_path)?;
        let iroh_secret = iroh_secret_from_signing_key(&signing_key);

        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(iroh_secret)
            .bind()
            .await
            .map_err(|e| anyhow!("bind: {e}"))?;

        let remote_key: iroh_base::PublicKey = remote.parse()
            .map_err(|e| anyhow!("invalid node ID '{remote}': {e}"))?;

        eprintln!("connecting to {}...", remote_key.fmt_short());
        let conn = endpoint.connect(remote_key, PILE_SYNC_ALPN).await
            .map_err(|e| anyhow!("connect: {e}"))?;
        eprintln!("connected.");

        // Request branch snapshot
        let (mut send, mut recv) = conn.open_bi().await
            .map_err(|e| anyhow!("open_bi: {e}"))?;

        let request = format!("PULL_BRANCH {branch_name}\n");
        send.write_all(request.as_bytes()).await
            .map_err(|e| anyhow!("write: {e}"))?;
        send.finish().map_err(|e| anyhow!("finish: {e}"))?;

        // Read response: first line is status + length, rest is blob data
        let header = recv_line(&mut recv).await?;

        if let Some(rest) = header.strip_prefix("SNAPSHOT ") {
            let len: usize = rest.trim().parse()
                .map_err(|e| anyhow!("bad length: {e}"))?;
            eprintln!("receiving {len} bytes...");

            let data = recv_exact(&mut recv, len).await?;

            // Import into local pile as a new commit on the branch
            let mut pile = open_pile(&pile_path)?;
            let signing_key = load_signing_key(&signing_key_path)?;
            let mut repo = Repository::new(pile, signing_key, TribleSet::new())
                .map_err(|e| anyhow!("create repo: {e:?}"))?;

            let branch_id = repo.ensure_branch(&branch_name, None)
                .map_err(|e| anyhow!("ensure branch: {e:?}"))?;

            // Deserialize the snapshot into a TribleSet
            let blob: Blob<SimpleArchive> = Blob::new(data.into());
            let remote_facts: TribleSet = blob.try_from_blob()
                .map_err(|e| anyhow!("bad archive: {e}"))?;

            // Checkout existing local state
            let mut ws = repo.pull(branch_id)
                .map_err(|e| anyhow!("pull: {e:?}"))?;
            let local_facts = ws.checkout(..)
                .map_err(|e| anyhow!("checkout: {e:?}"))?
                .into_facts();

            // Compute the delta (what the remote has that we don't)
            let delta = remote_facts.difference(&local_facts);
            if delta.is_empty() {
                eprintln!("already up to date.");
            } else {
                eprintln!("{} new tribles.", delta.len());
                ws.commit(delta.clone(), "sync: pull from remote");
                repo.try_push(&mut ws)
                    .map_err(|e| anyhow!("push: {e:?}"))?;
                eprintln!("branch '{branch_name}' updated.");
            }

            let _ = repo.close();
        } else if header.starts_with("ERROR") {
            eprintln!("remote error: {header}");
        } else {
            eprintln!("unexpected response: {header}");
        }

        conn.close(0u32.into(), b"done");
        endpoint.close().await;
        Ok(())
    })
}

// ── Protocol handler ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct PileSyncHandler {
    pile_path: PathBuf,
    signing_key: SigningKey,
}

impl ProtocolHandler for PileSyncHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let pile_path = self.pile_path.clone();
        let signing_key = self.signing_key.clone();
        let peer = format!("{}", connection.remote_id().fmt_short());
        eprintln!("request from {peer}");

        let result: Result<(), anyhow::Error> = async {
            let (mut send, mut recv) = connection.accept_bi().await
                .map_err(|e| anyhow!("accept_bi: {e}"))?;

            let request = recv.read_to_end(64 * 1024).await
                .map_err(|e| anyhow!("read: {e}"))?;
            let request_str = String::from_utf8_lossy(&request);
            let request_str = request_str.trim();

            if let Some(branch_name) = request_str.strip_prefix("PULL_BRANCH ") {
                eprintln!("  serving branch '{branch_name}'");

                let pile = open_pile(&pile_path)?;
                let mut repo = Repository::new(pile, signing_key, TribleSet::new())
                    .map_err(|e| anyhow!("create repo: {e:?}"))?;

                let branch_id = repo.ensure_branch(branch_name, None)
                    .map_err(|e| anyhow!("ensure branch: {e:?}"))?;

                let mut ws = repo.pull(branch_id)
                    .map_err(|e| anyhow!("pull: {e:?}"))?;
                let facts = ws.checkout(..)
                    .map_err(|e| anyhow!("checkout: {e:?}"))?
                    .into_facts();

                // Serialize as SimpleArchive
                let n_tribles = facts.len();
                let blob: Blob<SimpleArchive> = facts.to_blob();
                let data = &blob.bytes;

                let header = format!("SNAPSHOT {}\n", data.len());
                send.write_all(header.as_bytes()).await
                    .map_err(|e| anyhow!("write header: {e}"))?;
                send.write_all(data).await
                    .map_err(|e| anyhow!("write data: {e}"))?;
                send.finish().map_err(|e| anyhow!("finish: {e}"))?;

                eprintln!("  sent {} bytes ({} tribles)", data.len(), n_tribles);
                let _ = repo.close();
            } else {
                let msg = format!("ERROR unknown request: {request_str}\n");
                send.write_all(msg.as_bytes()).await
                    .map_err(|e| anyhow!("write: {e}"))?;
                send.finish().map_err(|e| anyhow!("finish: {e}"))?;
            }
            Ok(())
        }.await;

        if let Err(e) = result {
            eprintln!("handler error for {peer}: {e}");
        }
        connection.closed().await;
        Ok(())
    }
}

// ── Pile helpers ─────────────────────────────────────────────────────

fn open_pile(path: &PathBuf) -> Result<Pile<Blake3>> {
    let mut pile = Pile::<Blake3>::open(path)
        .map_err(|e| anyhow!("open pile: {e:?}"))?;
    pile.restore().map_err(|e| anyhow!("restore: {e:?}"))?;
    Ok(pile)
}

// ── Wire helpers ─────────────────────────────────────────────────────

async fn recv_line(recv: &mut iroh::endpoint::RecvStream) -> Result<String> {
    let mut buf = Vec::with_capacity(256);
    loop {
        let mut byte = [0u8; 1];
        match recv.read_exact(&mut byte).await {
            Ok(()) => {
                if byte[0] == b'\n' {
                    return Ok(String::from_utf8_lossy(&buf).into_owned());
                }
                buf.push(byte[0]);
                if buf.len() > 1024 * 1024 {
                    return Err(anyhow!("line too long"));
                }
            }
            Err(e) => {
                if buf.is_empty() {
                    return Err(anyhow!("recv_line: {e}"));
                }
                return Ok(String::from_utf8_lossy(&buf).into_owned());
            }
        }
    }
}

async fn recv_exact(recv: &mut iroh::endpoint::RecvStream, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await
        .map_err(|e| anyhow!("recv_exact({len}): {e}"))?;
    Ok(buf)
}

// ── Key conversion ───────────────────────────────────────────────────

fn iroh_secret_from_signing_key(key: &SigningKey) -> SecretKey {
    SecretKey::from(key.to_bytes())
}
