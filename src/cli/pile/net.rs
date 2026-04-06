//! Distributed pile sync over iroh.
//!
//! Uses iroh's QUIC connectivity (NAT traversal, relay, key-based dialing)
//! with a custom protocol for pile-native blob sync.  Every pile has an
//! identity derived from its ed25519 signing key — the same key that signs
//! commits also serves as the iroh node ID on the network.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::endpoint::{Connection, presets};
use iroh::Endpoint;
use iroh_base::SecretKey;
use triblespace_core::repo::BranchStore;
use triblespace_core::repo::pile::Pile;
use triblespace_core::value::schemas::hash::Blake3;

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
    ///
    /// Runs until interrupted (Ctrl-C).  While running, other nodes can
    /// connect and pull/push branches.
    Up {
        /// Path to the pile file
        pile: PathBuf,
        /// Signing key file
        #[arg(long)]
        signing_key: Option<PathBuf>,
    },
    /// Pull branches from a remote peer.
    Pull {
        /// Path to the local pile file
        pile: PathBuf,
        /// Remote node ID (iroh public key, base32)
        remote: String,
        /// Branches to pull (by name). If empty, pulls all.
        #[arg(long)]
        branch: Vec<String>,
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

// ── Up (listen for sync requests) ────────────────────────────────────

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
            .map_err(|e| anyhow!("bind endpoint: {e}"))?;

        let handler = PileSyncHandler::new(pile_path)?;

        let router = Router::builder(endpoint.clone())
            .accept(PILE_SYNC_ALPN, handler)
            .spawn();

        eprintln!("node: {public}");
        eprintln!("listening for sync requests... (Ctrl-C to stop)");

        tokio::signal::ctrl_c().await?;
        router.shutdown().await.map_err(|e| anyhow!("shutdown: {e}"))?;
        Ok(())
    })
}

// ── Pull ─────────────────────────────────────────────────────────────

fn run_pull(
    _pile_path: PathBuf,
    remote: String,
    _branches: Vec<String>,
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
            .map_err(|e| anyhow!("bind endpoint: {e}"))?;

        let remote_key: iroh_base::PublicKey = remote.parse()
            .map_err(|e| anyhow!("invalid remote node ID '{remote}': {e}"))?;

        eprintln!("connecting to {remote}...");
        let conn = endpoint.connect(remote_key, PILE_SYNC_ALPN).await
            .map_err(|e| anyhow!("connect: {e}"))?;

        eprintln!("connected, requesting branch list...");
        let (mut send, mut recv) = conn.open_bi().await
            .map_err(|e| anyhow!("open stream: {e}"))?;

        // Phase 1: Request branch list
        send.write_all(b"LIST_BRANCHES\n").await
            .map_err(|e| anyhow!("write: {e}"))?;
        send.finish().map_err(|e| anyhow!("finish: {e}"))?;

        let response = recv.read_to_end(1024 * 1024).await
            .map_err(|e| anyhow!("read: {e}"))?;
        let text = String::from_utf8_lossy(&response);
        eprintln!("remote branches:\n{text}");

        conn.close(0u32.into(), b"done");
        endpoint.close().await;
        Ok(())
    })
}

// ── Protocol handler ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct PileSyncHandler {
    pile_path: PathBuf,
}

impl PileSyncHandler {
    fn new(pile_path: PathBuf) -> Result<Self> {
        if !pile_path.exists() {
            anyhow::bail!("pile not found: {}", pile_path.display());
        }
        Ok(Self { pile_path })
    }
}

impl ProtocolHandler for PileSyncHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let pile_path = self.pile_path.clone();
        let peer = format!("{}", connection.remote_id().fmt_short());
        eprintln!("sync request from {peer}");

        let result: Result<(), anyhow::Error> = async {
            let (mut send, mut recv) = connection.accept_bi().await
                .map_err(|e| anyhow!("accept_bi: {e}"))?;

            let request = recv.read_to_end(64 * 1024).await
                .map_err(|e| anyhow!("read: {e}"))?;
            let request_str = String::from_utf8_lossy(&request);

            if request_str.trim() == "LIST_BRANCHES" {
                let response = list_branches_response(&pile_path)
                    .unwrap_or_else(|e| format!("ERROR: {e}\n"));
                send.write_all(response.as_bytes()).await
                    .map_err(|e| anyhow!("write: {e}"))?;
                send.finish().map_err(|e| anyhow!("finish: {e}"))?;
            } else {
                send.write_all(b"ERROR: unknown request\n").await
                    .map_err(|e| anyhow!("write: {e}"))?;
                send.finish().map_err(|e| anyhow!("finish: {e}"))?;
            }
            Ok(())
        }.await;

        if let Err(e) = result {
            eprintln!("sync handler error: {e}");
        }
        connection.closed().await;
        Ok(())
    }
}

fn list_branches_response(pile_path: &PathBuf) -> Result<String> {
    let mut pile = Pile::<Blake3>::open(pile_path)
        .map_err(|e| anyhow!("open pile: {e:?}"))?;
    pile.refresh().map_err(|e| anyhow!("refresh pile: {e:?}"))?;

    let mut out = String::new();
    let iter = pile.branches().map_err(|e| anyhow!("branches: {e:?}"))?;
    for branch_result in iter {
        let id = branch_result.map_err(|e| anyhow!("branch iter: {e:?}"))?;
        let has_head = pile.head(id)
            .map_err(|e| anyhow!("head: {e:?}"))?
            .is_some();
        let status = if has_head { "active" } else { "empty" };
        out.push_str(&format!("{id:x}\t{status}\n"));
    }

    pile.close().map_err(|e| anyhow!("close pile: {e:?}"))?;
    Ok(out)
}

// ── Key conversion ───────────────────────────────────────────────────

/// Convert an ed25519-dalek 2.x SigningKey to an iroh SecretKey.
///
/// Both are 32-byte ed25519 seeds — the bytes are identical, just
/// wrapped in different types from different crate versions.
fn iroh_secret_from_signing_key(key: &ed25519_dalek::SigningKey) -> SecretKey {
    SecretKey::from(key.to_bytes())
}
