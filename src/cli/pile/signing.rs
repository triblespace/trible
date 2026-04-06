use anyhow::Result;
use ed25519_dalek::SigningKey;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Load a signing key from an explicit path, the TRIBLES_SIGNING_KEY env var,
/// or generate an ephemeral key.  Used by commands that don't have a pile
/// (e.g. genid) or where persistence doesn't matter.
pub(super) fn load_signing_key(path_opt: &Option<PathBuf>) -> Result<SigningKey, anyhow::Error> {
    let key_path_opt: Option<PathBuf> = if let Some(p) = path_opt {
        Some(p.clone())
    } else if let Ok(s) = env::var("TRIBLES_SIGNING_KEY") {
        Some(PathBuf::from(s))
    } else {
        None
    };

    if let Some(p) = key_path_opt {
        return load_key_from_file(&p);
    }

    generate_ephemeral_key()
}

/// Load or create a persistent signing key for a pile.
///
/// Resolution order:
/// 1. Explicit `--signing-key <path>` flag
/// 2. `TRIBLES_SIGNING_KEY` env var
/// 3. `<pile-path>.key` — auto-created on first use
///
/// The key file contains a 64-char hex seed.  On first use a new key
/// is generated and written to the file, then loaded.
pub(super) fn load_or_create_pile_key(
    explicit_path: &Option<PathBuf>,
    pile_path: &Path,
) -> Result<SigningKey, anyhow::Error> {
    // 1. Explicit path
    if let Some(p) = explicit_path {
        return load_key_from_file(p);
    }
    // 2. Env var
    if let Ok(s) = env::var("TRIBLES_SIGNING_KEY") {
        return load_key_from_file(&PathBuf::from(s));
    }
    // 3. Auto-discover/create next to pile
    let key_path = pile_path.with_extension("pile.key");
    if key_path.exists() {
        return load_key_from_file(&key_path);
    }
    // Generate and persist
    let key = generate_ephemeral_key()?;
    let hex_str = hex::encode(key.to_bytes());
    fs::write(&key_path, &hex_str)
        .map_err(|e| anyhow::anyhow!("failed to write key to {}: {e}", key_path.display()))?;
    eprintln!("generated new node key: {}", key_path.display());
    Ok(key)
}

fn load_key_from_file(p: &Path) -> Result<SigningKey, anyhow::Error> {
    let content = fs::read_to_string(p)
        .map_err(|e| anyhow::anyhow!("failed to read signing key {}: {e}", p.display()))?;
    let hexstr = content.trim();
    if hexstr.len() != 64 || !hexstr.chars().all(|c| c.is_ascii_hexdigit()) {
        anyhow::bail!(
            "signing key file {} does not contain valid 64-char hex",
            p.display()
        );
    }
    let bytes = hex::decode(hexstr)
        .map_err(|e| anyhow::anyhow!("invalid hex in signing key file: {e}"))?;
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(SigningKey::from_bytes(&arr))
}

fn generate_ephemeral_key() -> Result<SigningKey, anyhow::Error> {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed)
        .map_err(|e| anyhow::anyhow!("failed to generate signing key: {e}"))?;
    Ok(SigningKey::from_bytes(&seed))
}
