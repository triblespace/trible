use anyhow::Result;
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use std::env;
use std::fs;
use std::path::PathBuf;

pub(super) fn load_signing_key(path_opt: &Option<PathBuf>) -> Result<SigningKey, anyhow::Error> {
    // Accept only a path to a file (via CLI flag or TRIBLES_SIGNING_KEY env var)
    // containing a 64-char hex seed. If the path is absent, generate an
    // ephemeral signing key.
    let key_path_opt: Option<PathBuf> = if let Some(p) = path_opt {
        Some(p.clone())
    } else if let Ok(s) = env::var("TRIBLES_SIGNING_KEY") {
        Some(PathBuf::from(s))
    } else {
        None
    };

    if let Some(p) = key_path_opt {
        let content = fs::read_to_string(&p)
            .map_err(|e| anyhow::anyhow!("failed to read signing key: {e}"))?;
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
        return Ok(SigningKey::from_bytes(&arr));
    }

    Ok(SigningKey::generate(&mut OsRng))
}
