use anyhow::Result;
use clap::Parser;
use rand::{rngs::OsRng, RngCore};

#[derive(Parser)]
/// A knowledge graph and meta file system for object stores.
///
enum TribleCli {
    /// Generate a new random id.
    IdGen {},
}

fn main() -> Result<()> {
    let args = TribleCli::parse();
    match args {
        TribleCli::IdGen {} => {
            let mut id = [0u8; 16];
            OsRng.fill_bytes(&mut id);
            let encoded_id = hex::encode(id);
            println!("{}", encoded_id.to_ascii_uppercase());
        }
    }
    Ok(())
}
