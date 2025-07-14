use anyhow::Result;
use rand::{rngs::OsRng, RngCore};
use structopt::StructOpt;

#[derive(StructOpt)]
/// A knowledge graph and meta file system for object stores.
///
enum TribleCli {
    /// Generate a new random id.
    IdGen {},
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = TribleCli::from_args();
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
