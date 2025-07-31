use anyhow::Result;
use clap::Parser;
use rand::{rngs::OsRng, RngCore};

pub const DEFAULT_MAX_PILE_SIZE: usize = 1 << 44; // 16 TiB

mod cli;

pub use cli::branch::BranchCommand;
pub use cli::pile::{BlobCommand, PileBranchCommand, PileCommand};
pub use cli::store::{StoreBlobCommand, StoreBranchCommand, StoreCommand};

#[derive(Parser)]
/// A knowledge graph and meta file system for object stores.
///
enum TribleCli {
    /// Generate a new random id.
    IdGen {},
    /// Synchronize branches between piles and remote stores.
    Branch {
        #[command(subcommand)]
        cmd: BranchCommand,
    },
    /// Commands for working with local pile files.
    Pile {
        #[command(subcommand)]
        cmd: PileCommand,
    },
    /// Inspect remote object stores.
    Store {
        #[command(subcommand)]
        cmd: StoreCommand,
    },
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
        TribleCli::Branch { cmd } => cli::branch::run(cmd)?,
        TribleCli::Pile { cmd } => cli::pile::run(cmd)?,
        TribleCli::Store { cmd } => cli::store::run(cmd)?,
    }
    Ok(())
}
