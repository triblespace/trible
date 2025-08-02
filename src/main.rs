use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::Shell;
use rand::{rngs::OsRng, RngCore};
use std::io;

pub const DEFAULT_MAX_PILE_SIZE: usize = 1 << 44; // 16 TiB

mod cli;

pub use cli::branch::BranchCommand;
pub use cli::pile::{BlobCommand, PileBranchCommand, PileCommand};
pub use cli::store::{StoreBlobCommand, StoreBranchCommand, StoreCommand};

#[derive(Parser)]
/// A knowledge graph and meta file system for object stores.
///
enum TribleCli {
    /// Generate a new random identifier.
    Genid,
    /// Generate shell completion scripts.
    Completion {
        #[arg(value_enum)]
        shell: Shell,
    },
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
        TribleCli::Genid => {
            let mut id = [0u8; 16];
            OsRng.fill_bytes(&mut id);
            let encoded_id = hex::encode(id);
            println!("{}", encoded_id.to_ascii_uppercase());
        }
        TribleCli::Completion { shell } => {
            let mut cmd = TribleCli::command();
            let bin_name = cmd.get_name().to_string();
            clap_complete::generate(shell, &mut cmd, bin_name, &mut io::stdout());
        }
        TribleCli::Branch { cmd } => cli::branch::run(cmd)?,
        TribleCli::Pile { cmd } => cli::pile::run(cmd)?,
        TribleCli::Store { cmd } => cli::store::run(cmd)?,
    }
    Ok(())
}
