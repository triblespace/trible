use anyhow::Result;
use clap::Parser;
use rand::{rngs::OsRng, RngCore};
use std::path::PathBuf;
use tribles::prelude::BranchStore;

#[derive(Parser)]
/// A knowledge graph and meta file system for object stores.
///
enum TribleCli {
    /// Generate a new random id.
    IdGen {},
    /// Commands for working with local pile files.
    Pile {
        #[command(subcommand)]
        cmd: PileCommand,
    },
}

#[derive(Parser)]
enum PileCommand {
    /// List all branch identifiers in a pile file.
    ListBranches {
        /// Path to the pile file to inspect
        path: PathBuf,
    },
    /// Create a new empty pile file.
    ///
    /// This is mainly a cross-platform convenience; a plain `touch` on
    /// Unix-like systems achieves the same result.
    Create {
        /// Path to the pile file to create
        path: PathBuf,
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
        TribleCli::Pile { cmd } => match cmd {
            PileCommand::ListBranches { path } => {
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::Blake3;

                const MAX_PILE_SIZE: usize = 1 << 30; // 1 GiB
                let pile: Pile<MAX_PILE_SIZE, Blake3> =
                    Pile::open(&path).map_err(|e| anyhow::anyhow!("{e:?}"))?;

                for branch in pile.branches() {
                    let id = branch?;
                    println!("{id:X}");
                }
            }
            PileCommand::Create { path } => {
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::Blake3;

                const MAX_PILE_SIZE: usize = 1 << 30; // 1 GiB
                let mut pile: Pile<MAX_PILE_SIZE, Blake3> =
                    Pile::open(&path).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                pile.flush().map_err(|e| anyhow::anyhow!("{e:?}"))?;
            }
        },
    }
    Ok(())
}
