use anyhow::Result;
use clap::Parser;
use rand::{rngs::OsRng, RngCore};
use std::fs::File;
use std::path::PathBuf;

use memmap2::Mmap;

const DEFAULT_MAX_PILE_SIZE: usize = 1 << 44; // 16 TiB
use tribles::prelude::{BlobStore, BlobStoreGet, BlobStorePut, BranchStore, TryToValue};

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
    /// Ingest a file into a pile, creating the pile if necessary.
    Put {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// File whose contents should be stored in the pile
        file: PathBuf,
    },
    /// Extract a blob from a pile by its handle.
    Get {
        /// Path to the pile file to read
        pile: PathBuf,
        /// Handle of the blob to retrieve (e.g. "blake3:HEX...")
        handle: String,
        /// Destination file path for the extracted blob
        output: PathBuf,
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

                let pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> =
                    Pile::open(&path).map_err(|e| anyhow::anyhow!("{e:?}"))?;

                for branch in pile.branches() {
                    let id = branch?;
                    println!("{id:X}");
                }
            }
            PileCommand::Create { path } => {
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::Blake3;

                let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> =
                    Pile::open(&path).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                pile.flush().map_err(|e| anyhow::anyhow!("{e:?}"))?;
            }
            PileCommand::Put { pile, file } => {
                use tribles::blob::{schemas::UnknownBlob, Bytes};
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::Blake3;

                let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> =
                    Pile::open(&pile).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                let file_handle = File::open(&file)?;
                let mmap = unsafe { Mmap::map(&file_handle)? };
                pile.put::<UnknownBlob, _>(Bytes::from_source(mmap))
                    .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                pile.flush().map_err(|e| anyhow::anyhow!("{e:?}"))?;
            }
            PileCommand::Get {
                pile,
                handle,
                output,
            } => {
                use std::io::Write;

                use tribles::blob::{schemas::UnknownBlob, Bytes};
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::{Blake3, Handle, Hash};

                let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> =
                    Pile::open(&pile).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                let hash: tribles::value::Value<Hash<Blake3>> = handle
                    .try_to_value()
                    .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                let handle: tribles::value::Value<Handle<Blake3, UnknownBlob>> = hash.into();
                let reader = pile.reader();
                let bytes: Bytes = reader.get(handle).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                let mut file = File::create(&output)?;
                file.write_all(&bytes)?;
            }
        },
    }
    Ok(())
}
