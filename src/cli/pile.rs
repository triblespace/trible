use anyhow::Result;
use clap::Parser;
use rand::rngs::OsRng;
use std::{fs::File, path::PathBuf};

use super::util::parse_blob_handle;
use crate::DEFAULT_MAX_PILE_SIZE;
use tribles::prelude::{BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut, BranchStore};

#[derive(Parser)]
pub enum PileCommand {
    /// Operations on branches stored in a pile file.
    Branch {
        #[command(subcommand)]
        cmd: PileBranchCommand,
    },
    /// Operations on blobs stored in a pile file.
    Blob {
        #[command(subcommand)]
        cmd: BlobCommand,
    },
    /// Create a new empty pile file.
    ///
    /// This is mainly a cross-platform convenience; a plain `touch` on
    /// Unix-like systems achieves the same result.
    Create {
        /// Path to the pile file to create
        path: PathBuf,
    },
    /// Run diagnostics and repair checks on a pile file.
    Diagnose {
        /// Path to the pile file to inspect
        pile: PathBuf,
    },
}

#[derive(Parser)]
pub enum PileBranchCommand {
    /// List all branch identifiers in a pile file.
    List {
        /// Path to the pile file to inspect
        path: PathBuf,
    },
    /// Create a new branch in a pile file.
    Create {
        /// Path to the pile file to modify
        pile: PathBuf,
        /// Name of the branch to create
        name: String,
    },
}

#[derive(Parser)]
pub enum BlobCommand {
    /// List all blob handles stored in a pile file.
    List {
        /// Path to the pile file to inspect
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
    /// Inspect a blob and print basic metadata.
    Inspect {
        /// Path to the pile file to read
        pile: PathBuf,
        /// Handle of the blob to inspect (e.g. "blake3:HEX...")
        handle: String,
    },
}

pub fn run(cmd: PileCommand) -> Result<()> {
    match cmd {
        PileCommand::Branch { cmd } => match cmd {
            PileBranchCommand::List { path } => {
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::Blake3;

                let pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&path)?;

                for branch in pile.branches() {
                    let id = branch?;
                    println!("{id:X}");
                }
            }
            PileBranchCommand::Create { pile, name } => {
                use ed25519_dalek::SigningKey;
                use tribles::repo::pile::Pile;
                use tribles::repo::Repository;
                use tribles::value::schemas::hash::Blake3;

                let pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;
                let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));
                let ws = repo.branch(&name).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                println!("{:#X}", ws.branch_id());
            }
        },
        PileCommand::Blob { cmd } => match cmd {
            BlobCommand::List { path } => {
                use tribles::blob::schemas::UnknownBlob;
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::{Blake3, Handle, Hash};

                let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&path)?;
                let reader = pile.reader();
                for handle in reader.blobs() {
                    let handle: tribles::value::Value<Handle<Blake3, UnknownBlob>> = handle?;
                    let hash: tribles::value::Value<Hash<Blake3>> = Handle::to_hash(handle);
                    let string: String = hash.from_value();
                    println!("{}", string);
                }
            }
            BlobCommand::Put { pile, file } => {
                use tribles::blob::{schemas::UnknownBlob, Bytes};
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::{Blake3, Handle, Hash};

                let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;
                let file_handle = File::open(&file)?;
                let bytes = unsafe { Bytes::map_file(&file_handle)? };
                let handle = pile.put::<UnknownBlob, _>(bytes)?;
                let hash: tribles::value::Value<Hash<Blake3>> = Handle::to_hash(handle);
                let string: String = hash.from_value();
                println!("{}", string);
                pile.flush().map_err(|e| anyhow::anyhow!("{e:?}"))?;
            }
            BlobCommand::Get {
                pile,
                handle,
                output,
            } => {
                use std::io::Write;

                use tribles::blob::{schemas::UnknownBlob, Bytes};
                use tribles::repo::pile::Pile;
                use tribles::value::schemas::hash::{Blake3, Handle};

                let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;
                let hash_val = parse_blob_handle(&handle)?;
                let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> =
                    hash_val.clone().into();
                let reader = pile.reader();
                let bytes: Bytes = reader.get(handle_val)?;
                let mut file = File::create(&output)?;
                file.write_all(&bytes)?;
            }
            BlobCommand::Inspect { pile, handle } => {
                use chrono::{DateTime, Utc};
                use file_type::FileType;
                use std::time::{Duration, UNIX_EPOCH};

                use tribles::blob::{schemas::UnknownBlob, Blob};
                use tribles::repo::pile::{BlobMetadata, Pile};
                use tribles::value::schemas::hash::{Blake3, Handle};

                let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;
                let hash_val = parse_blob_handle(&handle)?;
                let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> =
                    hash_val.clone().into();
                let reader = pile.reader();
                let blob: Blob<UnknownBlob> = reader.get(handle_val)?;
                let metadata: BlobMetadata = reader
                    .metadata(handle_val)
                    .ok_or_else(|| anyhow::anyhow!("blob not found"))?;

                let dt = UNIX_EPOCH + Duration::from_millis(metadata.timestamp);
                let time: DateTime<Utc> = DateTime::<Utc>::from(dt);

                let ftype = FileType::from_bytes(&blob.bytes);
                let name = ftype.name();

                let handle_str: String = hash_val.from_value();
                println!(
                    "Hash: {handle_str}\nTime: {}\nLength: {} bytes\nType: {}",
                    time.to_rfc3339(),
                    metadata.length,
                    name
                );
            }
        },
        PileCommand::Create { path } => {
            use std::fs;
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;

            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }

            let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&path)?;
            pile.flush().map_err(|e| anyhow::anyhow!("{e:?}"))?;
        }
        PileCommand::Diagnose { pile } => {
            use tribles::repo::pile::{OpenError, Pile};
            use tribles::value::schemas::hash::{Blake3, Handle, Hash};

            match Pile::<DEFAULT_MAX_PILE_SIZE, Blake3>::try_open(&pile) {
                Ok(mut pile) => {
                    let reader = pile.reader();
                    let mut invalid = 0usize;
                    let mut total = 0usize;
                    for (handle, blob) in reader.iter() {
                        total += 1;
                        let expected: tribles::value::Value<Hash<Blake3>> = Handle::to_hash(handle);
                        let computed = Hash::<Blake3>::digest(&blob.bytes);
                        if expected != computed {
                            invalid += 1;
                        }
                    }

                    if invalid == 0 {
                        println!("Pile appears healthy");
                    } else {
                        println!("Pile corrupt: {invalid} of {total} blobs have incorrect hashes");
                        anyhow::bail!("invalid blob hashes detected");
                    }
                }
                Err(OpenError::CorruptPile { valid_length }) => {
                    println!("Pile corrupt, valid portion: {valid_length} bytes");
                    anyhow::bail!("pile corruption detected");
                }
                Err(err) => return Err(anyhow::anyhow!("{err:?}")),
            }
        }
    }
    Ok(())
}
