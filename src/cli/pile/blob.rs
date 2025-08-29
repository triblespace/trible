use anyhow::Result;
use clap::Parser;
use std::fs::File;
use std::path::PathBuf;

// DEFAULT_MAX_PILE_SIZE removed; the new Pile API no longer uses a size const generic

use crate::cli::util::parse_blob_handle;

#[derive(Parser)]
pub enum Command {
    /// List all blob handles stored in a pile file.
    List {
        /// Path to the pile file to inspect
        path: PathBuf,
        /// Show creation time and size for each blob
        #[arg(long)]
        metadata: bool,
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

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::List { path, metadata } => {
            use chrono::DateTime;
            use chrono::Utc;
            use std::time::Duration;
            use std::time::UNIX_EPOCH;

            use tribles::blob::schemas::UnknownBlob;
            use tribles::prelude::BlobStore;
            use tribles::prelude::BlobStoreList;
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use tribles::value::schemas::hash::Hash;

            let mut pile: Pile<Blake3> = Pile::open(&path)?;
            let reader = pile
                .reader()
                .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
            for handle in reader.blobs() {
                let handle: tribles::value::Value<Handle<Blake3, UnknownBlob>> = handle?;
                let hash: tribles::value::Value<Hash<Blake3>> = Handle::to_hash(handle);
                let string: String = hash.from_value();
                if metadata {
                    if let Some(meta) = reader.metadata(handle) {
                        let dt = UNIX_EPOCH + Duration::from_millis(meta.timestamp);
                        let time: DateTime<Utc> = DateTime::<Utc>::from(dt);
                        println!("{}\t{}\t{}", string, time.to_rfc3339(), meta.length);
                    } else {
                        println!("{string}");
                    }
                } else {
                    println!("{string}");
                }
            }
        }
        Command::Put { pile, file } => {
            use tribles::blob::schemas::UnknownBlob;
            use tribles::blob::Bytes;
            use tribles::prelude::BlobStorePut;
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use tribles::value::schemas::hash::Hash;

            let mut pile: Pile<Blake3> = Pile::open(&pile)?;
            let file_handle = File::open(&file)?;
            let bytes = unsafe { Bytes::map_file(&file_handle)? };
            let handle = pile.put::<UnknownBlob, _>(bytes)?;
            let hash: tribles::value::Value<Hash<Blake3>> = Handle::to_hash(handle);
            let string: String = hash.from_value();
            println!("{string}");
            pile.flush().map_err(|e| anyhow::anyhow!("{e:?}"))?;
        }
        Command::Get {
            pile,
            handle,
            output,
        } => {
            use std::io::Write;

            use tribles::blob::schemas::UnknownBlob;
            use tribles::blob::Bytes;
            use tribles::prelude::BlobStore;
            use tribles::prelude::BlobStoreGet;
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;

            let mut pile: Pile<Blake3> = Pile::open(&pile)?;
            let hash_val = parse_blob_handle(&handle)?;
            let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> = hash_val.into();
            let reader = pile
                .reader()
                .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
            let bytes: Bytes = reader.get(handle_val)?;
            let mut file = File::create(&output)?;
            file.write_all(&bytes)?;
        }
        Command::Inspect { pile, handle } => {
            use chrono::DateTime;
            use chrono::Utc;
            use file_type::FileType;
            use std::time::Duration;
            use std::time::UNIX_EPOCH;

            use tribles::blob::schemas::UnknownBlob;
            use tribles::blob::Blob;
            use tribles::prelude::BlobStore;
            use tribles::prelude::BlobStoreGet;
            use tribles::repo::pile::BlobMetadata;
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;

            let mut pile: Pile<Blake3> = Pile::open(&pile)?;
            let hash_val = parse_blob_handle(&handle)?;
            let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> = hash_val.into();
            let reader = pile
                .reader()
                .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;
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
    }
    Ok(())
}
