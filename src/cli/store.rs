use super::util::parse_blob_handle;
use anyhow::Result;
use clap::Parser;
use std::{fs::File, path::PathBuf};
use tribles::prelude::{BlobStore, BlobStoreGet, BlobStorePut, BranchStore};

#[derive(Parser)]
pub enum StoreCommand {
    /// Operations on branches stored in a remote object store.
    Branch {
        #[command(subcommand)]
        cmd: StoreBranchCommand,
    },
    /// Operations on blobs stored in a remote object store.
    Blob {
        #[command(subcommand)]
        cmd: StoreBlobCommand,
    },
}

#[derive(Parser)]
pub enum StoreBranchCommand {
    /// List all branch identifiers at the given URL.
    List {
        /// URL of the object store to inspect (e.g. "s3://bucket/path" or "file:///path")
        url: String,
    },
}

#[derive(Parser)]
pub enum StoreBlobCommand {
    /// List objects at the given URL.
    List {
        /// URL of the object store to inspect (e.g. "s3://bucket/path" or "file:///path")
        url: String,
    },
    /// Upload a file to a remote object store.
    Put {
        /// URL of the destination object store (e.g. "s3://bucket/path" or "file:///path")
        url: String,
        /// File whose contents should be stored remotely
        file: PathBuf,
    },
    /// Download a blob from a remote object store.
    Get {
        /// URL of the source object store (e.g. "s3://bucket/path" or "file:///path")
        url: String,
        /// Handle of the blob to retrieve (e.g. "blake3:HEX...")
        handle: String,
        /// Destination file path for the extracted blob
        output: PathBuf,
    },
    /// Inspect a remote blob and print basic metadata.
    Inspect {
        /// URL of the source object store (e.g. "s3://bucket/path" or "file:///path")
        url: String,
        /// Handle of the blob to inspect (e.g. "blake3:HEX...")
        handle: String,
    },
    /// Remove a blob from a remote object store.
    Forget {
        /// URL of the object store (e.g. "s3://bucket/path" or "file:///path")
        url: String,
        /// Handle of the blob to delete (e.g. "blake3:HEX...")
        handle: String,
    },
}

pub fn run(cmd: StoreCommand) -> Result<()> {
    match cmd {
        StoreCommand::Blob { cmd } => match cmd {
            StoreBlobCommand::List { url } => {
                use futures::StreamExt;
                use object_store::{parse_url, ObjectStore};
                use url::Url;

                let url = Url::parse(&url)?;
                let (store, path) = parse_url(&url)?;
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(async move {
                    let mut stream = store.list(Some(&path));
                    while let Some(meta) = stream.next().await.transpose()? {
                        println!("{}", meta.location);
                    }
                    Ok::<(), anyhow::Error>(())
                })?;
            }
            StoreBlobCommand::Put { url, file } => {
                use tribles::blob::{schemas::UnknownBlob, Bytes};
                use tribles::repo::objectstore::ObjectStoreRemote;
                use tribles::value::schemas::hash::Blake3;
                use url::Url;

                let url = Url::parse(&url)?;
                let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
                let file_handle = File::open(&file)?;
                let bytes = unsafe { Bytes::map_file(&file_handle)? };
                remote.put::<UnknownBlob, _>(bytes)?;
            }
            StoreBlobCommand::Get {
                url,
                handle,
                output,
            } => {
                use std::io::Write;

                use tribles::blob::{schemas::UnknownBlob, Bytes};
                use tribles::repo::objectstore::ObjectStoreRemote;
                use tribles::value::schemas::hash::{Blake3, Handle};
                use url::Url;

                let url = Url::parse(&url)?;
                let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
                let hash_val = parse_blob_handle(&handle)?;
                let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> =
                    hash_val.clone().into();
                let reader = remote.reader();
                let bytes: Bytes = reader.get(handle_val)?;
                let mut file = File::create(&output)?;
                file.write_all(&bytes)?;
            }
            StoreBlobCommand::Inspect { url, handle } => {
                use file_type::FileType;
                use futures::executor::block_on;
                use object_store::{parse_url, ObjectStore};
                use tribles::blob::{schemas::UnknownBlob, Blob};
                use tribles::repo::objectstore::ObjectStoreRemote;
                use tribles::value::schemas::hash::{Blake3, Handle};
                use url::Url;

                let url = Url::parse(&url)?;
                let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
                let hash_val = parse_blob_handle(&handle)?;
                let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> =
                    hash_val.clone().into();
                let handle_str: String = hash_val.clone().from_value();
                let reader = remote.reader();
                let blob: Blob<UnknownBlob> = reader.get(handle_val)?;

                let (store, base) = parse_url(&url)?;
                let handle_hex = handle_str
                    .split(':')
                    .last()
                    .ok_or_else(|| anyhow::anyhow!("invalid handle"))?;
                let path = base.child("blobs").child(handle_hex);
                let meta = block_on(async { store.head(&path).await })?;
                let time = meta.last_modified;
                let length = meta.size;

                let ftype = FileType::from_bytes(&blob.bytes);
                let name = ftype.name();

                println!(
                    "Hash: {handle_str}\nTime: {}\nLength: {} bytes\nType: {}",
                    time.to_rfc3339(),
                    length,
                    name
                );
            }
            StoreBlobCommand::Forget { url, handle } => {
                use object_store::{parse_url, ObjectStore};
                use tribles::blob::schemas::UnknownBlob;
                use tribles::value::schemas::hash::{Blake3, Handle};
                use url::Url;

                let url = Url::parse(&url)?;
                let (store, path) = parse_url(&url)?;
                let hash_val = parse_blob_handle(&handle)?;
                let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> =
                    hash_val.clone().into();
                let blob_path = path.child("blobs").child(hex::encode(handle_val.raw));
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(async move { store.delete(&blob_path).await })?;
            }
        },
        StoreCommand::Branch { cmd } => match cmd {
            StoreBranchCommand::List { url } => {
                use tribles::repo::objectstore::ObjectStoreRemote;
                use tribles::value::schemas::hash::Blake3;
                use url::Url;

                let url = Url::parse(&url)?;
                let remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
                for branch in remote.branches() {
                    let id = branch?;
                    println!("{id:X}");
                }
            }
        },
    }
    Ok(())
}
