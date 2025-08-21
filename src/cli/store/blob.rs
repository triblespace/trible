use anyhow::Result;
use clap::Parser;
use std::fs::File;
use std::path::PathBuf;

use crate::cli::util::parse_blob_handle;

#[derive(Parser)]
pub enum Command {
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

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::List { url } => {
            use futures::StreamExt;
            use object_store::parse_url;
            use object_store::ObjectStore;
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
            Ok(())
        }
        Command::Put { url, file } => {
            use tribles::blob::schemas::UnknownBlob;
            use tribles::blob::Bytes;
            use tribles::prelude::BlobStorePut;
            use tribles::repo::objectstore::ObjectStoreRemote;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use tribles::value::schemas::hash::Hash;
            use url::Url;

            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            let file_handle = File::open(&file)?;
            let bytes = unsafe { Bytes::map_file(&file_handle)? };
            let handle = remote.put::<UnknownBlob, _>(bytes)?;
            let hash: tribles::value::Value<Hash<Blake3>> = Handle::to_hash(handle);
            let string: String = hash.from_value();
            println!("{string}");
            Ok(())
        }
        Command::Get {
            url,
            handle,
            output,
        } => {
            use std::io::Write;

            use tribles::blob::schemas::UnknownBlob;
            use tribles::blob::Bytes;
            use tribles::prelude::BlobStoreGet;
            use tribles::repo::objectstore::ObjectStoreRemote;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use url::Url;

            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            let hash_val = parse_blob_handle(&handle)?;
            let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> = hash_val.into();
            let reader = remote.reader();
            let bytes: Bytes = reader.get(handle_val)?;
            let mut file = File::create(&output)?;
            file.write_all(&bytes)?;
            Ok(())
        }
        Command::Inspect { url, handle } => {
            use file_type::FileType;
            use futures::executor::block_on;
            use object_store::parse_url;
            use object_store::ObjectStore;
            use tribles::blob::schemas::UnknownBlob;
            use tribles::blob::Blob;
            use tribles::prelude::BlobStoreGet;
            use tribles::repo::objectstore::ObjectStoreRemote;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use url::Url;

            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            let hash_val = parse_blob_handle(&handle)?;
            let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> = hash_val.into();
            let handle_str: String = hash_val.clone().from_value();
            let reader = remote.reader();
            let blob: Blob<UnknownBlob> = reader.get(handle_val)?;

            let (store, base) = parse_url(&url)?;
            let handle_hex = handle_str
                .split(':')
                .next_back()
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
            Ok(())
        }
        Command::Forget { url, handle } => {
            use object_store::parse_url;
            use object_store::ObjectStore;
            use tribles::blob::schemas::UnknownBlob;
            use tribles::value::schemas::hash::Blake3;
            use tribles::value::schemas::hash::Handle;
            use url::Url;

            let url = Url::parse(&url)?;
            let (store, path) = parse_url(&url)?;
            let hash_val = parse_blob_handle(&handle)?;
            let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> = hash_val.into();
            let blob_path = path.child("blobs").child(hex::encode(handle_val.raw));
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(async move { store.delete(&blob_path).await })?;
            Ok(())
        }
    }
}
