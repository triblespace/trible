use anyhow::Result;
use clap::Parser;
use std::fs::File;
use std::path::PathBuf;

use crate::cli::util::parse_blob_handle;
use tribles::repo::objectstore::ObjectStoreRemote;
use tribles::repo::BlobStore;
use tribles::repo::BlobStoreList;
use tribles::repo::BlobStoreGet;
use tribles::repo::BlobStoreForget;
use tribles::value::schemas::hash::Blake3;
use tribles::value::schemas::hash::Handle;
use url::Url;
use chrono::{NaiveDateTime, DateTime, Utc};
use tribles::repo::BlobStoreMeta;
use tribles::blob::schemas::UnknownBlob;
use tribles::blob::Bytes;
use object_store::parse_url;

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
            let url = Url::parse(&url)?;

            
// Prefer the repo-managed blob listing. Do not fall back to raw
            // listing automatically — bare files were a bug, not a feature.
            let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            let mut reader = remote.reader().map_err(|e| anyhow::anyhow!("remote reader error: {e:?}"))?;

            for item_res in reader.blobs() {
                match item_res {
                    Ok(handle_val) => {
                        let hash: tribles::value::Value<tribles::value::schemas::hash::Hash<Blake3>> = Handle::to_hash(handle_val);
                        let string: String = hash.from_value();
                        println!("{}", string);
                    }
                    Err(e) => return Err(anyhow::anyhow!("list failed: {e:?}")),
                }
            }

            Ok(())
        }
        Command::Put { url, file } => {
            use tribles::blob::schemas::UnknownBlob;
            use tribles::blob::Bytes;
            use tribles::prelude::BlobStorePut;

            use tribles::value::schemas::hash::Hash;

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

            use tribles::prelude::BlobStore;
            use tribles::prelude::BlobStoreGet;


            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            let hash_val = parse_blob_handle(&handle)?;
            let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> = hash_val.into();
            let reader = remote
                .reader()
                .map_err(|e| anyhow::anyhow!("remote reader error: {e:?}"))?;
            let bytes: Bytes = reader.get(handle_val)?;
            let mut file = File::create(&output)?;
            file.write_all(&bytes)?;
            Ok(())
        }
        Command::Inspect { url, handle } => {
            use file_type::FileType;
            use object_store::parse_url;
            use object_store::ObjectStore;
            use tribles::blob::Blob;


            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            let hash_val = parse_blob_handle(&handle)?;
            let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> = hash_val.into();
            let handle_str: String = hash_val.clone().from_value();
            let reader = remote
                .reader()
                .map_err(|e| anyhow::anyhow!("remote reader error: {e:?}"))?;
            let blob: Blob<UnknownBlob> = reader.get(handle_val)?;

            let (store, base) = parse_url(&url)?;
            let handle_hex = handle_str
                .split(':')
                .next_back()
                .ok_or_else(|| anyhow::anyhow!("invalid handle"))?;
            let path = base.child("blobs").child(handle_hex);
            let meta = reader.metadata(handle_val.clone())?;
            let length = meta.as_ref().map(|m| m.length).unwrap_or_default();
            let time_str = if let Some(m) = meta {
                let secs = (m.timestamp / 1000) as i64;
                let nsecs = ((m.timestamp % 1000) * 1_000_000) as u32;
                if let Some(ndt) = chrono::NaiveDateTime::from_timestamp_opt(secs, nsecs) {
                    let dt: chrono::DateTime<chrono::Utc> = chrono::DateTime::from_utc(ndt, chrono::Utc);
                    dt.to_rfc3339()
                } else {
                    "invalid".to_string()
                }
            } else {
                "missing".to_string()
            };

            let ftype = FileType::from_bytes(&blob.bytes);
            let name = ftype.name();

            println!(
                "Hash: {handle_str}\nTime: {}\nLength: {} bytes\nType: {}",
                time_str,
                length,
                name
            );
            Ok(())
        }
        Command::Forget { url, handle } => {


            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            let (store, path) = parse_url(&url)?;
            let hash_val = parse_blob_handle(&handle)?;
            let handle_val: tribles::value::Value<Handle<Blake3, UnknownBlob>> = hash_val.into();
            let blob_handle = handle_val;
            // forget is idempotent
            remote.forget(blob_handle)?;
            Ok(())
        }
    }
}
