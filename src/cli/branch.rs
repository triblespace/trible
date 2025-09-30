use anyhow::Result;
use clap::Parser;
use std::convert::TryInto;
use std::path::PathBuf;

// DEFAULT_MAX_PILE_SIZE removed; the new Pile API no longer uses a size const generic
use tribles::prelude::BlobStore;
use tribles::prelude::BlobStoreList;
use tribles::prelude::BranchStore;

#[derive(Parser)]
pub enum BranchCommand {
    /// Push a branch from a pile to a remote object store.
    Push {
        /// URL of the target object store
        url: String,
        /// Path to the source pile file
        pile: PathBuf,
        /// Branch identifier to push (hex encoded)
        branch: String,
    },
    /// Pull a branch from a remote object store into a pile.
    Pull {
        /// URL of the source object store
        url: String,
        /// Path to the destination pile file
        pile: PathBuf,
        /// Branch identifier to pull (hex encoded)
        branch: String,
    },
}

pub fn run(cmd: BranchCommand) -> Result<()> {
    match cmd {
        BranchCommand::Push { url, pile, branch } => {
            use tribles::id::Id;
            use tribles::repo;
            use tribles::repo::objectstore::ObjectStoreRemote;
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;
            use url::Url;

            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            let mut pile: Pile<Blake3> = Pile::open(&pile)?;

            let res = (|| -> Result<(), anyhow::Error> {
                let reader = pile
                    .reader()
                    .map_err(|e| anyhow::anyhow!("pile reader error: {e:?}"))?;

                // The transfer API now takes an explicit iterator of handles to
                // copy. Use the reader's blobs() listing and filter out any
                // listing errors so we have a plain iterator of handles.
                for r in repo::transfer(&reader, &mut remote, reader.blobs().filter_map(|r| r.ok()))
                // TODO: We should log these errors to stderr.
                {
                    r?;
                }

                let raw = hex::decode(branch)?;
                let raw: [u8; 16] = raw.as_slice().try_into()?;
                let id = Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?;

                let handle = pile
                    .head(id)?
                    .ok_or_else(|| anyhow::anyhow!("branch not found"))?;
                let old = remote.head(id)?;
                remote.update(id, old, handle)?;
                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
        BranchCommand::Pull { url, pile, branch } => {
            use tribles::id::Id;
            use tribles::repo;
            use tribles::repo::objectstore::ObjectStoreRemote;
            use tribles::repo::pile::Pile;
            use tribles::value::schemas::hash::Blake3;
            use url::Url;

            let url = Url::parse(&url)?;
            let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            let mut pile: Pile<Blake3> = Pile::open(&pile)?;

            let res = (|| -> Result<(), anyhow::Error> {
                let reader = remote
                    .reader()
                    .map_err(|e| anyhow::anyhow!("remote reader error: {e:?}"))?;

                // Copy all blobs reported by the remote reader into the local
                // pile. Ignore transient listing errors and rely on transfer()
                // to surface actual copy failures.
                for r in repo::transfer(&reader, &mut pile, reader.blobs().filter_map(|r| r.ok())) {
                    // TODO: We should log these errors to stderr.
                    r?;
                }

                let raw = hex::decode(branch)?;
                let raw: [u8; 16] = raw.as_slice().try_into()?;
                let id = Id::new(raw).ok_or_else(|| anyhow::anyhow!("bad id"))?;

                let handle = remote
                    .head(id)?
                    .ok_or_else(|| anyhow::anyhow!("branch not found"))?;
                let old = pile.head(id)?;
                pile.update(id, old, handle)?;
                Ok(())
            })();
            let close_res = pile.close().map_err(|e| anyhow::anyhow!("{e:?}"));
            res.and(close_res)?;
        }
    }
    Ok(())
}
