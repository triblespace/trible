use anyhow::Result;
use clap::Parser;
use std::convert::TryInto;
use std::path::PathBuf;

use crate::DEFAULT_MAX_PILE_SIZE;
use tribles::prelude::BlobStore;
use tribles::prelude::BlobStoreGet;
use tribles::prelude::BranchStore;
use tribles::prelude::Id;

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
            let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;

            let reader = pile.reader();
            for r in repo::transfer::<_, _, Blake3, Blake3, tribles::blob::schemas::UnknownBlob>(
                &reader,
                &mut remote,
            ) {
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
            let mut pile: Pile<DEFAULT_MAX_PILE_SIZE, Blake3> = Pile::open(&pile)?;

            let reader = remote.reader();
            for r in repo::transfer::<_, _, Blake3, Blake3, tribles::blob::schemas::UnknownBlob>(
                &reader, &mut pile,
            ) {
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
            pile.flush().map_err(|e| anyhow::anyhow!("{e:?}"))?;
        }
    }
    Ok(())
}
