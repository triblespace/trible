use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
pub enum Command {
    /// List all branch identifiers at the given URL.
    List {
        /// URL of the object store to inspect (e.g. "s3://bucket/path" or "file:///path")
        url: String,
    },
}

pub fn run(cmd: Command) -> Result<()> {
    match cmd {
        Command::List { url } => {
            use tribles::repo::objectstore::ObjectStoreRemote;
            use tribles::value::schemas::hash::Blake3;
            use url::Url;

            let url = Url::parse(&url)?;
            let remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;
            for branch in remote.branches() {
                let id = branch?;
                println!("{id:X}");
            }
            Ok(())
        }
    }
}
