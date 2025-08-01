use anyhow::Result;
use tribles::prelude::TryToValue;
use tribles::value::schemas::hash::{Blake3, Hash};

pub fn parse_blob_handle(handle: &str) -> Result<tribles::value::Value<Hash<Blake3>>> {
    handle.try_to_value().map_err(|e| anyhow::anyhow!("{e:?}"))
}
