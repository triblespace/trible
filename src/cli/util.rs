use anyhow::Result;
use triblespace::prelude::TryToValue;
use triblespace::value::schemas::hash::Blake3;
use triblespace::value::schemas::hash::Hash;

pub fn parse_blob_handle(handle: &str) -> Result<triblespace::value::Value<Hash<Blake3>>> {
    handle.try_to_value().map_err(|e| anyhow::anyhow!("{e:?}"))
}
