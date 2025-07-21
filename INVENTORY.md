# Inventory

## Potential Removals
- None at the moment.
## Desired Functionality
- Reintroduce commands for managing trible archives (creation, reading, writing).
- Networking capabilities to connect to remote archives/brokers.
- Import/export commands for moving data between files and a running archive.
- Diagnostics and repair tools similar to the old `diagnose` command.
- Basic inspection utilities (listing entities, attributes, etc.).
- Add support for inspecting remote object stores (S3, B2, etc.).
- Incorporate new `anybytes` memory-mapping helpers once they become
  available.

## Discovered Issues
- `OpenError` from the `Pile` API does not implement `std::error::Error`, which
  makes error handling with libraries like `anyhow` cumbersome. Consider adding
  an `Error` implementation upstream.
