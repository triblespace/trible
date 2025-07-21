# Inventory

## Potential Removals
- None at the moment.
## Desired Functionality
- Commands to put blobs into and get blobs from piles and object stores using
  their dedicated subcommands.
- Diagnostics and repair tools similar to the old `diagnose` command.
- Basic inspection utilities (listing entities, attributes, etc.).
- Add support for inspecting remote object stores (S3, B2, etc.).
- Incorporate new `anybytes` memory-mapping helpers once they become
  available.

## Discovered Issues
- `OpenError` from the `Pile` API does not implement `std::error::Error`, which
  makes error handling with libraries like `anyhow` cumbersome. Consider adding
  an `Error` implementation upstream.
