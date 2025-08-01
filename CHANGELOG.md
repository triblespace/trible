# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Initial changelog with Let's Changelog format.
- Integration tests for `id-gen` and `pile list-branches` commands.
- `pile create` command to initialize new pile files.
- Note that `touch` on Unix can also create an empty pile file.
- `pile put` command for ingesting a file into a pile.
- `pile put` now memory maps the input for efficient ingestion.
- `pile get` command to extract blobs from a pile by handle.
- `pile blob inspect` command to show blob metadata like timestamp and size.
- `pile list-blobs` command to enumerate blob handles in a pile.
- `pile list-blobs` output now uses built-in `Hash` formatting.
- `pile diagnose` command to check pile integrity.
- `pile diagnose` now verifies that all blob hashes match.
- `pile diagnose` now exits with a nonzero code when corruption is detected.
- `store blob list` command to enumerate object store contents.
- `store blob put` command to upload files to object stores.
- `store blob forget` command to remove objects from object stores.
- `store blob inspect` command to display metadata for remote blobs.
- `store blob get` command to download blobs from object stores.
- `store branch list` command to list branches in an object store.
- `pile branch create` command to create a new branch.
- `branch push` and `branch pull` commands to sync branches with remote stores.
- Tests for branch creation and branch push/pull using a file object store.
- Logged an inventory task to provide a structured command overview in the README.
- Structured command overview in the README.
- Logged inventory tasks for inspection utilities, shell completions, progress reporting, and migrating to the published `tribles` crate.
- Renamed the future `store delete` command to `store forget` in the inventory.
- Step-by-step quick-start example in the README.
### Changed
- Expanded `AGENTS.md` with sections from the Tribles project and a dedicated
  inventory subsection.
- Expanded crate metadata with additional keywords and categories.
- Removed explanatory comment about crate metadata from `Cargo.toml`.
- Increased default maximum pile size to 16 TiB.
- Fixed `pile put` compilation issues when using memmap.
- Renamed `pile pull` to `pile get` to avoid confusion with repository commands.
- Reworded inventory note about import/export commands to clarify blob
  transfers to piles and object stores via dedicated subcommands.
- Simplified `Pile::open` error handling now that `OpenError` implements
  `std::error::Error` upstream.
- `pile list-blobs` output uses lowercase hex instead of uppercase.
- Pile commands reorganized under `branch` and `blob` subcommands.
- Store commands reorganized under `branch` and `blob` subcommands.
- Simplified file ingestion using `anybytes::Bytes::map_file` and removed
  the `memmap2` dependency.
- Split CLI command groups into modules under `src/cli`.
- Rewrote README with a friendlier tone and clarified command list.
- Corrected pile file extension in README quick-start example.
### Removed
- Completed work entries have been trimmed from `INVENTORY.md` now that they are
  tracked here.
- Removed completed inventory item for crate metadata expansion.
- Removed note from README suggesting `touch` can create empty piles.
- Removed inventory entry for the old `diagnose` command now that the feature is
  implemented.
- Removed inventory item for the `pile list-blobs` command now that the feature
  exists.
- Removed inventory note for the `store blob forget` command now that the feature
  exists.
- Removed inventory notes for `store blob get` and `store blob inspect` now that those commands are implemented.
- Removed inventory note about `anybytes` helper integration.
- Removed stray `.orig` backup files from `src` and `tests` directories.
- Removed inventory note for a README quick-start example now that the section exists.
