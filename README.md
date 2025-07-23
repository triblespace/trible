# Trible CLI

A command line tool to interact with [Tribles](https://github.com/triblespace/tribles-rust).

## Commands

- `id-gen` – generate a random identifier.
- `pile create <PATH>` – initialize an empty pile file.
- `pile branch list <PILE>` – list branch identifiers.
- `pile blob list <PILE>` – list stored blob handles.
- `pile blob put <PILE> <FILE>` – store a file as a blob.
- `pile blob get <PILE> <HANDLE> <OUTPUT>` – extract a blob by handle.
- `pile blob inspect <PILE> <HANDLE>` – display metadata for a stored blob.
- `pile diagnose <PILE>` – verify pile integrity.
- `store blob list <URL>` – list objects at a remote store URL.
- `store branch list <URL>` – list branches at a remote store URL.

The project now depends on the unreleased `tribles` crate directly from Git.

See `INVENTORY.md` for notes on possible cleanup and future functionality.
