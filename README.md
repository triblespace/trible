# Trible CLI

A command line tool to interact with [Tribles](https://github.com/triblespace/tribles-rust).

## Commands

- `id-gen` – generate a random identifier.
- `pile create <PATH>` – initialize an empty pile file.
- `pile branch list <PILE>` – list branch identifiers.
- `pile branch create <PILE> <NAME>` – create a new branch.
- `pile blob list <PILE>` – list stored blob handles.
- `pile blob put <PILE> <FILE>` – store a file as a blob.
- `pile blob get <PILE> <HANDLE> <OUTPUT>` – extract a blob by handle.
- `pile blob inspect <PILE> <HANDLE>` – display metadata for a stored blob.
- `pile diagnose <PILE>` – verify pile integrity.
- `store blob list <URL>` – list objects at a remote store URL.
- `store blob put <URL> <FILE>` – upload a file to a remote store.
- `store blob inspect <URL> <HANDLE>` – display metadata for a remote blob.
- `store blob get <URL> <HANDLE> <OUTPUT>` – download a blob from a remote store.
- `store branch list <URL>` – list branches at a remote store URL.
- `branch push <URL> <PILE> <ID>` – push a branch to a remote store.
- `branch pull <URL> <PILE> <ID>` – pull a branch from a remote store.

The project now depends on the unreleased `tribles` crate directly from Git.

See `INVENTORY.md` for notes on possible cleanup and future functionality.
