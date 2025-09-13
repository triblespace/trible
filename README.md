# Trible CLI

Trible CLI is a friendly companion for exploring and managing
[Tribles](https://github.com/triblespace/tribles-rust) from the command line.

> **Note:** The project depends on the unreleased `tribles` crate directly
> from Git.

## Installation

```bash
cargo install --path .
```

## Quick Start

1. Create a new pile to hold your data:

   ```bash
   trible pile create demo.pile
   ```

2. Add a file as a blob. This command prints a handle for the stored blob:

   ```bash
   echo "hello" > greeting.txt
   trible pile blob put demo.pile greeting.txt
   ```

3. List the blobs in the pile to confirm the handle:

   ```bash
   trible pile blob list demo.pile
   ```

4. Retrieve the blob using its handle:

   ```bash
   trible pile blob get demo.pile <HANDLE> copy.txt
   ```

The file `copy.txt` now contains the original contents of `greeting.txt`.

## Usage

Run `trible <COMMAND>` to invoke a subcommand.

### Generate identifiers

- `genid` — generate a random identifier.

### Generate shell completions

- `completion <SHELL>` — output a completion script for `bash`, `zsh`, or `fish`.

### Work with piles

- `pile create <PATH>` — initialize an empty pile, creating parent directories as needed.
- `pile diagnose <PILE>` — verify pile integrity.

#### Branches

- `pile branch list <PILE>` — list branch identifiers.
- `pile branch create <PILE> <NAME>` — create a new branch.
 - `pile branch consolidate <PILE> <NAME>` — consolidate multiple branches that share the same logical name into a single new branch. The command creates a single merge commit whose parents are the matching branch heads and prints the new branch id.

Signing key format
 - Commands that create commits (e.g. `create`, `merge-import`, `consolidate`) accept a signing key file path via the `--signing-key` flag. The file must contain a single 64-character hex string (32 bytes encoded as hex). You can also set TRIBLES_SIGNING_KEY to the path of such a file. Generated keys (when created by Codex tooling) are written as hex text to the configured path.

#### Blobs

- `pile blob list [--metadata] <PILE>` — list stored blob handles. Pass `--metadata` to include timestamps and sizes.
- `pile blob put <PILE> <FILE>` — store a file as a blob and print its handle.
- `pile blob get <PILE> <HANDLE> <OUTPUT>` — extract a blob by handle.
- `pile blob inspect <PILE> <HANDLE>` — display metadata for a stored blob.

### Work with remote stores

#### Blobs

- `store blob list <URL>` — list objects at a remote store.
- `store blob put <URL> <FILE>` — upload a file to a remote store and print its handle.
- `store blob get <URL> <HANDLE> <OUTPUT>` — download a blob from a remote store.
- `store blob forget <URL> <HANDLE>` — remove an object from a remote store.
- `store blob inspect <URL> <HANDLE>` — display metadata for a remote blob.

#### Branches

- `store branch list <URL>` — list branches at a remote store.
- `branch push <URL> <PILE> <ID>` — push a branch to a remote store.
- `branch pull <URL> <PILE> <ID>` — pull a branch from a remote store.

See `INVENTORY.md` for notes on possible cleanup and future functionality.

## Development

Command implementations live in `src/cli/` with modules for `branch`, `pile`,
and `store`. The modules expose their subcommands and are re-exported from
`main.rs` to preserve the existing CLI interface. Contributions are always
welcome!
