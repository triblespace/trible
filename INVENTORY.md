# Inventory

## Potential Removals
- `futures`, `futures-util`, `tokio-util`, and `bytes` dependencies are listed in `Cargo.toml`
  but are unused by the current source code.
- The code base uses `structopt` which is deprecated in favour of `clap`. Migrating
  would simplify argument parsing and remove an outdated crate.
- The asynchronous `tokio` runtime is only used for the `#[tokio::main]` macro. If
  no asynchronous features are added back, this dependency could be dropped.

## Desired Functionality
- Reintroduce commands for managing trible archives (creation, reading, writing).
- Networking capabilities to connect to remote archives/brokers.
- Import/export commands for moving data between files and a running archive.
- Diagnostics and repair tools similar to the old `diagnose` command.
- Basic inspection utilities (listing entities, attributes, etc.).
