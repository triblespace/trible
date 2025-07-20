# Trible CLI

A command line tool to interact with [Tribles](https://github.com/triblespace/tribles-rust).
Currently the tool provides a simple `id-gen` command, `pile list-branches` for
inspecting local pile files, and `pile create` to initialize an empty pile
file. The latter is mostly a cross-platform convenience; on Unix-like systems a
plain `touch` achieves the same result.
It previously contained a
number of experimental features (such as a broker/archiver and a notebook
interface) which have been removed.

The project now depends on the unreleased `tribles` crate directly from Git.

See `INVENTORY.md` for notes on possible cleanup and future functionality.
