# Trible CLI

A command line tool to interact with [Tribles](https://github.com/triblespace/tribles-rust).
Currently the tool provides a simple `id-gen` command, `pile list-branches` for
inspecting local pile files, `pile create` to initialize an empty pile file, and
`pile put`/`pull` for transferring blobs. It previously contained a
number of experimental features (such as a broker/archiver and a notebook
interface) which have been removed.

The project now depends on the unreleased `tribles` crate directly from Git.

See `INVENTORY.md` for notes on possible cleanup and future functionality.
