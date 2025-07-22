# Trible CLI

A command line tool to interact with [Tribles](https://github.com/triblespace/tribles-rust).
Currently the tool provides a simple `id-gen` command, `pile list-branches` for
inspecting local pile files, `pile list-blobs` for enumerating stored blob
handles, `pile create` to initialize an empty pile file, `pile put`/`get` for
transferring blobs, and `pile diagnose` to verify pile integrity by ensuring all
blobs match their hashes. The diagnose command exits with a nonzero code if
corruption is found. It previously contained a
number of experimental features (such as a broker/archiver and a notebook
interface) which have been removed.

The project now depends on the unreleased `tribles` crate directly from Git.

See `INVENTORY.md` for notes on possible cleanup and future functionality.
