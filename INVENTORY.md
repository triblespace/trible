# Inventory

## Potential Removals
- None at the moment.
## Desired Functionality
- Inspection utilities for listing entities, attributes, and relations, with optional filtering.
- Provide progress reporting for blob transfers and other long-running operations.
- Switch to using the published `tribles` crate on crates.io once available.
- Allow specifying a custom maximum pile size when creating piles.
- Consolidate shared blob-handling logic across pile and store commands.
- Centralize branch ID resolution helpers across CLI commands.

## Discovered Issues
- Object store operations rely on an async runtime; consider synchronous alternatives.
- Preflight script and test suite take an unusually long time to run; investigate ways to reduce build and execution time.
- Address remaining compiler warnings from unused imports and variables.
