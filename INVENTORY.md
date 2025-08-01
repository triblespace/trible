# Inventory

## Potential Removals
- None at the moment.
## Desired Functionality
- Inspection utilities for listing entities, attributes, and relations, with optional filtering.
- Provide progress reporting for blob transfers and other long-running operations.
- Switch to using the published `tribles` crate on crates.io once available.
- Offer an option for the `completion` command to write scripts directly to a file.

## Discovered Issues
- Object store operations rely on an async runtime; consider synchronous alternatives.
