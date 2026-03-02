# Change: Add Table Engine and Write-Ahead Log

## Why
The database needs a foundational storage engine and durability layer before any
higher-level features (queries, snapshots, network) can be built. The table
engine and WAL are tightly coupled — the WAL records table mutations, and
recovery replays the WAL to reconstruct table state.

## What Changes
- Introduce in-memory table storage via STM with a typed column system
- Define core types: `Value`, `Column`, `Schema`, `Row` (`Vector Value`), `Table` (`IntMap`-backed), `Database`
- Implement basic CRUD operations (insert, select, update, delete) with type checking
- Implement table catalog management (create/drop tables)
- Define WAL entry format (with timestamps and row IDs) and binary serialization with versioned format
- Implement TQueue-based WAL write path: STM transactions enqueue WAL entries atomically with mutations; background flusher thread writes to disk and fsyncs
- Implement durability confirmation via TMVar callbacks
- Implement flusher thread supervision (restart or read-only on failure)
- Implement WAL replay for crash recovery (including row ID counter restoration)

## Impact
- Affected specs: none (new capabilities)
- Affected code: new modules — `HsDb.Types`, `HsDb.Table`, `HsDb.WAL`
- New dependency: `vector` (for `Row` representation)
