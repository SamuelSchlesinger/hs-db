# Project Context

## Purpose
hs-db is a Haskell library implementing a relational database engine. It uses
STM for concurrent in-memory table operations, a write-ahead log (WAL) for
durability, and an incremental snapshot system for efficient persistence and
crash recovery. The library is designed to be embedded in Haskell applications,
with a planned network layer exposing a subset of the PostgreSQL wire protocol.

## Tech Stack
- Haskell (GHC 9.6, Haskell2010)
- Cabal 3.14 build system
- STM for concurrency
- ByteString / binary serialization for WAL and snapshots
- Hedgehog for property-based and state-machine testing

## Project Conventions

### Code Style
- Follow standard Haskell conventions
- Use explicit export lists on all modules
- Prefer qualified imports for non-Prelude modules
- Use `-Wall` (already configured in cabal)

### Architecture Patterns
- Pure core with STM at the concurrency boundary
- IO limited to WAL writes, snapshot I/O, and network
- Separate modules for each concern: table engine, WAL, snapshots, query
- Types defined in dedicated modules, operations alongside their types

### Testing Strategy
- Hedgehog state-machine tests for verifying database operation sequences
- Hedgehog property tests for serialization round-trips and invariants
- Unit tests for edge cases and specific scenarios

### Git Workflow
- Feature branches with descriptive names
- OpenSpec proposal before implementation of new capabilities
- Commits scoped to logical units of work

## Domain Context
- **STM (Software Transactional Memory)**: GHC's composable concurrency
  primitive. Transactions are atomic, consistent, and isolated. STM retries
  on conflict, so no explicit locking is needed.
- **WAL (Write-Ahead Log)**: Every mutation is appended to a sequential log
  file before the in-memory state is modified. On crash, replaying the WAL
  restores the database to a consistent state.
- **Incremental Snapshots**: Periodic serialization of the in-memory state to
  disk. "Incremental" means only changed data since the last snapshot is
  written, reducing I/O. After a snapshot, the WAL can be truncated.

## Important Constraints
- Library-first: no network layer initially, pure Haskell API
- Correctness over performance: STM simplicity preferred over manual locking
- All persistent formats (WAL entries, snapshots) must be versioned for
  forward compatibility

## External Dependencies
- `stm` (GHC bundled)
- `bytestring` for binary data
- `hedgehog` for testing
- Additional dependencies TBD as implementation proceeds
