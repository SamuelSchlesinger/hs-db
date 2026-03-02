## Context
hs-db needs a storage engine and durability layer as its foundation. All future
features (queries, snapshots, network protocol) depend on these two components.
The table engine and WAL are designed together because they are tightly coupled:
mutations are recorded as WAL entries within the same STM transaction that
modifies in-memory state, and recovery replays WAL entries to reconstruct
tables.

## Goals / Non-Goals
- Goals:
  - Correct concurrent access to in-memory tables via STM
  - Durable mutations via write-ahead logging
  - Crash recovery by replaying WAL entries
  - Type-safe column system aligned with PostgreSQL basics
  - Versioned binary format for forward compatibility
- Non-Goals:
  - Query planning or SQL parsing (future work)
  - Incremental snapshots (future proposal)
  - Network protocol (future proposal)
  - Performance optimization (correctness first)

## Decisions

### Write Path: TBQueue + Background Flusher
The central architectural decision. STM cannot perform IO, so we cannot fsync
inside a transaction. Instead, inspired by PostgreSQL's WAL buffer design:

1. **STM transaction**: atomically mutates table state AND appends a `WALEntry`
   to a `TBQueue WALEntry`. If the STM transaction retries (conflict), the
   enqueue also rolls back — no orphaned WAL entries.
2. **Background flusher thread**: continuously drains the `TBQueue`, serializes
   entries, writes them to the WAL file, and calls `fsync`. Multiple entries
   that have accumulated are flushed together (natural group commit).
3. **Durability confirmation**: each WAL entry is paired with a `TMVar ()` that
   the flusher signals after the fsync completes. Callers that need synchronous
   durability call `atomically (takeTMVar callback)` after their STM
   transaction commits. Callers that accept asynchronous durability can ignore
   the callback.

This design preserves STM's concurrency: multiple writers can modify different
tables (or even different rows in the same table) concurrently, with WAL
serialization happening only at the IO layer (the flusher thread), not the STM
layer.

- Decision: the flusher thread is the single writer to the WAL file, so no
  concurrent file access synchronization is needed.
- Decision: if the flusher thread dies (exception), the database must stop
  accepting writes. The flusher runs inside a supervisor that either restarts
  it or transitions the database to a read-only error state.
- Decision: on clean shutdown, drain the TBQueue fully and fsync before closing
  the WAL file.

### Table Representation
- Row storage: `TVar (IntMap Row)` — `IntMap` is substantially faster than
  `Map` for integer keys. On 64-bit GHC (the target), `Int` is 64 bits, so
  `RowId = Int` provides sufficient range. A compile-time assertion will verify
  the 64-bit assumption.
- Table catalog: `TVar (Map TableName Table)` for concurrent table management.
  `Map` is appropriate here because table names are `Text` and the number of
  tables is small.

### Column Type System
- Types aligned with PostgreSQL basics: Int32, Int64, Float64, Text, Bool, Bytea, Null
- `Row = Vector Value` — `Vector` gives O(1) column access by index, which
  matters for future query operations. Adds a dependency on `vector`.
- `Value` is an algebraic sum: `VInt32 Int32 | VInt64 Int64 | VFloat64 Double
  | VText Text | VBool Bool | VBytea ByteString | VNull`
- Schema stored per-table: column names, types, nullability constraints
- Type checking enforced on insert and update against the table schema

### Row ID Generation
- Monotonic counter per table, stored in the `Table` record as `TVar Int`
- Row IDs are never reused within a table's lifetime
- Decision: `Int` (64-bit on target platform) provides sufficient range
- WAL `InsertRow` entries include the assigned row ID so that replay
  reconstructs the exact same state. On replay, the per-table counter is
  restored to `max(replayed row IDs) + 1`.

### WAL Entry Contents
Each WAL entry variant carries:
- `CreateTable`: table name, schema (column names, types, nullability)
- `InsertRow`: table name, row ID, values (as `Vector Value`)
- `UpdateRow`: table name, row ID, new values
- `DeleteRow`: table name, row ID
- `DropTable`: table name

All entries include:
- Monotonic sequence number (for ordering and replay verification)
- Wall-clock timestamp (`UTCTime`, 8 bytes) for debugging and auditing

Sequence numbers are assigned by the flusher thread at flush time, not inside
the STM transaction. This avoids ordering issues where concurrent STM
transactions would assign sequence numbers that don't match the flusher's
actual write order. The STM transaction enqueues an entry without a sequence
number; the flusher stamps it monotonically as it drains the queue.

### WAL Format
- Append-only file of length-prefixed binary entries
- File header: 4-byte magic (`HSDB`), 2-byte format version
- Each entry: 8-byte length prefix, payload bytes, 4-byte CRC32 checksum
- CRC32 covers the length prefix AND the payload (a corrupted length prefix
  would cause reading the wrong number of bytes)
- Sequence numbers are monotonically increasing across the entire WAL, not
  per-table

### Recovery
- Open WAL file, verify magic bytes and version
- Deserialize entries sequentially, verify CRC32 checksums
- Replay each entry against empty STM state in sequence-number order
- `InsertRow` entries carry the original row ID; replay inserts with that
  exact ID and advances the per-table counter accordingly
- `DropTable` followed by `CreateTable` with the same name is valid; replay
  handles this by dropping and re-creating
- Corrupted trailing entries (incomplete write) are truncated with a warning
- Empty WAL file (header only, no entries) is a valid state — produces an
  empty database
- After successful replay, start the flusher thread and accept new operations

### API Layering
Two API layers, each with a clear purpose:

1. **Low-level STM layer** (`HsDb.Table`): pure table operations that know
   nothing about the WAL. These are composable STM actions operating on `Table`
   and `Database` values.
   - `createTable :: Database -> TableName -> Schema -> STM (Either Error Table)`
   - `insertRow :: Table -> Vector Value -> STM (Either Error RowId)`
   - `selectAll :: Table -> STM [(RowId, Row)]`
   - etc.

2. **High-level IO layer** (`HsDb`): composes STM mutations with WAL enqueue
   and durability confirmation. This is the primary public API for users.
   - `durableInsert :: Database -> TableName -> Vector Value -> IO (Either Error RowId)`
   - `durableCreateTable :: Database -> TableName -> Schema -> IO (Either Error ())`
   - etc.
   - Each high-level operation: runs the STM transaction (which mutates state
     and enqueues the WAL entry), then waits for the flusher's durability
     callback before returning.
   - An `async` variant of each operation is available for callers that accept
     asynchronous durability (returns immediately after STM commit).

The low-level layer is exposed for advanced use cases (composing multi-operation
STM transactions), but most users should use the high-level IO API.

Multi-table transactions (e.g., create table + insert row atomically) are
supported by composing low-level STM operations. The resulting STM transaction
enqueues multiple WAL entries; the flusher writes them in the same batch.

### Resource Management
The `Database` owns a flusher thread and WAL file handle. A bracket-style
resource manager ensures cleanup:

- `withDatabase :: FilePath -> DatabaseConfig -> (Database -> IO a) -> IO a`
  — opens or replays the WAL, starts the flusher thread, runs the user action,
  then performs clean shutdown (drain queue, fsync, close file, kill flusher).
- `DatabaseConfig` includes the TBQueue capacity and any future tuning knobs.
- `openDatabase` / `closeDatabase` are available for cases where bracket style
  doesn't fit, but `withDatabase` is preferred.

### Binary Serialization
- Use `binary` package for encoding/decoding (or hand-rolled `ByteString`
  builders if `binary` dependency is undesirable)
- All formats versioned: version bump required for any structural change
- Decision: start with `Data.Binary` for simplicity; migrate to manual
  builders only if profiling shows serialization is a bottleneck

## Risks / Trade-offs
- **Flusher thread complexity**: the background flusher adds a concurrent
  component that must be supervised. If it crashes or falls behind, writes
  back up in the TBQueue. Mitigation: bounded TBQueue with configurable capacity;
  flusher supervised with restart-or-read-only policy.
- **Asynchronous durability by default**: callers that forget to wait on the
  durability callback may lose writes on crash. Mitigation: provide a
  high-level API that defaults to synchronous durability (waits for callback)
  with an explicit opt-in for async mode.
- **IntMap assumes 64-bit Int**: will silently truncate on 32-bit platforms.
  Mitigation: compile-time assertion `staticAssert (finiteBitSize (0 :: Int) >= 64)`.
- **No WAL compaction**: WAL file grows unbounded until snapshot truncation is
  implemented in a future proposal.
- **Single WAL file**: becomes a serialization point at the flusher thread
  under extreme write load. Acceptable for initial implementation; segmented
  WAL is a future optimization.

## Resolved Questions
- **Bounded TBQueue**: the flusher uses a `TBQueue` (bounded). Backpressure is
  important — if writes outpace flushing, STM transactions will block (retry)
  when the queue is full, naturally throttling writers. The queue capacity will
  be configurable at database creation time.
