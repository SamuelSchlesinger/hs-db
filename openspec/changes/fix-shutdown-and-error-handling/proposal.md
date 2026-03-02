# Change: Fix shutdown protocol, flusher supervision, and error handling

## Why
The current WAL flusher and database lifecycle implementation has several
correctness gaps: the shutdown protocol uses a timing-based race condition
instead of deterministic synchronization, the flusher thread has no supervisor
so its death silently hangs all writers, flush errors leave callers blocked
forever on unsignaled callbacks, and WAL replay silently discards corrupted
trailing entries without any warning. These issues violate guarantees stated in
the existing specs and can cause silent data loss or deadlocks.

## What Changes
- Replace `threadDelay` + `killThread` shutdown with deterministic flusher
  coordination via an `MVar ()` completion signal
- Add flusher thread supervision that detects thread death and transitions to
  read-only
- Signal durability callbacks with an error on flush failure instead of leaving
  them unsignaled
- Return replay warnings (truncated entries) to the caller instead of silently
  discarding
- Pass table name through `updateRow`/`deleteRow` so `RowNotFound` errors
  include context
- Make `insertRowWithId` reject duplicate row IDs during replay instead of
  silently overwriting
- Propagate flush errors from `drainRemaining` during shutdown

## Impact
- Affected specs: `wal`, `table-engine`
- Affected code: `HsDb.hs`, `HsDb/WAL/Writer.hs`, `HsDb/WAL/Replay.hs`,
  `HsDb/Table.hs`, `HsDb/Integration.hs`, `HsDb/WAL/Types.hs`, `HsDb/Types.hs`
