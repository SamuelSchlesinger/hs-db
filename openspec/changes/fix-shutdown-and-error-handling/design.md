# Design: Fix Shutdown Protocol, Flusher Supervision, and Error Handling

## Context

The initial implementation of the WAL flusher and database lifecycle was built
to prove out the architecture. It cut corners in several areas that affect
correctness under failure conditions: the shutdown path uses a timing-based
heuristic (`threadDelay 10ms` + `killThread`), the flusher thread runs
unsupervised, and errors during flush leave durability callbacks permanently
unsignaled. These issues span `HsDb.hs`, `HsDb/WAL/Writer.hs`,
`HsDb/WAL/Replay.hs`, `HsDb/Table.hs`, `HsDb/Integration.hs`, and
`HsDb/WAL/Types.hs`.

## Goals / Non-Goals

**Goals:**
- Deterministic, race-free shutdown protocol
- Flusher thread supervision with automatic transition to read-only on crash
- All durability callbacks signaled (with success or error), never left hanging
- WAL replay surfaces truncation warnings to the caller
- Correct error context in `RowNotFound` (include table name)
- Idempotent replay: reject duplicate row IDs instead of silent overwrite
- Flush errors during shutdown propagated to `closeDatabase`

**Non-Goals:**
- Flusher restart on crash (transition to read-only is sufficient for now)
- Changing the WAL file format or serialization
- Adding new WAL entry types
- Snapshot or compaction

## Decisions

### D1: Deterministic shutdown via MVar () completion signal

**Current:** `closeDatabase` waits for the queue to become empty via STM, then
sleeps 10ms and calls `killThread`. This races: the flusher may not have
finished writing + fsync before the thread is killed.

**New:** The flusher thread, upon seeing `DbShuttingDown`, drains remaining
entries, performs the final fsync, and then puts `()` into an `MVar ()` (the
"completion signal"). `closeDatabase` blocks on taking this MVar instead of
sleeping. This makes shutdown deterministic — the thread completes its work
before the caller proceeds to close the WAL file. No `threadDelay` or
`killThread` needed.

The `MVar ()` is stored in the `WALHandle` as `walDone :: MVar ()`.

### D2: Flusher supervision via `forkFinally`

**Current:** `forkIO (flusherThread wal)` — if the flusher dies from an
uncaught exception, no one notices. Writers block forever on their TMVar
callbacks.

**New:** Use a wrapper that catches the flusher's exit reason:
- Normal exit → the flusher completed shutdown cleanly.
- Exception → set `dbStatus` to `DbReadOnly` with the exception message, and
  signal `walDone` so `closeDatabase` doesn't hang if the flusher crashes during
  shutdown.

This means `closeDatabase` always unblocks regardless of flusher fate.

### D3: Signal callbacks with error on flush failure

**Current:** When `flushBatch` catches an IO exception, it returns `Left err`
but does not signal the TMVar callbacks for that batch. Those callers block
forever.

**New:** In the error branch of `flushBatch`, signal all callbacks in the failed
batch before returning the error. The callbacks are currently `TMVar ()`, so we
signal them with `()` — the error is communicated through the `DbReadOnly`
status. Any subsequent write attempts will be rejected by `checkWritable`.

An alternative would be to change `TMVar ()` to `TMVar (Either Text ())` so
callers learn that their specific entry may not have been durably written. This
adds complexity to every caller. For now, signaling with `()` plus the
read-only transition is sufficient — callers who care about durability after a
crash should detect the `DatabaseNotWritable` error on their next operation.

### D4: Return replay warnings to caller

**Current:** `replayWAL` returns `Either DbError (TableCatalog, Word64)`.
Corrupted trailing entries are silently skipped — the caller has no way to know
data was truncated.

**New:** Add a `[Text]` warnings list to the success return type:
`Either DbError (TableCatalog, Word64, [Text])`. When a corrupted trailing
entry is encountered, append a descriptive warning to this list. The caller
(e.g., `openDatabase`) can log or surface these warnings.

### D5: Pass table name through updateRow / deleteRow

**Current:** `updateRow` and `deleteRow` in `Table.hs` produce
`RowNotFound "" rowId` because they don't know the table name.

**New:** Add a `TableName` parameter to `updateRow` and `deleteRow`. The
Integration layer already has the table name available and passes it through.
This also requires updating `Replay.hs` which calls these functions.

### D6: Reject duplicate row IDs in insertRowWithId

**Current:** `insertRowWithId` unconditionally overwrites existing rows during
replay.

**New:** Check `IntMap.member rowId rows` before inserting. If the row ID
already exists, return `Left (DuplicateRowId tableName rowId)`. A new
`DuplicateRowId` constructor is added to `DbError`. During replay, this is a
fatal error (WAL corruption), not a warning.

### D7: Propagate flush errors from drainRemaining

**Current:** `drainRemaining` in the flusher thread ignores the `Either`
returned by `flushBatch` during shutdown drain.

**New:** `drainRemaining` returns its flush result. The flusher thread's
completion signal communicates whether shutdown was clean. `closeDatabase` can
check whether the shutdown drain succeeded.

To communicate this, the completion signal becomes `MVar (Maybe Text)`:
`Nothing` for clean shutdown, `Just errMsg` for errors during final drain.
`closeDatabase` can then throw an exception or return an error.

## Risks / Trade-offs

- **D3 (signal with `()`)**: Callers who waited on their TMVar may believe their
  entry was durable when it wasn't. However, the immediate transition to
  read-only means subsequent operations will fail, and the caller can detect
  the issue. Changing TMVar to carry error information would be cleaner but
  touches every caller. We accept the simpler approach for now.
- **D6 (reject duplicates)**: If a WAL somehow has a legitimate duplicate row ID
  (e.g., from a bug in an older version), replay will now fail instead of
  silently recovering. This is the correct behavior — silent overwrites hide
  corruption.

## Migration Plan

No data migration needed. The WAL file format does not change. The code changes
are backward-compatible with existing WAL files. The only API changes are:
- `updateRow`/`deleteRow` gain a `TableName` parameter (internal API)
- `replayWAL` return type gains a `[Text]` warnings list
- New `DuplicateRowId` constructor in `DbError`
- `closeDatabase` may now throw on flush errors during shutdown

## Open Questions

None — all decisions are scoped to fixing known correctness issues with minimal
API surface change.
