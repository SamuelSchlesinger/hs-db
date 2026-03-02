## Context

After the `harden-wal-and-lifecycle-safety` changes, a follow-up code review
found 8 remaining issues: 3 resource leaks / liveness bugs in shutdown paths,
3 missing deserialization size limits, 1 dead platform assertion, and 1 flaky
test.  All fixes are localised — no new modules or architectural patterns.

## Goals / Non-Goals

- **Goals**: eliminate every identified resource leak and OOM vector; make the
  flusher-crash test deterministic; validate config at startup.
- **Non-Goals**: adding structured error types to `decodeFramed`; adding a retry
  mechanism for flusher crashes; adding per-field size configurability.

## Decisions

### 1. closeDatabase: close WAL even on error
Use `finally` around the shutdown-result check so `closeWAL` is called
regardless of whether `throwIO` fires.  The shutdown error is re-raised after
cleanup.

### 2. Orphaned callbacks on flusher crash
When `flushBatch` returns `Left`, the flusher currently sets `DbReadOnly` and
exits.  Items that were enqueued between the batch drain and the status change
are left orphaned.  Fix: after transitioning to `DbReadOnly`, drain remaining
items with `tryDrainQueue` and signal their callbacks without writing them.
This mirrors `drainRemaining` but skips the write step.

### 3. closeWAL exception safety
Wrap `hClose` and `closeFd` in `finally` so the sync fd is closed even if
`hClose` throws.

### 4. Deserialization size limits
Add `maxListLen` (1000 — schemas shouldn't have millions of columns),
`maxTextLen` (16 MiB), and `maxByteStringLen` (64 MiB) constants.  These are
checked in the `Get` monad before allocation, matching the existing
`maxVectorLen` pattern.

### 5. _assert64 must be evaluated
Replace the unused `Bool` binding with a top-level `()` binding that uses
`seq` to force evaluation.  GHC evaluates top-level `()` bindings at module
load time via CAF semantics, so the assertion fires on import.

### 6. Deterministic flusher-crash test
Replace `threadDelay 10000` with a polling loop that checks `dbStatus` via
STM `readTVar` + `check`, wrapped in `System.Timeout.timeout`.  This is both
deterministic and bounded.

### 7. Config validation
In `openDatabase`, check `configQueueCapacity > 0` and throw a clear error
if not.

## Risks / Trade-offs

- The `maxTextLen` / `maxByteStringLen` limits are conservative.  If a future
  use-case needs larger blobs, the constants can be raised.  They only affect
  WAL deserialization, not in-memory storage.
- Forcing `_assert64` at module load adds a negligible one-time cost.

## Open Questions
None.
