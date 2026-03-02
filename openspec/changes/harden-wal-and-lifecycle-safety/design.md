# Design: Harden WAL and Lifecycle Safety

## Context

A comprehensive review identified correctness gaps across the WAL pipeline and
database lifecycle. The fixes span 6 source modules and touch concurrency
primitives (MVar, STM, IORef), exception handling, and binary deserialization.
Each fix is small in isolation, but they interact: for example, making callback
signaling exception-safe is prerequisite to the fsync error-checking change
(since fsync failures now propagate through `flushBatch`, which must still
signal callbacks).

## Goals / Non-Goals

**Goals:**
- Eliminate all identified deadlock, data-loss, and crash-on-corrupt-input paths
- Every `error()` call in library code replaced with proper error handling
- Defensively cap deserialization allocations
- Comprehensive test coverage for each fix

**Non-Goals:**
- Changing the WAL wire format (all fixes are decode-side only)
- Adding logging infrastructure (warnings still returned as `[Text]`)
- Performance optimization (these are correctness fixes)
- Changing the IORef for the sequence counter to TVar — the IORef is only
  accessed by the flusher thread after the initial `writeIORef` in
  `openDatabase`, and `forkIO` establishes a happens-before edge in GHC, so
  there is no data race. Replacing it with TVar would add unnecessary overhead
  to every flush batch.

## Decisions

### 1. Sequence validation: remove `lastSeq > 0` guard

**Current:** `if seq' <= lastSeq && lastSeq > 0` in `replayEntries`

**Problem:** When `lastSeq = 0` (no entries replayed yet), the guard `lastSeq > 0`
is false, so the entire check is skipped. This accepts any sequence number,
including 0 or negative values in a corrupted WAL.

**Fix:** Change to `if seq' <= lastSeq`. With `lastSeq` initialized to 0, the
first entry must have `seq' > 0` (i.e., seq >= 1), which matches the flusher's
behavior of starting at `lastSeq + 1`.

**Alternative considered:** Starting `lastSeq` at `maxBound` and using a
sentinel. Rejected — more complex, no benefit.

### 2. `closeDatabase` re-entrance via `MVar ()` shutdown guard

**Current:** `readMVar (dbWALHandle db)` peeks at the WAL handle without
removing it. Two concurrent `closeDatabase` calls both succeed at `readMVar`,
both set `DbShuttingDown`, but only one can `takeMVar (walDone walHandle)` —
the second blocks forever.

**Fix:** Add a `dbShutdownOnce :: MVar ()` to `Database`, initialized full.
`closeDatabase` starts by `tryTakeMVar dbShutdownOnce`; if it gets `Nothing`,
the shutdown is already in progress and it returns immediately (or blocks on a
completion signal). This is the standard "run-once" MVar pattern.

**Alternative considered:** Using `takeMVar` on `dbWALHandle` instead of
`readMVar`. This works but makes the WAL handle unavailable for the rest of
the shutdown sequence (e.g., `closeWAL` needs it). We'd have to restructure
the flow. The shutdown guard is simpler.

### 3. Exception-safe callback signaling

**Current:** In `flushBatch`, the success path signals callbacks via
`mapM_ (\(_, cb) -> atomically (putTMVar cb ())) entries`. If an exception
occurs mid-iteration (e.g., the thread is killed via `killThread`), remaining
callbacks are never signaled.

**Fix:** Use `mapM_` with a `catch` wrapper per callback:
```haskell
mapM_ (\(_, cb) -> atomically (putTMVar cb ()) `catch` \(_ :: SomeException) -> return ()) entries
```
This ensures each callback is attempted independently. Since `putTMVar` into an
empty `TMVar` cannot throw in STM (it's always successful if the TMVar is
empty), the `catch` only guards against asynchronous exceptions delivered
between callbacks.

Apply the same pattern to the error branch (which already signals callbacks but
needs the same exception safety).

### 4. `fsync` return value checking

**Current:** `c_fsync` return value is discarded with `_ <-`.

**Fix:** Check for -1 and throw an `IOError`:
```haskell
fsyncFd :: Fd -> IO ()
fsyncFd (Fd fd) = do
  result <- c_fsync fd
  when (result /= 0) $
    throwIO (mkIOError illegalOperationErrorType "fsync failed" Nothing Nothing)
```

This exception is caught by `flushBatch`'s existing `catch` handler, which
transitions the DB to read-only and signals callbacks. No additional plumbing
needed.

### 5. Replace `error()` with proper error handling

Three call sites:

1. **`openWAL` (Writer.hs:64-67):** Replace `error(...)` with
   `throwIO (userError ...)`. The caller (`openDatabase`) already catches
   exceptions via `try`.

2. **`decodeFramed` (Serialize.hs:208, 218):** These parse exactly 8 bytes
   as Word64 and 4 bytes as Word32 after a length guard, so they are genuinely
   unreachable. Replace with `Left` returns for defense-in-depth.

3. **`readWALHeader` (Serialize.hs:241):** Same pattern — 2 bytes as Word16
   after a 6-byte length guard. Replace with `Left`.

### 6. Deserialized vector size limit

**Fix:** Add a constant `maxVectorLen = 1_000_000` in `WAL/Serialize.hs` and
check in `getVector`:
```haskell
getVector = do
  len <- Get.getWord32be
  when (len > maxVectorLen) $ fail ("Vector length " ++ show len ++ " exceeds maximum")
  V.replicateM (fromIntegral len) Binary.get
```

1 million elements is generous for any realistic row. This prevents OOM from
malicious WAL files while not restricting normal use.

### 7. Remove custom `when`

**Fix:** Import `Control.Monad (when)` in `Table.hs` and delete the local
`where` clause. The Prelude `when` has the same semantics for `STM`.

## Risks / Trade-offs

- **fsync error checking adds a failure path to every flush.** Mitigated by the
  existing `catch` handler in `flushBatch` which already handles IO exceptions.
- **Vector size limit is arbitrary.** 1 million is large enough for any real
  database row (rows have ~100 columns at most). If needed, this can be made
  configurable later.
- **`closeDatabase` idempotency means the second caller gets no error.** This
  matches standard resource-cleanup semantics (e.g., `hClose` is idempotent).

## Open Questions

None — all fixes are straightforward and self-contained.
