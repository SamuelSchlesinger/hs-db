# Change: Harden WAL and Lifecycle Safety

## Why
A comprehensive code review identified 10 correctness and robustness issues
spanning the WAL writer, WAL replay, serialization layer, and database
lifecycle. Several are data-loss or deadlock risks that must be fixed before the
database can be considered reliable. The remaining issues are defense-in-depth
hardening that prevents future regressions.

## What Changes

### Critical
- **WAL replay sequence validation**: Remove the `lastSeq > 0` guard so
  sequence monotonicity is enforced from the very first entry
- **`closeDatabase` re-entrance safety**: Prevent concurrent `closeDatabase`
  calls from deadlocking on the `walDone` MVar
- **Callback signaling exception safety**: Wrap callback signaling in
  exception-safe code so callers never block forever on `takeTMVar`
- **`fsync` return value checking**: Check `c_fsync` return value and surface
  IO errors instead of silently succeeding

### High
- **Replace `error()` with proper errors in `openWAL`**: Use `throwIO` instead
  of `error()` for WAL header/version errors so callers can catch them
- **Replace `error()` with `Left` in `decodeFramed`/`readWALHeader`**: Return
  proper `Either` errors for all decode paths instead of crashing

### Medium
- **Deserialized vector size limit**: Cap `getVector` at a configurable maximum
  to prevent OOM from malicious WAL files
- **Remove custom `when` shadowing Prelude**: Use `Control.Monad.when` from
  the standard library

### Tests
- Concurrent `closeDatabase` does not deadlock
- `fsync` failure transitions DB to read-only
- Callback signaling under exception does not block callers
- Sequence validation rejects non-monotonic entries from seq 0

## Impact
- Affected specs: `wal`, `table-engine`
- Affected code: `WAL/Writer.hs`, `WAL/Replay.hs`, `WAL/Serialize.hs`,
  `Table.hs`, `HsDb.hs`, `Integration.hs`, and corresponding test files
- No API changes visible to users of the public `HsDb` module
- No WAL format changes (serialization is unchanged, only decoding is hardened)
