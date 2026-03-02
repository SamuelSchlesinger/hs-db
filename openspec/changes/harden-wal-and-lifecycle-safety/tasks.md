# Tasks: Harden WAL and Lifecycle Safety

## 1. WAL Serialize hardening (no downstream dependencies)
- [x] 1.1 Replace `error()` with `Left` returns in `decodeFramed` (lines 208, 218) and `readWALHeader` (line 241) in `WAL/Serialize.hs`
- [x] 1.2 Add `maxVectorLen` constant and size check in `getVector` in `WAL/Serialize.hs`
- [x] 1.3 `cabal build` — verify no regressions

## 2. WAL Writer: fsync checking and error() removal
- [x] 2.1 Check `c_fsync` return value in `fsyncFd`; throw `IOError` on failure (`WAL/Writer.hs:32-35`)
- [x] 2.2 Replace `error()` with `throwIO` in `openWAL` header/version checks (`WAL/Writer.hs:64-67`)
- [x] 2.3 `cabal build`

## 3. Exception-safe callback signaling
- [x] 3.1 Wrap each `putTMVar` callback signal in a per-callback `catch` in `flushBatch` success path (`WAL/Writer.hs:145`)
- [x] 3.2 Apply same exception-safe pattern to `flushBatch` error path (`WAL/Writer.hs:148-149`)
- [x] 3.3 `cabal build`

## 4. WAL Replay: strict sequence validation
- [x] 4.1 Remove `&& lastSeq > 0` guard from `replayEntries` (`WAL/Replay.hs:61`)
- [x] 4.2 `cabal test` — verify existing replay tests still pass

## 5. Table.hs: remove custom `when`
- [x] 5.1 Import `Control.Monad (when)` and delete local `when` definition in `insertRowWithId` (`Table.hs:142-144`)
- [x] 5.2 `cabal build`

## 6. closeDatabase re-entrance safety
- [x] 6.1 Add `dbShutdownOnce :: MVar ()` field to `Database` in `Integration.hs`; initialize full in `openDatabase`
- [x] 6.2 In `closeDatabase`, use `tryTakeMVar dbShutdownOnce` — if `Nothing`, return immediately; otherwise proceed with shutdown (`HsDb.hs`)
- [x] 6.3 `cabal test` — verify existing lifecycle tests still pass

## 7. Tests
- [x] 7.1 Add test: sequence validation rejects `seq=0` as first entry and rejects non-monotonic sequences (`test/Test/HsDb/WAL/Replay.hs`)
- [x] 7.2 Add test: `decodeFramed` returns `Left` (not crash) for each former `error()` path (`test/Test/HsDb/WAL/Serialize.hs`)
- [x] 7.3 Add test: `getVector` rejects oversized vector length (`test/Test/HsDb/WAL/Serialize.hs`)
- [x] 7.4 Add test: concurrent `closeDatabase` does not deadlock (use `timeout` to detect) (`test/Test/HsDb/Integration.hs`)
- [x] 7.5 Add test: fsync failure transitions DB to read-only (mock via closed fd, similar to existing `prop_flusher_crash_readonly`) (`test/Test/HsDb/Integration.hs`)
- [x] 7.6 `cabal test` — all tests pass
