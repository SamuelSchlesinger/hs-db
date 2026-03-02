# Tasks: Fix Resource Leaks and Bounds Checking

## 1. closeWAL exception safety
- [x] 1.1 In `closeWAL` (`WAL/Writer.hs`), wrap `hClose` and `closeFd` with `finally` so `closeFd` runs even if `hClose` throws
- [x] 1.2 `cabal build`

## 2. Orphaned callbacks on flusher crash
- [x] 2.1 In `flusherThread` (`WAL/Writer.hs`), after `flushBatch` returns `Left` and status is set to `DbReadOnly`, drain remaining queue items with `tryDrainQueue` and call `signalCallbacks` on them (without writing)
- [x] 2.2 `cabal build`

## 3. closeDatabase resource leak on shutdown error
- [x] 3.1 In `closeDatabase` (`HsDb.hs`), use `finally` to ensure `closeWAL` runs before re-raising the shutdown error
- [x] 3.2 `cabal build`

## 4. Deserialization size limits for lists, text, and byte strings
- [x] 4.1 Add `maxListLen`, `maxTextLen`, and `maxByteStringLen` constants in `WAL/Serialize.hs`
- [x] 4.2 Add size check in `getListOf` before allocation, matching `getVector` pattern
- [x] 4.3 Add size check in `getText` before `getByteString` call
- [x] 4.4 Add size check in `getByteString` before `Get.getByteString` call
- [x] 4.5 `cabal test` — verify existing serialization roundtrip tests still pass

## 5. Force _assert64 evaluation
- [x] 5.1 In `Types.hs`, replace `_assert64 :: Bool` with `assert64Bit :: ()` and export it; reference via `seq` in `openDatabase`
- [x] 5.2 `cabal build`

## 6. Config validation
- [x] 6.1 In `openDatabase` (`HsDb.hs`), add a guard that throws if `configQueueCapacity <= 0`
- [x] 6.2 `cabal build`

## 7. Deterministic flusher-crash test
- [x] 7.1 In `prop_flusher_crash_readonly` (`test/Test/HsDb/Integration.hs`), replace `threadDelay 10000` with an STM-based poll of `dbStatus` under `System.Timeout.timeout`
- [x] 7.2 `cabal test` — all tests pass

## 8. Final verification
- [x] 8.1 `cabal test` — all 41 tests pass
- [x] 8.2 Mark all tasks complete
