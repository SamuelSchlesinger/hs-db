# Change: Fix Resource Leaks and Bounds Checking

## Why
A code review identified 8 issues across resource management, deserialization
safety, and test robustness. Two are resource leaks where file handles are never
closed on error paths, one is a liveness bug where callbacks are orphaned after
a flusher crash, three are missing size limits in deserialization that allow OOM
from corrupted WAL files, one is a dead platform assertion, and one is a
timing-dependent test.

## What Changes
- **closeDatabase**: use `finally` to ensure `closeWAL` runs even when the
  flusher reported an error (currently `throwIO` fires before `closeWAL`)
- **Orphaned callbacks**: after a flusher crash, drain remaining queue items and
  signal their callbacks before exiting, so callers don't block forever
- **closeWAL**: use `finally` so `closeFd` runs even if `hClose` throws
- **getListOf**: add a `maxListLen` size limit matching the existing `maxVectorLen`
  pattern in `getVector`
- **getText / getByteString**: add size limits to prevent multi-GB allocations
  from corrupted length fields
- **_assert64**: force evaluation at module load so the 64-bit platform check
  actually fires on 32-bit systems
- **prop_flusher_crash_readonly**: replace `threadDelay` with STM-based polling
  of `dbStatus` under a timeout, eliminating timing flakiness
- **configQueueCapacity**: validate that capacity is positive in `openDatabase`

## Impact
- Affected specs: `wal`, `table-engine`
- Affected code: `HsDb.hs`, `HsDb/WAL/Writer.hs`, `HsDb/WAL/Serialize.hs`,
  `HsDb/Types.hs`, `test/Test/HsDb/Integration.hs`
