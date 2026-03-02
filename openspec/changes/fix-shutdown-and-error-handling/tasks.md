# Tasks: Fix Shutdown Protocol, Flusher Supervision, and Error Handling

## 1. Type-level changes
- [x] 1.1 Add `DuplicateRowId TableName RowId` constructor to `DbError` in `HsDb/Types.hs`
- [x] 1.2 Add `walDone :: MVar (Maybe Text)` field to `WALHandle` in `HsDb/WAL/Writer.hs`
- [x] 1.3 Change `replayWAL` return type to `Either DbError (TableCatalog, Word64, [Text])` in `HsDb/WAL/Replay.hs`

## 2. Table operations: pass table name for error context
- [x] 2.1 Add `TableName` parameter to `updateRow` and `deleteRow` in `HsDb/Table.hs`; use it in `RowNotFound`
- [x] 2.2 Update callers in `HsDb/Integration.hs` to pass table name through `updateRow` / `deleteRow`
- [x] 2.3 Update callers in `HsDb/WAL/Replay.hs` to pass table name through `updateRow` / `deleteRow`
- [x] 2.4 Update tests in `test/Test/HsDb/Table.hs` for new `updateRow` / `deleteRow` signatures

## 3. Replay correctness
- [x] 3.1 Make `insertRowWithId` reject duplicate row IDs (check `IntMap.member` before insert) in `HsDb/Table.hs`
- [x] 3.2 Surface corrupted trailing entry warnings in `replayWAL` return value in `HsDb/WAL/Replay.hs`
- [x] 3.3 Update `openDatabase` in `HsDb.hs` to accept and handle replay warnings
- [x] 3.4 Add replay test for duplicate row ID rejection in `test/Test/HsDb/WAL/Replay.hs`
- [x] 3.5 Update existing replay truncation test to verify warnings are returned

## 4. Flusher error handling
- [x] 4.1 Signal all callbacks in the failed batch on flush error in `flushBatch` (`HsDb/WAL/Writer.hs`)
- [x] 4.2 Propagate flush errors from `drainRemaining` during shutdown (`HsDb/WAL/Writer.hs`)
- [x] 4.3 Add `walDone` MVar to `openWAL` and signal it at flusher exit (`HsDb/WAL/Writer.hs`)

## 5. Deterministic shutdown and flusher supervision
- [x] 5.1 Replace `forkIO` with a supervision wrapper that catches flusher exit and sets `DbReadOnly` + signals `walDone` on exception (`HsDb.hs`)
- [x] 5.2 Replace `threadDelay` + `killThread` shutdown in `closeDatabase` with blocking on `walDone` (`HsDb.hs`)
- [x] 5.3 Propagate shutdown drain errors from `walDone` in `closeDatabase` (`HsDb.hs`)

## 6. Tests
- [x] 6.1 Add integration test: flusher crash transitions DB to read-only
- [x] 6.2 Add integration test: `closeDatabase` completes without timing delays
- [x] 6.3 Verify all existing tests still pass with the updated signatures
