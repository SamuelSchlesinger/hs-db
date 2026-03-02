## 1. Core Types
- [x] 1.1 Define `Value` sum type (`VInt32`, `VInt64`, `VFloat64`, `VText`, `VBool`, `VBytea`, `VNull`)
- [x] 1.2 Define `Column` (name, type, nullable flag)
- [x] 1.3 Define `Schema` (list of columns)
- [x] 1.4 Define `Row` as `Vector Value`
- [x] 1.5 Define `RowId` as `Int` with compile-time 64-bit assertion
- [x] 1.6 Define `Table` (schema, `TVar (IntMap Row)`, `TVar Int` row counter)
- [x] 1.7 Define `Database` (table catalog, WAL queue, flusher handle, database status)
- [x] 1.8 Define `DatabaseConfig` (TBQueue capacity, WAL file path)
- [x] 1.9 Define error types for table and WAL operations

## 2. Low-Level STM Table Catalog
- [x] 2.1 Implement `createTable :: Database -> TableName -> Schema -> STM (Either Error Table)`
- [x] 2.2 Implement `dropTable :: Database -> TableName -> STM (Either Error ())`

## 3. Low-Level STM Row Operations
- [x] 3.1 Implement `insertRow :: Table -> Vector Value -> STM (Either Error RowId)`
- [x] 3.2 Implement `selectAll :: Table -> STM [(RowId, Row)]`
- [x] 3.3 Implement `updateRow :: Table -> RowId -> Vector Value -> STM (Either Error ())`
- [x] 3.4 Implement `deleteRow :: Table -> RowId -> STM (Either Error ())`
- [x] 3.5 Implement schema type checking for insert and update

## 4. WAL Types and Serialization
- [x] 4.1 Define `WALEntry` tagged union (sequence number and timestamp are populated by the flusher, not the STM transaction)
- [x] 4.2 Define entry variant payloads: `CreateTable` (name, schema), `InsertRow` (name, row ID, values), `UpdateRow` (name, row ID, values), `DeleteRow` (name, row ID), `DropTable` (name)
- [x] 4.3 Implement binary serialization for `WALEntry` (encode/decode)
- [x] 4.4 Define WAL file header format (magic bytes `HSDB`, 2-byte version)
- [x] 4.5 Implement CRC32 checksum covering length prefix + payload

## 5. WAL Writer (Flusher Thread)
- [x] 5.1 Implement `openWAL :: FilePath -> IO WALHandle`
- [x] 5.2 Define durability callback type (`TMVar ()`)
- [x] 5.3 Implement flusher thread: drain `TBQueue (WALEntry, TMVar ())`, assign sequence numbers and timestamps, serialize, write, fsync, signal callbacks
- [x] 5.4 Implement group commit: batch multiple pending entries per fsync
- [x] 5.5 Implement flusher supervision: restart or transition to read-only on exception
- [x] 5.6 Implement clean shutdown: drain queue fully, fsync, close file
- [x] 5.7 Implement `closeWAL :: WALHandle -> IO ()`

## 6. WAL Replay
- [x] 6.1 Implement `replayWAL :: FilePath -> IO (Either Error Database)`
- [x] 6.2 Restore row ID counters from replayed InsertRow entries
- [x] 6.3 Handle drop-then-recreate of same table name
- [x] 6.4 Handle corrupted trailing entries (truncate with warning)
- [x] 6.5 Verify sequence number ordering during replay
- [x] 6.6 Handle empty WAL file (header only) as valid empty database
- [x] 6.7 Handle missing WAL file (fresh database, create new WAL)

## 7. High-Level IO API and Integration
- [x] 7.1 Wire STM mutations to enqueue WAL entries within the same STM transaction
- [x] 7.2 Implement synchronous-durability operations: `durableCreateTable`, `durableInsert`, `durableUpdate`, `durableDelete`, `durableDrop`
- [x] 7.3 Implement async-durability variants for callers that don't need to wait for fsync
- [x] 7.4 Implement `withDatabase :: FilePath -> DatabaseConfig -> (Database -> IO a) -> IO a`
- [x] 7.5 Implement `openDatabase` / `closeDatabase` for non-bracket use cases
- [x] 7.6 Implement stub interface for WAL truncation (for future snapshot integration)

## 8. Testing
- [x] 8.1 Hedgehog state-machine tests for low-level STM table operations (create, insert, select, update, delete, drop)
- [x] 8.2 Hedgehog property tests for WAL serialization round-trips
- [x] 8.3 Integration test: write operations via high-level API, shut down, replay WAL, verify state matches
- [x] 8.4 Test flusher thread: verify durability callbacks are signaled after fsync
- [x] 8.5 Test edge cases: drop + recreate same table, insert into dropped table WAL replay, empty WAL
- [x] 8.6 Test resource management: withDatabase cleans up on normal exit and on exception
