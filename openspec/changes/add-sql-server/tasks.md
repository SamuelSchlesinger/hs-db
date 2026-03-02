## 1. SQL Parser
- [ ] 1.1 Define SQL AST types (Statement, Expr, TableDef, ColumnDef, etc.) in `src/HsDb/SQL/Types.hs`
- [ ] 1.2 Implement SQL parser in `src/HsDb/SQL/Parser.hs` (CREATE TABLE, DROP TABLE, INSERT, SELECT, UPDATE, DELETE)
- [ ] 1.3 Implement SQL pretty-printer for error messages in `src/HsDb/SQL/Render.hs`
- [ ] 1.4 Write Hedgehog tests for parser round-trips and error cases

## 2. Query Execution Engine
- [ ] 2.1 Implement query executor in `src/HsDb/SQL/Execute.hs` mapping SQL AST to table engine operations
- [ ] 2.2 Implement type mapping (SQL types → HsDb ColumnType)
- [ ] 2.3 Implement WHERE clause evaluation for SELECT/UPDATE/DELETE
- [ ] 2.4 Implement result formatting (rows → column names + text values)
- [ ] 2.5 Write integration tests for query execution against a live Database

## 3. PostgreSQL Wire Protocol
- [ ] 3.1 Define protocol message types in `src/HsDb/Server/Protocol.hs`
- [ ] 3.2 Implement message serialization/deserialization (binary format)
- [ ] 3.3 Implement startup handshake (StartupMessage → AuthenticationOk → ReadyForQuery)
- [ ] 3.4 Implement simple query flow (Query → RowDescription → DataRow* → CommandComplete → ReadyForQuery)
- [ ] 3.5 Implement error response formatting (ErrorResponse with SQLSTATE codes)
- [ ] 3.6 Write tests for protocol message round-trips

## 4. Server Executable
- [ ] 4.1 Implement TCP server with per-connection threads in `src/HsDb/Server.hs`
- [ ] 4.2 Implement connection lifecycle (handshake, query loop, termination)
- [ ] 4.3 Create executable entry point in `app/Main.hs` with CLI flag parsing (--host, --port, --data-dir)
- [ ] 4.4 Update `hs-db.cabal` with new executable target and dependencies
- [ ] 4.5 Write end-to-end test: start server, connect with psql-style client, run queries

## 5. Documentation
- [ ] 5.1 Update project.md to reflect the new network layer capability
