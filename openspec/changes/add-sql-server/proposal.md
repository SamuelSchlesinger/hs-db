# Change: Add SQL Server Executable with PostgreSQL Wire Protocol

## Why
hs-db is currently a library-only project with no way for external clients to
connect and run queries. Adding a server executable that speaks the PostgreSQL
wire protocol allows any PostgreSQL-compatible client (psql, pgcli, language
drivers) to connect using standard connection strings and execute SQL queries
against the hs-db engine.

## What Changes
- Add a SQL parser supporting a subset of SQL: `CREATE TABLE`, `DROP TABLE`,
  `INSERT INTO`, `SELECT ... FROM ... WHERE`, `UPDATE ... SET ... WHERE`,
  `DELETE FROM ... WHERE`
- Add a query execution engine that maps parsed SQL to existing table engine
  operations (HsDb.Table / HsDb.Integration)
- Implement the PostgreSQL v3 wire protocol (server side): startup handshake,
  simple query flow, error responses, and connection termination
- Add a new executable target `hs-db-server` that listens on a configurable
  host/port and accepts connections
- Support PostgreSQL-style connection strings for client connections
  (e.g., `psql "postgresql://localhost:5432/mydb"`)

## Impact
- Affected specs: new `sql-query` capability, new `sql-server` capability
- Affected code: new modules under `src/HsDb/SQL/` and `src/HsDb/Server/`,
  new executable in `app/Main.hs`, updates to `hs-db.cabal`
- No changes to existing table-engine or WAL specs
