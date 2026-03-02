# hs-db

An experiment in building a relational database from scratch in Haskell.

The goal is to explore the design space — STM for concurrency, write-ahead
logging for durability, and the PostgreSQL wire protocol for connectivity —
while keeping the codebase small and readable. If you're curious about how
databases work under the hood, or how Haskell's concurrency primitives map
onto database internals, this might be interesting to poke around in.

**This is not a production database.** There is no query optimizer, no
indexing, no authentication, no transactions across tables, and the WAL
grows without bound. It is a learning tool and a playground.

## What's here

- **Table engine** — In-memory tables backed by `TVar (IntMap Row)`. Schema
  validation on every write. Row IDs are monotonically assigned and never
  reused.

- **Write-ahead log** — Every mutation is serialized with CRC32 checksums and
  fsynced before being acknowledged. On startup, the WAL is replayed to
  restore state. Corrupted trailing entries are truncated with a warning.

- **SQL parser** — Hand-written recursive-descent parser for a subset of SQL:
  `CREATE TABLE`, `DROP TABLE`, `INSERT INTO`, `SELECT ... WHERE`,
  `UPDATE ... SET ... WHERE`, `DELETE FROM ... WHERE`.

- **PostgreSQL wire protocol** — Implements enough of the v3 simple query
  protocol that `psql` and standard PostgreSQL drivers can connect and run
  queries.

- **Server executable** — `hs-db-server` listens on a configurable host/port
  and handles multiple concurrent connections, each in its own lightweight
  thread.

## Trying it out

You'll need GHC 9.6 and Cabal 3.14+.

```
cabal build all
cabal run hs-db-server
```

By default it listens on `127.0.0.1:5433` with data stored in `./hs-db-data/`.
You can change that:

```
cabal run hs-db-server -- --host 0.0.0.0 --port 9999 --data-dir /tmp/mydb
```

Then connect with `psql`:

```
psql "host=localhost port=5433 dbname=mydb sslmode=disable"
```

And run queries:

```sql
CREATE TABLE books (title TEXT NOT NULL, pages INT, in_print BOOLEAN);
INSERT INTO books (title, pages, in_print) VALUES ('SICP', 657, TRUE);
INSERT INTO books (title, pages, in_print) VALUES ('TAOCP Vol 1', 672, TRUE);
INSERT INTO books (title, pages, in_print) VALUES ('K&R', 288, FALSE);
SELECT title, pages FROM books WHERE in_print = TRUE;
UPDATE books SET in_print = TRUE WHERE title = 'K&R';
DELETE FROM books WHERE pages < 300;
DROP TABLE books;
```

## Supported types

| SQL | Haskell |
|-----|---------|
| `INT` / `INTEGER` | `Int32` |
| `BIGINT` | `Int64` |
| `FLOAT` / `DOUBLE` | `Double` |
| `TEXT` / `VARCHAR` | `Text` |
| `BOOLEAN` / `BOOL` | `Bool` |
| `BYTEA` | `ByteString` |

Columns are nullable by default. Add `NOT NULL` to prohibit nulls.

## Running the tests

```
cabal test
```

The test suite uses Hedgehog for property-based testing of the table engine,
WAL serialization round-trips, WAL replay/recovery, and end-to-end integration.

## Architecture

```
src/
  HsDb.hs                  -- Public API: withDatabase, durable operations
  HsDb/
    Types.hs                -- Value, ColumnType, Column, Schema, DbError
    Table.hs                -- In-memory table engine (STM)
    Integration.hs          -- Database handle combining table engine + WAL
    WAL/
      Types.hs              -- WAL commands, entries, config
      Serialize.hs          -- Binary encoding with CRC32
      Writer.hs             -- Background flusher thread
      Replay.hs             -- Crash recovery
    SQL/
      Types.hs              -- SQL AST
      Parser.hs             -- Recursive-descent SQL parser
      Execute.hs            -- SQL → table engine operations
    Server/
      Protocol.hs           -- PostgreSQL wire protocol messages
    Server.hs               -- TCP server, connection lifecycle
app/
  Main.hs                   -- hs-db-server executable
```

The concurrency story is simple: all table mutations happen inside STM
transactions. The WAL flusher runs in a background thread, draining a
bounded queue and calling fsync. If the flusher crashes, the database
transitions to read-only mode. Shutdown is deterministic — the flusher
drains all remaining entries before closing.

## License

None specified. This is an experiment.
