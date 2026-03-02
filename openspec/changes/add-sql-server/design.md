## Context
hs-db has a solid in-memory table engine with STM concurrency and WAL
durability, but no way for external programs to use it. The project.md already
envisions "a planned network layer exposing a subset of the PostgreSQL wire
protocol." This change delivers that layer plus the SQL parsing needed to
bridge textual queries to the existing Haskell API.

## Goals / Non-Goals
- Goals:
  - Clients using `psql` or any PostgreSQL driver can connect and run basic SQL
  - Support the core SQL subset: DDL (CREATE/DROP TABLE) and DML
    (INSERT/SELECT/UPDATE/DELETE) with WHERE clauses on SELECT/UPDATE/DELETE
  - Single-database, multi-connection server
  - Clean integration with existing WAL durability (durable operations)
- Non-Goals:
  - Full PostgreSQL compatibility (joins, subqueries, aggregates, transactions)
  - Authentication beyond the startup handshake (accept all connections)
  - SSL/TLS support
  - Prepared statements / extended query protocol (simple query only)
  - Query optimizer or cost-based planning

## Decisions

### SQL Parser
- **Decision**: Hand-written recursive-descent parser using Haskell's
  `Text`-based parsing (no parser combinator library dependency)
- **Alternatives considered**:
  - `megaparsec` — excellent library but adds a dependency for what is a
    small grammar; can be introduced later if the grammar grows
  - `attoparsec` — byte-oriented, less natural for SQL text
- **Rationale**: Keeps dependencies minimal (project convention). The SQL
  subset is small enough that a hand-written parser is manageable and
  produces clear error messages.

### PostgreSQL Wire Protocol
- **Decision**: Implement a minimal subset of the v3 frontend/backend protocol
  covering: StartupMessage, AuthenticationOk, ReadyForQuery, Query,
  RowDescription, DataRow, CommandComplete, ErrorResponse, Terminate.
- **Alternatives considered**:
  - Using an existing Haskell PG protocol library — none exist for the
    server side
  - Custom protocol — defeats the purpose of PostgreSQL compatibility
- **Rationale**: The simple query sub-protocol is straightforward to
  implement (message type byte + 4-byte length + payload). This covers
  `psql` interactive use and most driver simple-query paths.

### Server Architecture
- **Decision**: One OS thread per connection using `forkIO`, all sharing
  the same `Database` handle. The STM-based table engine already handles
  concurrent access safely.
- **Alternatives considered**:
  - Event loop with `async` — more complex, not needed at this scale
  - Thread pool — premature optimization
- **Rationale**: GHC's lightweight threads + STM make this the simplest
  correct approach. Matches the project's "correctness over performance"
  principle.

### Executable Configuration
- **Decision**: CLI flags for `--host`, `--port`, `--data-dir` (WAL location).
  The server listens on the specified host:port and stores WAL in data-dir.
- **Rationale**: Simple, standard CLI interface. No config file needed yet.

### Type Mapping (SQL ↔ HsDb)
| SQL Type         | HsDb ColumnType |
|------------------|-----------------|
| INT / INTEGER    | TInt32          |
| BIGINT           | TInt64          |
| FLOAT / DOUBLE   | TFloat64        |
| TEXT / VARCHAR    | TText           |
| BOOLEAN / BOOL   | TBool           |
| BYTEA            | TBytea          |

### WHERE Clause Support
- **Decision**: Support simple comparison expressions on single columns:
  `=`, `!=`/`<>`, `<`, `>`, `<=`, `>=`, `IS NULL`, `IS NOT NULL`,
  combined with `AND`/`OR`. No subqueries, no function calls.
- **Rationale**: Covers the most common filtering patterns without
  requiring an expression evaluator or query planner.

## Risks / Trade-offs
- **Risk**: Wire protocol bugs could cause client hangs or crashes
  → Mitigation: test with `psql` and at least one language driver;
  property-test message serialization round-trips
- **Risk**: SQL parser edge cases
  → Mitigation: Hedgehog generators for SQL AST → render → parse round-trips
- **Trade-off**: No authentication means anyone who can reach the port can
  read/write data → acceptable for a development/embedded database; can be
  addressed in a future proposal

## Open Questions
- Should the server support multiple databases (multiple WAL files) or
  just a single database? Proposed: single database for now.
- Should we add a `--config` file option, or are CLI flags sufficient?
  Proposed: CLI flags only for now.
