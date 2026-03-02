## ADDED Requirements

### Requirement: SQL Parsing
The system SHALL parse a subset of SQL into a typed AST. The supported
statements SHALL be: `CREATE TABLE`, `DROP TABLE`, `INSERT INTO`,
`SELECT ... FROM ... [WHERE ...]`, `UPDATE ... SET ... [WHERE ...]`, and
`DELETE FROM ... [WHERE ...]`. Parsing an unsupported or malformed statement
SHALL return a descriptive error message. SQL keywords SHALL be
case-insensitive. Identifiers MAY be optionally quoted with double quotes.

#### Scenario: Parse CREATE TABLE
- **WHEN** the input is `CREATE TABLE users (name TEXT NOT NULL, age INT)`
- **THEN** the parser returns a CreateTable AST node with table name "users" and two columns: (name, Text, not nullable) and (age, Int, nullable)

#### Scenario: Parse INSERT INTO
- **WHEN** the input is `INSERT INTO users (name, age) VALUES ('alice', 30)`
- **THEN** the parser returns an Insert AST node with table name "users", column list [name, age], and values [TextLit "alice", IntLit 30]

#### Scenario: Parse SELECT with WHERE
- **WHEN** the input is `SELECT name, age FROM users WHERE age > 25`
- **THEN** the parser returns a Select AST node with columns [name, age], table "users", and a WHERE expression (age > 25)

#### Scenario: Parse SELECT *
- **WHEN** the input is `SELECT * FROM users`
- **THEN** the parser returns a Select AST node with all-columns marker and table "users"

#### Scenario: Parse UPDATE with WHERE
- **WHEN** the input is `UPDATE users SET age = 31 WHERE name = 'alice'`
- **THEN** the parser returns an Update AST node with table "users", assignments [(age, 31)], and WHERE expression (name = 'alice')

#### Scenario: Parse DELETE with WHERE
- **WHEN** the input is `DELETE FROM users WHERE name = 'alice'`
- **THEN** the parser returns a Delete AST node with table "users" and WHERE expression (name = 'alice')

#### Scenario: Parse DROP TABLE
- **WHEN** the input is `DROP TABLE users`
- **THEN** the parser returns a DropTable AST node with table name "users"

#### Scenario: Malformed SQL
- **WHEN** the input is `SELECTT * FROM users`
- **THEN** the parser returns an error describing the unexpected token

### Requirement: SQL Type Mapping
The system SHALL map SQL type names to HsDb column types as follows:
`INT`/`INTEGER` → TInt32, `BIGINT` → TInt64, `FLOAT`/`DOUBLE`/`DOUBLE PRECISION` → TFloat64,
`TEXT`/`VARCHAR` → TText, `BOOLEAN`/`BOOL` → TBool, `BYTEA` → TBytea.
Unrecognized type names SHALL produce a parse error.

#### Scenario: Map standard SQL types
- **WHEN** a CREATE TABLE statement uses types INT, BIGINT, TEXT, BOOLEAN, FLOAT, BYTEA
- **THEN** each type is mapped to the corresponding HsDb ColumnType

#### Scenario: Unrecognized type
- **WHEN** a CREATE TABLE statement uses type `UUID`
- **THEN** a parse error is returned indicating the type is not supported

### Requirement: WHERE Clause Evaluation
The system SHALL evaluate WHERE clause expressions against rows to filter
results for SELECT, identify rows for UPDATE, and identify rows for DELETE.
Supported operators SHALL be: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`,
`IS NULL`, `IS NOT NULL`. Expressions MAY be combined with `AND` and `OR`.
Operator precedence SHALL follow SQL conventions (AND binds tighter than OR).

#### Scenario: Equality filter
- **WHEN** a SELECT has WHERE clause `name = 'alice'`
- **AND** the table contains rows with name "alice" and name "bob"
- **THEN** only the row with name "alice" is returned

#### Scenario: Comparison filter
- **WHEN** a SELECT has WHERE clause `age > 25`
- **AND** the table contains rows with ages 20, 25, 30
- **THEN** only the row with age 30 is returned

#### Scenario: NULL check
- **WHEN** a SELECT has WHERE clause `email IS NULL`
- **AND** the table contains rows with email NULL and email "a@b.com"
- **THEN** only the row with NULL email is returned

#### Scenario: AND/OR combination
- **WHEN** a SELECT has WHERE clause `age > 25 AND name = 'alice'`
- **THEN** only rows matching both conditions are returned

### Requirement: Query Execution
The system SHALL execute parsed SQL statements against a Database, using
durable operations for mutations and selectAll for reads. SELECT results
SHALL include column names and text-formatted values. INSERT SHALL return
the count of inserted rows. UPDATE and DELETE SHALL return the count of
affected rows. CREATE TABLE and DROP TABLE SHALL return a command tag.
Execution errors (schema violations, missing tables, type mismatches)
SHALL be reported with descriptive messages.

#### Scenario: Execute CREATE TABLE
- **WHEN** `CREATE TABLE users (name TEXT NOT NULL, age INT)` is executed
- **THEN** the table is created via durableCreateTable
- **AND** a "CREATE TABLE" command tag is returned

#### Scenario: Execute INSERT
- **WHEN** `INSERT INTO users (name, age) VALUES ('alice', 30)` is executed against a table with matching schema
- **THEN** the row is inserted via durableInsert
- **AND** an "INSERT 0 1" command tag is returned

#### Scenario: Execute SELECT with results
- **WHEN** `SELECT name, age FROM users` is executed against a table with rows
- **THEN** column descriptions and matching rows are returned as text values

#### Scenario: Execute UPDATE
- **WHEN** `UPDATE users SET age = 31 WHERE name = 'alice'` is executed
- **THEN** matching rows are updated via durableUpdate
- **AND** an "UPDATE N" command tag is returned where N is the count of updated rows

#### Scenario: Execute DELETE
- **WHEN** `DELETE FROM users WHERE name = 'alice'` is executed
- **THEN** matching rows are deleted via durableDelete
- **AND** a "DELETE N" command tag is returned where N is the count of deleted rows

#### Scenario: Execution error
- **WHEN** `INSERT INTO nonexistent (x) VALUES (1)` is executed
- **THEN** an error is returned indicating the table does not exist
