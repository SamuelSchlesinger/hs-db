# table-engine Specification

## Purpose
TBD - created by archiving change add-table-engine-and-wal. Update Purpose after archive.
## Requirements
### Requirement: Table Creation and Deletion
The system SHALL allow creating tables with a typed schema (column name, column
type, nullable flag) and dropping tables by name. Creating a table with a name
that already exists SHALL return an error. Dropping a table that does not exist
SHALL return an error.

#### Scenario: Create a new table
- **WHEN** a table is created with name "users" and schema [(name, Text, false), (age, Int32, false)]
- **THEN** the table is added to the database catalog and is available for row operations

#### Scenario: Create a table with duplicate name
- **WHEN** a table named "users" already exists
- **AND** a table creation is attempted with name "users"
- **THEN** an error is returned indicating the table already exists

#### Scenario: Drop an existing table
- **WHEN** a table named "users" exists
- **AND** a drop operation is performed on "users"
- **THEN** the table is removed from the catalog and its data is discarded

#### Scenario: Drop a nonexistent table
- **WHEN** no table named "users" exists
- **AND** a drop operation is performed on "users"
- **THEN** an error is returned indicating the table does not exist

#### Scenario: Drop and recreate a table with the same name
- **WHEN** a table named "users" exists with rows
- **AND** the table is dropped
- **AND** a new table named "users" is created with a different schema
- **THEN** the new table is empty and uses the new schema

### Requirement: Row Insertion with Type Checking
The system SHALL allow inserting rows into a table. Each row's values MUST match
the table's schema in count, column types, and nullability constraints. A
monotonically increasing row ID SHALL be assigned to each inserted row. Row IDs
SHALL NOT be reused within a table's lifetime.

#### Scenario: Insert a valid row
- **WHEN** a row with values [VText "alice", VInt32 30] is inserted into a table with schema [(name, Text, false), (age, Int32, false)]
- **THEN** the row is stored and a unique row ID is returned

#### Scenario: Insert a row with type mismatch
- **WHEN** a row with values [VInt32 42, VInt32 30] is inserted into a table with schema [(name, Text, false), (age, Int32, false)]
- **THEN** an error is returned indicating a type mismatch on the first column

#### Scenario: Insert a row with wrong column count
- **WHEN** a row with values [VText "alice"] is inserted into a table with schema [(name, Text, false), (age, Int32, false)]
- **THEN** an error is returned indicating a column count mismatch

#### Scenario: Insert null into non-nullable column
- **WHEN** a row with values [VNull, VInt32 30] is inserted into a table with schema [(name, Text, false), (age, Int32, false)]
- **THEN** an error is returned indicating the column does not allow null values

### Requirement: Row Selection
The system SHALL allow selecting all rows from a table, returning each row with
its row ID.

#### Scenario: Select from a table with rows
- **WHEN** a table has rows with IDs 1, 2, 3
- **AND** a select-all operation is performed
- **THEN** all three rows are returned with their IDs and values

#### Scenario: Select from an empty table
- **WHEN** a table has no rows
- **AND** a select-all operation is performed
- **THEN** an empty result set is returned

### Requirement: Row Update with Type Checking
The system SHALL allow updating an existing row by row ID. The new values MUST
pass the same type checking as insertion. Updating a nonexistent row SHALL
return an error.

#### Scenario: Update an existing row
- **WHEN** a row with ID 1 exists in the table
- **AND** an update is performed on row 1 with valid new values
- **THEN** the row's values are replaced with the new values

#### Scenario: Update a nonexistent row
- **WHEN** no row with ID 99 exists in the table
- **AND** an update is performed on row 99
- **THEN** an error is returned indicating the row does not exist

### Requirement: Row Deletion
The system SHALL allow deleting a row by row ID. Deleting a nonexistent row
SHALL return an error.

#### Scenario: Delete an existing row
- **WHEN** a row with ID 1 exists in the table
- **AND** a delete operation is performed on row 1
- **THEN** the row is removed and subsequent select-all no longer includes it

#### Scenario: Delete a nonexistent row
- **WHEN** no row with ID 99 exists in the table
- **AND** a delete operation is performed on row 99
- **THEN** an error is returned indicating the row does not exist

### Requirement: Concurrent Access via STM
The system SHALL support concurrent access to tables through STM. Each
transaction SHALL observe a consistent snapshot of the database. Conflicting
concurrent transactions SHALL be automatically retried by the STM runtime. No
explicit locking SHALL be required.

#### Scenario: Concurrent reads
- **WHEN** multiple threads perform select-all on the same table concurrently
- **THEN** each thread observes a consistent snapshot of the table data

#### Scenario: Concurrent writes to different rows
- **WHEN** multiple threads insert rows into the same table concurrently
- **THEN** all inserts succeed with unique row IDs and no data corruption

#### Scenario: Conflicting concurrent writes
- **WHEN** two threads attempt to update the same row concurrently
- **THEN** both updates eventually succeed via STM retry
- **AND** the final row value reflects one of the two updates (last writer wins)

### Requirement: Database Lifecycle Management
The system SHALL provide a bracket-style resource manager
(`withDatabase :: FilePath -> DatabaseConfig -> (Database -> IO a) -> IO a`)
that opens or replays the WAL, starts the flusher thread, runs the user action,
and performs clean shutdown. The system SHALL also expose `openDatabase` and
`closeDatabase` for non-bracket use cases.

#### Scenario: withDatabase ensures cleanup on normal exit
- **WHEN** `withDatabase` is called and the user action completes normally
- **THEN** the flusher thread drains all pending entries, fsyncs, and the WAL file is closed

#### Scenario: withDatabase ensures cleanup on exception
- **WHEN** `withDatabase` is called and the user action throws an exception
- **THEN** the flusher thread drains all pending entries, fsyncs, the WAL file is closed
- **AND** the exception is re-raised to the caller

### Requirement: High-Level Durable Operations
The system SHALL provide high-level IO operations (`durableInsert`,
`durableCreateTable`, etc.) that compose the STM mutation with WAL enqueue and
block until the flusher confirms durability via fsync. Async variants SHALL be
available for callers that accept asynchronous durability.

#### Scenario: Synchronous durable insert
- **WHEN** `durableInsert` is called with valid values
- **THEN** the row is inserted into the table (STM)
- **AND** the WAL entry is enqueued (STM)
- **AND** the call blocks until the flusher has written and fsynced the entry
- **AND** the row ID is returned

#### Scenario: Async durable insert
- **WHEN** the async variant of insert is called with valid values
- **THEN** the row is inserted and the WAL entry is enqueued (STM)
- **AND** the call returns immediately without waiting for fsync
- **AND** the row ID and a durability callback are returned

