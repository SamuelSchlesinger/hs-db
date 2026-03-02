## MODIFIED Requirements

### Requirement: Row Update with Type Checking
The system SHALL allow updating an existing row by row ID. The new values MUST
pass the same type checking as insertion. Updating a nonexistent row SHALL
return an error that includes the table name for diagnostic context.

#### Scenario: Update an existing row
- **WHEN** a row with ID 1 exists in the table
- **AND** an update is performed on row 1 with valid new values
- **THEN** the row's values are replaced with the new values

#### Scenario: Update a nonexistent row
- **WHEN** no row with ID 99 exists in the table
- **AND** an update is performed on row 99
- **THEN** an error is returned indicating the row does not exist
- **AND** the error includes the table name

### Requirement: Row Deletion
The system SHALL allow deleting a row by row ID. Deleting a nonexistent row
SHALL return an error that includes the table name for diagnostic context.

#### Scenario: Delete an existing row
- **WHEN** a row with ID 1 exists in the table
- **AND** a delete operation is performed on row 1
- **THEN** the row is removed and subsequent select-all no longer includes it

#### Scenario: Delete a nonexistent row
- **WHEN** no row with ID 99 exists in the table
- **AND** a delete operation is performed on row 99
- **THEN** an error is returned indicating the row does not exist
- **AND** the error includes the table name

### Requirement: Database Lifecycle Management
The system SHALL provide a bracket-style resource manager
(`withDatabase :: FilePath -> DatabaseConfig -> (Database -> IO a) -> IO a`)
that opens or replays the WAL, starts the flusher thread, runs the user action,
and performs clean shutdown. The system SHALL also expose `openDatabase` and
`closeDatabase` for non-bracket use cases. Shutdown SHALL use deterministic
synchronization with the flusher thread via a completion signal rather than
timing-based heuristics. If the flusher experienced errors during the shutdown
drain, `closeDatabase` SHALL propagate the error.

#### Scenario: withDatabase ensures cleanup on normal exit
- **WHEN** `withDatabase` is called and the user action completes normally
- **THEN** the flusher thread drains all pending entries, fsyncs, and the WAL file is closed

#### Scenario: withDatabase ensures cleanup on exception
- **WHEN** `withDatabase` is called and the user action throws an exception
- **THEN** the flusher thread drains all pending entries, fsyncs, the WAL file is closed
- **AND** the exception is re-raised to the caller

#### Scenario: Deterministic shutdown synchronization
- **WHEN** `closeDatabase` is called
- **THEN** it signals the flusher to shut down
- **AND** blocks on a completion signal from the flusher thread
- **AND** does not use timing-based delays or thread killing
- **AND** proceeds to close the WAL file only after the flusher confirms completion
