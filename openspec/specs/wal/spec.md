# wal Specification

## Purpose
TBD - created by archiving change add-table-engine-and-wal. Update Purpose after archive.
## Requirements
### Requirement: WAL Entry Append via TBQueue
The system SHALL record WAL entries by enqueuing them to a `TBQueue` within the
same STM transaction that mutates table state. A background flusher thread SHALL
drain the queue, serialize entries, write them to the WAL file, and call fsync.
Each entry SHALL be paired with a durability callback (`TMVar ()`) that the
flusher signals after fsync completes. Callers requiring synchronous durability
SHALL wait on this callback.

#### Scenario: Enqueue within STM transaction
- **WHEN** an insert operation is performed inside an STM transaction
- **THEN** the corresponding WAL entry (without sequence number or timestamp) is enqueued to the TBQueue atomically with the table mutation
- **AND** if the STM transaction retries due to conflict, the enqueue is also rolled back

#### Scenario: Flusher assigns sequence numbers and writes
- **WHEN** the flusher thread drains one or more entries from the TBQueue
- **THEN** it assigns monotonically increasing sequence numbers and wall-clock timestamps
- **AND** serializes and writes them to the WAL file
- **AND** calls fsync
- **AND** signals each entry's durability callback

#### Scenario: Group commit
- **WHEN** multiple entries have accumulated in the TBQueue
- **AND** the flusher drains them in a batch
- **THEN** all entries are written and a single fsync covers the entire batch

### Requirement: WAL File Format
The WAL file SHALL begin with a header containing 4-byte magic bytes (`HSDB`)
and a 2-byte format version. Each subsequent entry SHALL consist of an 8-byte
length prefix, the serialized payload, and a 4-byte CRC32 checksum. The CRC32
SHALL cover both the length prefix and the payload. The payload is a tagged
union covering: `CreateTable`, `InsertRow`, `DeleteRow`, `UpdateRow`,
`DropTable`. Each entry SHALL include a monotonic sequence number and a
wall-clock timestamp.

#### Scenario: Valid WAL file structure
- **WHEN** a new WAL file is created
- **THEN** the file begins with the 4-byte magic `HSDB` followed by a 2-byte version number
- **AND** entries follow the header in length-prefixed format

#### Scenario: Entry types and contents
- **WHEN** table and row mutations are performed
- **THEN** each mutation type is serialized as a distinct tagged entry in the WAL
- **AND** `InsertRow` entries include the assigned row ID
- **AND** all entries include a sequence number and timestamp

### Requirement: WAL Replay
The system SHALL be able to replay a WAL file to reconstruct in-memory database
state. Replay SHALL process entries in sequence-number order, applying each
mutation to an empty database. Row ID counters SHALL be restored from the
replayed `InsertRow` entries. Corrupted trailing entries (from incomplete
writes) SHALL be truncated with a warning rather than causing a fatal error.

#### Scenario: Full replay from empty state
- **WHEN** a WAL file contains entries for creating a table and inserting three rows
- **AND** replay is performed against an empty database
- **THEN** the database contains the table with exactly those three rows
- **AND** the row ID counter is set to max(replayed row IDs) + 1

#### Scenario: Replay with corrupted trailing entry
- **WHEN** a WAL file has valid entries followed by a truncated or corrupted final entry
- **AND** replay is performed
- **THEN** all valid entries are applied successfully
- **AND** the corrupted entry is skipped with a warning
- **AND** the WAL file is truncated to remove the corrupted bytes

#### Scenario: Replay verifies sequence ordering
- **WHEN** a WAL file is replayed
- **THEN** sequence numbers are verified to be monotonically increasing
- **AND** an error is raised if sequence numbers are out of order

#### Scenario: Replay drop and recreate same table
- **WHEN** a WAL contains CreateTable "t", InsertRow "t", DropTable "t", CreateTable "t"
- **AND** replay is performed
- **THEN** the database contains an empty table "t" with the second schema

#### Scenario: Replay empty WAL file
- **WHEN** a WAL file contains only the header (no entries)
- **AND** replay is performed
- **THEN** an empty database is produced with no tables

#### Scenario: WAL file does not exist
- **WHEN** replay is requested for a path where no WAL file exists
- **THEN** a fresh empty database is created and a new WAL file is initialized

### Requirement: WAL Truncation Interface
The system SHALL provide a stub interface for truncating the WAL after a
successful snapshot. The implementation of snapshot-triggered truncation is
deferred to a future proposal.

#### Scenario: Truncation stub
- **WHEN** the WAL truncation function is called
- **THEN** the WAL file is reset to contain only the header (magic bytes and version)
- **AND** the sequence number counter continues from where it left off

### Requirement: Flusher Thread Supervision
The system SHALL supervise the background flusher thread. If the flusher thread
terminates due to an exception, the system SHALL either restart it or transition
the database to a read-only error state. On clean shutdown, the flusher SHALL
drain the TBQueue fully and fsync before closing the WAL file.

#### Scenario: Flusher crash transitions to error state
- **WHEN** the flusher thread terminates due to an IO exception
- **THEN** subsequent write operations return an error indicating the WAL is unavailable
- **AND** read operations continue to work

#### Scenario: Clean shutdown
- **WHEN** the database is shut down gracefully
- **THEN** the flusher drains all remaining entries from the TBQueue
- **AND** writes and fsyncs them to disk
- **AND** closes the WAL file handle

