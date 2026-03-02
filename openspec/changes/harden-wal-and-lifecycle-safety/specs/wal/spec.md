## MODIFIED Requirements

### Requirement: WAL Entry Append via TBQueue
The system SHALL record WAL entries by enqueuing them to a `TBQueue` within the
same STM transaction that mutates table state. A background flusher thread SHALL
drain the queue, serialize entries, write them to the WAL file, and call fsync.
The fsync call MUST check for errors and treat fsync failure as an IO exception
that transitions the database to read-only. Each entry SHALL be paired with a
durability callback (`TMVar ()`) that the flusher signals after fsync completes.
Callback signaling MUST be exception-safe: if an exception interrupts signaling,
all callbacks in the batch MUST still be signaled so that callers never block
indefinitely. Callers requiring synchronous durability SHALL wait on this
callback.

#### Scenario: Enqueue within STM transaction
- **WHEN** an insert operation is performed inside an STM transaction
- **THEN** the corresponding WAL entry (without sequence number or timestamp) is enqueued to the TBQueue atomically with the table mutation
- **AND** if the STM transaction retries due to conflict, the enqueue is also rolled back

#### Scenario: Flusher assigns sequence numbers and writes
- **WHEN** the flusher thread drains one or more entries from the TBQueue
- **THEN** it assigns monotonically increasing sequence numbers and wall-clock timestamps
- **AND** serializes and writes them to the WAL file
- **AND** calls fsync and verifies it succeeds
- **AND** signals each entry's durability callback

#### Scenario: Group commit
- **WHEN** multiple entries have accumulated in the TBQueue
- **AND** the flusher drains them in a batch
- **THEN** all entries are written and a single fsync covers the entire batch

#### Scenario: fsync failure transitions to read-only
- **WHEN** the flusher calls fsync and the system call returns an error
- **THEN** all callbacks in the current batch are signaled
- **AND** the database transitions to read-only
- **AND** subsequent write operations return a DatabaseNotWritable error

#### Scenario: Callback signaling is exception-safe
- **WHEN** an asynchronous exception is delivered during callback signaling
- **THEN** each callback is signaled independently with exception handling
- **AND** no caller blocks indefinitely on its durability callback

### Requirement: WAL File Format
The WAL file SHALL begin with a header containing 4-byte magic bytes (`HSDB`)
and a 2-byte format version. Each subsequent entry SHALL consist of an 8-byte
length prefix, the serialized payload, and a 4-byte CRC32 checksum. The CRC32
SHALL cover both the length prefix and the payload. The payload is a tagged
union covering: `CreateTable`, `InsertRow`, `DeleteRow`, `UpdateRow`,
`DropTable`. Each entry SHALL include a monotonic sequence number and a
wall-clock timestamp. Deserialization of vectors within entries MUST enforce a
maximum element count to prevent memory exhaustion from malformed data. All
decoding functions MUST return structured errors (`Either String`) instead of
calling `error()`.

#### Scenario: Valid WAL file structure
- **WHEN** a new WAL file is created
- **THEN** the file begins with the 4-byte magic `HSDB` followed by a 2-byte version number
- **AND** entries follow the header in length-prefixed format

#### Scenario: Entry types and contents
- **WHEN** table and row mutations are performed
- **THEN** each mutation type is serialized as a distinct tagged entry in the WAL
- **AND** `InsertRow` entries include the assigned row ID
- **AND** all entries include a sequence number and timestamp

#### Scenario: Oversized vector rejected during deserialization
- **WHEN** a WAL entry claims a vector length exceeding the maximum allowed elements
- **THEN** decoding returns an error instead of attempting allocation
- **AND** the WAL replay treats this as a corrupted entry

#### Scenario: Decoding never calls error()
- **WHEN** any decoding function encounters malformed data
- **THEN** it returns a Left error value
- **AND** the calling code can handle the error gracefully without a program crash

### Requirement: WAL Replay
The system SHALL be able to replay a WAL file to reconstruct in-memory database
state. Replay SHALL process entries in sequence-number order, applying each
mutation to an empty database. Sequence number monotonicity MUST be enforced
from the first entry onward without exception: every entry's sequence number
MUST be strictly greater than the previous entry's sequence number (or greater
than zero for the first entry). Row ID counters SHALL be restored from the
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

#### Scenario: Replay verifies sequence ordering from first entry
- **WHEN** a WAL file is replayed
- **THEN** the first entry's sequence number MUST be greater than zero
- **AND** each subsequent entry's sequence number MUST be strictly greater than its predecessor
- **AND** an error is raised if any entry violates monotonicity

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

### Requirement: Flusher Thread Supervision
The system SHALL supervise the background flusher thread. If the flusher thread
terminates due to an exception, the system SHALL transition the database to a
read-only error state and signal the shutdown MVar. Opening the WAL file MUST
use proper exception handling (`throwIO`) instead of `error()` for header and
version errors. On clean shutdown, the flusher SHALL drain the TBQueue fully
and fsync before closing the WAL file.

#### Scenario: Flusher crash transitions to error state
- **WHEN** the flusher thread terminates due to an IO exception
- **THEN** subsequent write operations return an error indicating the WAL is unavailable
- **AND** read operations continue to work

#### Scenario: Clean shutdown
- **WHEN** the database is shut down gracefully
- **THEN** the flusher drains all remaining entries from the TBQueue
- **AND** writes and fsyncs them to disk
- **AND** closes the WAL file handle

#### Scenario: WAL open with corrupted header returns error
- **WHEN** `openWAL` encounters a corrupted header or version mismatch
- **THEN** a proper IO exception is thrown (not `error()`)
- **AND** the caller can catch and handle the error

## ADDED Requirements

### Requirement: WAL Open Error Handling
The system SHALL handle WAL file errors during open via proper IO exceptions
rather than calling `error()`. Header parsing failures and version mismatches
MUST throw `IOError` values that callers can catch with standard exception
handling.

#### Scenario: Corrupted header throws catchable exception
- **WHEN** `openWAL` reads a WAL file with an invalid header
- **THEN** an `IOError` is thrown
- **AND** the exception can be caught by the caller without crashing the process
