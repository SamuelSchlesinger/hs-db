## MODIFIED Requirements

### Requirement: WAL File Format
The WAL file SHALL begin with a header containing 4-byte magic bytes (`HSDB`)
and a 2-byte format version. Each subsequent entry SHALL consist of an 8-byte
length prefix, the serialized payload, and a 4-byte CRC32 checksum. The CRC32
SHALL cover both the length prefix and the payload. The payload is a tagged
union covering: `CreateTable`, `InsertRow`, `DeleteRow`, `UpdateRow`,
`DropTable`. Each entry SHALL include a monotonic sequence number and a
wall-clock timestamp. Deserialization of variable-length fields (vectors, lists,
text, byte strings) SHALL enforce maximum size limits to prevent out-of-memory
conditions from corrupted or malicious WAL data.

#### Scenario: Valid WAL file structure
- **WHEN** a new WAL file is created
- **THEN** the file begins with the 4-byte magic `HSDB` followed by a 2-byte version number
- **AND** entries follow the header in length-prefixed format

#### Scenario: Entry types and contents
- **WHEN** table and row mutations are performed
- **THEN** each mutation type is serialized as a distinct tagged entry in the WAL
- **AND** `InsertRow` entries include the assigned row ID
- **AND** all entries include a sequence number and timestamp

#### Scenario: Oversized text field rejected during deserialization
- **WHEN** a WAL entry contains a text length field exceeding the maximum allowed size
- **THEN** deserialization returns an error without allocating the claimed bytes

#### Scenario: Oversized byte string field rejected during deserialization
- **WHEN** a WAL entry contains a byte string length field exceeding the maximum allowed size
- **THEN** deserialization returns an error without allocating the claimed bytes

#### Scenario: Oversized list rejected during deserialization
- **WHEN** a WAL entry contains a list length (e.g. schema column count) exceeding the maximum
- **THEN** deserialization returns an error without allocating the claimed elements

### Requirement: Flusher Thread Supervision
The system SHALL supervise the background flusher thread. If the flusher thread
terminates due to an exception, the system SHALL transition the database to a
read-only error state and signal all pending durability callbacks so that no
caller blocks indefinitely. On clean shutdown, the flusher SHALL drain the
TBQueue fully and fsync before closing the WAL file.

#### Scenario: Flusher crash transitions to error state
- **WHEN** the flusher thread terminates due to an IO exception
- **THEN** subsequent write operations return an error indicating the WAL is unavailable
- **AND** read operations continue to work

#### Scenario: Flusher crash signals orphaned callbacks
- **WHEN** the flusher thread crashes while items remain in the WAL queue
- **THEN** all queued durability callbacks are signaled
- **AND** no caller remains blocked waiting for a callback that will never arrive

#### Scenario: Clean shutdown
- **WHEN** the database is shut down gracefully
- **THEN** the flusher drains all remaining entries from the TBQueue
- **AND** writes and fsyncs them to disk
- **AND** closes the WAL file handle

## ADDED Requirements

### Requirement: WAL Handle Cleanup Exception Safety
The system SHALL close the WAL sync file descriptor even if closing the
buffered file handle throws an exception. The `closeWAL` operation SHALL use
exception-safe resource cleanup (e.g. `finally`) to guarantee both handles are
released.

#### Scenario: hClose failure does not leak sync fd
- **WHEN** `closeWAL` is called and `hClose` throws an IO exception
- **THEN** the sync file descriptor is still closed via `closeFd`
- **AND** the original exception is re-raised

### Requirement: Database Configuration Validation
The system SHALL validate `DatabaseConfig` when opening a database. The WAL
queue capacity MUST be a positive integer. If validation fails, `openDatabase`
SHALL throw an error describing the invalid configuration.

#### Scenario: Zero queue capacity rejected
- **WHEN** `openDatabase` is called with `configQueueCapacity` set to 0
- **THEN** an error is raised indicating the queue capacity must be positive

#### Scenario: Negative queue capacity rejected
- **WHEN** `openDatabase` is called with `configQueueCapacity` set to a negative value
- **THEN** an error is raised indicating the queue capacity must be positive
