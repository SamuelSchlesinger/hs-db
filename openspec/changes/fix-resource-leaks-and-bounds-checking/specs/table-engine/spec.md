## MODIFIED Requirements

### Requirement: Database Lifecycle Management
The system SHALL provide a bracket-style resource manager
(`withDatabase :: FilePath -> DatabaseConfig -> (Database -> IO a) -> IO a`)
that opens or replays the WAL, starts the flusher thread, runs the user action,
and performs clean shutdown. The system SHALL also expose `openDatabase` and
`closeDatabase` for non-bracket use cases. `closeDatabase` SHALL guarantee that
WAL file handles are closed even if the flusher reported an error during
shutdown. The 64-bit platform assertion SHALL be enforced at module load time.

#### Scenario: withDatabase ensures cleanup on normal exit
- **WHEN** `withDatabase` is called and the user action completes normally
- **THEN** the flusher thread drains all pending entries, fsyncs, and the WAL file is closed

#### Scenario: withDatabase ensures cleanup on exception
- **WHEN** `withDatabase` is called and the user action throws an exception
- **THEN** the flusher thread drains all pending entries, fsyncs, the WAL file is closed
- **AND** the exception is re-raised to the caller

#### Scenario: closeDatabase closes WAL handles on flusher error
- **WHEN** `closeDatabase` is called and the flusher reported a shutdown error
- **THEN** the WAL file handles are still closed before the error is raised
- **AND** no file descriptors are leaked

#### Scenario: Platform assertion fires on 32-bit systems
- **WHEN** the database module is loaded on a platform where Int is less than 64 bits
- **THEN** a runtime error is raised immediately at module load time
