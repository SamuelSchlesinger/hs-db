## MODIFIED Requirements

### Requirement: Database Lifecycle Management
The system SHALL provide a bracket-style resource manager
(`withDatabase :: FilePath -> DatabaseConfig -> (Database -> IO a) -> IO a`)
that opens or replays the WAL, starts the flusher thread, runs the user action,
and performs clean shutdown. The system SHALL also expose `openDatabase` and
`closeDatabase` for non-bracket use cases. `closeDatabase` MUST be safe to call
concurrently from multiple threads: the first call performs the shutdown and
subsequent concurrent calls return immediately without deadlocking.

#### Scenario: withDatabase ensures cleanup on normal exit
- **WHEN** `withDatabase` is called and the user action completes normally
- **THEN** the flusher thread drains all pending entries, fsyncs, and the WAL file is closed

#### Scenario: withDatabase ensures cleanup on exception
- **WHEN** `withDatabase` is called and the user action throws an exception
- **THEN** the flusher thread drains all pending entries, fsyncs, the WAL file is closed
- **AND** the exception is re-raised to the caller

#### Scenario: Concurrent closeDatabase calls do not deadlock
- **WHEN** two threads call `closeDatabase` on the same database concurrently
- **THEN** exactly one thread performs the shutdown sequence
- **AND** the other thread returns without error and without blocking indefinitely
