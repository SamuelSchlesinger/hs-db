## ADDED Requirements

### Requirement: PostgreSQL Wire Protocol (Simple Query)
The server SHALL implement the PostgreSQL v3 frontend/backend wire protocol
for the simple query sub-protocol. The server SHALL handle the following
message types: StartupMessage, Query, and Terminate from the client;
AuthenticationOk, ParameterStatus, BackendKeyData, ReadyForQuery,
RowDescription, DataRow, CommandComplete, EmptyQueryResponse, and
ErrorResponse to the client. Each message (except StartupMessage) SHALL
consist of a 1-byte type identifier, a 4-byte length (including itself),
and the payload. Messages SHALL be serialized in network byte order.

#### Scenario: Message framing
- **WHEN** the server sends a ReadyForQuery message
- **THEN** the message is framed as: type byte 'Z', 4-byte length (5), 1-byte status ('I' for idle)

#### Scenario: Parse client Query message
- **WHEN** the client sends a Query message containing `SELECT * FROM users`
- **THEN** the server extracts the SQL string from the message payload

#### Scenario: ErrorResponse format
- **WHEN** a query execution fails
- **THEN** the server sends an ErrorResponse with severity, SQLSTATE code, and human-readable message fields

### Requirement: Connection Startup Handshake
The server SHALL perform the PostgreSQL startup handshake: read the
StartupMessage (protocol version 3.0, connection parameters), respond with
AuthenticationOk (no actual authentication), send server ParameterStatus
messages (server_version, client_encoding, server_encoding), send
BackendKeyData, and send ReadyForQuery with idle status. The server SHALL
handle SSL negotiation requests by responding with 'N' (SSL not supported)
before reading the actual StartupMessage.

#### Scenario: Successful startup
- **WHEN** a client sends a StartupMessage with protocol version 3.0
- **THEN** the server responds with AuthenticationOk, ParameterStatus messages, BackendKeyData, and ReadyForQuery 'I'
- **AND** the connection enters the query loop

#### Scenario: SSL negotiation rejection
- **WHEN** a client sends an SSLRequest before the StartupMessage
- **THEN** the server responds with byte 'N'
- **AND** waits for the actual StartupMessage

### Requirement: Query Processing Loop
After startup, the server SHALL enter a loop: read a frontend message, and
if it is a Query message, parse and execute the SQL, send the appropriate
response messages (RowDescription + DataRows for SELECT, CommandComplete
for mutations, ErrorResponse for errors), and send ReadyForQuery. If the
message is Terminate, the server SHALL close the connection gracefully.

#### Scenario: SELECT query response
- **WHEN** the client sends a SELECT query that returns 3 rows with 2 columns
- **THEN** the server sends RowDescription (2 fields), 3 DataRow messages, CommandComplete ("SELECT 3"), and ReadyForQuery

#### Scenario: INSERT response
- **WHEN** the client sends an INSERT query that succeeds
- **THEN** the server sends CommandComplete ("INSERT 0 1") and ReadyForQuery

#### Scenario: Query error response
- **WHEN** the client sends a query that fails (e.g., table not found)
- **THEN** the server sends ErrorResponse with the error details and ReadyForQuery

#### Scenario: Empty query
- **WHEN** the client sends a Query message with an empty string
- **THEN** the server sends EmptyQueryResponse and ReadyForQuery

#### Scenario: Client disconnect
- **WHEN** the client sends a Terminate message
- **THEN** the server closes the connection and frees associated resources

### Requirement: TCP Server
The server SHALL listen for TCP connections on a configurable host and port
(default `127.0.0.1:5433`). Each accepted connection SHALL be handled in a
separate lightweight thread. All connections SHALL share the same Database
handle. The server SHALL handle client disconnections gracefully without
affecting other connections.

#### Scenario: Accept multiple connections
- **WHEN** multiple clients connect to the server simultaneously
- **THEN** each client is handled in its own thread
- **AND** each client can independently run queries

#### Scenario: Client crash does not affect others
- **WHEN** a client abruptly disconnects (TCP reset)
- **THEN** the server cleans up that connection's resources
- **AND** other active connections continue functioning normally

#### Scenario: Server listens on configured address
- **WHEN** the server is started with `--host 0.0.0.0 --port 9999`
- **THEN** the server listens on 0.0.0.0:9999

### Requirement: Server Executable
The system SHALL provide an executable `hs-db-server` with the following
CLI flags: `--host` (default "127.0.0.1"), `--port` (default 5433),
`--data-dir` (default "./hs-db-data", directory for WAL file). The server
SHALL open a Database using `withDatabase`, start the TCP listener, and
handle SIGINT/SIGTERM for graceful shutdown.

#### Scenario: Start with defaults
- **WHEN** `hs-db-server` is run with no arguments
- **THEN** the server listens on 127.0.0.1:5433 with WAL stored in ./hs-db-data/wal

#### Scenario: Start with custom options
- **WHEN** `hs-db-server --host 0.0.0.0 --port 9999 --data-dir /tmp/mydb` is run
- **THEN** the server listens on 0.0.0.0:9999 with WAL stored in /tmp/mydb/wal

#### Scenario: Graceful shutdown
- **WHEN** the server receives SIGINT
- **THEN** the server stops accepting new connections
- **AND** waits for active connections to complete (with a timeout)
- **AND** closes the Database cleanly (flushing WAL)
