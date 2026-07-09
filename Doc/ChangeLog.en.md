# NewLife.MySql Changelog

Pure C# high-performance MySQL driver. Zero third-party dependencies, pipelined batch execution, and true async.

---

## v1.3.2026.0709 (2026-07-09)

### New Features
- **Apache Doris Compatibility**: Auto-detect Apache Doris analytical database, new `Doris` enum in `DatabaseType`

## v1.3.2026.0702 (2026-07-02)

### New Features
- **Unix Socket**: Unix Domain Socket local connection support with zero TCP overhead on Linux
- **Compression Protocol**: zlib/zstd protocol-level compression to significantly reduce bandwidth
- **WebAuthn Authentication**: MySQL 8.4+ `authentication_webauthn` (FIDO2) passwordless authentication
- **MySqlBulkCopy**: Full `MySqlBulkCopy` bulk import API with DataTable / IDataReader data sources
- **DbDataSource**: New `MySqlDataSource` for dependency-injection-style data source management
- **Spatial Data Types**: MySQL Geometry type support with `MySqlGeometry` WKB wrapper
- **Aurora / CloudSQL Compatibility**: Auto-detect AWS Aurora and GCP Cloud SQL, enhanced cloud database support

### Connection Pool Enhancements
- **Lifetime Management**: New `ConnectionLifeTime` parameter for periodic connection recycling
- **Idle Validation**: New `ConnectionIdleTime` / `ValidationInterval` parameters for idle connection health checks
- **Configurable Parameters**: `MinPoolSize` / `MaxPoolSize` and other pool parameters fully configurable via connection string

### Schema Extensions
- **Extended Metadata**: Enhanced `GetSchema` with index, foreign key, and extended metadata queries

### Performance & Testing
- **Benchmark Refresh**: Re-ran all performance benchmarks; Pipeline(tx) leads competitors by 41–59%
- **Test Coverage**: Significant unit test additions covering core components and edge cases

### Bug Fixes
- **[fix]** Fixed read packet timeout during batch insert operations
- **[fix]** Fixed two existing test failures caused by connection pool refactoring

---

## v1.2.2026.0601 (2026-06-01)

### Tracing & Observability
- **ITracerFeature Support**: MySqlClientFactory implements ITracerFeature with Tracer enabled by default for enhanced observability
- **Secure Tracing**: MySqlPoolManager trace parameters minimized to Server/Database to prevent sensitive data leaks
- **Debug Tracing**: SqlClient.PingAsync adds tracing and exception logging in DEBUG mode for easier diagnostics

---

## v1.1.2026.0501 (2026-05-01)

### Connection Stability
- **Auto-Reconnect**: Automatic reconnect/retry on network disconnection for long-lived connections
- **Concurrency Fix**: Fixed MySqlConnection thread-safety issues and 3-byte integer read ordering
- **Empty Password**: Fixed NullReferenceException when connecting with an empty password

### Timeout Management
- **Command Timeout Inheritance**: MySqlCommand timeout auto-inherits from connection; DataReader read timeout is flexible
- **Timeout Recovery**: MySqlDataReader restores original timeout after reading, preventing side effects

### Connection & Resource Management
- **Buffer Reuse**: Optimized connection release to reuse protocol header buffers, reducing allocations
- **Database Switching**: Optimized database switching logic and resource cleanup on connection close
- **Server Info Cache**: Refactored MySqlPool server info cache to reduce redundant queries
- **Parameter Binding Refactor**: Refactored MySqlCommand parameter binding for better SQL parse compatibility
- **Protocol Packet Logging**: Added protocol packet send/receive logging for easier diagnostics

---

## v1.0 (2026-03-01)

### Features
- Full ADO.NET implementation: MySqlConnection, MySqlCommand, MySqlDataReader, MySqlParameter, MySqlTransaction
- MySQL client SSL/TLS secure connections
- Parameterized queries with named parameters and positional mapping
- Transaction commit, rollback, and auto-rollback
- Multi-statement and multi-result-set parsing
- GetSchema metadata queries for full ADO.NET compatibility
- MySqlDataAdapter with async operations and events
- Stored procedure parameter support
- .NET 6.0+ DbBatch API
- Server-side prepared statements (PrepareStatement) with binary parameter binding
- Pipeline batch execution for dramatically improved batch performance
- ChangeDatabase mechanism with SetDatabaseAsync API
- Client heartbeat to prevent connection timeout
- Read timeout control for MySQL client
- Async connection acquisition with heartbeat detection
- MySQL Binlog subscription and event parsing
- Full async/await: all core operations support async with CancellationToken
- EF Core Provider for full MySQL integration
- EF6 adapter layer with type mapping, migrations, etc.
- OceanBase and TiDB database auto-detection
- Multiple MySQL protocol commands via SqlClient
- Cross-platform: .NET Framework 4.5 / 4.6.1, .NET Standard 2.0 / 2.1, .NET 6.0 / 8.0 / 10.0

### Optimizations
- Buffer-optimized response packet parsing to reduce GC
- BufferedReader for streaming large packets
- Frame size limit on BufferedReader to avoid reading excess data
- SpanReader for improved protocol parsing efficiency
- Connection management and authentication flow optimizations
- SqlClient connection and memory management improvements
- WelcomeMessage handshake packet refactoring
- MySqlColumn field parsing refactoring for better reusability
- MySQL type system refactoring with BLOB/TEXT auto-mapping
- MySQL field codec refactoring for unified protocol parsing
- Response → ServerPacket rename and sync
- ServerPacket resource disposal to prevent memory leaks
- SqlClient protocol handling and error parsing optimizations
- Tracing on client open/close with exception logging
- Network stream reset to clean up unprocessed data
- Connection pool borrow: clear unread network stream data
- Configuration parameters prioritize cached pool variables
- SpanReader pass-by-ref fix and parameter packet construction simplification
- MySqlCommand parameter ordering and SQL parse compatibility
- Connection string assertion refactored to property-level validation
- NuGet package metadata and tags optimization

### Bug Fixes
- Fixed ReadZero causing password login failure
- Precise length reading to prevent incomplete data from network issues
- Convert.To*() replacement for direct type casts; fixed cross-type conversion in MySqlDataReader.GetXxx
- Fixed Bit type parsing
- Enabled ReadExactly for complete data reads

### Testing
- All unit tests passing
- Dynamic temporary tables for test isolation and robustness
- Enhanced Authentication testability with supplementary unit tests
- Concurrency isolation improvements and coverage increases
- Batch operation performance benchmarks
- Three-driver comparison and full-operation batch benchmarks
- Core component unit test additions

### Documentation
- Architecture documentation and user manual
- Consolidated documentation: feature descriptions in Readme.MD, design details in architecture docs
- Performance test documentation and benchmark sizing optimization

### Compatibility
- Zero third-party dependencies (only NewLife.Core)
- .NET Framework 4.5 through .NET 10 cross-platform support
