# NewLife.MySql Changelog

Pure C# high-performance MySQL driver. Zero third-party dependencies, pipelined batch execution, and true async.

---

## v1.5.2026.1024 (2026-10-24)

### New Features
- **OpenTelemetry tracing**: Built-in `ActivitySource` (source name `NewLife.MySql`) emitting Open / Execute / Commit / Rollback spans that follow the OpenTelemetry stable semantic conventions, with zero overhead when there are no subscribers
- **NativeAOT support**: `net8.0+` completes the trimming/AOT work with full metadata annotations and 0 IL warnings; a new `AotSmoke` project (26 scenarios) drives win-x64 / linux-x64 / linux-arm64 / linux-musl / LoongArch64 publish-and-run matrices
- **Runtime localization**: Adopts the NewLife.Core 12 compile-time word lists — 49 keys in the driver, 6 in EF6 and 4 in EF Core — covering 11 languages (Simplified/Traditional Chinese, Spanish, German, Japanese, French, Portuguese, Russian, Korean, Arabic), with automatic English fallback for missing translations; works under NativeAOT
- **Mobile smoke**: New Android/iOS smoke project and mobile CI verifying that the driver loads and executes core APIs on mobile platforms
- **Instruction-set matrix**: CI adds riscv64 / s390x smoke runs and a LoongArch64 observation job covering non-x64 publish-and-run paths
- **MySqlDataSource**: Public constructor and dependency-injection style usage restored; `CreateCommand` no longer throws `InvalidCastException`

### Bug Fixes
- **[fix] caching_sha2 full authentication over secure transport**: Fixed the `caching_sha2_password` full-authentication flow inside TLS / Unix Socket channels, with new unit tests
- **[fix] Pool slot leak**: Failed and close paths now return their slot; fixed false database-consistency detection after `ChangeDatabase` that could reuse a wrong connection
- **[fix] SchemaOnly / SingleRow**: `SQL_SELECT_LIMIT` session-variable semantics restored so behavior no longer drifts across executions
- **[fix] Localized runtime messages**: Corrected message argument order and missing keys; improved Arabic and Traditional Chinese translations
- **[fix] Typed reads and random packets**: Fixed intermittent random-packet failures and added typed-read, `DataAdapter` and CBOR coverage
- **[fix] EF Core provider**: Annotations completed alongside the trimming/AOT work; `MySqlDatabaseCreator` `Create`/`Delete`/`Exists` remain functional

### Performance
- Unit test suite is about 10× faster, with parallel-run flakiness eliminated
- Mobile and AOT smoke scenarios reuse connections, cutting connect overhead and timeout risk in integration tests

### Tests & Quality
- Test cases 905 → 929 (including AOT, mobile and localization); EF6 137 and EF Core 82 per target framework
- New assertions for data sources, async disposal, reconnect, array-bound batches and stored-procedure spans
- Test code no longer reflects over private members and depends on public APIs only

### Documentation
- README translated into 11 languages with new platform / architecture / mobile badges plus ecosystem map and team introduction
- New "AOT Compatibility Assessment", "Localization" and "Test Optimization" documents; feature list and competitive analysis kept in sync
- Migration guides add a NativeAOT comparison; zstd compression is explicitly deferred

### CI & Tooling
- Release pipeline aligned with NewLife.Core: publishes to both NuGet.org and GitHub Packages and creates a GitHub Release whose notes come from the matching ChangeLog section
- Localization scripts consolidated under `tools/` with stricter checks (word-list keys, README coverage, NuGet README sync)
- test / aot workflows narrowed to main branches plus related paths, with new platform matrices and localization gates

---

## v1.4.2026.0802 (2026-08-02)

### New Features
- **Apache Doris Compatibility**: Auto-detect Apache Doris analytical database, new `Doris` enum in `DatabaseType`
- **EF Core Database Creation**: `MySqlDatabaseCreator` implements `Create/Delete`, making `EnsureCreated` usable; `Exists` queries `information_schema.SCHEMATA` to avoid mis-detection from pooled stale connections
- **Value-Array Batch Binding**: New `ExecuteBatchValues`/`ExecuteBatchValuesAsync` on `MySqlCommand` for zero dictionary/parameter-object allocation in bulk imports

### Security
- **Packet Size Limit**: Enhanced packet size upper-bound detection to prevent OOM from malicious oversized packets

### Bug Fixes
- **[fix] Compression protocol overhaul**: `UseCompression=true` never actually worked; this release fixes 5 stacked defects (missing 4-byte plain frame header on compressed payload, DEFLATE→zlib matching MySQL protocol, unified compressed row reading, cross-packet fragmentation support, whole-chain compression) with new `CompressionTests` + `ZlibCodecTests`
- **[fix] Pool config pollution**: `CreatePool` clones connection string snapshot so `ChangeDatabase` can't corrupt shared pool config
- **[fix] `MySqlDataSource` public constructor**: was internal and unusable; also fixed `CreateCommand` InvalidCastException on `DbCommandWrapper` cast
- **[fix] Auto-reconnect restricted to read statements** (SELECT/SHOW/DESC/EXPLAIN); DML no longer retried to avoid duplicate writes
- **[fix] `SerializeValue` datetime uses InvariantCulture** to avoid invalid SQL literals from culture-specific separators
- **[fix] `MySqlCommand.Cancel()` throws NotSupportedException** instead of silent no-op
- **[fix] OwnerPacket Dispose on send paths** returns ArrayPool buffers, avoiding buffer leaks in batch scenarios

### Performance
- `ReadModelsAsync<T>` caches compiled property setter delegates, eliminating per-row reflection (10k-row entity mapping 6.93ms→6.66ms)
- Binary row reading reuses null_bitmap buffer, eliminating per-row allocation
- `GetOrdinal` column-name→index mapping cache, eliminating O(n) lookup and delegate allocation
- Compression send path pools MemoryStream + GetBuffer, eliminating two allocations per packet
- OwnerPacket Dispose on precompile/compression paths returns ArrayPool memory
- **Value-array bulk import**: `SqlClient` adds value-array batch core (`ExecuteStatementValuesAsync`); `MySqlBulkCopy` builds `Object?[]` per row, eliminating per-row `Dictionary` and parameter object allocations (~74.8% lower allocation for 100k rows); fixed `@name` not replaced with `?` causing NULL user-variable binding

### Architecture
- `MySqlCommand.DisposeAsync` truly asynchronously closes prepared statements instead of blocking Dispose on network IO
- `MySqlPool` synchronous lease rebuilds idle-over-threshold connections, consistent with async PING validation path

### Code Hygiene
- Cleaned empty if in `GetColumnsAsync`, dead comments in `MySqlFieldCodec`, indentation in `ExecuteBatch*Async`

---

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
- **Benchmark Refresh**: Re-ran all performance benchmarks with updated Pipeline(tx) batch DML data
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
