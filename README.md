# NewLife.MySql — Pure C# High-Performance MySQL Driver

[![AOT](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/aot.yml/badge.svg?label=AOT)](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/aot.yml)


![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.MySql?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.MySql?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.MySql?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.MySql?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/test.yml)
[![locales](https://img.shields.io/badge/locales-11-2ea44f?logo=dotnet)](Doc/多语言本地化.md)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS%20%7C%20Android%20%7C%20iOS-blue)
![arch](https://img.shields.io/badge/arch-x64%20%7C%20arm64%20%7C%20loongarch64%20%7C%20riscv64%20%7C%20s390x-orange)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.MySql?logo=github)](https://github.com/NewLifeX/NewLife.MySql/graphs/contributors)


<!-- lang:start -->
🌐 [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**NewLife.MySql** is a pure C# MySQL client driver from the NewLife team. Built on the standard ADO.NET interfaces, it implements the MySQL protocol layer (Protocol Version 10) directly over TCP. It depends only on `NewLife.Core`, with **zero third-party dependencies**, **true end-to-end async**, and an **MIT license** for worry-free commercial use.

Its innovative **Pipelined Batch Execution** (Pipeline) delivers a **2×~3×** speedup for batch writes in the tens-of-thousands-of-rows range (vs the row-by-row baseline), making it an ideal choice for big-data batch operations and domestic (信创) replacement scenarios.

---

## Design Goals

| Goal | Description |
|------|------|
| **Lightweight and minimal** | Only standard ADO.NET CRUD, transactions, parameterized queries, stored procedures, and batch operations |
| **High performance** | Zero extra memory allocations (`ArrayPool` / `OwnerPacket`), true async I/O, pipelined batch execution |
| **Pure domestic (Chinese) stack** | Core code is fully self-controlled with no GPL licensing risk, suitable for 信创 environments |
| **Big-data friendly** | Dictionary parameter sets / array binding / pipelining / multi-row VALUES / DbBatch, covering everything from tens of thousands to millions of rows |
| **Broad compatibility** | `net45` ~ `net10` across all versions, MySQL 5.x ~ 9.0+, compatible with OceanBase / TiDB / Doris / Aurora / CloudSQL, and NativeAOT support on `net8.0+` |

## Core Features

| Feature | Description |
|------|------|
| Connection management | MySQL 5.x / 8.0 / 9.0+, multiple authentication methods: `mysql_native_password` + `caching_sha2_password` + `authentication_webauthn` |
| Unix Socket | Supports Unix Domain Socket local connections with zero TCP overhead |
| Compression protocol | Supports zlib compressed transport (`UseCompression`) to reduce network bandwidth consumption; zstd is deferred because of the zero-third-party-dependency constraint |
| Query execution | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`, complete ADO.NET interfaces |
| Parameterized queries | Client-side parameter substitution with `@parameter-name` syntax and automatic escaping to prevent injection |
| Prepared statements | `Prepare()` uses the `COM_STMT_PREPARE` binary protocol, with an optional global `UseServerPrepare` switch |
| Batch operations | `ExecuteBatch` / `ExecuteArrayBatch` / pipelining / multi-row VALUES / DbBatch / `MySqlBulkCopy` |
| Pipelined execution | `Pipeline=true` sends in batch and receives in batch, with only one network round trip: 35× for thousands of rows and nearly 100× for tens of thousands of rows |
| Transactions | `BeginTransaction` / `Commit` / `Rollback`, four isolation levels, automatic rollback |
| Stored procedures | `CommandType.StoredProcedure`, supports IN/OUT parameters |
| SSL/TLS | `SslMode=Preferred/Required`, supports TLS 1.2/1.3 |
| Connection pooling | Enabled by default, pooled per connection string, configurable lifetime and idle validation, automatic health checks and recycling |
| Async methods | Full-chain `async/await`: `OpenAsync` / `ExecuteReaderAsync` / `ReadAsync`, etc. |
| Schema queries | Metadata such as Tables / Columns / Indexes / Databases, plus extended index/foreign-key queries |
| DbDataSource | .NET 7+ style `MySqlDataSource`, supports dependency injection |
| Tracing | Built-in `ActivitySource` instrumentation (source name `NewLife.MySql`), emitting Open / Execute / Commit / Rollback spans that follow the OpenTelemetry stable semantic conventions, with zero overhead when there are no subscribers |
| DbBatch | .NET 6+ standard ADO.NET batch API |
| Spatial data | MySQL Geometry types, `MySqlGeometry` WKB wrapper |
| EF Core | Supported through the separate `NewLife.MySql.EntityFrameworkCore` package |
| XCode integration | Auto-registered through `MySqlClientFactory`, seamlessly integrating with the XCode ORM |

---

## Highlights

### 🚀 Pipelined Batch Execution (Pipeline)

An innovative pipelined mode based on the `COM_STMT_PREPARE + COM_STMT_EXECUTE` binary protocol:

- **Phase 1**: Build and send all EXECUTE packets in batch, flushing only the last packet
- **Phase 2**: Read all OK/Error responses in order in batch, accumulating the affected row count
- The TCP stack coalesces small packets (Nagle algorithm), so there is only one network round trip

**Measured data** (.NET 10 + MySQL 8.0.26 localhost, pipelining + transaction vs row-by-row baseline):

| Scenario | Row-by-row baseline | Pipeline(tx) | Speedup |
|------|------:|------:|------:|
| 100-row INSERT | 244ms | 20ms | 12.2× |
| 1,000-row INSERT | 2,473ms | 70ms | **35.3×** |
| 10,000-row INSERT | ~70s¹ | ~0.7s¹ | ~98× |

> ¹ The 10,000-row row-by-row figure without a transaction is extrapolated (2.4ms/row); the Pipeline figure is measured without a transaction. With a transaction, 10,000 rows take about 70ms.

**Three-driver performance data** (10,000-row batch operations):

| Operation | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | 899ms | 1,927ms | 1,906ms |
| UPDATE | 710ms | 2,265ms | 2,041ms |
| DELETE | 661ms | 1,961ms | 1,767ms |

> Thousands of rows already achieve a **35×** speedup on a local LAN; the benefit is even greater across data centers. See the [Performance Test Report](Doc/性能测试报告.md) for detailed data.

### 🔧 Six Batch Operation Options

| Option | API | Use case |
|------|-----|---------|
| Dictionary parameter sets | `ExecuteBatch` | Dynamic parameters supplied by a collection |
| Value array binding | `ExecuteBatchValues` | Bulk import (`MySqlBulkCopy` internals), zero dictionary/parameter object allocations |
| Array binding | `ExecuteArrayBatch` | Large-volume DML, Oracle style |
| Pipelining | `Pipeline=true` | Tens of thousands / hundreds of thousands of rows, cross-data-center |
| Multi-row VALUES | Concatenated SQL | Pure INSERT batches |
| DbBatch (.NET 6+) | `CreateBatch` | Mixed batches of different SQL statements |

### 🛡️ Zero Third-Party Dependencies

Depends only on `NewLife.Core` (also from the NewLife team), with no GPL licensing risk, an MIT license for worry-free commercial use, and suitability for 信创 environments and scenarios with strict security audits.

### ⚡ Extreme Performance Optimizations

- `ArrayPool<T>` + `OwnerPacket` for zero extra allocations
- Fully pooled `Pool.StringBuilder` / `Pool.MemoryStream`
- Full-chain `async/await`, with no `sync-over-async` anti-pattern
- Lean protocol parsing that reads only the necessary fields

### 🌐 Broad Compatibility

- **Frameworks**: `net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net8.0` / `net10.0`
- **NativeAOT**: `net8.0+` supports trimming and NativeAOT (`IsAotCompatible`, with an `AotSmoke` smoke test and a CI mainstream-RID matrix for regression protection; required win-x64 / linux-x64 / linux-arm64; LoongArch linux-la64 is covered by QEMU emulation (observation period: it runs, and failures do not block)), see [AOT Compatibility Assessment](Doc/AOT兼容性评估.md)
- **MySQL**: 5.5 ~ 9.0+, `mysql_native_password` + `caching_sha2_password`
- **Compatible databases**: OceanBase, TiDB, Doris (auto-detect), Aurora / CloudSQL (cloud databases), MariaDB (basic)
- **Conditional compilation**: automatically selects the optimal implementation per framework

---

## Feature Comparison

| Feature | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:---:|:---:|:---:|
| **License** | **MIT** | MIT | GPL (paid for commercial use) |
| **Domestic (Chinese) autonomy** | ✅ | ❌ | ❌ |
| **Third-party dependencies** | **None** | None | None |
| **True async I/O** | ✅ | ✅ | ❌ |
| **Pipelined batch execution** | ✅ | ❌ | ❌ |
| **Array-bound batch** | ✅ | ❌ | ❌ |
| **Dictionary parameter-set batch** | ✅ | ❌ | ❌ |
| **DbBatch (.NET 6+)** | ✅ | ✅ | ❌ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **EF Core** | ✅ | ✅ (Pomelo) | ✅ |
| **XCode ORM integration** | ✅ | ❌ | ❌ |
| **Target frameworks** | net45~net10 | netstandard2.0+ | net462+ |
| **NativeAOT (net8.0+)** | ✅ | ✅ | ❌ |
| **OpenTelemetry tracing** | ✅ | ✅ | ✅ (8.1+) |
| **OceanBase / TiDB / Aurora / Doris** | ✅ Auto-detect | ❌ | ❌ |
| **MySqlBulkCopy** | ✅ | ✅ | ✅ |
| **Compression protocol (zlib)** | ✅ | ✅ | ❌ |
| **Unix Socket** | ✅ | ✅ | ❌ |
| **WebAuthn authentication** | ✅ | ✅ | ❌ |
| **DbDataSource** | ✅ | ❌ | ❌ |

> For the complete feature and architecture analysis, see [Architecture](Doc/架构设计.md).

---

## Installation

```shell
dotnet add package NewLife.MySql
```

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

---

## Quick Start

### Connection String

```
Server=localhost;Port=3306;Database=mydb;User Id=root;******;
```

| Parameter | Aliases | Default | Description |
|------|------|--------|------|
| Server | DataSource, Data Source | - | Server address |
| Port | - | 3306 | Port number |
| Database | - | - | Database name |
| UserID | Uid, User Id, User | - | Username |
| Password | Pass, Pwd | - | Password |
| ConnectionTimeout | Connection Timeout | 15 | Connection timeout (seconds) |
| CommandTimeout | Default Command Timeout | 30 | Command timeout (seconds) |
| SslMode | Ssl Mode | None | SSL mode (None/Preferred/Required) |
| UseServerPrepare | Use Server Prepare | false | Global server-side prepare switch |
| Pipeline | Pipelining | false | Pipelined execution switch |

### Opening a Connection

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;******;");
conn.Open();
// The connection is closed automatically and returned to the pool when the using scope ends
```

### Querying a Result Set

```csharp
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "SELECT id, name, age FROM users WHERE age > 18");
using var reader = cmd.ExecuteReader();

while (reader.Read())
{
    var id = reader.GetInt64(0);
    var name = reader.GetString(1);
    var age = reader.GetInt64(2);
    Console.WriteLine($"{id}: {name}, {age}");
}
```

### Querying a Scalar

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### Executing DML

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// Shortcut
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

---

## Parameterized Queries

Use the `@parameter-name` syntax to bind parameters; values are escaped automatically to prevent injection:

```csharp
using var cmd = new MySqlCommand(conn, "SELECT * FROM users WHERE name = @name AND age > @age");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 18);

using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    Console.WriteLine(reader.GetString(0));
}
```

```csharp
// Parameterized insert
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age, created) VALUES(@name, @age, @dt)");
cmd.Parameters.AddWithValue("name", "Alice");
cmd.Parameters.AddWithValue("age", 30);
cmd.Parameters.AddWithValue("dt", DateTime.Now);
cmd.ExecuteNonQuery();
```

```csharp
// Passing NULL values
cmd.Parameters.AddWithValue("email", DBNull.Value);
```

**Supported parameter types**:

| .NET Type | SQL Literal Example |
|-----------|---------------|
| `String` | `'hello'` (special characters escaped automatically) |
| `Int32` / `Int64` and other numeric types | `42` |
| `Boolean` | `1` or `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `MySqlGeometry` | WKB-format geometry data |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | Converted to a number |
| `null` / `DBNull.Value` | `NULL` |

---

## Transactions

```csharp
using var conn = new MySqlConnection(connStr);
conn.Open();

using var tr = conn.BeginTransaction();
try
{
    conn.ExecuteNonQuery("INSERT INTO orders(product, qty) VALUES('Widget', 10)");
    conn.ExecuteNonQuery("UPDATE inventory SET qty = qty - 10 WHERE product = 'Widget'");
    tr.Commit();
}
catch
{
    tr.Rollback();
    throw;
}
```

Four isolation levels are supported: `ReadUncommitted` / `ReadCommitted` / `RepeatableRead` (default) / `Serializable`

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

If a transaction is disposed without being committed or rolled back, it rolls back automatically.

---

## Stored Procedures

```csharp
using var cmd = new MySqlCommand { Connection = conn, CommandType = CommandType.StoredProcedure };
cmd.CommandText = "my_proc";
cmd.Parameters.AddWithValue("p_id", 1);

var outParam = new MySqlParameter { ParameterName = "p_result", Direction = ParameterDirection.Output };
cmd.Parameters.Add(outParam);

cmd.ExecuteNonQuery();
var result = outParam.Value; // Output parameter value
```

---

## Prepared Statements

Use `Prepare()` to compile the SQL on the server; subsequent executions use the binary protocol:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 25);

cmd.Prepare(); // Server-side prepare

cmd.ExecuteNonQuery(); // First execution

// Execute again after changing parameters (no recompilation needed)
cmd.Parameters[0].Value = "Jerry";
cmd.Parameters[1].Value = 30;
cmd.ExecuteNonQuery();
```

You can also enable it globally through the connection string, so that every parameterized query uses the binary protocol automatically:

```
UseServerPrepare=true;
```

---

## Batch Operations

### Option A: Dictionary Parameter Sets

Execute the same SQL with multiple sets of parameters bound; prepared internally and automatically:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", "");
cmd.Parameters.AddWithValue("age", 0);

var paramSets = new List<IDictionary<String, Object?>>
{
    new Dictionary<String, Object?> { ["name"] = "Alice", ["age"] = 25 },
    new Dictionary<String, Object?> { ["name"] = "Bob", ["age"] = 30 },
    new Dictionary<String, Object?> { ["name"] = "Charlie", ["age"] = 22 },
};
var totalAffected = cmd.ExecuteBatch(paramSets);
// or await cmd.ExecuteBatchAsync(paramSets);
```

### Option B: Array Binding (Oracle style)

Set parameter values to arrays and specify the execution count:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// or await cmd.ExecuteArrayBatchAsync(3);
```

### Option C: Pipelined Execution

Enable `Pipeline=true` in the connection string to send in batch and receive in batch with only one network round trip:

```csharp
var connStr = "Server=localhost;Database=mydb;User Id=root;******;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

Pipelining works for INSERT / UPDATE / DELETE and can be combined with dictionary parameter sets or array binding. The higher the network latency (for example, across data centers), the greater the benefit.

### Option D: Multi-row VALUES

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### Option E: DbBatch API (.NET 6+)

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
```

---

## EF Core Support

The `NewLife.MySql.EntityFrameworkCore` package provides an EF Core database provider supporting .NET 6 / .NET 8 / .NET 10.

```shell
dotnet add package NewLife.MySql.EntityFrameworkCore
```

```csharp
public class AppDbContext : DbContext
{
    public DbSet<User> Users { get; set; } = null!;

    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>(entity =>
        {
            entity.ToTable("users");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Name).HasMaxLength(100).IsRequired();
        });
    }
}

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseMySql("Server=localhost;Database=mydb;User Id=root;******;")
    .Options;

using var context = new AppDbContext(options);
```

**Supported features**:

| Feature | Description |
|------|------|
| Type mapping | Int / Long / String / DateTime / Decimal / Guid / Boolean / Byte[] |
| Query translation | LINQ → MySQL syntax (backticks, LIMIT/OFFSET) |
| Method translation | Contains / StartsWith / EndsWith / ToUpper / ToLower / Trim / Replace / Substring / Length |
| Member translation | DateTime.Year / Month / Day / Hour / Minute / Second |
| Migration support | CreateTable / AlterColumn / AddColumn / DropColumn / RenameTable / CreateIndex / DropIndex |
| Value generation strategy | AUTO_INCREMENT identity primary keys |

---

## SSL/TLS Encrypted Connections

```csharp
// Encrypt if the server supports it, otherwise use plaintext
var connStr = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Preferred;";

// Require encryption; throw if the server does not support it
var connStr2 = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Required;";
```

| SslMode | Behavior |
|---------|------|
| `None` / `Disabled` | Do not use SSL (default) |
| `Preferred` | Prefer encryption, fall back to plaintext if unsupported |
| `Required` | Force encryption |

---

## Schema Information Queries

```csharp
var databases = conn.GetSchema("Databases");  // Database list
var tables = conn.GetSchema("Tables");         // Table list
var columns = conn.GetSchema("Columns");       // Column information
var indexes = conn.GetSchema("Indexes");       // Index information
```

---

## Async Methods

All core operations support asynchronous versions:

```csharp
using var conn = new MySqlConnection(connStr);
await conn.OpenAsync();

using var cmd = new MySqlCommand(conn, "SELECT * FROM users");
using var reader = await cmd.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(0));
}
```

---

## Tracing (OpenTelemetry)

The driver has built-in `ActivitySource` instrumentation (source name `NewLife.MySql`), emitting four kinds of spans: `Open` (connection), `Execute` (command, covering the reader lifetime), `Commit` / `Rollback` (transactions). Attributes follow the OpenTelemetry stable database semantic conventions (`db.system.name` / `db.query.text` / `server.address` / `error.type`, etc.). The OpenTelemetry SDK is supplied by the consumer; the driver adds no dependencies:

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("NewLife.MySql")
    .AddOtlpExporter()
    .Build();
```

- **Scope**: `net6.0` and above; on lower versions and with no subscribers it is a zero-overhead no-op
- **Privacy note**: `db.query.text` records the command text; prefer parameterized queries so literals do not enter telemetry

---

## Connection Pool

The connection pool is enabled by default with no extra configuration needed. Each unique connection string maps to its own independent pool.

- Connections are taken from the pool automatically when opened
- Connections are returned automatically when closed (the TCP connection is not dropped)
- Invalid connections are removed automatically
- New connections are created on demand
- **Automatic reconnect after network disconnection**: retries automatically when a dropped connection is detected, improving stability in long-lived scenarios

### Connection Pool Parameters

| Parameter | Default | Description |
|------|--------|------|
| MinPoolSize | 0 | Minimum number of connections |
| MaxPoolSize | 100 | Maximum number of connections |
| ConnectionLifeTime | 0 | Connection lifetime (seconds); 0 means never expire |
| ConnectionIdleTime | 300 | Idle timeout (seconds); connections are reclaimed after it elapses |
| ValidationInterval | 30 | Idle validation interval (seconds) |

```csharp
var connStr = "Server=localhost;Database=mydb;...;MinPoolSize=5;MaxPoolSize=50;ConnectionLifeTime=3600;";
```

---

## XCode ORM Integration

Auto-registered as the MySQL driver for XCode through `MySqlClientFactory`:

```csharp
using XCode.DataAccessLayer;

DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;******;", null, "MySql");

var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

---

## Switching Databases

### ChangeDatabase (recommended)

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();
conn.ChangeDatabase("db2"); // Internally Close + Reopen
```

> ⚠️ Switching in the middle of a transaction is not supported (the transaction is lost); create an independent connection for each database.

### SqlClient.SetDatabaseAsync (low-level API)

```csharp
await conn.Client.SetDatabaseAsync("information_schema"); // COM_INIT_DB
```

Does not close the connection; only switches the server-side current database, suitable for temporary query scenarios.

---

## OceanBase / TiDB Support

The driver detects the database type automatically from the version string in the handshake packet; the connection method is exactly the same as for MySQL:

```csharp
// OceanBase
var connStr = "Server=oceanbase-host;Port=2881;Database=test;User Id=root;******;";
using var conn = new MySqlConnection(connStr);
conn.Open();
Console.WriteLine($"Database type: {conn.DatabaseType}");  // OceanBase

// TiDB
var connStr2 = "Server=tidb-host;Port=4000;Database=test;User Id=root;******;";
using var conn2 = new MySqlConnection(connStr2);
conn2.Open();
Console.WriteLine($"Database type: {conn2.DatabaseType}");  // TiDB
```

Basic SQL, transactions, parameterized queries, and batch operations are all compatible.

---

## Target Frameworks

| Framework | Description |
|------|------|
| `net45` / `net461` | .NET Framework, compatible with legacy projects |
| `netstandard2.0` / `netstandard2.1` | Cross-platform compatibility, supports async Dispose |
| `net6.0` | Additionally supports the DbBatch API |
| `net10.0` | Latest .NET, optimal performance |

---

## NativeAOT Deployment

With the `net8.0+` target frameworks, the driver has passed real NativeAOT publish validation (local measurements and CI on win-x64 and linux-x64; all 26 smoke scenarios passed with 0 ILC warnings). Publishing native binaries requires **no caller-side patches**; CI continuously guards against regressions across a mainstream-RID matrix (win-x64 / linux-x64 / linux-arm64 / win-arm64 / osx-x64 / osx-arm64 / musl, plus a QEMU-emulated smoke test for LoongArch linux-la64 during the observation period, which is non-blocking):

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

| Item | Content |
|------|------|
| Scope | `net8.0+`; `net6.0` requires an experimental switch, and earlier frameworks are not supported |
| Verified capabilities | Connection/authentication (including handshake connection attributes), sync and async queries, server-side prepare, batch (dictionary/array/value-array/pipelined), transactions, stored procedures, Schema metadata, DbDataSource, DataAdapter, MySqlBulkCopy, compression protocol, binlog subscription, tracing |
| Notes | The `MySqlDataAdapter` Fill/FillSchema/Update family carries `RequiresUnreferencedCode` annotations; trimmed applications declare the preservation responsibility at the call site per System.Data semantics |
| Detailed assessment | [AOT Compatibility Assessment](Doc/AOT兼容性评估.md) |

---

## Notes

1. **MySQL version**: 5.x and above supported, 8.0+ recommended
2. **Character encoding**: UTF-8 by default
3. **ChangeDatabase**: implemented via Close/Reopen; switching in the middle of a transaction is not supported, so use an independent connection
4. **Pipelined execution**: only for batch DML operations, not for SELECT queries

---

## Related Documentation

| Document | Description |
|------|------|
| [Requirements](Doc/需求文档.md) | Vision, core goals, feature directions |
| [Feature List](Doc/功能清单.md) | Three-dimensional status tracking of implementation/testing/comments |
| [Architecture](Doc/架构设计.md) | Architecture overview, protocol implementation, technology choices, design decisions, driver comparison |
| [Performance Test Report](Doc/性能测试报告.md) | Detailed benchmark data for batch operations |
| [Changelog (中文)](Doc/ChangeLog.md) | Chinese release changelog |
| [Changelog (EN)](Doc/ChangeLog.en.md) | English release changelog |
| [简体中文 README](README.zh-CN.md) | Chinese documentation, maintained in the same batch as this English version |
| [Migration Guide: MySql.Data → NL](Doc/迁移指南_MySqlData.md) | Steps and notes for migrating from the Oracle official driver |
| [Migration Guide: MySqlConnector → NL](Doc/迁移指南_MySqlConnector.md) | Steps and notes for migrating from the mainstream community driver |

## Ecosystem Map

NewLife is a layered ecosystem of independently versioned open-source projects. The map below is shared verbatim by every NewLife repository; each project links to its repository and its primary NuGet package.

| Project | What it does | Downloads |
| --- | --- | --- |
| **Foundation** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Logging, configuration, caching, networking, serialization, scheduling and APM tracing. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Data middleware: single tables at the tens-of-billions scale, automatic sharding, read-write splitting, 10+ databases. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | High-performance network library: 1.77M pps / 14.5 Gbps on a single machine (TCP 1KB, 2026-10), 4M long-lived TCP connections. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Protocol communication and RPC framework for client/server applications and IoT devices. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Installs .NET applications as OS daemons: Windows services and Linux systemd. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | `dotnet new` project templates for console, web, API, service, data and IoT projects. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Data & Messaging** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Redis client with microsecond latency, millions of ops/s and built-in message queues. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Pure managed RocketMQ client for Apache RocketMQ and Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT client and server, including Alibaba Cloud IoT connectivity. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Hybrid database in C#: embedded or networked, relational, time-series, message queue and KV in one. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | MySql driver and helper layer built on the NewLife data stack. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, AI & Distributed** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Rapid admin-platform framework: permissions, SSO, OAuth server and CRUD scaffolding. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Distributed service platform: node management, APM, configuration center, registry and release center. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Distributed task and real-time data scheduling platform with sharding, retries and backfill. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Unified client for 46 LLM providers plus ChatAI, a ready-to-run web chat application. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Industrial & IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | IoT standard library defining protocol contracts and device abstractions. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Modbus TCP/RTU/ASCII implementation on top of NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Siemens PLC protocol on top of NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Omron PLC protocol on top of NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Mitsubishi Melsec PLC protocol on top of NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | OPC client for industrial data access. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet building-automation protocol stack. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Lightweight open-source IoT platform with gateway and device management. | — |
| **Vehicle Telematics** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T 808 vehicle positioning protocol: 70+ message bodies, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | JT/T 809 platform-to-platform protocol for transport authorities. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T 1078 in-vehicle video: RTP parsing, FLV/fMP4 remuxing and live streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Domain Components** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | One package for Excel, Word, PPT, PDF and 15+ document formats. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Cross-platform audio: codecs, device I/O, DSP, containers and streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Video codecs, container formats, frame processing and streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Unified interface over Baidu, AMap, Tencent and Tianditu with geocoding and coordinate conversion. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | IPv4 geolocation with an offline memory-mapped database and an online API fallback. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Tools & Products** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Developer toolbox: network, serial port, crypto, regex, Modbus and MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Self-hosted Git source code management system. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Installs the matching .NET runtime automatically when launched from an older host. | — |

> Badges show the primary package of each project; theme, extension and satellite packages are listed on their NuGet pages. The 30+ packages not listed here (FTP, LoRa, Office extensions, protocol drivers and more) are available under the [NewLifeX organization](https://github.com/NewLifeX) and on [newlifex.com](https://newlifex.com).

## Team

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

Founded in 2002, the NewLife team has spent more than two decades on the .NET and IoT ecosystem, maintains nearly 100 open-source projects (34 of them core to this map), and delivers integrated software and hardware solutions for power, logistics, healthcare, industrial control and internet industries. We live by the motto "learning has no seniority — mastery earns respect".

Open-source components have been downloaded <!--nuget-total:start-->6.48M<!--nuget-total:end--> times from NuGet (ecosystem-wide, as of 2026-10).

Website: https://newlifex.com · Open source organization: https://github.com/NewLifeX

<details>
<summary>WeChat Official Account: 智能大石头 (scan to follow)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
