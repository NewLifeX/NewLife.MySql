# NewLife.MySql — 纯国产高性能 MySQL 驱动

[![NuGet](https://img.shields.io/nuget/v/NewLife.MySql.svg)](https://www.nuget.org/packages/NewLife.MySql)
[![License](https://img.shields.io/github/license/NewLifeX/NewLife.MySql)](https://github.com/NewLifeX/NewLife.MySql/blob/master/LICENSE)

> 🌐 **中文** | [English](README.en.md) | [日本語](README.ja.md) | [한국어](README.ko.md) | [Español](README.es.md) | [Português](README.pt.md)

**NewLife.MySql** 是新生命团队出品的纯国产 MySQL 客户端驱动，基于 ADO.NET 标准接口，直接通过 TCP 实现 MySQL 协议层（Protocol Version 10）。仅依赖 `NewLife.Core`，**零第三方依赖**、**全链路真异步**、**MIT 协议商用无忧**。

独创的**管道化批量执行**（Pipeline）在万级行批量写入场景下可达 **2×~3×** 加速（vs 逐行基线），是大数据批量操作、信创国产化替代的理想选择。

---

## 设计目标

| 目标 | 说明 |
|------|------|
| **轻量极简** | 只做 ADO.NET 标准的增删改查、事务、参数化查询、存储过程和批量操作 |
| **高性能** | 零额外内存分配（`ArrayPool` / `OwnerPacket`）、真异步 IO、管道化批量执行 |
| **纯国产** | 核心代码完全自主可控，无 GPL 许可风险，适合信创环境 |
| **大数据友好** | 字典参数集 / 数组绑定 / 管道化 / 多行 VALUES / DbBatch，覆盖万级到百万级行 |
| **广泛兼容** | `net45` ~ `net10` 全版本，MySQL 5.x ~ 9.0+，兼容 OceanBase / TiDB / Doris / Aurora / CloudSQL |

## 核心功能

| 功能 | 说明 |
|------|------|
| 连接管理 | MySQL 5.x / 8.0 / 9.0+，`mysql_native_password` + `caching_sha2_password` + `authentication_webauthn` 多认证 |
| Unix Socket | 支持 Unix Domain Socket 本地连接，零 TCP 开销 |
| 压缩协议 | 支持 zlib / zstd 压缩传输，减少网络带宽消耗 |
| 查询执行 | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`，完整 ADO.NET 接口 |
| 参数化查询 | 客户端参数替换，`@参数名` 语法，自动转义防注入 |
| 预编译语句 | `Prepare()` 走 `COM_STMT_PREPARE` 二进制协议，可选 `UseServerPrepare` 全局开关 |
| 批量操作 | `ExecuteBatch` / `ExecuteArrayBatch` / 管道化 / 多行 VALUES / DbBatch / `MySqlBulkCopy` |
| 管道化执行 | `Pipeline=true` 批量发送后批量接收，网络延迟仅一次，万级行性能提升 3~10× |
| 事务支持 | `BeginTransaction` / `Commit` / `Rollback`，四种隔离级别，自动回滚 |
| 存储过程 | `CommandType.StoredProcedure`，支持 IN/OUT 参数 |
| SSL/TLS | `SslMode=Preferred/Required`，支持 TLS 1.2/1.3 |
| 连接池 | 默认开启，按连接字符串分池，可配置存活期与空闲验活，自动健康检查与回收 |
| 异步方法 | 全链路 `async/await`，`OpenAsync` / `ExecuteReaderAsync` / `ReadAsync` 等 |
| Schema 查询 | Tables / Columns / Indexes / Databases 等元数据，扩展索引/外键查询 |
| DbDataSource | .NET 7+ 风格 `MySqlDataSource`，支持依赖注入 |
| DbBatch | .NET 6+ ADO.NET 标准批量 API |
| 空间数据 | MySQL Geometry 类型，`MySqlGeometry` WKB 封装 |
| EF Core | 通过 `NewLife.MySql.EntityFrameworkCore` 独立包支持 |
| XCode 集成 | 通过 `MySqlClientFactory` 自动注册，无缝对接 XCode ORM |

---

## 亮点特性

### 🚀 管道化批量执行（Pipeline）

独创的管道化模式，基于 `COM_STMT_PREPARE + COM_STMT_EXECUTE` 二进制协议：

- **Phase 1**：批量构建并发送所有 EXECUTE 包，仅最后一个包 Flush
- **Phase 2**：按顺序批量读取所有 OK/Error 响应，累加影响行数
- TCP 协议栈合并小包（Nagle 算法），网络往返仅一次

**实测数据**（.NET 10 + MySQL 8.0.26 本机，管道化+事务 vs 逐行基线）：

| 场景 | 逐行基线 | 管道化+事务 | 加速比 |
|------|------:|------:|------:|
| 1,000 行 INSERT | 437ms | 54ms | **8.1×** |
| 10,000 行 INSERT | 3,150ms | 680ms | **4.6×** |
| 10,000 行 UPDATE | 3,469ms | 757ms | **4.6×** |
| 10,000 行 DELETE | 3,351ms | 673ms | **5.0×** |

**三驱动性能数据**（10,000 行批量操作）：

| 操作 | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | 899ms | 1,927ms | 1,906ms |
| UPDATE | 710ms | 2,265ms | 2,041ms |
| DELETE | 661ms | 1,961ms | 1,767ms |

> 本机内网已有 **4×~8×** 加速，跨机房场景收益更大。详细数据参见 [性能测试报告](Doc/性能测试报告.md)。

### 🔧 五种批量操作方案

| 方案 | API | 适用场景 |
|------|-----|---------|
| 字典参数集 | `ExecuteBatch` | 动态参数，参数来自集合 |
| 数组绑定 | `ExecuteArrayBatch` | 大批量 DML，Oracle 风格 |
| 管道化 | `Pipeline=true` | 万级/十万级行，跨机房 |
| 多行 VALUES | 拼接 SQL | 纯 INSERT 批量 |
| DbBatch (.NET 6+) | `CreateBatch` | 不同 SQL 混合批量 |

### 🛡️ 零第三方依赖

仅依赖 `NewLife.Core`（同为 NewLife 团队出品），无 GPL 许可风险，MIT 协议商用无忧，适合信创环境和安全审计严格的场景。

### ⚡ 极致性能优化

- `ArrayPool<T>` + `OwnerPacket` 零额外分配
- `Pool.StringBuilder` / `Pool.MemoryStream` 全面池化
- 全链路 `async/await`，无 `sync-over-async` 反模式
- 精简协议解析，只解析必要字段

### 🌐 广泛兼容

- **框架**：`net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net10.0`
- **MySQL**：5.5 ~ 9.0+，`mysql_native_password` + `caching_sha2_password`
- **兼容库**：OceanBase、TiDB、Doris（自动检测）、Aurora / CloudSQL（云数据库）、MariaDB（基础）
- **条件编译**：针对不同框架自动选择最优实现

---

## 功能特性

| 功能 | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:---:|:---:|:---:|
| **许可协议** | **MIT** | MIT | GPL (商用付费) |
| **国产自主** | ✅ | ❌ | ❌ |
| **第三方依赖** | **无** | 无 | 无 |
| **真异步 IO** | ✅ | ✅ | ❌ |
| **管道化批量执行** | ✅ | ❌ | ❌ |
| **数组绑定批量** | ✅ | ❌ | ❌ |
| **字典参数集批量** | ✅ | ❌ | ❌ |
| **DbBatch (.NET 6+)** | ✅ | ✅ | ❌ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **EF Core** | ✅ | ✅ (Pomelo) | ✅ |
| **XCode ORM 集成** | ✅ | ❌ | ❌ |
| **多目标框架** | net45~net10 | netstandard2.0+ | net462+ |
| **OceanBase / TiDB / Aurora / Doris** | ✅ 自动检测 | ❌ | ❌ |
| **MySqlBulkCopy** | ✅ | ✅ | ✅ |
| **压缩协议 (zlib/zstd)** | ✅ | ✅ | ❌ |
| **Unix Socket** | ✅ | ✅ | ❌ |
| **WebAuthn 认证** | ✅ | ✅ | ❌ |
| **DbDataSource** | ✅ | ❌ | ❌ |

> 完整功能和架构分析参见 [架构设计](Doc/架构设计.md)。

---

## 安装

```shell
dotnet add package NewLife.MySql
```

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

---

## 快速开始

### 连接字符串

```
Server=localhost;Port=3306;Database=mydb;User Id=root;Password=pass;
```

| 参数 | 别名 | 默认值 | 说明 |
|------|------|--------|------|
| Server | DataSource, Data Source | - | 服务器地址 |
| Port | - | 3306 | 端口号 |
| Database | - | - | 数据库名 |
| UserID | Uid, User Id, User | - | 用户名 |
| Password | Pass, Pwd | - | 密码 |
| ConnectionTimeout | Connection Timeout | 15 | 连接超时（秒） |
| CommandTimeout | Default Command Timeout | 30 | 命令超时（秒） |
| SslMode | Ssl Mode | None | SSL 模式（None/Preferred/Required） |
| UseServerPrepare | Use Server Prepare | false | 全局服务端预编译开关 |
| Pipeline | Pipelining | false | 管道化执行开关 |

### 打开连接

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;Password=pass;");
conn.Open();
// using 结束时自动关闭并归还连接池
```

### 查询结果集

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

### 查询单值

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### 执行 DML

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// 快捷方式
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

---

## 参数化查询

使用 `@参数名` 语法绑定参数，自动转义防注入：

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
// 参数化插入
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age, created) VALUES(@name, @age, @dt)");
cmd.Parameters.AddWithValue("name", "Alice");
cmd.Parameters.AddWithValue("age", 30);
cmd.Parameters.AddWithValue("dt", DateTime.Now);
cmd.ExecuteNonQuery();
```

```csharp
// NULL 值传递
cmd.Parameters.AddWithValue("email", DBNull.Value);
```

**支持的参数类型**：

| .NET 类型 | SQL 字面量示例 |
|-----------|---------------|
| `String` | `'hello'`（自动转义特殊字符） |
| `Int32` / `Int64` 等数字 | `42` |
| `Boolean` | `1` 或 `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `MySqlGeometry` | WKB 格式几何数据 |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | 转为数字 |
| `null` / `DBNull.Value` | `NULL` |

---

## 事务

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

支持四种隔离级别：`ReadUncommitted` / `ReadCommitted` / `RepeatableRead`（默认）/ `Serializable`

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

事务 `Dispose` 时如果未提交也未回滚，会自动执行回滚。

---

## 存储过程

```csharp
using var cmd = new MySqlCommand { Connection = conn, CommandType = CommandType.StoredProcedure };
cmd.CommandText = "my_proc";
cmd.Parameters.AddWithValue("p_id", 1);

var outParam = new MySqlParameter { ParameterName = "p_result", Direction = ParameterDirection.Output };
cmd.Parameters.Add(outParam);

cmd.ExecuteNonQuery();
var result = outParam.Value; // 输出参数值
```

---

## 预编译语句

通过 `Prepare()` 在服务端预编译 SQL，后续执行走二进制协议：

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 25);

cmd.Prepare(); // 服务端预编译

cmd.ExecuteNonQuery(); // 第一次执行

// 修改参数后再次执行（无需重新编译）
cmd.Parameters[0].Value = "Jerry";
cmd.Parameters[1].Value = 30;
cmd.ExecuteNonQuery();
```

也可通过连接字符串全局启用，所有参数化查询自动走二进制协议：

```
UseServerPrepare=true;
```

---

## 批量操作

### 方案 A：字典参数集

同一 SQL 绑定多组参数执行，内部自动预编译：

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
// 或 await cmd.ExecuteBatchAsync(paramSets);
```

### 方案 B：数组绑定（Oracle 风格）

参数值设为数组，指定执行次数：

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// 或 await cmd.ExecuteArrayBatchAsync(3);
```

### 方案 C：管道化执行

连接字符串开启 `Pipeline=true`，批量发送后批量接收，网络往返仅一次：

```csharp
var connStr = "Server=localhost;Database=mydb;User Id=root;Password=pass;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

管道化对 INSERT / UPDATE / DELETE 均有效，搭配字典参数集或数组绑定使用。网络延迟越高（如跨机房），收益越大。

### 方案 D：多行 VALUES

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### 方案 E：DbBatch API（.NET 6+）

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
```

---

## EF Core 支持

`NewLife.MySql.EntityFrameworkCore` 包提供 EF Core 数据库提供程序，支持 .NET 6 / .NET 8 / .NET 10。

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
    .UseMySql("Server=localhost;Database=mydb;User Id=root;Password=pass;")
    .Options;

using var context = new AppDbContext(options);
```

**已支持功能**：

| 功能 | 说明 |
|------|------|
| 类型映射 | Int / Long / String / DateTime / Decimal / Guid / Boolean / Byte[] |
| 查询翻译 | LINQ → MySQL 语法（反引号、LIMIT/OFFSET） |
| 方法翻译 | Contains / StartsWith / EndsWith / ToUpper / ToLower / Trim / Replace / Substring / Length |
| 成员翻译 | DateTime.Year / Month / Day / Hour / Minute / Second |
| 迁移支持 | CreateTable / AlterColumn / AddColumn / DropColumn / RenameTable / CreateIndex / DropIndex |
| 值生成策略 | AUTO_INCREMENT 自增主键 |

---

## SSL/TLS 加密连接

```csharp
// 服务器支持则加密，不支持则明文
var connStr = "Server=localhost;Database=mydb;User Id=root;Password=pass;SslMode=Preferred;";

// 必须加密，不支持则抛异常
var connStr2 = "Server=localhost;Database=mydb;User Id=root;Password=pass;SslMode=Required;";
```

| SslMode | 行为 |
|---------|------|
| `None` / `Disabled` | 不使用 SSL（默认） |
| `Preferred` | 优先加密，不支持则明文 |
| `Required` | 强制加密 |

---

## Schema 信息查询

```csharp
var databases = conn.GetSchema("Databases");  // 数据库列表
var tables = conn.GetSchema("Tables");         // 表列表
var columns = conn.GetSchema("Columns");       // 列信息
var indexes = conn.GetSchema("Indexes");       // 索引信息
```

---

## 异步方法

所有核心操作均支持异步版本：

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

## 连接池

连接池默认启用，无需额外配置。每个唯一连接字符串对应独立连接池。

- 打开连接时自动从池中获取
- 关闭连接时自动归还（不断开 TCP）
- 无效连接自动剔除
- 新连接按需创建
- **网络断线自动重连**：检测到连接断开时自动重试，提升长连接场景稳定性

### 连接池参数配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| MinPoolSize | 0 | 最小连接数 |
| MaxPoolSize | 100 | 最大连接数 |
| ConnectionLifeTime | 0 | 连接存活期（秒），0 表示永不过期 |
| ConnectionIdleTime | 300 | 空闲超时（秒），超时后回收 |
| ValidationInterval | 30 | 空闲验活间隔（秒） |

```csharp
var connStr = "Server=localhost;Database=mydb;...;MinPoolSize=5;MaxPoolSize=50;ConnectionLifeTime=3600;";
```

---

## XCode ORM 集成

通过 `MySqlClientFactory` 自动注册为 XCode 的 MySQL 驱动：

```csharp
using XCode.DataAccessLayer;

DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;Password=pass;", null, "MySql");

var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

---

## 切换数据库

### ChangeDatabase（推荐）

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();
conn.ChangeDatabase("db2"); // 内部 Close + Reopen
```

> ⚠️ 不支持在事务中途切换（事务会丢失），推荐为每个数据库创建独立连接。

### SqlClient.SetDatabaseAsync（低级 API）

```csharp
await conn.Client.SetDatabaseAsync("information_schema"); // COM_INIT_DB
```

不关闭连接，仅切换服务端当前数据库，适合临时查询场景。

---

## OceanBase / TiDB 支持

驱动根据握手包版本字符串自动检测数据库类型，连接方式与 MySQL 完全相同：

```csharp
// OceanBase
var connStr = "Server=oceanbase-host;Port=2881;Database=test;User Id=root;Password=pass;";
using var conn = new MySqlConnection(connStr);
conn.Open();
Console.WriteLine($"数据库类型: {conn.DatabaseType}");  // OceanBase

// TiDB
var connStr2 = "Server=tidb-host;Port=4000;Database=test;User Id=root;Password=pass;";
using var conn2 = new MySqlConnection(connStr2);
conn2.Open();
Console.WriteLine($"数据库类型: {conn2.DatabaseType}");  // TiDB
```

基础 SQL、事务、参数化查询、批量操作均兼容。

---

## 多目标框架

| 框架 | 说明 |
|------|------|
| `net45` / `net461` | .NET Framework，兼容老项目 |
| `netstandard2.0` / `netstandard2.1` | 跨平台兼容，支持异步 Dispose |
| `net6.0` | 额外支持 DbBatch API |
| `net10.0` | 最新 .NET，最优性能 |

---

## 注意事项

1. **MySQL 版本**：支持 5.x 及以上，推荐 8.0+
2. **字符编码**：默认 UTF-8
3. **ChangeDatabase**：通过 Close/Reopen 实现，不支持事务中切换，推荐独立连接
4. **管道化执行**：仅适用于 DML 批量操作，不适用于 SELECT 查询

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [需求文档](Doc/需求文档.md) | 功能需求、验收条件、迭代计划 |
| [架构设计](Doc/架构设计.md) | 架构概览、协议实现、技术选型、设计决策、驱动对比 |
| [性能测试报告](Doc/性能测试报告.md) | 批量操作基准性能测试详细数据 |
| [更新日志 (中文)](Doc/ChangeLog.md) | 中文版版本更新记录 |
| [Changelog (EN)](Doc/ChangeLog.en.md) | English release changelog |
| [英文 README](README.en.md) | English documentation with performance charts |
| [迁移指南：MySql.Data → NL](Doc/迁移指南_MySqlData.md) | 从 Oracle 官方驱动迁移的步骤和注意事项 |
| [迁移指南：MySqlConnector → NL](Doc/迁移指南_MySqlConnector.md) | 从社区主流驱动迁移的步骤和注意事项 |
