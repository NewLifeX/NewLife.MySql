# NewLife.MySql

[![NuGet](https://img.shields.io/nuget/v/NewLife.MySql.svg)](https://www.nuget.org/packages/NewLife.MySql)
[![License](https://img.shields.io/github/license/NewLifeX/NewLife.MySql)](https://github.com/NewLifeX/NewLife.MySql/blob/master/LICENSE)

**NewLife.MySql** 是新生命团队（NewLife）出品的**纯国产高性能 MySQL 驱动**，基于 ADO.NET 标准接口实现，直接通过 TCP 通信实现 MySQL 协议层（Protocol Version 10），完全不依赖 NewLife 团队以外的任何第三方组件，采用 **MIT 开源协议**。

仅依赖 `NewLife.Core`（同为新生命团队出品），实现了一个**零第三方依赖**、**全链路真异步**、**支持大数据批量操作**的纯托管 MySQL 客户端。

## 设计目标

- **轻量极简**：只做 ADO.NET 标准的增删改查、事务、参数化查询、存储过程和批量操作，不追求大而全
- **高性能**：零额外内存分配（`ArrayPool` / `OwnerPacket`）、真异步 IO、管道化批量执行
- **纯国产**：核心代码完全自主可控，无 GPL 许可风险，适合国产化替代场景
- **大数据友好**：内置多种批量操作方案（字典参数集 / 数组绑定 / 管道化 / 多行 VALUES / DbBatch），覆盖万级到百万级行操作
- **广泛兼容**：支持 `net45` ~ `net10` 全版本，MySQL 5.x ~ 9.0+ 全系列

## 核心功能

| 功能 | 说明 |
|------|------|
| 连接管理 | 支持 MySQL 5.x / 8.0 / 9.0+，`mysql_native_password` 和 `caching_sha2_password` 双认证 |
| 查询执行 | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`，完整 ADO.NET 接口 |
| 参数化查询 | 客户端参数替换，`@参数名` 语法，自动转义防注入 |
| 预编译语句 | `Prepare()` 走 `COM_STMT_PREPARE` 二进制协议，可选 `UseServerPrepare` 全局开关 |
| 批量操作 | `ExecuteBatch`（字典参数集）/ `ExecuteArrayBatch`（Oracle 数组绑定）/ 管道化 / 多行 VALUES / DbBatch |
| 管道化执行 | `Pipeline=true` 批量发送后批量接收，网络延迟仅一次，万级行性能提升 3~10× |
| 事务支持 | `BeginTransaction` / `Commit` / `Rollback`，四种隔离级别，自动回滚 |
| 存储过程 | `CommandType.StoredProcedure`，支持 IN/OUT 参数 |
| SSL/TLS | `SslMode=Preferred/Required`，支持 TLS 1.2/1.3 |
| 连接池 | 默认开启，按连接字符串分池，自动健康检查与回收 |
| 异步方法 | 全链路 `async/await`，`OpenAsync` / `ExecuteReaderAsync` / `ReadAsync` 等 |
| Schema 查询 | Tables / Columns / Indexes / Databases 等元数据 |
| DbBatch | .NET 6+ ADO.NET 标准批量 API |
| XCode 集成 | 通过 `MySqlClientFactory` 自动注册，无缝对接 XCode ORM |

## 亮点特性

### 🚀 真管道化批量执行（Pipeline）

独创的管道化执行模式，基于 `COM_STMT_PREPARE + COM_STMT_EXECUTE` 二进制协议：

- **Phase 1**：批量构建并发送所有 EXECUTE 包到网络缓冲区，仅最后一个包 Flush
- **Phase 2**：按顺序批量读取所有 OK/Error 响应，累加影响行数
- **原理**：TCP 协议栈合并小包为大包（Nagle 算法），网络往返仅一次

| 场景 | 串行模式 | 管道化模式 | 提升 |
|------|---------|-----------|------|
| 1000 行 INSERT（内网） | ~1s | ~0.3s | **3×** |
| 10000 行 INSERT（跨机房） | ~30s | ~3s | **10×** |

### 🔧 多种批量操作方案

| 方案 | API | 适用场景 |
|------|-----|---------|
| 字典参数集 | `ExecuteBatch` | 动态参数，参数来自集合 |
| 数组绑定 | `ExecuteArrayBatch` | 大批量 DML，Oracle 风格 |
| 管道化 | `Pipeline=true` | 万级/十万级行，跨机房 |
| 多行 VALUES | 拼接 SQL | 纯 INSERT 批量 |
| DbBatch (.NET 6+) | `CreateBatch` | 不同 SQL 混合批量 |

### 🛡️ 零第三方依赖

- 仅依赖 `NewLife.Core`（同为 NewLife 团队出品），无任何外部第三方库
- 无 GPL 许可风险，MIT 协议，商用无忧
- 适合国产化替代、信创环境、安全审计严格的场景

### ⚡ 极致性能优化

- **零额外分配**：使用 `ArrayPool<T>` 和 `OwnerPacket` 管理内存，减少 GC 压力
- **对象池复用**：`Pool.StringBuilder` / `Pool.MemoryStream` / `Pool.Shared` 全面池化
- **真异步 IO**：全链路 `async/await`，无 `sync-over-async` 反模式
- **精简协议解析**：只解析必要字段，跳过冗余数据，最小化 CPU 开销

### 🌐 广泛兼容性

- **目标框架**：`net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net10.0`
- **MySQL 版本**：5.5 ~ 9.0+，支持 `mysql_native_password` 和 `caching_sha2_password`
- **条件编译**：针对不同框架自动选择最优实现（如 TLS 1.3 仅 .NET 5.0+）

## 与主流 MySQL 驱动对比

### 功能对比

| 功能 | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:------------:|:--------------:|:-------------------:|
| **许可协议** | **MIT** | MIT | GPL (商用需付费) |
| **国产自主** | ✅ 完全自主 | ❌ | ❌ |
| **第三方依赖** | **无**（仅 NewLife.Core） | 无 | 无 |
| **纯托管实现** | ✅ | ✅ | ✅ |
| **真异步 IO** | ✅ | ✅ | ❌ (sync-over-async) |
| **连接池** | ✅ 自动分池 | ✅ | ✅ |
| **参数化查询** | ✅ 客户端替换 | ✅ 客户端/服务端 | ✅ 客户端替换 |
| **预编译语句** | ✅ Prepare/Execute | ✅ | ✅ |
| **存储过程** | ✅ IN/OUT 参数 | ✅ | ✅ |
| **事务** | ✅ 4种隔离级别 | ✅ | ✅ |
| **SSL/TLS** | ✅ TLS 1.2/1.3 | ✅ | ✅ |
| **mysql_native_password** | ✅ | ✅ | ✅ |
| **caching_sha2_password** | ✅ | ✅ | ✅ |
| **管道化批量执行** | ✅ **独创** | ❌ | ❌ |
| **数组绑定批量** | ✅ Oracle 风格 | ❌ | ❌ |
| **字典参数集批量** | ✅ | ❌ | ❌ |
| **DbBatch (.NET 6+)** | ✅ | ✅ | ❌ |
| **Schema 查询** | ✅ | ✅ | ✅ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **XCode ORM 集成** | ✅ 原生支持 | ❌ | ❌ |
| **多目标框架** | net45~net10 | netstandard2.0+ | net462+ |
| **MySQL 5.x~9.0** | ✅ | ✅ | ✅ |
| **MariaDB** | ✅ 基础 | ✅ | ❌ |
| sha256_password | ❌ | ✅ | ✅ |
| ed25519 (MariaDB) | ❌ | ✅ | ❌ |
| DbDataSource (.NET 7+) | ❌ | ✅ | ❌ |
| EF Core 支持 | ❌ (定位轻量) | ✅ (via Pomelo) | ✅ |
| 压缩协议 | ❌ | ✅ | ✅ |
| 负载均衡/故障转移 | ❌ | ✅ | ✅ |
| LOAD DATA LOCAL | ❌ | ✅ | ✅ |

### 核心优势总结

| 维度 | NewLife.MySql 优势 |
|------|-------------------|
| **批量性能** | 独创管道化执行 + 数组绑定 + 字典参数集，大数据场景性能领先 |
| **许可安全** | MIT 协议，无 GPL 风险，商用友好 |
| **国产自主** | 完全自主可控，适合信创/国产化替代 |
| **轻量部署** | 零第三方依赖，包体积小 |
| **框架兼容** | 支持 .NET Framework 4.5 到 .NET 10，覆盖最广 |
| **XCode 生态** | 原生集成 XCode ORM，开箱即用 |

## 安装

通过 NuGet 安装：

```
dotnet add package NewLife.MySql
```

或在项目文件中添加引用：

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

## 连接字符串

```
Server=localhost;Port=3306;Database=mydb;User Id=root;Password=pass;
```

支持的参数：

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
| UseServerPrepare | Use Server Prepare | false | 参数化查询是否走服务端预编译（COM_STMT_PREPARE/EXECUTE 二进制协议） |
| Pipeline | Pipelining | false | 批量操作是否启用真管道化执行（批量发送请求后批量接收响应，大幅减少网络延迟） |

## 快速开始

### 打开和关闭连接

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;Password=pass;");
conn.Open();

// 使用连接执行操作...
// using 结束时自动关闭并归还连接池
```

### 查询并读取结果集

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

### 查询单个值

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### 执行非查询（INSERT/UPDATE/DELETE）

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// 快捷方式
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

## 参数化查询

使用 `@参数名` 语法绑定参数，自动进行值转义，防止 SQL 注入：

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

参数化插入：

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age, created) VALUES(@name, @age, @dt)");
cmd.Parameters.AddWithValue("name", "Alice");
cmd.Parameters.AddWithValue("age", 30);
cmd.Parameters.AddWithValue("dt", DateTime.Now);
var rows = cmd.ExecuteNonQuery();
```

NULL 值传递：

```csharp
cmd.Parameters.AddWithValue("email", DBNull.Value);
```

支持的参数类型：

| .NET 类型 | SQL 字面量示例 |
|-----------|---------------|
| `String` | `'hello'`（自动转义特殊字符） |
| `Int32` / `Int64` 等数字 | `42` |
| `Boolean` | `1` 或 `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | 转为数字 |
| `null` / `DBNull.Value` | `NULL` |

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

支持的隔离级别：

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

- `ReadUncommitted`
- `ReadCommitted`
- `RepeatableRead`（MySQL 默认）
- `Serializable`

事务 `Dispose` 时如果未提交也未回滚，会自动执行回滚：

```csharp
using (var tr = conn.BeginTransaction())
{
    conn.ExecuteNonQuery("INSERT INTO ...");
    // 忘记 Commit/Rollback —— Dispose 时自动回滚
}
```

## 存储过程

通过 `CommandType.StoredProcedure` 调用存储过程，支持输入/输出参数：

```csharp
using var cmd = new MySqlCommand { Connection = conn, CommandType = CommandType.StoredProcedure };
cmd.CommandText = "my_proc";
cmd.Parameters.AddWithValue("p_id", 1);

var outParam = new MySqlParameter { ParameterName = "p_result", Direction = ParameterDirection.Output };
cmd.Parameters.Add(outParam);

cmd.ExecuteNonQuery();
var result = outParam.Value; // 输出参数值
```

## 预编译语句（Prepare）

通过 `Prepare()` 在服务端预编译 SQL，后续执行走二进制协议，参数无需客户端替换：

```csharp
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 25);

// 预编译（服务端解析 SQL，返回 statementId）
cmd.Prepare();

// 第一次执行
cmd.ExecuteNonQuery();

// 修改参数后再次执行（无需重新编译）
cmd.Parameters[0].Value = "Jerry";
cmd.Parameters[1].Value = 30;
cmd.ExecuteNonQuery();
```

也可以通过连接字符串全局启用服务端预编译，所有参数化查询自动走二进制协议：

```
Server=localhost;Database=mydb;User Id=root;Password=pass;UseServerPrepare=true;
```

## 批量操作

### 方案 A：字典参数集（Prepare + Execute × N）

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
// 或异步版本
var totalAffected2 = await cmd.ExecuteBatchAsync(paramSets);
```

### 方案 B：Oracle 风格数组绑定

参数值设为数组，指定执行次数，类似 Oracle 的 ArrayBindCount：

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// 或异步版本
var totalAffected2 = await cmd.ExecuteArrayBatchAsync(3);
```

### 方案 C：管道化执行

通过连接字符串开启 `Pipeline=true`，启用真管道化批量执行。底层会先批量发送所有 COM_STMT_EXECUTE 请求包到网络缓冲区，然后按顺序批量读取响应。网络往返延迟仅发生一次，适合大批量 DML 场景（万级/十万级行）：

```
Server=localhost;Database=mydb;User Id=root;Password=pass;Pipeline=true;
```

管道化对 INSERT、UPDATE、DELETE 均有效，搭配数组绑定或字典参数集使用：

```csharp
// 管道化 + 数组绑定，批量 UPDATE 一万行
var connStr = "Server=localhost;Database=mydb;User Id=root;Password=pass;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

**工作原理**：
- 默认模式（Pipeline=false）：逐条发送请求并等待响应，耗时 ≈ N × 网络延迟
- 管道化模式（Pipeline=true）：批量发送 → 一次 Flush → 批量读取，耗时 ≈ 1 × 网络延迟 + N × 服务器处理时间
- 网络延迟越高（如跨机房），管道化收益越大

### 方案 D：多行 INSERT VALUES

XCode 已支持的多行 INSERT 语法，直接通过文本协议发送：

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### DbBatch API（.NET 6+）

.NET 6 及以上版本支持 ADO.NET 标准的 `DbBatch` API：

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
// 遍历每个命令的结果...
```

## SSL/TLS 加密连接

在连接字符串中设置 `SslMode`：

```csharp
// 如果服务器支持则加密，不支持则明文
var connStr = "Server=localhost;Database=mydb;User Id=root;Password=pass;SslMode=Preferred;";

// 必须加密，服务器不支持则抛异常
var connStr2 = "Server=localhost;Database=mydb;User Id=root;Password=pass;SslMode=Required;";
```

| SslMode | 行为 |
|---------|------|
| `None` / `Disabled` | 不使用 SSL（默认） |
| `Preferred` | 服务器支持则使用，不支持则明文 |
| `Required` | 必须使用 SSL，否则抛异常 |

## Schema 信息查询

```csharp
// 获取数据库列表
var databases = conn.GetSchema("Databases");

// 获取表列表
var tables = conn.GetSchema("Tables");

// 获取列信息
var columns = conn.GetSchema("Columns");
```

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

## 连接池

连接池默认启用，无需额外配置。每个唯一的连接字符串对应一个独立的连接池。

- 打开连接时自动从池中获取空闲连接
- 关闭连接时自动归还到池中（不断开 TCP 连接）
- 无效连接（网络断开等）在获取时自动剔除
- 新连接按需创建

## 切换数据库

### 方式一：通过 ChangeDatabase（推荐）

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();

// 切换数据库（会关闭并重新打开连接）
conn.ChangeDatabase("db2");
```

**注意事项**：
- **不支持在事务中途切换**（事务会丢失）
- 有连接关闭/打开的开销
- 推荐为每个数据库创建独立连接对象

### 方式二：通过 SqlClient.SetDatabaseAsync（低级 API）

```csharp
using var conn = new MySqlConnection(connStr);
conn.Open();

// 使用 COM_INIT_DB 二进制命令切换数据库（不关闭连接）
await conn.Client.SetDatabaseAsync("information_schema");
```

**特点**：
- 使用 MySQL COM_INIT_DB 协议命令，等效于 `USE database`
- 不会关闭连接，性能略优
- 仅切换服务器端当前数据库，不会更新连接字符串
- 适合临时查询场景，不适合连接池复用场景

## 与 XCode ORM 集成

NewLife.MySql 通过 `MySqlClientFactory` 自动注册为 XCode 的 MySQL 数据库驱动：

```csharp
using XCode.DataAccessLayer;

// 注册连接字符串
DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;Password=pass;", null, "MySql");

// 使用 DAL
var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

## 多目标框架

支持 `net45`、`net461`、`netstandard2.0`、`netstandard2.1`、`net6.0`、`net10.0`。

| 框架 | 说明 |
|------|------|
| `net45` / `net461` | .NET Framework，兼容老项目 |
| `netstandard2.0` / `netstandard2.1` | 跨平台兼容，支持异步 Dispose |
| `net6.0` | 额外支持 `DbBatch` API |
| `net10.0` | 最新 .NET，享受最优性能 |

## 注意事项

1. **MySQL 版本**：支持 MySQL 5.x 及以上，推荐 MySQL 8.0+
2. **字符编码**：默认使用 UTF-8 编码
3. **ChangeDatabase 使用约束**：
   - `ChangeDatabase` 通过关闭/重新打开连接实现数据库切换
   - **不支持在事务中途切换数据库**（事务会丢失）
   - 不建议频繁调用（有连接关闭/打开开销）
   - 推荐为每个数据库创建独立的连接对象

## 相关文档

- [架构设计](Doc/架构设计.md) — 核心功能的架构设计与协议实现细节
- [性能测试报告](Doc/性能测试报告.md) — 批量操作基准性能测试报告



