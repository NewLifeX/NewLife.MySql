# NewLife.MySql

轻量级高性能 MySql 驱动，基于 ADO.NET 标准接口实现，直接通过 TCP 通信实现 MySQL 协议层，无需依赖官方驱动或第三方库。

## 核心功能

- 连接数据库（支持 MySQL 5.x / 8.0 / 9.0+，支持 `mysql_native_password` 和 `caching_sha2_password` 认证）
- 执行 SQL 查询并返回数据集（`ExecuteReader` / `ExecuteScalar`）
- 执行增删改 SQL 并返回影响行数（`ExecuteNonQuery`）
- 参数化查询（客户端参数替换，支持 `@参数名` 语法，自动转义防注入）
- 事务支持（`BeginTransaction` / `Commit` / `Rollback`，支持多种隔离级别）
- 存储过程调用（`CommandType.StoredProcedure`，支持输入/输出参数）
- 批量操作（`ExecuteBatch` / `ExecuteBatchAsync`，多语句一次发送）
- SSL/TLS 加密连接（`SslMode=Preferred` 或 `Required`）
- 连接池（默认开启，按连接字符串分池管理）
- ADO.NET 异步方法（`OpenAsync` / `ExecuteReaderAsync` / `ReadAsync` 等）
- Schema 信息查询（Tables / Columns / Indexes 等）

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

## 批量操作

通过 `ExecuteBatch` / `ExecuteBatchAsync` 批量执行多条 SQL，利用多语句能力一次发送，减少网络往返：

```csharp
var sqls = new[] {
    "INSERT INTO users(name, age) VALUES('Alice', 25)",
    "INSERT INTO users(name, age) VALUES('Bob', 30)",
    "UPDATE users SET age = age + 1 WHERE name = 'Alice'"
};
var totalAffected = conn.ExecuteBatch(sqls);

// 异步版本
var totalAffected2 = await conn.ExecuteBatchAsync(sqls);
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

支持 `net45`、`net461`、`netstandard2.0`、`netstandard2.1`。

## 注意事项

1. **MySQL 版本**：支持 MySQL 5.x 及以上，推荐 MySQL 8.0+
2. **字符编码**：默认使用 UTF-8 编码

## 相关文档

- [架构设计](Doc/架构设计.md)



