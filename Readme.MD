# NewLife.MySql

#### 项目介绍
轻量级高性能MySql驱动，基于ADO.NET标准接口实现，零第三方依赖。

#### 软件架构
基于Tcp通信的MySql驱动，直接实现MySql协议层，无需依赖官方MySql驱动。

**核心功能：**
- 连接数据库（支持MySql 5.x/8.0+，支持 `mysql_native_password` 和 `caching_sha2_password` 认证）
- 执行SQL查询并返回数据集（`ExecuteReader`/`ExecuteScalar`）
- 执行添删改SQL并返回影响行数（`ExecuteNonQuery`）
- 参数化执行SQL（客户端参数替换，支持 `@参数名` 语法）
- 事务支持（`BeginTransaction`/`Commit`/`Rollback`，支持多种隔离级别）
- SSL/TLS加密连接
- 连接池（默认开启，按连接字符串分池管理）
- ADO.NET异步方法（`OpenAsync`/`ExecuteReaderAsync`/`ReadAsync` 等）
- Schema信息查询（Tables/Columns/Indexes等）

**计划中功能：**
- 批量添删改数据
- 存储过程调用
- 负载均衡

#### 快速开始

```csharp
// 基本查询
using var conn = new MySqlConnection("Server=localhost;Database=test;User Id=root;Password=pass;");
conn.Open();

using var cmd = new MySqlCommand(conn, "select * from user where id=@id");
cmd.Parameters.AddWithValue("id", 1);
using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    Console.WriteLine(reader.GetString(0));
}
```

```csharp
// 事务操作
using var conn = new MySqlConnection("Server=localhost;Database=test;User Id=root;Password=pass;");
conn.Open();

using var tr = conn.BeginTransaction();
conn.ExecuteNonQuery("insert into log(msg) values('hello')");
tr.Commit();
```

```csharp
// SSL连接
var connStr = "Server=localhost;Database=test;User Id=root;Password=pass;SslMode=Required;";
using var conn = new MySqlConnection(connStr);
conn.Open();
```

#### 多目标框架
支持 `net45`、`net461`、`netstandard2.0`、`netstandard2.1`。

#### 相关文档
- [架构设计](Doc/架构设计.md)
- [使用指南](Doc/使用指南.md)


