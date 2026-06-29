# 从 MySqlConnector 迁移到 NewLife.MySql

> MySqlConnector 是社区主流的高性能 MySQL ADO.NET 驱动，NewLife.MySql 在批量操作和国产化方面具有显著优势。

## 为什么要迁移？

| 对比维度 | MySqlConnector | NewLife.MySql |
|---------|:---:|:---:|
| 批量 DML 性能 | 基线 | **2~3× 更快** 🚀 |
| 管道化批量执行 | ❌ | ✅ **独创** |
| 数组绑定批量 | ❌ | ✅ |
| 字典参数集批量 | ❌ | ✅ |
| 批量方案数量 | 1 种（DbBatch） | **5 种**（覆盖百级到百万级行） |
| OceanBase / TiDB | ❌ | ✅ 自动检测 |
| 国产自主可控 | ❌ | ✅ |
| Binlog 解析 | ❌ | ✅ |
| 框架下限 | net462 | **net45** |
| 代码体积 | ~30,000 行 | **~3,000 行** |

## 迁移步骤

### 1. 替换 NuGet 包

```shell
# 移除旧包
dotnet remove package MySqlConnector

# 安装新包
dotnet add package NewLife.MySql
```

### 2. 更新 using 语句

```csharp
// 旧
using MySqlConnector;

// 新
using NewLife.MySql;
```

### 3. 类型映射

NewLife.MySql 使用与 MySqlConnector 相同的 `MySql*` 命名约定，大部分类型直接替换：

| MySqlConnector | NewLife.MySql | 兼容性 |
|---------------|:---:|:---:|
| `MySqlConnection` | `MySqlConnection` | ✅ 同名 |
| `MySqlCommand` | `MySqlCommand` | ✅ 同名 |
| `MySqlDataReader` | `MySqlDataReader` | ✅ 同名 |
| `MySqlParameter` | `MySqlParameter` | ✅ 同名 |
| `MySqlTransaction` | `MySqlTransaction` | ✅ 同名 |
| `MySqlBatch` | `MySqlBatch` | ✅ 同名，.NET 6+ DbBatch API |
| `MySqlException` | `MySqlException` | ✅ 同名 |
| `MySqlConnectionStringBuilder` | `MySqlConnectionStringBuilder` | ✅ 同名 |
| `MySqlDataAdapter` | `MySqlDataAdapter` | ✅ NewLife.MySql 支持，MySqlConnector 不支持 |
| `MySqlBulkCopy` | ❌ | 用管道化批量或 DbBatch 替代 |
| `MySqlDataSource` | ❌ | 用 XCode DAL 或原生连接池替代 |
| `MySqlGeometry` | `MySqlGeometry` | ✅ 均支持 WKB 格式 |

### 4. 连接字符串

连接字符串参数基本兼容：

```
Server=localhost;Port=3306;Database=mydb;User Id=root;Password=pass;
```

额外支持 `Pipeline=true` 开启管道化批量执行（NewLife.MySql 独有）。

### 5. 代码示例

#### 基本查询

```csharp
// MySqlConnector
using (var conn = new MySqlConnector.MySqlConnection(connStr))
{
    await conn.OpenAsync();
    using (var cmd = new MySqlConnector.MySqlCommand("SELECT * FROM users", conn))
    using (var reader = await cmd.ExecuteReaderAsync())
    {
        while (await reader.ReadAsync())
        {
            Console.WriteLine(reader.GetString(0));
        }
    }
}

// NewLife.MySql — 完全相同的异步 API
using var conn = new MySqlConnection(connStr);
await conn.OpenAsync();
using var cmd = new MySqlCommand(conn, "SELECT * FROM users");
using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(0));
}
```

> **注意**：NewLife.MySql 的 `MySqlConnection` 继承 `DbConnection`，支持 `using var` 简化写法。

#### 批量操作（NewLife.MySql 优势）

```csharp
// MySqlConnector：只能用 DbBatch
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlConnector.MySqlBatchCommand("INSERT INTO users VALUES(@name, @age)")
{
    Parameters = { new("name", "Alice"), new("age", 25) }
});
batch.BatchCommands.Add(new MySqlConnector.MySqlBatchCommand("INSERT INTO users VALUES(@name, @age)")
{
    Parameters = { new("name", "Bob"), new("age", 30) }
});
batch.ExecuteNonQuery();

// NewLife.MySql：管道化批量，更简洁且 2~3× 更快
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });
var total = cmd.ExecuteArrayBatch(3);
```

### 注意事项

#### 移除/替代项

| MySqlConnector 特性 | NewLife.MySql 替代方案 |
|-------------------|---------------------|
| `MySqlBulkCopy` | 管道化批量 / 多行 VALUES 拼接 |
| `MySqlDataSource` (.NET 7+) | XCode DAL 或原生连接池 |
| 压缩协议 | 暂不支持（内网部署为主） |
| Unix Socket | 暂不支持 |
| `MySqlDecimal` | `Decimal` |
| `MySqlDateTime` | `DateTime` |

#### 参数化查询区别

| 特性 | MySqlConnector | NewLife.MySql |
|------|:---:|:---:|
| 参数替换方式 | 服务端（COM_STMT_EXECUTE） | 客户端替换（默认），支持 `UseServerPrepare=true` |
| 参数前缀 | `@` | `@` / `?` |

两种方式在功能和安全性上等价——客户端替换自动转义防注入，服务端预编译可选开启。

---

## 性能提升

迁移后，批量操作场景可获得 **2~3× 性能提升**：

| 操作 (10,000 行) | MySqlConnector | NewLife.MySql | 加速比 |
|-----------|------:|------:|------:|
| INSERT | 1,906ms | 899ms | **2.1×** |
| UPDATE | 2,041ms | 710ms | **2.9×** |
| DELETE | 1,767ms | 661ms | **2.7×** |

详见 [性能测试报告](性能测试报告.md)。
