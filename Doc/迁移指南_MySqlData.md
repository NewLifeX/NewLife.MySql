# 从 MySql.Data 迁移到 NewLife.MySql

> MySql.Data 是 Oracle 官方的 MySQL ADO.NET 驱动，NewLife.MySql 是纯国产高性能替代。

## 为什么要迁移？

| 对比维度 | MySql.Data | NewLife.MySql |
|---------|-----------|:---:|
| 许可证 | GPLv2（商用需付费）⚠️ | **MIT 免费商用** ✅ |
| 异步实现 | sync-over-async 伪异步 ❌ | **全链路真异步** ✅ |
| 批量 DML 性能 | 基线 | **2~3× 更快** 🚀 |
| 第三方依赖 | 6 个 ⚠️ | **0 个** ✅ |
| 国产自主可控 | ❌ | ✅ |
| 代码体积 | ~50,000 行 | **~3,000 行** |
| 管道化批量执行 | ❌ | ✅ **独创** |
| 数组绑定批量 | ❌ | ✅ |
| 字典参数集批量 | ❌ | ✅ |

## 迁移步骤

### 1. 替换 NuGet 包

```shell
# 移除旧包
dotnet remove package MySql.Data

# 安装新包
dotnet add package NewLife.MySql
```

### 2. 更新 using 语句

```csharp
// 旧
using MySql.Data.MySqlClient;

// 新
using NewLife.MySql;
```

### 3. 类型映射（几乎无需改动）

NewLife.MySql 严格遵循 ADO.NET 标准接口，类名遵循 `MySql*` 命名约定，与 MySql.Data 高度一致：

| MySql.Data | NewLife.MySql | 兼容性 |
|-----------|:---:|:---:|
| `MySqlConnection` | `MySqlConnection` | ✅ 同名，直接替换 |
| `MySqlCommand` | `MySqlCommand` | ✅ 同名，直接替换 |
| `MySqlDataReader` | `MySqlDataReader` | ✅ 同名，直接替换 |
| `MySqlParameter` | `MySqlParameter` | ✅ 同名，直接替换 |
| `MySqlTransaction` | `MySqlTransaction` | ✅ 同名，直接替换 |
| `MySqlDataAdapter` | `MySqlDataAdapter` | ✅ 同名，直接替换 |
| `MySqlConnectionStringBuilder` | `MySqlConnectionStringBuilder` | ✅ 同名，直接替换 |
| `MySqlException` | `MySqlException` | ✅ 同名，直接替换 |
| `MySqlClientFactory` | `MySqlClientFactory` | ✅ 同名，直接替换 |

### 4. 连接字符串

连接字符串参数名称兼容 MySql.Data，常用参数无需修改：

```
Server=localhost;Port=3306;Database=mydb;User Id=root;Password=pass;
```

支持的全部参数别名参见 [快速开始](Readme.MD#连接字符串)。

### 5. 代码示例

#### 基本查询

```csharp
// MySql.Data
using (var conn = new MySql.Data.MySqlClient.MySqlConnection(connStr))
{
    conn.Open();
    using (var cmd = new MySql.Data.MySqlClient.MySqlCommand("SELECT * FROM users", conn))
    using (var reader = cmd.ExecuteReader())
    {
        while (reader.Read())
        {
            Console.WriteLine(reader.GetString(0));
        }
    }
}

// NewLife.MySql — 完全相同的 API
using var conn = new MySqlConnection(connStr);
conn.Open();
using var cmd = new MySqlCommand(conn, "SELECT * FROM users");
using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    Console.WriteLine(reader.GetString(0));
}
```

#### 参数化查询

```csharp
// MySql.Data
cmd.Parameters.AddWithValue("@name", "Tom");
cmd.Parameters.AddWithValue("@age", 25);

// NewLife.MySql — 完全相同
cmd.Parameters.AddWithValue("name", "Tom");   // 带不带 @ 前缀均可
cmd.Parameters.AddWithValue("age", 25);
```

#### 事务

```csharp
// MySql.Data
using (var tr = conn.BeginTransaction())
{
    // ... DML 操作
    tr.Commit();
}

// NewLife.MySql — 完全相同
using var tr = conn.BeginTransaction();
// ... DML 操作
tr.Commit();
```

#### 批量操作（NewLife.MySql 独有）

```csharp
// MySql.Data：只能逐行插入
foreach (var row in data)
{
    cmd.Parameters["@name"].Value = row.Name;
    cmd.Parameters["@age"].Value = row.Age;
    cmd.ExecuteNonQuery();  // 每行一次网络往返
}

// NewLife.MySql：管道化批量执行，2~3× 更快
cmd.Parameters.AddWithValue("name", namesArray);    // String[10000]
cmd.Parameters.AddWithValue("age", agesArray);      // Int32[10000]
var total = cmd.ExecuteArrayBatch(10000);           // 一次网络往返！
```

## 注意事项

### 移除项

- `MySqlHelper` 类：NewLife.MySql 未提供此辅助类
- `MySqlBulkLoader`：NewLife.MySql 使用管道化批量或 DbBatch 替代
- `MySqlCommandBuilder`：NewLife.MySql 未内置，XCode ORM 可自动生成 CRUD

### 行为差异

| 场景 | MySql.Data | NewLife.MySql | 说明 |
|------|-----------|:---:|------|
| 参数前缀 | 允许 `@` / `?` | 允许 `@` / `?` | ✅ 兼容 |
| 参数替换 | 服务端 | 客户端替换（默认），支持 `UseServerPrepare=true` 服务端预编译 | 功能等价 |
| `ChangeDatabase` | 服务端切换 | Close + Reopen | 不支持事务中切换 |
| 连接池 | 默认最大 100 | 无上限，按需创建 | 可放心使用 |

---

## 性能提升

迁移后，批量操作场景可获得 **2~3× 性能提升**，详见 [性能测试报告](性能测试报告.md)。
