# DataReader 架构设计说明

本文档详细说明 `MySqlDataReader` 的读取架构、状态管理和各个方法的语义。

## 核心概念

### 1. 结果集类型

MySQL 多语句执行会返回多个结果，每个结果可以是：

| 结果类型 | FieldCount | 说明 | 示例 |
|---------|------------|------|------|
| **OK 包** | 0 | DML 语句的执行结果 | INSERT, UPDATE, DELETE |
| **结果集** | > 0 | 查询语句的数据结果 | SELECT |
| **错误包** | - | 执行失败 | 语法错误、权限错误 |

### 2. Reader 状态转换

```
[初始状态]
    ↓ ExecuteReaderAsync
[定位在第一个结果]
    ↓ Read() / NextResult()
[读取数据或移动到下一结果]
    ↓ Close() / Dispose()
[关闭状态]
```

## 方法语义详解

### ExecuteReaderAsync

**用途**：执行 SQL 命令并返回一个可遍历结果的 DataReader

**返回时的状态**：
- ? Reader 已经定位在**第一个结果集**上
- ? 可以立即调用 `Read()` 读取数据
- ? 可以检查 `FieldCount` 判断结果类型
- ? 可以调用 `NextResult()` 移动到下一个结果

**示例**：
```csharp
// 单条 SELECT 语句
using var reader = await cmd.ExecuteReaderAsync();
// reader 已定位在 SELECT 结果集上，FieldCount > 0
while (await reader.ReadAsync())
{
    var value = reader.GetString(0);
}

// 多语句：INSERT + SELECT
cmd.CommandText = "INSERT INTO ...; SELECT * FROM ...;";
using var reader = await cmd.ExecuteReaderAsync();
// reader 定位在第一个结果（INSERT 的 OK 包），FieldCount = 0

// 移动到 SELECT 结果集
if (await reader.NextResultAsync())
{
    // 现在 FieldCount > 0，可以读取数据
    while (await reader.ReadAsync())
    {
        // ...
    }
}
```

**何时可以调用**：
- ? 每次需要执行新的 SQL 命令时
- ? 上一个 reader 已经 Dispose 后
- ? 不应该重复调用而不关闭之前的 reader（会导致资源泄露）

### NextResultAsync

**用途**：移动到下一个结果集

**返回值语义**：
- **true**：成功移动到下一个结果（可能是 OK 包或结果集）
- **false**：没有更多结果

**行为规范**（符合 ADO.NET 标准）：
1. 每次调用只移动**一个**结果
2. 不会跳过任何结果（包括 OK 包）
3. 可以多次调用 `NextResult()`，返回 false 后继续调用仍返回 false
4. 自动消费当前结果集的剩余行（如果有）

**示例**：
```csharp
// 多语句：DELETE; INSERT; INSERT; SELECT
cmd.CommandText = @"
    DELETE FROM users WHERE id = 1;
    INSERT INTO users VALUES (2, 'Alice');
    INSERT INTO users VALUES (3, 'Bob');
    SELECT * FROM users;
";

using var reader = await cmd.ExecuteReaderAsync();

// 第一个结果：DELETE 的 OK 包
Assert.Equal(0, reader.FieldCount);
Assert.Equal(1, reader.RecordsAffected);  // 删除了 1 行

// 第二个结果：INSERT 的 OK 包
Assert.True(await reader.NextResultAsync());
Assert.Equal(0, reader.FieldCount);
Assert.Equal(2, reader.RecordsAffected);  // 累加：1 + 1 = 2

// 第三个结果：INSERT 的 OK 包
Assert.True(await reader.NextResultAsync());
Assert.Equal(0, reader.FieldCount);
Assert.Equal(3, reader.RecordsAffected);  // 累加：2 + 1 = 3

// 第四个结果：SELECT 结果集
Assert.True(await reader.NextResultAsync());
Assert.True(reader.FieldCount > 0);
Assert.Equal(3, reader.RecordsAffected);  // SELECT 不计入 RecordsAffected

// 没有更多结果
Assert.False(await reader.NextResultAsync());
```

### ReadAsync

**用途**：读取当前结果集的下一行数据

**返回值语义**：
- **true**：成功读取到下一行，可以调用 `GetValue()` 等方法
- **false**：当前结果集没有更多行

**行为规范**：
- 只在当前结果集内移动
- 不会自动移动到下一个结果集
- 对 OK 包（FieldCount = 0）调用会立即返回 false

**示例**：
```csharp
using var reader = await cmd.ExecuteReaderAsync();

if (reader.FieldCount > 0)
{
    // 第一行
    if (await reader.ReadAsync())
    {
        var id = reader.GetInt32(0);
        var name = reader.GetString(1);
    }
    
    // 第二行
    if (await reader.ReadAsync())
    {
        // ...
    }
    
    // 读取所有行
    while (await reader.ReadAsync())
    {
        // ...
    }
}
```

## 高层方法实现

### ExecuteNonQueryAsync

**目的**：执行所有语句并返回累加的影响行数

**实现策略**：
```csharp
public override async Task<Int32> ExecuteNonQueryAsync(CancellationToken ct)
{
    using var reader = await ExecuteReaderAsync(ct);
    
    // 消费所有结果集，RecordsAffected 会自动累加
    var dr = reader as MySqlDataReader;
    while (dr != null && dr.HasMoreResults && await reader.NextResultAsync(ct))
    {
        // 只是消费结果集，RecordsAffected 已在 NextResultAsync 中累加
    }
    
    return reader.RecordsAffected;
}
```

**关键点**：
- ? `RecordsAffected` 在 `NextResultAsync` 中自动累加
- ? 只累加 DML（INSERT/UPDATE/DELETE）的影响行数
- ? SELECT 不计入 RecordsAffected

### ExecuteScalarAsync

**目的**：返回第一个有数据的结果集的第一行第一列

**实现策略**：
```csharp
public override async Task<Object?> ExecuteScalarAsync(CancellationToken ct)
{
    using var reader = await ExecuteReaderAsync(ct);
    
    // 循环查找第一个有数据的结果集
    do
    {
        // 如果当前结果集有列（非 OK 包）
        if (reader.FieldCount > 0)
        {
            // 尝试读取第一行第一列
            if (await reader.ReadAsync(ct))
                return reader.GetValue(0);
        }
        // 当前结果集没有列或没有数据，尝试下一个
    } while (await reader.NextResultAsync(ct));
    
    return null;
}
```

**行为**：
- ? 自动跳过 OK 包（FieldCount = 0）
- ? 跳过空结果集（有列但没有行）
- ? 返回第一个非空结果集的第一行第一列
- ? 如果所有结果集都是空的，返回 null

**示例**：
```csharp
// 示例1：INSERT + SELECT
cmd.CommandText = "INSERT INTO ...; SELECT value FROM ...;";
var result = await cmd.ExecuteScalarAsync();
// 跳过 INSERT 的 OK 包，返回 SELECT 的第一个 value

// 示例2：DELETE + INSERT + SELECT
cmd.CommandText = "DELETE ...; INSERT ...; SELECT COUNT(*) FROM ...;";
var result = await cmd.ExecuteScalarAsync();
// 跳过两个 OK 包，返回 COUNT(*) 的结果

// 示例3：只有 DML
cmd.CommandText = "INSERT ...; UPDATE ...; DELETE ...;";
var result = await cmd.ExecuteScalarAsync();
// 所有都是 OK 包，返回 null
```

## 状态管理

### Reader 内部状态

```csharp
public class MySqlDataReader
{
    private Int32 _FieldCount;           // 当前结果集的列数
    private Int32 _RecordsAffected;      // 累加的影响行数
    private Boolean _hasMoreResults;     // 是否还有更多结果
    private Boolean _allRowsConsumed;    // 当前结果集的行是否已消费完
    private MySqlColumn[]? _Columns;     // 当前结果集的列信息
    private Object[]? _Values;           // 当前行的数据
}
```

### 状态转换图

```
ExecuteReaderAsync 返回时：
┌────────────────────────────────┐
│ Reader 定位在第一个结果         │
│ _FieldCount = 0 或 > 0         │
│ _hasMoreResults = true/false   │
│ _allRowsConsumed = true        │
└────────────────────────────────┘
            ↓
    调用 ReadAsync()
            ↓
┌────────────────────────────────┐
│ 如果 FieldCount > 0             │
│   读取一行数据                  │
│   _allRowsConsumed = false     │
└────────────────────────────────┘
            ↓
    调用 NextResultAsync()
            ↓
┌────────────────────────────────┐
│ 消费当前结果集的剩余行          │
│ 读取下一个结果                  │
│ 更新 _FieldCount                │
│ 累加 _RecordsAffected           │
│ 更新 _hasMoreResults            │
└────────────────────────────────┘
```

## 最佳实践

### 1. 单个结果集

```csharp
using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    var id = reader.GetInt32(0);
    var name = reader.GetString(1);
}
```

### 2. 多个结果集

```csharp
using var reader = await cmd.ExecuteReaderAsync();
do
{
    if (reader.FieldCount > 0)
    {
        while (await reader.ReadAsync())
        {
            // 处理当前结果集的数据
        }
    }
} while (await reader.NextResultAsync());
```

### 3. 检查结果类型

```csharp
using var reader = await cmd.ExecuteReaderAsync();
do
{
    if (reader.FieldCount == 0)
    {
        // OK 包（DML 结果）
        Console.WriteLine($"Affected rows: {reader.RecordsAffected}");
    }
    else
    {
        // 结果集（SELECT 结果）
        Console.WriteLine($"Columns: {reader.FieldCount}");
        while (await reader.ReadAsync())
        {
            // 处理数据
        }
    }
} while (await reader.NextResultAsync());
```

### 4. 部分读取（提前关闭）

```csharp
using var reader = await cmd.ExecuteReaderAsync();
// 只读第一行
if (await reader.ReadAsync())
{
    var value = reader.GetValue(0);
}
// reader.Dispose() 会自动清理残余数据
```

## 性能考虑

### 1. 自动消费剩余数据

`NextResultAsync` 会自动消费当前结果集的剩余行，这确保了：
- ? 网络流状态正确
- ? 可以正确读取下一个结果集
- ? 如果当前结果集很大但你只需要前几行，性能会受影响

**解决方案**：如果只需要部分数据，直接 Dispose reader，不要调用 NextResult()

### 2. RecordsAffected 累加

`RecordsAffected` 在每次 `NextResultAsync` 时自动累加：
```csharp
_RecordsAffected += qr.AffectedRows;
```

这意味着：
- ? `ExecuteNonQueryAsync` 不需要手动累加
- ? 随时可以查看当前累加的影响行数
- ? 无法单独获取每个语句的影响行数

**解决方案**：如需单独统计，需要在每次 NextResult() 前后记录差值

## 与标准 ADO.NET 的对比

| 特性 | MySqlDataReader | SqlDataReader (SQL Server) |
|------|----------------|---------------------------|
| ExecuteReader 返回时状态 | 定位在第一个结果 | 定位在第一个结果 |
| NextResult 返回值 | true/false | true/false |
| NextResult 自动消费剩余行 | ? | ? |
| RecordsAffected 自动累加 | ? | ? |
| OK 包的 FieldCount | 0 | -1 或 0 |
| 部分读取安全性 | ? 自动清理 | ? 自动清理 |

## 总结

**ExecuteReaderAsync 的返回值**表示：
- ? 一个已经定位在第一个结果集上的 Reader
- ? 可以立即使用，不需要先调用 NextResult()
- ? 需要检查 FieldCount 判断当前结果类型

**NextResultAsync 的返回值**表示：
- **true** = 成功移动到下一个结果
- **false** = 没有更多结果

**何时可以持续调用**：
- ? 只要 NextResult() 返回 true，就可以继续调用
- ? 返回 false 后仍可调用（会继续返回 false）
- ? 直到 reader 被 Dispose

**架构优势**：
1. **一致性**：与标准 ADO.NET 行为一致
2. **简洁性**：高层方法（ExecuteNonQuery/ExecuteScalar）实现简单
3. **灵活性**：支持部分读取、多结果集遍历
4. **安全性**：自动资源清理，防止网络流污染
