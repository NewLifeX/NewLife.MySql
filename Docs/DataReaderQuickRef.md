# DataReader 架构快速参考

## 核心问题回答

### 1?? ExecuteReaderAsync 的返回值表示什么？

**答案**：返回一个**已经定位在第一个结果集上**的 DataReader

```csharp
// ExecuteReaderAsync 的内部流程
protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(...)
{
    var reader = new MySqlDataReader { Command = this };
    await ExecuteAsync(cancellationToken);              // 发送SQL命令
    await reader.NextResultAsync(cancellationToken);     // ★ 读取第一个结果
    return reader;                                       // 返回已定位的reader
}

// 使用时
using var reader = await cmd.ExecuteReaderAsync();
// reader 已经定位在第一个结果上，可以立即：
// 1. 调用 Read() 读取数据
// 2. 检查 FieldCount 判断结果类型（0 = OK包, >0 = 结果集）
// 3. 调用 NextResult() 移动到下一个结果
```

### 2?? NextResultAsync 的返回值应该怎么定义？

**答案**：
- **返回 `true`** = 成功移动到下一个结果（可能是 OK 包或结果集）
- **返回 `false`** = 没有更多结果了

**关键特性**：
- ? 每次只移动**一个**结果
- ? 不跳过任何结果（包括 OK 包）
- ? 自动消费当前结果集的剩余行
- ? 自动累加 `RecordsAffected`

```csharp
// 多语句示例：INSERT; SELECT; UPDATE;
using var reader = await cmd.ExecuteReaderAsync();

// 第一个结果：INSERT 的 OK 包
reader.FieldCount;        // = 0
reader.RecordsAffected;   // = 1

// 移动到第二个结果
await reader.NextResultAsync();  // 返回 true

// 第二个结果：SELECT 结果集
reader.FieldCount;        // > 0
while (await reader.ReadAsync()) { ... }

// 移动到第三个结果
await reader.NextResultAsync();  // 返回 true

// 第三个结果：UPDATE 的 OK 包
reader.FieldCount;        // = 0
reader.RecordsAffected;   // = 2 (累加了 UPDATE 的影响行数)

// 没有更多结果
await reader.NextResultAsync();  // 返回 false
```

### 3?? 什么时候可以持续调用 NextResultAsync？

**答案**：只要 `NextResultAsync()` 返回 `true`，就可以继续调用

**遍历所有结果集的标准模式**：
```csharp
using var reader = await cmd.ExecuteReaderAsync();
do
{
    // 处理当前结果集
    if (reader.FieldCount > 0)
    {
        // 有列的结果集（SELECT）
        while (await reader.ReadAsync())
        {
            var value = reader.GetValue(0);
        }
    }
    else
    {
        // OK 包（INSERT/UPDATE/DELETE）
        Console.WriteLine($"Affected: {reader.RecordsAffected}");
    }
} while (await reader.NextResultAsync());
```

**边界情况**：
- ? 返回 `false` 后可以继续调用（会继续返回 `false`）
- ? Reader 关闭/Dispose 后不应该再调用

## 方法调用链关系

```
用户调用
    │
    ├─? ExecuteNonQueryAsync()
    │       └─? ExecuteReaderAsync()
    │               └─? NextResultAsync() [循环消费所有结果]
    │                       └─? 累加 RecordsAffected
    │
    ├─? ExecuteScalarAsync()
    │       └─? ExecuteReaderAsync()
    │               └─? NextResultAsync() [循环查找第一个有数据的结果]
    │                       └─? ReadAsync() → GetValue(0)
    │
    └─? ExecuteReaderAsync()
            └─? NextResultAsync() [调用一次，定位在第一个结果]
                    用户手动调用 NextResult() 遍历
```

## 高层方法的实现逻辑

### ExecuteNonQueryAsync

**目标**：返回所有 DML 语句的累加影响行数

```csharp
public override async Task<Int32> ExecuteNonQueryAsync(CancellationToken ct)
{
    using var reader = await ExecuteReaderAsync(ct);
    
    // 消费所有结果集
    var dr = reader as MySqlDataReader;
    while (dr != null && dr.HasMoreResults && 
           await reader.NextResultAsync(ct))
    {
        // RecordsAffected 已在 NextResultAsync 中自动累加
    }
    
    // 直接返回累加值
    return reader.RecordsAffected;
}
```

**关键点**：
- 不需要手动累加 `RecordsAffected`（已经自动累加）
- 只需消费所有结果集即可

### ExecuteScalarAsync（改进后）

**目标**：返回第一个有数据的结果集的第一行第一列

```csharp
public override async Task<Object?> ExecuteScalarAsync(CancellationToken ct)
{
    using var reader = await ExecuteReaderAsync(ct);
    
    // 循环查找第一个有数据的结果集
    do
    {
        // 有列才尝试读取
        if (reader.FieldCount > 0)
        {
            if (await reader.ReadAsync(ct))
                return reader.GetValue(0);
        }
    } while (await reader.NextResultAsync(ct));
    
    return null;
}
```

**逻辑改进**：
- ? 使用 `do-while` 循环，逻辑更清晰
- ? 检查 `FieldCount > 0` 确保有列
- ? 尝试读取第一行，成功就返回
- ? 失败则继续查找下一个结果集

## 常见使用模式

### 单个 SELECT

```csharp
cmd.CommandText = "SELECT * FROM users";
using var reader = await cmd.ExecuteReaderAsync();
// reader 已定位在结果集上，FieldCount > 0
while (await reader.ReadAsync())
{
    var id = reader.GetInt32(0);
}
```

### 多个 SELECT

```csharp
cmd.CommandText = "SELECT * FROM users; SELECT * FROM orders;";
using var reader = await cmd.ExecuteReaderAsync();

// 第一个结果集
while (await reader.ReadAsync())
{
    var user = reader.GetString(0);
}

// 移动到第二个结果集
if (await reader.NextResultAsync())
{
    while (await reader.ReadAsync())
    {
        var order = reader.GetString(0);
    }
}
```

### 混合 DML 和 SELECT

```csharp
cmd.CommandText = @"
    DELETE FROM users WHERE inactive = 1;
    INSERT INTO users VALUES (1, 'Alice');
    SELECT * FROM users;
";
using var reader = await cmd.ExecuteReaderAsync();

// 第一个结果：DELETE 的 OK 包
Assert.Equal(0, reader.FieldCount);

// 第二个结果：INSERT 的 OK 包
await reader.NextResultAsync();
Assert.Equal(0, reader.FieldCount);

// 第三个结果：SELECT 结果集
await reader.NextResultAsync();
Assert.True(reader.FieldCount > 0);
while (await reader.ReadAsync())
{
    var name = reader.GetString(1);
}
```

## 与标准 ADO.NET 的一致性

| 行为 | MySqlDataReader | SqlClient | MySqlConnector |
|-----|----------------|-----------|----------------|
| ExecuteReader 返回时状态 | 定位在第一个结果 | ? | ? |
| NextResult 每次移动一个 | ? | ? | ? |
| NextResult 不跳过 OK 包 | ? | ? | ? |
| RecordsAffected 自动累加 | ? | ? | ? |
| ExecuteScalar 跳过 OK 包 | ? | ? | ? |

## 总结

**ExecuteReaderAsync**：
- 返回已定位在第一个结果上的 Reader
- 可以立即使用，无需先调用 NextResult()

**NextResultAsync**：
- 返回 true = 有下一个结果
- 返回 false = 没有更多结果
- 每次只移动一个结果

**何时可以持续调用**：
- 只要返回 true 就可以继续
- 返回 false 后不应该再依赖其返回值进行逻辑判断

**架构优势**：
1. 与标准 ADO.NET 完全一致
2. 高层方法实现简洁清晰
3. 支持灵活的结果集遍历
4. 自动资源管理和清理
