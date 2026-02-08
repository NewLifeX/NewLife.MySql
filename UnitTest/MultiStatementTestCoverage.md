# 多语句执行单元测试覆盖说明

本文档说明了 `MySqlCommand` 多语句执行功能的完整测试覆盖情况。

## 测试覆盖概览

### 原有测试（6个）

1. **MultiStatement_ExecuteNonQuery** - 多条 INSERT 语句，验证 RecordsAffected 累加
2. **MultiStatement_ExecuteScalar** - SELECT + INSERT 混合，验证返回第一个有数据的结果集
3. **MultiStatement_ExecuteReader** - INSERT + SELECT，验证 NextResult() 遍历
4. **MultiStatement_WithSemicolonInString** - 验证字符串内分号不被误拆分
5. **MultiStatement_TrailingSemicolon** - 尾部分号不影响执行
6. **MultiStatement_ThreeStatements** - DELETE + INSERT + SELECT 三条语句

### 新增测试（13个）

#### 1. DML 语句组合测试

##### MultiStatement_MixedDML_InsertUpdateDelete
- **场景**：INSERT + INSERT + UPDATE + DELETE 混合执行
- **验证点**：
  - RecordsAffected 正确累加（2 INSERT + 1 UPDATE + 1 DELETE = 4）
  - 所有 DML 操作都正确执行
  - UPDATE 和 DELETE 的影响可见

##### MultiStatement_RecordsAffected_OnlyDML
- **场景**：只有 DML 语句（UPDATE + UPDATE + DELETE）
- **验证点**：
  - RecordsAffected 正确累加
  - 不包含 SELECT 语句

##### MultiStatement_RecordsAffected_WithSelect
- **场景**：DML + SELECT 混合
- **验证点**：
  - RecordsAffected 只累加 DML 的影响行数
  - SELECT 不计入 RecordsAffected

#### 2. ExecuteScalar 场景测试

##### MultiStatement_SelectFirst_ThenInsert
- **场景**：SELECT 在前，INSERT 在后
- **验证点**：
  - ExecuteScalar 返回第一个 SELECT 的结果
  - 后续的 INSERT 也正确执行

##### MultiStatement_InsertFirst_ThenSelect
- **场景**：INSERT 在前，SELECT 在后
- **验证点**：
  - ExecuteScalar 自动跳过 OK 包（INSERT）
  - 返回第一个有数据的结果集（SELECT）

##### MultiStatement_EmptyResult_ExecuteScalar
- **场景**：所有语句都是 DML（无结果集）
- **验证点**：
  - ExecuteScalar 返回 null
  - 不会因为没有结果集而失败

#### 3. ExecuteReader 遍历测试

##### MultiStatement_ExecuteReader_MultipleResultSets
- **场景**：三个 SELECT 语句
- **验证点**：
  - 能正确遍历所有结果集
  - 每个结果集的数据正确
  - NextResult() 返回值正确
  - 最后返回 false 表示没有更多结果

##### MultiStatement_ExecuteReader_MixedOKAndResultSet
- **场景**：INSERT + SELECT + UPDATE + SELECT
- **验证点**：
  - 能正确识别 OK 包（FieldCount=0）
  - 能正确读取结果集（FieldCount>0）
  - NextResult() 能正确遍历混合类型的结果
  - RecordsAffected 正确累加

##### MultiStatement_ExecuteReader_PartialRead
- **场景**：不完全读取结果集就关闭 reader
- **验证点**：
  - 部分读取不影响连接状态
  - 关闭 reader 后连接仍可正常使用
  - 验证资源清理正确

#### 4. 边界和特殊场景测试

##### MultiStatement_OnlySelects_NoAffectedRows
- **场景**：只有 SELECT 语句，无 DML
- **验证点**：
  - RecordsAffected 应为 0 或 -1
  - SELECT 不计入影响行数

##### MultiStatement_LargeNumberOfStatements
- **场景**：10 条 INSERT 语句
- **验证点**：
  - 大量语句的 RecordsAffected 正确累加
  - 所有语句都正确执行
  - 性能可接受

## 测试覆盖的关键场景

### 1. 语句类型组合
- ? 纯 DML（INSERT/UPDATE/DELETE）
- ? 纯 SELECT
- ? DML + SELECT 混合
- ? SELECT + DML 混合
- ? 多个 SELECT

### 2. 执行方法
- ? ExecuteNonQuery - DML 累加
- ? ExecuteScalar - 跳过 OK 包，找到第一个有数据的结果
- ? ExecuteReader - 遍历所有结果集

### 3. RecordsAffected 计算
- ? 只有 DML 时的累加
- ? 混合 SELECT 时只累加 DML
- ? 只有 SELECT 时返回 0/-1
- ? 大量语句的累加

### 4. NextResult 遍历
- ? 遍历多个结果集
- ? 混合 OK 包和结果集
- ? 部分读取的处理
- ? 最后返回 false

### 5. 边界情况
- ? 字符串内分号
- ? 尾部分号
- ? 空结果集
- ? 大量语句

## 测试数据清理

所有测试都遵循以下模式：
1. **前置清理**：删除可能存在的测试数据
2. **执行测试**：运行多语句 SQL
3. **验证结果**：断言执行结果正确
4. **后置清理**：删除测试数据

这确保了测试的独立性和可重复性。

## 运行测试

```bash
# 运行所有多语句测试
dotnet test --filter "FullyQualifiedName~MultiStatement"

# 运行特定测试
dotnet test --filter "FullyQualifiedName=UnitTest.MySqlCommandTests.MultiStatement_MixedDML_InsertUpdateDelete"
```

## 覆盖率总结

- **总测试数**：19 个（6 个原有 + 13 个新增）
- **覆盖的 API**：ExecuteNonQuery、ExecuteScalar、ExecuteReader
- **覆盖的场景**：DML 组合、SELECT 组合、混合语句、边界情况
- **验证的关键逻辑**：
  - ? NextResultAsync 每次只移动一个结果
  - ? RecordsAffected 正确累加
  - ? ExecuteScalar 自动跳过 OK 包
  - ? ExecuteReader 正确遍历所有结果集
  - ? 资源正确清理
