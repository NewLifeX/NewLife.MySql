using System;
using System.ComponentModel;
using System.Data.Common;
using NewLife.MySql;
using Xunit.Abstractions;

namespace UnitTest;

/// <summary>MySqlBatch 和 MySqlBatchCommand 纯内存单元测试</summary>
[Collection(TestCollections.InMemory)]
public class MySqlBatchTests
{
    private readonly ITestOutputHelper _output;

    public MySqlBatchTests(ITestOutputHelper output) => _output = output;

    #region MySqlBatchCommand
    [Fact]
    [DisplayName("MySqlBatchCommand默认CommandText为空字符串")]
    public void WhenNewBatchCommandThenCommandTextIsEmpty()
    {
        var cmd = new MySqlBatchCommand();

        Assert.Equal("", cmd.CommandText);
    }

    [Fact]
    [DisplayName("MySqlBatchCommand使用SQL实例化")]
    public void WhenNewBatchCommandWithSqlThenCommandTextIsSet()
    {
        var cmd = new MySqlBatchCommand("SELECT 1");

        Assert.Equal("SELECT 1", cmd.CommandText);
    }

    [Fact]
    [DisplayName("MySqlBatchCommand参数集合初始化为空")]
    public void WhenNewBatchCommandThenParametersIsEmpty()
    {
        var cmd = new MySqlBatchCommand();

        Assert.NotNull(cmd.Parameters);
        Assert.Equal(0, cmd.Parameters.Count);
    }

    [Fact]
    [DisplayName("MySqlBatchCommand可添加参数")]
    public void WhenAddParameterToBatchCommandThenCountIncreases()
    {
        var cmd = new MySqlBatchCommand("SELECT @id");
        cmd.Parameters.AddWithValue("id", 42);

        Assert.Equal(1, cmd.Parameters.Count);
    }
    #endregion

    #region MySqlBatchCommandCollection
    [Fact]
    [DisplayName("MySqlBatchCommandCollection初始化为空")]
    public void WhenNewBatchThenCommandsIsEmpty()
    {
        var batch = new MySqlBatch();

        Assert.Equal(0, batch.BatchCommands.Count);
    }

    [Fact]
    [DisplayName("MySqlBatchCommandCollection添加和索引访问")]
    public void WhenAddCommandThenCanAccessByIndex()
    {
        var batch = new MySqlBatch();
        var cmd = new MySqlBatchCommand("SELECT 1");

        batch.BatchCommands.Add(cmd);

        Assert.Equal(1, batch.BatchCommands.Count);
        Assert.Same(cmd, batch.BatchCommands[0]);
    }

    [Fact]
    [DisplayName("MySqlBatchCommandCollection清空")]
    public void WhenClearThenCountIsZero()
    {
        var batch = new MySqlBatch();
        batch.BatchCommands.Add(new MySqlBatchCommand("SELECT 1"));
        batch.BatchCommands.Add(new MySqlBatchCommand("SELECT 2"));

        batch.BatchCommands.Clear();

        Assert.Equal(0, batch.BatchCommands.Count);
    }

    [Fact]
    [DisplayName("MySqlBatchCommandCollection移除命令")]
    public void WhenRemoveCommandThenCountDecreases()
    {
        var batch = new MySqlBatch();
        var cmd = new MySqlBatchCommand("SELECT 1");
        batch.BatchCommands.Add(cmd);

        batch.BatchCommands.Remove(cmd);

        Assert.Equal(0, batch.BatchCommands.Count);
    }

    [Fact]
    [DisplayName("MySqlBatchCommandCollection插入命令")]
    public void WhenInsertCommandThenAtCorrectPosition()
    {
        var batch = new MySqlBatch();
        batch.BatchCommands.Add(new MySqlBatchCommand("SELECT 1"));
        batch.BatchCommands.Add(new MySqlBatchCommand("SELECT 3"));

        var cmd2 = new MySqlBatchCommand("SELECT 2");
        batch.BatchCommands.Insert(1, cmd2);

        Assert.Equal(3, batch.BatchCommands.Count);
        Assert.Same(cmd2, batch.BatchCommands[1]);
    }

    [Fact]
    [DisplayName("MySqlBatchCommandCollection IsReadOnly为false")]
    public void WhenCheckIsReadOnlyThenFalse()
    {
        var batch = new MySqlBatch();

        Assert.False(batch.BatchCommands.IsReadOnly);
    }

    [Fact]
    [DisplayName("MySqlBatchCommandCollection Contains查找")]
    public void WhenContainsExistingCommandThenReturnsTrue()
    {
        var batch = new MySqlBatch();
        var cmd = new MySqlBatchCommand("SELECT 1");
        batch.BatchCommands.Add(cmd);

        Assert.True(batch.BatchCommands.Contains(cmd));
        Assert.False(batch.BatchCommands.Contains(new MySqlBatchCommand("SELECT 2")));
    }

    [Fact]
    [DisplayName("MySqlBatchCommandCollection IndexOf查找")]
    public void WhenIndexOfExistingCommandThenReturnsCorrectIndex()
    {
        var batch = new MySqlBatch();
        var cmd1 = new MySqlBatchCommand("SELECT 1");
        var cmd2 = new MySqlBatchCommand("SELECT 2");
        batch.BatchCommands.Add(cmd1);
        batch.BatchCommands.Add(cmd2);

        Assert.Equal(0, batch.BatchCommands.IndexOf(cmd1));
        Assert.Equal(1, batch.BatchCommands.IndexOf(cmd2));
    }
    #endregion

    #region MySqlBatch
    [Fact]
    [DisplayName("MySqlBatch默认构造函数")]
    public void WhenNewBatchThenDefaultProperties()
    {
        var batch = new MySqlBatch();

        Assert.Null(batch.Connection);
        Assert.Null(batch.Transaction);
        Assert.Equal(30, batch.Timeout);
    }

    [Fact]
    [DisplayName("MySqlBatch使用连接实例化")]
    public void WhenNewBatchWithConnectionThenConnectionIsSet()
    {
        var conn = new MySqlConnection("Server=localhost;Database=test;uid=root;pwd=root");
        var batch = new MySqlBatch(conn);

        Assert.Same(conn, batch.Connection);
    }

    [Fact]
    [DisplayName("通过工厂创建MySqlBatch")]
    public void WhenCreateBatchFromFactoryThenCorrectType()
    {
        var factory = MySqlClientFactory.Instance;
        var batch = factory.CreateBatch();

        Assert.IsType<MySqlBatch>(batch);
    }

    [Fact]
    [DisplayName("通过工厂创建MySqlBatchCommand")]
    public void WhenCreateBatchCommandFromFactoryThenCorrectType()
    {
        var factory = MySqlClientFactory.Instance;
        var cmd = factory.CreateBatchCommand();

        Assert.IsType<MySqlBatchCommand>(cmd);
    }

    [Fact]
    [DisplayName("ExecuteNonQuery无连接时抛出InvalidOperationException")]
    public void WhenExecuteNonQueryWithoutConnectionThenThrows()
    {
        var batch = new MySqlBatch();
        batch.BatchCommands.Add(new MySqlBatchCommand("SELECT 1"));

        Assert.Throws<InvalidOperationException>(() => batch.ExecuteNonQuery());
    }

    [Fact]
    [DisplayName("ExecuteNonQuery无命令时抛出InvalidOperationException")]
    public void WhenExecuteNonQueryWithoutCommandsThenThrows()
    {
        var conn = new MySqlConnection("Server=localhost;Database=test;uid=root;pwd=root");
        var batch = new MySqlBatch(conn);

        Assert.Throws<InvalidOperationException>(() => batch.ExecuteNonQuery());
    }
    #endregion

    #region 批量数据结构分配对比（L3 值数组化收益量化）
    [Fact]
    [DisplayName("批量导入值数组 vs 字典+参数对象分配对比")]
    public void BatchValueArrayAllocation()
    {
        const Int32 rows = 100000;
        const Int32 cols = 5;

        // JIT 预热，避免首次调用分配污染测量
        _ = new Dictionary<String, Object?>(cols);
        _ = new MySqlParameterCollection();
        _ = new Object?[cols];

        // 旧路径模型：每行 Dictionary + 每组 MySqlParameterCollection + 每参数 MySqlParameter
        var allocOld = GC.GetAllocatedBytesForCurrentThread();
        var oldBatch = new List<IDictionary<String, Object?>>(rows);
        for (var i = 0; i < rows; i++)
        {
            var dict = new Dictionary<String, Object?>(cols);
            dict["name"] = $"user_{i}";
            dict["age"] = 20 + (i % 50);
            dict["score"] = 80.5;
            dict["email"] = $"u{i}@test.com";
            dict["created"] = DateTime.Now;
            oldBatch.Add(dict);
        }
        var psList = new List<MySqlParameterCollection>(rows);
        foreach (var dict in oldBatch)
        {
            var ps = new MySqlParameterCollection();
            foreach (var kv in dict)
                ps.AddWithValue(kv.Key, kv.Value);
            psList.Add(ps);
        }
        allocOld = GC.GetAllocatedBytesForCurrentThread() - allocOld;

        // 新路径模型：每行直接构建值数组（按 SQL 参数顺序）
        var allocNew = GC.GetAllocatedBytesForCurrentThread();
        var newBatch = new List<Object?[]>(rows);
        for (var i = 0; i < rows; i++)
            newBatch.Add(new Object?[] { $"user_{i}", 20 + (i % 50), 80.5, $"u{i}@test.com", DateTime.Now });
        allocNew = GC.GetAllocatedBytesForCurrentThread() - allocNew;

        _output.WriteLine($"旧路径(字典+参数对象) {rows:N0} 行分配: {allocOld:N0} B，每行 {allocOld / rows:N0} B");
        _output.WriteLine($"新路径(值数组)     {rows:N0} 行分配: {allocNew:N0} B，每行 {allocNew / rows:N0} B");
        _output.WriteLine($"降幅: {100.0 * (allocOld - allocNew) / allocOld:N1}%");

        // 值数组消除每行 Dictionary 桶/条目数组与每参数 MySqlParameter 对象，分配应显著低于旧路径
        Assert.True(allocNew < allocOld,
            $"值数组分配 {allocNew:N0}B 应低于字典+参数对象 {allocOld:N0}B");
    }
    #endregion
}
