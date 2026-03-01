using System.ComponentModel;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>DbBatch API 数据库集成测试。验证 MySqlBatch 对真实数据库的操作</summary>
[Collection(TestCollections.DataModification)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class DbBatchIntegrationTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public DbBatchIntegrationTests()
    {
        _table = "dbbatch_test_" + Rand.Next(10000);
        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();
        var sql = $"CREATE TABLE IF NOT EXISTS `{_table}` (`id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(128) DEFAULT NULL, `age` INT DEFAULT NULL, `score` DECIMAL(10,2) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        _conn.ExecuteNonQuery(sql);
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    [Fact]
    [DisplayName("DbBatch_ExecuteNonQuery插入多行")]
    public void WhenBatchInsertThenReturnsAffectedRows()
    {
        var batch = new MySqlBatch(_conn);
        batch.BatchCommands.Add(new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES ('Alice', 25)"));
        batch.BatchCommands.Add(new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES ('Bob', 30)"));
        batch.BatchCommands.Add(new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES ('Charlie', 22)"));

        var affected = batch.ExecuteNonQuery();
        Assert.Equal(3, affected);

        // 验证
        using var cmd = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}`");
        Assert.Equal(3, cmd.ExecuteScalar().ToInt());
    }

    [Fact]
    [DisplayName("DbBatch_ExecuteScalar返回首个标量")]
    public void WhenBatchSelectThenReturnsFirstScalar()
    {
        // 先插入数据
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name, age) VALUES ('Alice', 25)");

        var batch = new MySqlBatch(_conn);
        batch.BatchCommands.Add(new MySqlBatchCommand($"SELECT name FROM `{_table}` WHERE age=25"));
        batch.BatchCommands.Add(new MySqlBatchCommand($"SELECT COUNT(*) FROM `{_table}`"));

        var rs = batch.ExecuteScalar();
        Assert.Equal("Alice", rs);
    }

    [Fact]
    [DisplayName("DbBatch_ExecuteReader遍历多结果集")]
    public void WhenBatchReaderThenTraversesResultSets()
    {
        // 先插入数据
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name, age) VALUES ('Alice', 25), ('Bob', 30)");

        var batch = new MySqlBatch(_conn);
        batch.BatchCommands.Add(new MySqlBatchCommand($"SELECT name FROM `{_table}` WHERE age=25"));
        batch.BatchCommands.Add(new MySqlBatchCommand($"SELECT name FROM `{_table}` WHERE age=30"));

        using var dr = batch.ExecuteReader();

        // 第一个结果集
        Assert.True(dr.Read());
        Assert.Equal("Alice", dr.GetString(0));
        Assert.False(dr.Read());

        // 第二个结果集
        Assert.True(dr.NextResult());
        Assert.True(dr.Read());
        Assert.Equal("Bob", dr.GetString(0));
        Assert.False(dr.Read());

        Assert.False(dr.NextResult());
    }

    [Fact]
    [DisplayName("DbBatch_混合DML和SELECT")]
    public void WhenBatchMixedThenAllExecuted()
    {
        var batch = new MySqlBatch(_conn);
        batch.BatchCommands.Add(new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES ('Test1', 10)"));
        batch.BatchCommands.Add(new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES ('Test2', 20)"));

        var affected = batch.ExecuteNonQuery();
        Assert.Equal(2, affected);

        // 验证两条都已插入
        using var cmd = new MySqlCommand(_conn, $"SELECT name FROM `{_table}` ORDER BY age");
        using var dr = cmd.ExecuteReader();

        Assert.True(dr.Read());
        Assert.Equal("Test1", dr.GetString(0));
        Assert.True(dr.Read());
        Assert.Equal("Test2", dr.GetString(0));
        Assert.False(dr.Read());
    }

    [Fact]
    [DisplayName("DbBatch_带参数的批量命令")]
    public void WhenBatchWithParametersThenSubstituted()
    {
        var batch = new MySqlBatch(_conn);

        var cmd1 = new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES (@name, @age)");
        cmd1.Parameters.AddWithValue("name", "ParamAlice");
        cmd1.Parameters.AddWithValue("age", 25);
        batch.BatchCommands.Add(cmd1);

        var cmd2 = new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES (@name, @age)");
        cmd2.Parameters.AddWithValue("name", "ParamBob");
        cmd2.Parameters.AddWithValue("age", 30);
        batch.BatchCommands.Add(cmd2);

        var affected = batch.ExecuteNonQuery();
        Assert.Equal(2, affected);

        // 验证
        using var verifyCmd = new MySqlCommand(_conn, $"SELECT name FROM `{_table}` WHERE age=25");
        Assert.Equal("ParamAlice", verifyCmd.ExecuteScalar());
    }

    [Fact]
    [DisplayName("DbBatch_异步ExecuteNonQuery")]
    public async Task WhenBatchAsyncThenReturnsAffectedRows()
    {
        var batch = new MySqlBatch(_conn);
        batch.BatchCommands.Add(new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES ('AsyncA', 1)"));
        batch.BatchCommands.Add(new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES ('AsyncB', 2)"));

        var affected = await batch.ExecuteNonQueryAsync(CancellationToken.None);
        Assert.Equal(2, affected);

        using var cmd = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}`");
        Assert.Equal(2, cmd.ExecuteScalar().ToInt());
    }

    [Fact]
    [DisplayName("DbBatch_异步ExecuteScalar")]
    public async Task WhenBatchAsyncScalarThenReturnsValue()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name, age) VALUES ('ScalarTest', 99)");

        var batch = new MySqlBatch(_conn);
        batch.BatchCommands.Add(new MySqlBatchCommand($"SELECT age FROM `{_table}` WHERE name='ScalarTest'"));

        var rs = await batch.ExecuteScalarAsync(CancellationToken.None);
        Assert.Equal(99, Convert.ToInt32(rs));
    }

    [Fact]
    [DisplayName("DbBatch_INSERT后UPDATE")]
    public void WhenBatchInsertThenUpdateThenDataCorrect()
    {
        var batch = new MySqlBatch(_conn);
        batch.BatchCommands.Add(new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES ('UpdTest', 10)"));
        batch.BatchCommands.Add(new MySqlBatchCommand($"UPDATE `{_table}` SET age=20 WHERE name='UpdTest'"));

        var affected = batch.ExecuteNonQuery();
        Assert.Equal(2, affected);

        using var cmd = new MySqlCommand(_conn, $"SELECT age FROM `{_table}` WHERE name='UpdTest'");
        Assert.Equal(20, cmd.ExecuteScalar().ToInt());
    }

    [Fact]
    [DisplayName("DbBatch_INSERT后DELETE")]
    public void WhenBatchInsertThenDeleteThenDataRemoved()
    {
        // 先插入一条供删除
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name, age) VALUES ('DelTarget', 50)");

        var batch = new MySqlBatch(_conn);
        batch.BatchCommands.Add(new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES ('KeepMe', 60)"));
        batch.BatchCommands.Add(new MySqlBatchCommand($"DELETE FROM `{_table}` WHERE name='DelTarget'"));

        var affected = batch.ExecuteNonQuery();
        Assert.Equal(2, affected);

        using var cmd = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}` WHERE name='DelTarget'");
        Assert.Equal(0, cmd.ExecuteScalar().ToInt());

        using var cmd2 = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}` WHERE name='KeepMe'");
        Assert.Equal(1, cmd2.ExecuteScalar().ToInt());
    }

    [Fact]
    [DisplayName("DbBatch_通过Connection.CreateBatch创建")]
    public void WhenCreateBatchFromConnectionThenWorks()
    {
        var batch = _conn.CreateBatch() as MySqlBatch;
        Assert.NotNull(batch);
        Assert.Same(_conn, batch!.Connection);

        batch.BatchCommands.Add(new MySqlBatchCommand($"INSERT INTO `{_table}` (name, age) VALUES ('Factory', 1)"));
        var affected = batch.ExecuteNonQuery();
        Assert.Equal(1, affected);
    }
}
