using System.ComponentModel;
using System.Data;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>异步方法测试</summary>
[Collection(TestCollections.ReadOnly)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class AsyncTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public AsyncTests()
    {
        _table = "async_test_" + Rand.Next(10000);
        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();
        var sql = $"CREATE TABLE IF NOT EXISTS `{_table}` (`variable` VARCHAR(128) NOT NULL PRIMARY KEY, `value` VARCHAR(1024) DEFAULT NULL, `set_time` DATETIME DEFAULT CURRENT_TIMESTAMP, `set_by` VARCHAR(128) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        _conn.ExecuteNonQuery(sql);
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    [Fact]
    [DisplayName("异步打开关闭连接")]
    public async Task WhenOpenAsyncCalledThenConnectionOpens()
    {
        using var conn = new MySqlConnection(_ConnStr);

        Assert.Equal(ConnectionState.Closed, conn.State);

        await conn.OpenAsync();

        Assert.Equal(ConnectionState.Open, conn.State);
        Assert.NotNull(conn.Client);
        Assert.NotNull(conn.ServerVersion);

        conn.Close();

        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    [DisplayName("异步执行查询")]
    public async Task WhenExecuteReaderAsyncThenReturnsData()
    {
        using var conn = new MySqlConnection(_ConnStr);
        await conn.OpenAsync();

        using var cmd = new MySqlCommand(conn, "select * from sys.user_summary");
        using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(reader.FieldCount > 0);

        var hasRows = await reader.ReadAsync();
        Assert.True(hasRows);
        Assert.Equal("root", reader.GetString(0));
    }

    [Fact]
    [DisplayName("异步执行NonQuery")]
    public async Task WhenExecuteNonQueryAsyncThenReturnsAffectedRows()
    {
        // 清理
        {
            using var cmd = new MySqlCommand(_conn, $"delete from `{_table}` where variable='async_test'");
            await cmd.ExecuteNonQueryAsync();
        }

        // 插入
        {
            using var cmd = new MySqlCommand(_conn, $"insert into `{_table}`(variable,value,set_time,set_by) values('async_test','abc',now(),'test')");
            var rs = await cmd.ExecuteNonQueryAsync();
            Assert.Equal(1, rs);
        }

        // 验证
        {
            using var cmd = new MySqlCommand(_conn, $"select value v from `{_table}` where variable='async_test'");
            var rs = await cmd.ExecuteScalarAsync();
            Assert.Equal("abc", rs);
        }

        // 清理
        {
            using var cmd = new MySqlCommand(_conn, $"delete from `{_table}` where variable='async_test'");
            var rs = await cmd.ExecuteNonQueryAsync();
            Assert.Equal(1, rs);
        }
    }

    [Fact]
    [DisplayName("异步ExecuteScalar")]
    public async Task WhenExecuteScalarAsyncThenReturnsValue()
    {
        using var conn = new MySqlConnection(_ConnStr);
        await conn.OpenAsync();

        using var cmd = new MySqlCommand(conn, "select statements u from sys.user_summary where user='root'");
        var rs = await cmd.ExecuteScalarAsync();

        Assert.NotNull(rs);
    }

    [Fact]
    [DisplayName("多个连接并发异步打开")]
    public async Task WhenMultipleConnectionsOpenConcurrentlyThenAllSucceed()
    {
        var tasks = Enumerable.Range(0, 5).Select(async _ =>
        {
            using var conn = new MySqlConnection(_ConnStr);
            await conn.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn.State);

            using var cmd = new MySqlCommand(conn, "select 1");
            var rs = await cmd.ExecuteScalarAsync();
            Assert.NotNull(rs);

            conn.Close();
        });

        await Task.WhenAll(tasks);
    }

    [Fact]
    [DisplayName("异步取消令牌")]
    public async Task WhenCancellationRequestedThenThrows()
    {
        using var conn = new MySqlConnection(_ConnStr);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await conn.OpenAsync(cts.Token);
        });
    }

    [Fact]
    [DisplayName("异步读取多行数据")]
    public async Task WhenReadMultipleRowsAsyncThenAllRowsReturned()
    {
        using var conn = new MySqlConnection(_ConnStr);
        await conn.OpenAsync();

        using var cmd = new MySqlCommand(conn, "select * from sys.user_summary");
        using var reader = await cmd.ExecuteReaderAsync();

        var rows = 0;
        while (await reader.ReadAsync())
        {
            rows++;
            Assert.True(reader.FieldCount > 0);
        }
        Assert.True(rows > 0);
    }

    [Fact]
    [DisplayName("同步Open也正常工作")]
    public void WhenSyncOpenCalledThenConnectionOpens()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        Assert.Equal(ConnectionState.Open, conn.State);
        Assert.NotNull(conn.Client);
        Assert.NotNull(conn.ServerVersion);

        conn.Close();
        Assert.Equal(ConnectionState.Closed, conn.State);
    }
}
