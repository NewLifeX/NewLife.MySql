using System.ComponentModel;
using System.Data;
using NewLife.MySql;

namespace UnitTest;

/// <summary>异步方法测试</summary>
public class AsyncTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    [Fact]
    [DisplayName("异步打开关闭连接")]
    public async Task WhenOpenAsyncCalledThenConnectionOpens()
    {
        using var conn = new MySqlConnection(_ConnStr);

        Assert.Equal(ConnectionState.Closed, conn.State);

        await conn.OpenAsync().ConfigureAwait(false);

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
        await conn.OpenAsync().ConfigureAwait(false);

        using var cmd = new MySqlCommand(conn, "select * from sys.user_summary");
        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

        Assert.True(reader.FieldCount > 0);

        var hasRows = await reader.ReadAsync().ConfigureAwait(false);
        Assert.True(hasRows);
        Assert.Equal("root", reader.GetString(0));
    }

    [Fact]
    [DisplayName("异步执行NonQuery")]
    public async Task WhenExecuteNonQueryAsyncThenReturnsAffectedRows()
    {
        using var conn = new MySqlConnection(_ConnStr);
        await conn.OpenAsync().ConfigureAwait(false);

        // 清理
        {
            using var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='async_test'");
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        // 插入
        {
            using var cmd = new MySqlCommand(conn, "insert into sys.sys_config(variable,value,set_time,set_by) values('async_test','abc',now(),'test')");
            var rs = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            Assert.Equal(1, rs);
        }

        // 验证
        {
            using var cmd = new MySqlCommand(conn, "select value v from sys.sys_config where variable='async_test'");
            var rs = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            Assert.Equal("abc", rs);
        }

        // 清理
        {
            using var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='async_test'");
            var rs = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            Assert.Equal(1, rs);
        }
    }

    [Fact]
    [DisplayName("异步ExecuteScalar")]
    public async Task WhenExecuteScalarAsyncThenReturnsValue()
    {
        using var conn = new MySqlConnection(_ConnStr);
        await conn.OpenAsync().ConfigureAwait(false);

        using var cmd = new MySqlCommand(conn, "select statements u from sys.user_summary where user='root'");
        var rs = await cmd.ExecuteScalarAsync().ConfigureAwait(false);

        Assert.NotNull(rs);
    }

    [Fact]
    [DisplayName("多个连接并发异步打开")]
    public async Task WhenMultipleConnectionsOpenConcurrentlyThenAllSucceed()
    {
        var tasks = Enumerable.Range(0, 5).Select(async _ =>
        {
            using var conn = new MySqlConnection(_ConnStr);
            await conn.OpenAsync().ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);

            using var cmd = new MySqlCommand(conn, "select 1");
            var rs = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            Assert.NotNull(rs);

            conn.Close();
        });

        await Task.WhenAll(tasks).ConfigureAwait(false);
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
            await conn.OpenAsync(cts.Token).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    [Fact]
    [DisplayName("异步读取多行数据")]
    public async Task WhenReadMultipleRowsAsyncThenAllRowsReturned()
    {
        using var conn = new MySqlConnection(_ConnStr);
        await conn.OpenAsync().ConfigureAwait(false);

        using var cmd = new MySqlCommand(conn, "select * from sys.user_summary");
        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

        var rows = 0;
        while (await reader.ReadAsync().ConfigureAwait(false))
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
