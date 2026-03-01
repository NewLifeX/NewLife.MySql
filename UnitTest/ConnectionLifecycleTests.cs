using System.ComponentModel;
using System.Data;
using NewLife;
using NewLife.MySql;

namespace UnitTest;

/// <summary>连接生命周期集成测试。验证连接池复用、多次 Open/Close 循环、状态转换等</summary>
[Collection(TestCollections.ReadOnly)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class ConnectionLifecycleTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    [Fact]
    [DisplayName("多次Open_Close循环正常工作")]
    public void WhenMultipleOpenCloseCyclesThenAllSucceed()
    {
        using var conn = new MySqlConnection(_ConnStr);

        for (var i = 0; i < 10; i++)
        {
            Assert.Equal(ConnectionState.Closed, conn.State);

            conn.Open();
            Assert.Equal(ConnectionState.Open, conn.State);

            // 每次都能正常执行查询
            using var cmd = new MySqlCommand(conn, "SELECT 1");
            var rs = cmd.ExecuteScalar();
            Assert.NotNull(rs);

            conn.Close();
            Assert.Equal(ConnectionState.Closed, conn.State);
        }
    }

    [Fact]
    [DisplayName("不同连接对象复用连接池")]
    public void WhenDifferentConnectionObjectsThenReusePool()
    {
        // 第一个连接获取客户端
        using var conn1 = new MySqlConnection(_ConnStr);
        conn1.Open();
        var client1 = conn1.Client;
        Assert.NotNull(client1);
        conn1.Close();

        // 第二个连接应该复用池中的客户端
        using var conn2 = new MySqlConnection(_ConnStr);
        conn2.Open();
        var client2 = conn2.Client;
        Assert.NotNull(client2);

        // 能正常执行查询
        using var cmd = new MySqlCommand(conn2, "SELECT 1");
        var rs = cmd.ExecuteScalar();
        Assert.NotNull(rs);

        conn2.Close();
    }

    [Fact]
    [DisplayName("连接关闭后Client为null")]
    public void WhenConnectionClosedThenClientIsNull()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        Assert.NotNull(conn.Client);
        Assert.Equal(ConnectionState.Open, conn.State);

        conn.Close();

        Assert.Null(conn.Client);
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    [DisplayName("Dispose后状态为Closed")]
    public void WhenDisposedThenStateClosed()
    {
        var conn = new MySqlConnection(_ConnStr);
        conn.Open();
        Assert.Equal(ConnectionState.Open, conn.State);

        conn.Dispose();
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    [DisplayName("重复Close不抛异常")]
    public void WhenDoubleCloseThenNoException()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        conn.Close();
        conn.Close(); // 第二次关闭不应抛异常
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    [DisplayName("重复Dispose不抛异常")]
    public void WhenDoubleDisposeThenNoException()
    {
        var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        conn.Dispose();
        conn.Dispose(); // 第二次释放不应抛异常
    }

    [Fact]
    [DisplayName("ServerVersion在Open后有值")]
    public void WhenOpenedThenServerVersionAvailable()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        Assert.NotNull(conn.ServerVersion);
        Assert.NotEmpty(conn.ServerVersion);
        // MySQL 版本通常以数字开头
        Assert.True(Char.IsDigit(conn.ServerVersion[0]),
            $"ServerVersion should start with digit, got: {conn.ServerVersion}");

        conn.Close();
    }

    [Fact]
    [DisplayName("Database属性返回正确数据库名")]
    public void WhenOpenedThenDatabaseNameCorrect()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        Assert.Equal("sys", conn.Database);

        conn.Close();
    }

    [Fact]
    [DisplayName("ChangeDatabase切换数据库")]
    public void WhenChangeDatabaseThenQueryNewDatabase()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();
        Assert.Equal("sys", conn.Database);

        conn.ChangeDatabase("information_schema");

        // 验证当前数据库已切换
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT DATABASE()";
        var db = cmd.ExecuteScalar()?.ToString();
        Assert.Equal("information_schema", db);

        // 切换回原数据库
        conn.ChangeDatabase("sys");
    }

    [Fact]
    [DisplayName("异步Open_Close循环")]
    public async Task WhenAsyncOpenCloseCyclesThenAllSucceed()
    {
        for (var i = 0; i < 5; i++)
        {
            using var conn = new MySqlConnection(_ConnStr);
            await conn.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn.State);

            using var cmd = new MySqlCommand(conn, "SELECT 1");
            var rs = await cmd.ExecuteScalarAsync();
            Assert.NotNull(rs);

            conn.Close();
            Assert.Equal(ConnectionState.Closed, conn.State);
        }
    }

    [Fact]
    [DisplayName("并发多连接独立工作")]
    public async Task WhenConcurrentConnectionsThenAllIndependent()
    {
        var tasks = Enumerable.Range(0, 10).Select(async i =>
        {
            using var conn = new MySqlConnection(_ConnStr);
            await conn.OpenAsync();

            using var cmd = new MySqlCommand(conn, $"SELECT {i}");
            var rs = await cmd.ExecuteScalarAsync();
            Assert.Equal(i, Convert.ToInt32(rs));

            conn.Close();
        });

        await Task.WhenAll(tasks);
    }

    [Fact]
    [DisplayName("连接Open后可执行多条命令")]
    public void WhenOpenedThenMultipleCommandsWork()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 连续执行多条命令
        for (var i = 0; i < 20; i++)
        {
            using var cmd = new MySqlCommand(conn, $"SELECT {i} + 1");
            var rs = cmd.ExecuteScalar();
            Assert.Equal(i + 1, Convert.ToInt32(rs));
        }

        conn.Close();
    }

    [Fact]
    [DisplayName("CreateCommand返回正确类型")]
    public void WhenCreateCommandThenCorrectType()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        using var cmd = conn.CreateCommand();
        Assert.IsType<MySqlCommand>(cmd);
        Assert.Same(conn, (cmd as MySqlCommand)?.Connection);

        conn.Close();
    }
}
