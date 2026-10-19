using System.Data;
using NewLife;
using NewLife.MySql;

namespace UnitTest;

[Collection(TestCollections.ReadOnly)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class MySqlConnectionTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    [Fact]
    public void TestOpenConnection()
    {
        var connStr = _ConnStr;
        var connection = new MySqlConnection(connStr);

        Assert.Equal(ConnectionState.Closed, connection.State);
        Assert.NotNull(connection.Setting);
        Assert.Equal(connStr.Replace("User Id", "UserID").TrimEnd(';'), connection.ConnectionString);
        Assert.Equal("sys", connection.Database);
        //Assert.Equal("localhost", connection.DataSource);
        Assert.Null(connection.ServerVersion);
        Assert.NotNull(connection.Factory);
        Assert.Null(connection.Client);

        connection.Open();

        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.NotNull(connection.Client);
        Assert.NotNull(connection.ServerVersion);

        connection.Close();

        Assert.Equal(ConnectionState.Closed, connection.State);
        Assert.Null(connection.Client);
    }

    [Fact]
    public void TestCloseConnection()
    {
        var connStr = _ConnStr;
        var connection = new MySqlConnection(connStr);

        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);

        connection.Close();
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public void TestChangeDatabase()
    {
        var connStr = _ConnStr;

        // 场景1：未打开连接时切换数据库
        using (var conn = new MySqlConnection(connStr))
        {
            var originalDb = conn.Database;
            Assert.Equal("sys", originalDb);

            // 未打开连接时切换数据库，不会抛出异常
            conn.ChangeDatabase("information_schema");
            Assert.Equal("information_schema", conn.Database);

            // 打开连接，验证连接到新数据库
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT DATABASE()";
            var currentDb = cmd.ExecuteScalar()?.ToString();
            Assert.Equal("information_schema", currentDb);
        }

        // 场景2：已打开连接时切换数据库
        using (var conn = new MySqlConnection(connStr))
        {
            conn.Open();
            var originalDb = conn.Database;
            Assert.Equal("sys", originalDb);

            // 切换数据库（内部会 Close + Open）
            conn.ChangeDatabase("information_schema");

            // 验证：连接仍然打开
            Assert.Equal(ConnectionState.Open, conn.State);
            Assert.Equal("information_schema", conn.Database);

            // 验证：实际数据库已切换
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT DATABASE()";
            var currentDb = cmd.ExecuteScalar()?.ToString();
            Assert.Equal("information_schema", currentDb);
        }
    }

    [Fact]
    public void TestChangeDatabasePoolIsolation()
    {
        var connStr = _ConnStr;

        // 第一个连接：使用 sys 数据库
        using (var conn1 = new MySqlConnection(connStr))
        {
            conn1.Open();
            Assert.Equal("sys", conn1.Database);

            using var cmd = conn1.CreateCommand();
            cmd.CommandText = "SELECT DATABASE()";
            var db = cmd.ExecuteScalar()?.ToString();
            Assert.Equal("sys", db);
        }

        // 第二个连接：切换到 information_schema
        using (var conn2 = new MySqlConnection(connStr))
        {
            conn2.Open();
            conn2.ChangeDatabase("information_schema");

            using var cmd = conn2.CreateCommand();
            cmd.CommandText = "SELECT DATABASE()";
            var db = cmd.ExecuteScalar()?.ToString();
            Assert.Equal("information_schema", db);
        }

        // 第三个连接：验证连接池隔离，应该连接到 sys
        using (var conn3 = new MySqlConnection(connStr))
        {
            conn3.Open();

            using var cmd = conn3.CreateCommand();
            cmd.CommandText = "SELECT DATABASE()";
            var db = cmd.ExecuteScalar()?.ToString();
            Assert.Equal("sys", db);
        }
    }

    [Fact]
    public void TestBeginTransaction()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        {
            var rs = conn.ExecuteNonQuery("delete from sys.sys_config where variable='test_tt'");
            Assert.True(rs >= 0);
        }

        // 插入 & 回滚
        {
            using var tr = conn.BeginTransaction();
            var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('test_tt','123',now(),'Stone')";
            var rs = conn.ExecuteNonQuery(sql);
            tr.Rollback();
            Assert.Equal(1, rs);
        }
        // 验证
        {
            var sql = "select count(*) from sys.sys_config where variable='test_tt'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteScalar();
            Assert.Equal(0, rs.ToInt());
        }

        // 插入 & 提交
        {
            using var tr = conn.BeginTransaction();
            var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('test_tt','123',now(),'Stone')";
            var rs = conn.ExecuteNonQuery(sql);
            tr.Commit();
            Assert.Equal(1, rs);
        }
        // 验证
        {
            var sql = "select count(*) from sys.sys_config where variable='test_tt'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteScalar();
            Assert.Equal(1, rs.ToInt());
        }

        {
            var sql = "delete from sys.sys_config where variable='test_tt'";
            var rs = conn.ExecuteNonQuery(sql);
            Assert.Equal(1, rs);
        }
    }

    [Fact]
    public void TestGetSchema()
    {
        var connStr = _ConnStr;
        using var connection = new MySqlConnection(connStr);

        connection.Open();

        var dt = connection.GetSchema();
        Assert.NotNull(dt);
        Assert.Equal(15, dt.Rows.Count);
        //Assert.Throws<NotSupportedException>(() => connection.GetSchema());

        // sys 数据库中可能没有视图/触发器/外键，跳过这些集合的非空断言
        var skipNonEmptyCheck = new HashSet<String>(StringComparer.OrdinalIgnoreCase)
        {
            "Views", "Triggers", "ForeignKeys"
        };

        foreach (DataRow dr in dt.Rows)
        {
            var name = dr["CollectionName"]?.ToString();

            var dt2 = connection.GetSchema(name);
            Assert.NotNull(dt2);

            if (!skipNonEmptyCheck.Contains(name!))
                Assert.NotEmpty(dt2.Rows);
        }
    }

    [Fact(DisplayName = "数据库一致性判断忽略空值与大小写")]
    public void IsSameDatabase_MatchesEmptyAndCaseInsensitive()
    {
        Assert.True(MySqlConnection.IsSameDatabase(null, null));
        Assert.True(MySqlConnection.IsSameDatabase("", ""));
        Assert.True(MySqlConnection.IsSameDatabase(null, ""));
        Assert.True(MySqlConnection.IsSameDatabase("sys", "SYS"));
        Assert.False(MySqlConnection.IsSameDatabase("sys", "information_schema"));
        Assert.False(MySqlConnection.IsSameDatabase("sys", null));
        Assert.False(MySqlConnection.IsSameDatabase(null, "sys"));
    }

    [Fact(DisplayName = "无数据库连接关闭后归还池槽位")]
    public async Task Close_WithoutDatabase_ReturnsPoolSlot()
    {
        var builder = new MySqlConnectionStringBuilder(_ConnStr) { Database = null };
        var factory = new MySqlClientFactory { PoolManager = new MySqlPoolManager() };
        var pool = factory.PoolManager.GetPool(new MySqlConnectionStringBuilder(builder.ConnectionString));

        using var connection = new MySqlConnection(builder.ConnectionString) { Factory = factory };
        await connection.OpenAsync();
        Assert.Equal(1, pool.BusyCount);

        connection.Close();
        Assert.Equal(0, pool.BusyCount);

        pool.Clear();
    }

    [Fact(DisplayName = "切换数据库后切回关闭仍归还池槽位")]
    public async Task Close_AfterSwitchBack_ReturnsPoolSlot()
    {
        var factory = new MySqlClientFactory { PoolManager = new MySqlPoolManager() };
        var setting = new MySqlConnectionStringBuilder(_ConnStr);
        var pool = factory.PoolManager.GetPool(setting);

        using var connection = new MySqlConnection(setting.ConnectionString) { Factory = factory };
        await connection.OpenAsync();

        var client = connection.Client!;
        var original = client.Database;
        await client.SetDatabaseAsync("information_schema");
        await client.SetDatabaseAsync(original);

        connection.Close();
        Assert.Equal(0, pool.BusyCount);

        pool.Clear();
    }

    [Fact(DisplayName = "切换数据库未切回关闭时销毁连接并释放池槽位")]
    public async Task Close_AfterSwitchWithoutSwitchBack_ReleasesPoolSlot()
    {
        var factory = new MySqlClientFactory { PoolManager = new MySqlPoolManager() };
        var setting = new MySqlConnectionStringBuilder(_ConnStr);
        var pool = factory.PoolManager.GetPool(setting);

        using var connection = new MySqlConnection(setting.ConnectionString) { Factory = factory };
        await connection.OpenAsync();

        var client = connection.Client!;
        await client.SetDatabaseAsync("information_schema");

        connection.Close();
        Assert.Equal(0, pool.BusyCount);

        pool.Clear();
    }

    [Fact(DisplayName = "DisposeAsync关闭连接并释放池槽位")]
    public async Task DisposeAsync_ClosesConnection()
    {
        var factory = new MySqlClientFactory { PoolManager = new MySqlPoolManager() };
        var setting = new MySqlConnectionStringBuilder(_ConnStr);
        var pool = factory.PoolManager.GetPool(setting);

        var connection = new MySqlConnection(setting.ConnectionString) { Factory = factory };
        await connection.OpenAsync();
        Assert.Equal(1, pool.BusyCount);

        await connection.DisposeAsync();

        Assert.Equal(ConnectionState.Closed, connection.State);
        Assert.Equal(0, pool.BusyCount);

        pool.Clear();
    }

    [Fact(DisplayName = "PingAsync在活动连接上返回true")]
    public async Task Ping_WhenActive_ReturnsTrue()
    {
        using var connection = new MySqlConnection(_ConnStr);
        await connection.OpenAsync();

        Assert.True(await connection.Client!.PingAsync());
    }

    [Fact(DisplayName = "ReconnectAsync重建连接并可继续执行")]
    public async Task ReconnectAsync_RebuildsConnection()
    {
        using var connection = new MySqlConnection(_ConnStr);
        await connection.OpenAsync();

        var oldClient = connection.Client!;
        oldClient.TryDispose();

        await connection.ReconnectAsync(CancellationToken.None);

        Assert.NotSame(oldClient, connection.Client);
        Assert.True(connection.Client!.Active);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1";
        Assert.Equal(1, Convert.ToInt32(cmd.ExecuteScalar()));
    }
}
