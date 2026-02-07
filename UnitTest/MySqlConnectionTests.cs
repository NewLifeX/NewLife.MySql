using System.Data;
using NewLife;
using NewLife.MySql;

namespace UnitTest;

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

        var pool = connection.Factory.PoolManager.GetPool(connection.Setting);
        Assert.True(pool.Total > 0);

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
        var connStr = DALTests.GetConnStr().Replace("Database=sys;", "Database=myDataBase;");
        var connection = new MySqlConnection(connStr);

        Assert.Equal("myDataBase", connection.Database);

        connection.ChangeDatabase("newDatabase");

        Assert.Equal("newDatabase", connection.Database);
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
        Assert.Equal(8, dt.Rows.Count);
        //Assert.Throws<NotSupportedException>(() => connection.GetSchema());

        foreach (DataRow dr in dt.Rows)
        {
            var name = dr["CollectionName"]?.ToString();

            var dt2 = connection.GetSchema(name);
            Assert.NotNull(dt2);
            Assert.NotEmpty(dt2.Rows);
        }
    }
}
