using System.Data;
using NewLife.MySql;

namespace UnitTest;

public class MySqlConnectionTests
{
    private String _ConnStr = "Server=localhost;Database=sys;User Id=root;Password=root;";

    [Fact]
    public void TestOpenConnection()
    {
        var connStr = _ConnStr;
        var connection = new MySqlConnection(connStr);

        Assert.Equal(ConnectionState.Closed, connection.State);
        Assert.NotNull(connection.Setting);
        Assert.Equal(connStr.Replace("User Id", "UserID").TrimEnd(';'), connection.ConnectionString);
        Assert.Equal("sys", connection.Database);
        Assert.Equal("localhost", connection.DataSource);
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
        var connStr = "Server=localhost;Database=myDataBase;User Id=root;Password=root;";
        var connection = new MySqlConnection(connStr);

        Assert.Equal("myDataBase", connection.Database);

        connection.ChangeDatabase("newDatabase");

        Assert.Equal("newDatabase", connection.Database);

        //Assert.Throws<NotImplementedException>(() => connection.ChangeDatabase("newDatabase"));
    }

    [Fact]
    public void TestBeginTransaction_NotImplemented()
    {
        var connStr = "Server=localhost;Database=myDataBase;User Id=root;Password=root;";
        var connection = new MySqlConnection(connStr);

        Assert.Throws<NotImplementedException>(() => connection.BeginTransaction(IsolationLevel.ReadCommitted));
    }

    [Fact]
    public void TestGetSchema()
    {
        var connStr = _ConnStr;
        using var connection = new MySqlConnection(connStr);

        connection.Open();

        var dt = connection.GetSchema();
        Assert.NotNull(dt);
    }
}
