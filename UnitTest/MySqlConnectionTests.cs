using System.Data;
using NewLife.MySql;

namespace UnitTest;

public class MySqlConnectionTests
{
    [Fact]
    public void TestOpenConnection()
    {
        var connStr = "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;";
        var connection = new MySqlConnection(connStr);

        Assert.Equal(ConnectionState.Closed, connection.State);

        connection.Open();

        Assert.Equal(ConnectionState.Open, connection.State);

        connection.Close();

        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public void TestCloseConnection()
    {
        var connStr = "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;";
        var connection = new MySqlConnection(connStr);

        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);

        connection.Close();
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public void TestChangeDatabase_NotImplemented()
    {
        var connStr = "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;";
        var connection = new MySqlConnection(connStr);

        Assert.Throws<NotImplementedException>(() => connection.ChangeDatabase("newDatabase"));
    }

    [Fact]
    public void TestBeginTransaction_NotImplemented()
    {
        var connStr = "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;";
        var connection = new MySqlConnection(connStr);

        Assert.Throws<NotImplementedException>(() => connection.BeginTransaction(IsolationLevel.ReadCommitted));
    }
}
