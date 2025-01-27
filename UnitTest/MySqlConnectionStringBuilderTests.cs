using NewLife.MySql;

namespace UnitTest;

public class MySqlConnectionStringBuilderTests
{
    [Fact]
    public void TestDefaultConstructor()
    {
        var builder = new MySqlConnectionStringBuilder();

        Assert.Null(builder.Server);
        Assert.Equal(3306, builder.Port);
        Assert.Null(builder.Database);
        Assert.Null(builder.UserID);
        Assert.Null(builder.Password);
        Assert.Equal(15, builder.ConnectionTimeout);
        Assert.Equal(30, builder.CommandTimeout);
    }

    [Fact]
    public void TestConstructorWithConnectionString()
    {
        var connStr = "server=localhost;port=3306;database=testdb;uid=root;pwd=1234;connectiontimeout=15;command timeout=30";
        var builder = new MySqlConnectionStringBuilder(connStr);

        Assert.Equal("localhost", builder.Server);
        Assert.Equal(3306, builder.Port);
        Assert.Equal("testdb", builder.Database);
        Assert.Equal("root", builder.UserID);
        Assert.Equal("1234", builder.Password);
        Assert.Equal(15, builder.ConnectionTimeout);
        Assert.Equal(30, builder.CommandTimeout);
    }

    [Fact]
    public void TestIndexerGetSet()
    {
        var builder = new MySqlConnectionStringBuilder
        {
            ["server"] = "localhost",
            ["port"] = 3306,
            ["database"] = "testdb",
            ["uid"] = "root",
            ["pwd"] = "1234",
            ["connectiontimeout"] = 15,
            ["command timeout"] = 30
        };

        Assert.Equal("localhost", builder.Server);
        Assert.Equal(3306, builder.Port);
        Assert.Equal("testdb", builder.Database);
        Assert.Equal("root", builder.UserID);
        Assert.Equal("1234", builder.Password);
        Assert.Equal(15, builder.ConnectionTimeout);
        Assert.Equal(30, builder.CommandTimeout);
    }
}