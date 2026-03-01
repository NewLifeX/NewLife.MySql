using System.ComponentModel;
using NewLife.MySql;

namespace UnitTest;

[Collection(TestCollections.InMemory)]
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

    [Theory]
    [InlineData("datasource")]
    [InlineData("data source")]
    [InlineData("server")]
    [DisplayName("Server属性支持多种别名")]
    public void TestServerAliases(String alias)
    {
        var builder = new MySqlConnectionStringBuilder();
        builder[alias] = "myhost";
        Assert.Equal("myhost", builder.Server);
    }

    [Theory]
    [InlineData("uid")]
    [InlineData("user id")]
    [InlineData("userid")]
    [InlineData("user")]
    [InlineData("username")]
    [InlineData("user name")]
    [DisplayName("UserID属性支持多种别名")]
    public void TestUserIDAliases(String alias)
    {
        var builder = new MySqlConnectionStringBuilder();
        builder[alias] = "admin";
        Assert.Equal("admin", builder.UserID);
    }

    [Theory]
    [InlineData("pass")]
    [InlineData("password")]
    [InlineData("pwd")]
    [DisplayName("Password属性支持多种别名")]
    public void TestPasswordAliases(String alias)
    {
        var builder = new MySqlConnectionStringBuilder();
        builder[alias] = "secret";
        Assert.Equal("secret", builder.Password);
    }

    [Theory]
    [InlineData("commandtimeout")]
    [InlineData("defaultcommandtimeout")]
    [InlineData("command timeout")]
    [InlineData("default command timeout")]
    [DisplayName("CommandTimeout属性支持多种别名")]
    public void TestCommandTimeoutAliases(String alias)
    {
        var builder = new MySqlConnectionStringBuilder();
        builder[alias] = 60;
        Assert.Equal(60, builder.CommandTimeout);
    }

    [Theory]
    [InlineData("connectiontimeout")]
    [InlineData("connection timeout")]
    [DisplayName("ConnectionTimeout属性支持多种别名")]
    public void TestConnectionTimeoutAliases(String alias)
    {
        var builder = new MySqlConnectionStringBuilder();
        builder[alias] = 20;
        Assert.Equal(20, builder.ConnectionTimeout);
    }

    [Theory]
    [InlineData("sslmode")]
    [InlineData("ssl mode")]
    [InlineData("ssl-mode")]
    [DisplayName("SslMode属性支持多种别名")]
    public void TestSslModeAliases(String alias)
    {
        var builder = new MySqlConnectionStringBuilder();
        builder[alias] = "Required";
        Assert.Equal("Required", builder.SslMode);
    }

    [Fact]
    [DisplayName("SslMode默认值为None")]
    public void TestSslModeDefault()
    {
        var builder = new MySqlConnectionStringBuilder();
        Assert.Null(builder.SslMode);
    }

    [Fact]
    [DisplayName("UseServerPrepare默认值为false")]
    public void TestUseServerPrepareDefault()
    {
        var builder = new MySqlConnectionStringBuilder();
        Assert.False(builder.UseServerPrepare);
    }

    [Fact]
    [DisplayName("Pipeline默认值为false")]
    public void TestPipelineDefault()
    {
        var builder = new MySqlConnectionStringBuilder();
        Assert.False(builder.Pipeline);
    }

    [Fact]
    [DisplayName("解析包含所有参数的连接字符串")]
    public void TestFullConnectionString()
    {
        var connStr = "server=myhost;port=3307;database=mydb;uid=admin;pwd=secret;connectiontimeout=20;commandtimeout=60;sslmode=Required;useserverprepare=true;pipeline=true";
        var builder = new MySqlConnectionStringBuilder(connStr);

        Assert.Equal("myhost", builder.Server);
        Assert.Equal(3307, builder.Port);
        Assert.Equal("mydb", builder.Database);
        Assert.Equal("admin", builder.UserID);
        Assert.Equal("secret", builder.Password);
        Assert.Equal(20, builder.ConnectionTimeout);
        Assert.Equal(60, builder.CommandTimeout);
        Assert.Equal("Required", builder.SslMode);
        Assert.True(builder.UseServerPrepare);
        Assert.True(builder.Pipeline);
    }

    [Fact]
    [DisplayName("Pipeline别名pipelining可正确解析")]
    public void TestPipelineAlias()
    {
        var builder = new MySqlConnectionStringBuilder();
        builder["pipelining"] = true;
        Assert.True(builder.Pipeline);
    }
}