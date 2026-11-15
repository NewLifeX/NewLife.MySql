using System.ComponentModel;
using NewLife.MySql;
using NewLife.MySql.Common;

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
        Assert.True(builder.AllowMultiStatements);
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
    [DisplayName("SslMode默认值为Preferred")]
    public void TestSslModeDefault()
    {
        var builder = new MySqlConnectionStringBuilder();
        Assert.Equal("Preferred", builder.SslMode);
    }

    [Theory]
    [DisplayName("常见UTF-8排序规则可解析")]
    [InlineData("utf8mb4_0900_ai_ci", MySqlCharSet.Utf8Mb4)]
    [InlineData("utf8mb4_bin", MySqlCharSet.Utf8Mb4)]
    [InlineData("utf8mb4_unicode_520_ci", MySqlCharSet.Utf8Mb4)]
    [InlineData("utf8_unicode_ci", MySqlCharSet.Utf8)]
    [InlineData("utf8mb3_general_ci", MySqlCharSet.Utf8)]
    public void ParseCharSet_Utf8Collations(String name, MySqlCharSet expected)
    {
        Assert.Equal(expected, MySqlConnectionStringBuilder.ParseCharSet(name));
    }

    [Fact]
    [DisplayName("未知字符集名称直接报错")]
    public void UnknownCharSet_Throws()
    {
        var builder = new MySqlConnectionStringBuilder("charset=gb18030");

        Assert.Throws<ArgumentException>(() => _ = builder.CharSet);
    }

    [Fact]
    [DisplayName("TracePacketPayload默认值为false")]
    public void TestTracePacketPayloadDefault()
    {
        var builder = new MySqlConnectionStringBuilder();

        Assert.False(builder.TracePacketPayload);
    }

    [Fact]
    [DisplayName("TracePacketPayload别名可正确解析")]
    public void TestTracePacketPayloadAlias()
    {
        var builder = new MySqlConnectionStringBuilder("tracepacketpayload=true");

        Assert.True(builder.TracePacketPayload);
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

    [Fact]
    [DisplayName("连接池标识对不同连接串无歧义")]
    public void TestPoolKeyNoCollision()
    {
        // 值中包含 ; 时，旧的 key=value; 拼接会与“拆成两个键”的连接串撞车：
        // A 的数据库名是 a;UserID=u，B 的数据库是 a 且用户是 u
        var a = new MySqlConnectionStringBuilder("server=s;database='a;UserID=u'").GetPoolKey();
        var b = new MySqlConnectionStringBuilder("server=s;database=a;uid=u").GetPoolKey();

        Assert.NotEqual(a, b);
    }

    [Fact]
    [DisplayName("解析连接字符串后仍保留默认值")]
    public void ParsedConnectionString_KeepsDefaults()
    {
        var builder = new MySqlConnectionStringBuilder("server=s");

        Assert.Equal(3306, builder.Port);
        Assert.Equal(15, builder.ConnectionTimeout);
        Assert.Equal(30, builder.CommandTimeout);
        Assert.Equal("Preferred", builder.SslMode);
        Assert.True(builder.RetryOnNetworkFailure);
        Assert.Equal(600, builder.ConnectionLifeTime);
        Assert.Equal(60, builder.IdlePoolTime);
    }

    [Fact]
    [DisplayName("连接池标识不含明文口令且能区分不同口令")]
    public void TestPoolKeyMaskPassword()
    {
        var a1 = new MySqlConnectionStringBuilder("server=s;database=d;uid=u;pwd=secret123").GetPoolKey();
        var a2 = new MySqlConnectionStringBuilder("server=s;database=d;uid=u;pwd=secret123").GetPoolKey();
        var b = new MySqlConnectionStringBuilder("server=s;database=d;uid=u;pwd=other456").GetPoolKey();

        Assert.Equal(a1, a2);
        Assert.NotEqual(a1, b);
        Assert.DoesNotContain("secret123", a1);
        Assert.DoesNotContain("other456", b);
    }
}