using System.ComponentModel;
using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using NewLife.MySql;
using NewLife.MySql.EntityFramework;

namespace UnitTest.EntityFramework;

/// <summary>EF6 提供程序注册测试</summary>
public class MySqlEFConfigurationTests
{
    [Fact]
    [DisplayName("配置类应继承DbConfiguration")]
    public void Configuration_ShouldExtendDbConfiguration()
    {
        var config = new MySqlEFConfiguration();

        Assert.IsAssignableFrom<DbConfiguration>(config);
    }

    [Fact]
    [DisplayName("连接工厂应创建MySqlConnection")]
    public void ConnectionFactory_ShouldCreateMySqlConnection()
    {
        var factory = new MySqlConnectionFactory();
        var connStr = "Server=localhost;Port=3306;Database=test;User Id=root;Password=pass;";

        var conn = factory.CreateConnection(connStr);

        Assert.NotNull(conn);
        Assert.IsType<MySqlConnection>(conn);

        // 连接字符串经过 MySqlConnectionStringBuilder 标准化，键名可能变化，验证解析后的属性值
        var builder = new MySqlConnectionStringBuilder(conn.ConnectionString);
        Assert.Equal("localhost", builder.Server);
        Assert.Equal(3306, builder.Port);
        Assert.Equal("test", builder.Database);
        Assert.Equal("root", builder.UserID);
        Assert.Equal("pass", builder.Password);
    }

    [Fact]
    [DisplayName("连接工厂应通过数据库名创建连接")]
    public void ConnectionFactory_ShouldCreateConnectionByDatabaseName()
    {
        var factory = new MySqlConnectionFactory
        {
            BaseConnectionString = "Server=localhost;Port=3306;User Id=root;Password=pass;"
        };

        var conn = factory.CreateConnection("mydb");

        Assert.NotNull(conn);
        Assert.IsType<MySqlConnection>(conn);
        Assert.Contains("mydb", conn.ConnectionString, StringComparison.OrdinalIgnoreCase);
    }
}
