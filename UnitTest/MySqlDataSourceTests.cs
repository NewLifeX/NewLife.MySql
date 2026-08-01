using System.ComponentModel;
using NewLife.MySql;

namespace UnitTest;

/// <summary>MySqlDataSource 测试。验证 DbDataSource 公共构造与连接/命令工厂</summary>
[Collection(TestCollections.InMemory)]
public class MySqlDataSourceTests
{
    private const String ConnStr = "Server=localhost;Port=3306;Database=sys;UserID=root;Password=root";

    [Fact]
    [DisplayName("公共构造创建数据源")]
    public void CreateDataSource()
    {
        var ds = new MySqlDataSource(ConnStr);

        Assert.Equal(ConnStr, ds.ConnectionString);
    }

    [Fact]
    [DisplayName("CreateDbConnection返回MySqlConnection")]
    public void CreateConnection()
    {
        var ds = new MySqlDataSource(ConnStr);

        using var conn = ds.CreateConnection();
        Assert.IsType<MySqlConnection>(conn);
        Assert.Equal(ConnStr, conn.ConnectionString);
        Assert.Equal("sys", conn.Database);
    }

    [Fact]
    [DisplayName("CreateCommand设置命令文本")]
    public void CreateCommand()
    {
        var ds = new MySqlDataSource(ConnStr);

        using var cmd = ds.CreateCommand("SELECT 1");
        Assert.IsType<MySqlCommand>(cmd);
        Assert.Equal("SELECT 1", cmd.CommandText);
    }
}
