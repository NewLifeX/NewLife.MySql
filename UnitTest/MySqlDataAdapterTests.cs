using System.ComponentModel;
using NewLife.MySql;

namespace UnitTest;

/// <summary>MySqlDataAdapter 纯内存构造测试</summary>
[Collection(TestCollections.InMemory)]
public class MySqlDataAdapterTests
{
    [Fact]
    [DisplayName("默认构造函数创建实例")]
    public void WhenNewDefaultThenSelectCommandIsNull()
    {
        var adapter = new MySqlDataAdapter();

        Assert.Null(adapter.SelectCommand);
    }

    [Fact]
    [DisplayName("使用MySqlCommand构造")]
    public void WhenNewWithCommandThenSelectCommandIsSet()
    {
        var cmd = new MySqlCommand { CommandText = "SELECT 1" };
        var adapter = new MySqlDataAdapter(cmd);

        Assert.Same(cmd, adapter.SelectCommand);
    }

    [Fact]
    [DisplayName("使用SQL文本和连接构造")]
    public void WhenNewWithSqlAndConnectionThenBothSet()
    {
        var conn = new MySqlConnection("Server=localhost;Database=test;uid=root;pwd=root");
        var adapter = new MySqlDataAdapter("SELECT 1", conn);

        Assert.NotNull(adapter.SelectCommand);
        Assert.Equal("SELECT 1", adapter.SelectCommand!.CommandText);
        Assert.Same(conn, adapter.SelectCommand.Connection);
    }

    [Fact]
    [DisplayName("使用SQL文本和连接字符串构造")]
    public void WhenNewWithSqlAndConnStrThenBothSet()
    {
        var connStr = "Server=localhost;Database=test;uid=root;pwd=root";
        var adapter = new MySqlDataAdapter("SELECT 1", connStr);

        Assert.NotNull(adapter.SelectCommand);
        Assert.Equal("SELECT 1", adapter.SelectCommand!.CommandText);
        Assert.NotNull(adapter.SelectCommand.Connection);
    }

    [Fact]
    [DisplayName("通过工厂创建DataAdapter")]
    public void WhenCreateFromFactoryThenCorrectType()
    {
        var factory = MySqlClientFactory.Instance;
        var adapter = factory.CreateDataAdapter();

        Assert.NotNull(adapter);
        Assert.IsType<MySqlDataAdapter>(adapter);
    }
}
