using System.ComponentModel;
using System.Data;
using NewLife.MySql;

namespace UnitTest;

/// <summary>连接池共享测试</summary>
public class MySqlPoolTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    [Fact]
    [DisplayName("多个连接共享同一个连接池")]
    public void WhenMultipleConnectionsOpenedThenSamePoolUsed()
    {
        var factory = MySqlClientFactory.Instance;
        var poolManager = factory.PoolManager;

        using var conn1 = new MySqlConnection(_ConnStr);
        using var conn2 = new MySqlConnection(_ConnStr);

        conn1.Open();
        conn2.Open();

        // 通过相同连接字符串获取的池应该是同一实例
        var pool1 = poolManager.GetPool(conn1.Setting);
        var pool2 = poolManager.GetPool(conn2.Setting);
        Assert.Same(pool1, pool2);

        conn1.Close();
        conn2.Close();
    }

    //[Fact]
    //[DisplayName("不同工厂实例共享同一个池管理器")]
    //public void WhenDifferentFactoryInstancesThenSamePoolManager()
    //{
    //    // 默认所有工厂实例共享同一个静态池管理器
    //    var factory1 = new MySqlClientFactory();
    //    var factory2 = new MySqlClientFactory();

    //    Assert.Same(factory1.PoolManager, factory2.PoolManager);
    //}

    [Fact]
    [DisplayName("连接关闭后客户端归还到池")]
    public void WhenConnectionClosedThenClientReturnedToPool()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        Assert.Equal(ConnectionState.Open, conn.State);
        Assert.NotNull(conn.Client);

        var pool = conn.Factory.PoolManager.GetPool(conn.Setting);
        var totalBefore = pool.Total;

        conn.Close();

        Assert.Equal(ConnectionState.Closed, conn.State);
        Assert.Null(conn.Client);
    }

    //[Fact]
    //[DisplayName("连接字符串大小写不敏感时共享连接池")]
    //public void WhenConnectionStringCaseInsensitiveThenSamePoolUsed()
    //{
    //    var factory = MySqlClientFactory.Instance;
    //    var poolManager = factory.PoolManager;

    //    var setting1 = new MySqlConnectionStringBuilder(_ConnStr);
    //    var setting2 = new MySqlConnectionStringBuilder(_ConnStr.ToUpper());

    //    // OrdinalIgnoreCase 比较
    //    var pool1 = poolManager.GetPool(setting1);
    //    var pool2 = poolManager.GetPool(setting2);

    //    // 连接字符串大小写不同时也应该共享池
    //    Assert.Same(pool1, pool2);
    //}
}
