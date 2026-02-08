using System.ComponentModel;
using System.Data;
using NewLife;
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

    [Fact]
    [DisplayName("OnCreate在Setting为null时抛出异常")]
    public void WhenSettingNullThenOnCreateThrows()
    {
        var pool = new MySqlPool();

        Assert.Throws<ArgumentNullException>(() => pool.Get());
    }

    [Fact]
    [DisplayName("OnCreate使用Setting创建SqlClient")]
    public void WhenSettingProvidedThenClientHasSameSetting()
    {
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "localhost",
            Port = 3306,
            UserID = "root",
            Password = "test",
        };
        var pool = new MySqlPool { Setting = setting };

        var client = pool.Get();

        Assert.NotNull(client);
        Assert.Same(setting, client.Setting);

        client.Dispose();
    }

    [Fact]
    [DisplayName("Variables缓存设置后可读取")]
    public void WhenVariablesSetThenCanGet()
    {
        var pool = new MySqlPool();
        var vars = new Dictionary<String, String> { ["max_allowed_packet"] = "67108864" };

        pool.Variables = vars;

        Assert.NotNull(pool.Variables);
        Assert.Same(vars, pool.Variables);
    }

    [Fact]
    [DisplayName("Variables未设置时返回null")]
    public void WhenVariablesNotSetThenReturnsNull()
    {
        var pool = new MySqlPool();

        Assert.Null(pool.Variables);
    }

    [Fact]
    [DisplayName("Variables设置null后返回null")]
    public void WhenVariablesSetNullThenReturnsNull()
    {
        var pool = new MySqlPool();
        pool.Variables = new Dictionary<String, String> { ["key"] = "value" };
        pool.Variables = null;

        // 设置 null 后内部值为 null，getter 返回 null
        Assert.Null(pool.Variables);
    }

    [Fact]
    [DisplayName("CreatePool设置默认连接池参数")]
    public void WhenCreatePoolThenDefaultSettingsApplied()
    {
        var manager = new MySqlPoolManager();
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "localhost",
            Port = 3306,
            UserID = "root",
            Password = "test",
        };

        var pool = manager.GetPool(setting);

        Assert.NotNull(pool);
        Assert.Same(setting, pool.Setting);
        Assert.Equal(10, pool.Min);
        Assert.Equal(100000, pool.Max);
        Assert.Equal(30, pool.IdleTime);
        Assert.Equal(300, pool.AllIdleTime);
    }

    [Fact]
    [DisplayName("相同连接字符串返回同一个连接池")]
    public void WhenSameConnectionStringThenSamePoolReturned()
    {
        var manager = new MySqlPoolManager();
        var connStr = "Server=localhost;Port=3306;UserID=root;Password=test";
        var setting1 = new MySqlConnectionStringBuilder(connStr);
        var setting2 = new MySqlConnectionStringBuilder(connStr);

        var pool1 = manager.GetPool(setting1);
        var pool2 = manager.GetPool(setting2);

        Assert.Same(pool1, pool2);
    }

    [Fact]
    [DisplayName("不同连接字符串返回不同连接池")]
    public void WhenDifferentConnectionStringThenDifferentPoolReturned()
    {
        var manager = new MySqlPoolManager();
        var setting1 = new MySqlConnectionStringBuilder
        {
            Server = "host1",
            Port = 3306,
            UserID = "root",
            Password = "test",
        };
        var setting2 = new MySqlConnectionStringBuilder
        {
            Server = "host2",
            Port = 3306,
            UserID = "root",
            Password = "test",
        };

        var pool1 = manager.GetPool(setting1);
        var pool2 = manager.GetPool(setting2);

        Assert.NotSame(pool1, pool2);
    }

    [Fact]
    [DisplayName("Get获取新连接时Welcome为null")]
    public void WhenGetNewClientThenWelcomeIsNull()
    {
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "localhost",
            Port = 3306,
            UserID = "root",
            Password = "test",
        };
        var pool = new MySqlPool { Setting = setting };

        var client = pool.Get();

        Assert.NotNull(client);
        Assert.Null(client.Welcome);
        Assert.False(client.Active);

        client.Dispose();
    }
}
