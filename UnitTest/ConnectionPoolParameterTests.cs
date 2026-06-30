using NewLife.MySql;

namespace UnitTest;

/// <summary>连接池参数配置测试。验证 MinPoolSize/MaxPoolSize/LoadBalanceTimeout 连接字符串参数</summary>
[Collection(TestCollections.InMemory)]
public class ConnectionPoolParameterTests
{
    [Fact(DisplayName = "连接字符串_MinPoolSize默认值为0")]
    public void MinPoolSize_DefaultValue()
    {
        var builder = new MySqlConnectionStringBuilder();

        Assert.Equal(0, builder.MinPoolSize);
    }

    [Fact(DisplayName = "连接字符串_MaxPoolSize默认值为0")]
    public void MaxPoolSize_DefaultValue()
    {
        var builder = new MySqlConnectionStringBuilder();

        Assert.Equal(0, builder.MaxPoolSize);
    }

    [Fact(DisplayName = "连接字符串_LoadBalanceTimeout默认值为0")]
    public void LoadBalanceTimeout_DefaultValue()
    {
        var builder = new MySqlConnectionStringBuilder();

        Assert.Equal(0, builder.LoadBalanceTimeout);
    }

    [Fact(DisplayName = "连接字符串_MinPoolSize通过连接字符串设置")]
    public void MinPoolSize_SetViaConnectionString()
    {
        var builder = new MySqlConnectionStringBuilder("server=localhost;port=3306;MinPoolSize=5");

        Assert.Equal(5, builder.MinPoolSize);
    }

    [Fact(DisplayName = "连接字符串_MaxPoolSize通过连接字符串设置")]
    public void MaxPoolSize_SetViaConnectionString()
    {
        var builder = new MySqlConnectionStringBuilder("server=localhost;port=3306;MaxPoolSize=50");

        Assert.Equal(50, builder.MaxPoolSize);
    }

    [Fact(DisplayName = "连接字符串_LoadBalanceTimeout通过连接字符串设置")]
    public void LoadBalanceTimeout_SetViaConnectionString()
    {
        var builder = new MySqlConnectionStringBuilder("server=localhost;port=3306;LoadBalanceTimeout=300");

        Assert.Equal(300, builder.LoadBalanceTimeout);
    }

    [Fact(DisplayName = "连接字符串_MinPoolSize支持别名min pool size")]
    public void MinPoolSize_AliasMinPoolSize()
    {
        var builder = new MySqlConnectionStringBuilder();
        builder["min pool size"] = 10;

        Assert.Equal(10, builder.MinPoolSize);
    }

    [Fact(DisplayName = "连接字符串_MinPoolSize支持别名minimum pool size")]
    public void MinPoolSize_AliasMinimumPoolSize()
    {
        var builder = new MySqlConnectionStringBuilder();
        builder["minimum pool size"] = 3;

        Assert.Equal(3, builder.MinPoolSize);
    }

    [Fact(DisplayName = "连接字符串_MaxPoolSize支持别名max pool size")]
    public void MaxPoolSize_AliasMaxPoolSize()
    {
        var builder = new MySqlConnectionStringBuilder();
        builder["max pool size"] = 200;

        Assert.Equal(200, builder.MaxPoolSize);
    }

    [Fact(DisplayName = "连接字符串_MaxPoolSize支持别名maximum pool size")]
    public void MaxPoolSize_AliasMaximumPoolSize()
    {
        var builder = new MySqlConnectionStringBuilder();
        builder["maximum pool size"] = 150;

        Assert.Equal(150, builder.MaxPoolSize);
    }

    [Fact(DisplayName = "连接字符串_LoadBalanceTimeout支持别名load balance timeout")]
    public void LoadBalanceTimeout_AliasLoadBalanceTimeout()
    {
        var builder = new MySqlConnectionStringBuilder();
        builder["load balance timeout"] = 600;

        Assert.Equal(600, builder.LoadBalanceTimeout);
    }

    [Fact(DisplayName = "连接字符串_connection lifetime别名映射到ConnectionLifeTime")]
    public void ConnectionLifeTime_AliasConnectionLifetime()
    {
        var builder = new MySqlConnectionStringBuilder();
        builder["connection lifetime"] = 900;

        // connection lifetime 现归属 ConnectionLifeTime（MySQL/MySqlConnector 同名语义），不再映射到遗留的 LoadBalanceTimeout
        Assert.Equal(900, builder.ConnectionLifeTime);
    }

    [Fact(DisplayName = "连接字符串_ConnectionLifeTime默认值为600")]
    public void ConnectionLifeTime_DefaultValue()
    {
        var builder = new MySqlConnectionStringBuilder();

        Assert.Equal(600, builder.ConnectionLifeTime);
    }

    [Fact(DisplayName = "连接字符串_ConnectionLifeTime通过连接字符串设置")]
    public void ConnectionLifeTime_SetViaConnectionString()
    {
        var builder = new MySqlConnectionStringBuilder("server=localhost;port=3306;ConnectionLifeTime=120");

        Assert.Equal(120, builder.ConnectionLifeTime);
    }

    [Fact(DisplayName = "连接字符串_ConnectionLifeTime设为0禁用")]
    public void ConnectionLifeTime_Zero_Disables()
    {
        var builder = new MySqlConnectionStringBuilder("server=localhost;ConnectionLifeTime=0");

        Assert.Equal(0, builder.ConnectionLifeTime);
    }

    [Fact(DisplayName = "连接字符串_ConnectionIdlePingTime默认值为30")]
    public void ConnectionIdlePingTime_DefaultValue()
    {
        var builder = new MySqlConnectionStringBuilder();

        Assert.Equal(30, builder.ConnectionIdlePingTime);
    }

    [Fact(DisplayName = "连接字符串_ConnectionIdlePingTime通过连接字符串设置")]
    public void ConnectionIdlePingTime_SetViaConnectionString()
    {
        var builder = new MySqlConnectionStringBuilder("server=localhost;port=3306;ConnectionIdlePingTime=15");

        Assert.Equal(15, builder.ConnectionIdlePingTime);
    }

    [Fact(DisplayName = "连接字符串_ConnectionIdlePingTime支持别名idle ping time")]
    public void ConnectionIdlePingTime_AliasIdlePingTime()
    {
        var builder = new MySqlConnectionStringBuilder();
        builder["idle ping time"] = 45;

        Assert.Equal(45, builder.ConnectionIdlePingTime);
    }

    [Fact(DisplayName = "连接池_MinPoolSize=0时不预创建连接")]
    public void Pool_MinPoolSizeZero_NoPreCreate()
    {
        var poolManager = new MySqlPoolManager();
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "127.0.0.1",
            Port = 3306,
            UserID = "root",
            Password = "root",
            Database = "sys",
            MinPoolSize = 0
        };

        var pool = poolManager.GetPool(setting);

        Assert.NotNull(pool);
        Assert.Equal(0, pool.Min);
    }

    [Fact(DisplayName = "连接池_MaxPoolSize设置生效")]
    public void Pool_MaxPoolSize_Applied()
    {
        var poolManager = new MySqlPoolManager();
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "127.0.0.1",
            Port = 3306,
            UserID = "root",
            Password = "root",
            Database = "sys",
            MaxPoolSize = 25
        };

        var pool = poolManager.GetPool(setting);

        Assert.NotNull(pool);
        Assert.Equal(25, pool.Max);
    }

    [Fact(DisplayName = "连接池_未设置时使用默认Max=100")]
    public void Pool_DefaultMaxPoolSize()
    {
        var poolManager = new MySqlPoolManager();
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "127.0.0.1",
            Port = 3306,
            UserID = "root",
            Password = "root",
            Database = "sys"
        };

        var pool = poolManager.GetPool(setting);

        Assert.NotNull(pool);
        // 未设置 MaxPoolSize 时默认 100
        Assert.Equal(100, pool.Max);
    }

    [Fact(DisplayName = "连接池_IdleTime和AllIdleTime保持默认值")]
    public void Pool_IdleTime_Defaults()
    {
        var poolManager = new MySqlPoolManager();
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "127.0.0.1",
            Port = 3306,
            UserID = "root",
            Password = "root",
            Database = "sys"
        };

        var pool = poolManager.GetPool(setting);

        Assert.Equal(30, pool.IdleTime);
        Assert.Equal(300, pool.AllIdleTime);
    }

    [Fact(DisplayName = "连接池_不同连接字符串创建不同池")]
    public void Pool_DifferentConnectionStrings_DifferentPools()
    {
        var poolManager = new MySqlPoolManager();
        var setting1 = new MySqlConnectionStringBuilder
        {
            Server = "127.0.0.1",
            Port = 3306,
            UserID = "root",
            Password = "root",
            Database = "db1"
        };
        var setting2 = new MySqlConnectionStringBuilder
        {
            Server = "127.0.0.1",
            Port = 3306,
            UserID = "root",
            Password = "root",
            Database = "db2"
        };

        var pool1 = poolManager.GetPool(setting1);
        var pool2 = poolManager.GetPool(setting2);

        Assert.NotSame(pool1, pool2);
    }

    [Fact(DisplayName = "连接池_相同连接字符串共享同一池")]
    public void Pool_SameConnectionStrings_SamePool()
    {
        var poolManager = new MySqlPoolManager();
        var setting1 = new MySqlConnectionStringBuilder("server=127.0.0.1;port=3306;uid=root;pwd=root;database=sys;MinPoolSize=2;MaxPoolSize=10");
        var setting2 = new MySqlConnectionStringBuilder("server=127.0.0.1;port=3306;uid=root;pwd=root;database=sys;MinPoolSize=2;MaxPoolSize=10");

        var pool1 = poolManager.GetPool(setting1);
        var pool2 = poolManager.GetPool(setting2);

        Assert.Same(pool1, pool2);
    }
}
