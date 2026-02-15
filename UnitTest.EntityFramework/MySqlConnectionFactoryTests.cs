using System.ComponentModel;
using NewLife.MySql;
using NewLife.MySql.EntityFramework;

namespace UnitTest.EntityFramework;

/// <summary>连接工厂补充测试</summary>
public class MySqlConnectionFactoryTests
{
    [Fact]
    [DisplayName("完整连接字符串应直接使用")]
    public void CreateConnection_FullConnectionString_ShouldUseDirect()
    {
        var factory = new MySqlConnectionFactory();
        var connStr = "Server=db.example.com;Port=3307;Database=myapp;User Id=admin;Password=secret;";

        var conn = factory.CreateConnection(connStr);

        Assert.NotNull(conn);
        Assert.IsType<MySqlConnection>(conn);
        var builder = new MySqlConnectionStringBuilder(conn.ConnectionString);
        Assert.Equal("db.example.com", builder.Server);
        Assert.Equal(3307, builder.Port);
        Assert.Equal("myapp", builder.Database);
    }

    [Fact]
    [DisplayName("数据库名不含分号应拼接到基础连接字符串")]
    public void CreateConnection_DatabaseName_ShouldAppendToBase()
    {
        var factory = new MySqlConnectionFactory
        {
            BaseConnectionString = "Server=localhost;Port=3306;User Id=root;Password=;"
        };

        var conn = factory.CreateConnection("testdb");

        Assert.NotNull(conn);
        Assert.IsType<MySqlConnection>(conn);
        Assert.Contains("testdb", conn.ConnectionString, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("默认BaseConnectionString应连接到localhost")]
    public void DefaultBaseConnectionString_ShouldPointToLocalhost()
    {
        var factory = new MySqlConnectionFactory();

        Assert.Contains("localhost", factory.BaseConnectionString);
        Assert.Contains("3306", factory.BaseConnectionString);
    }

    [Fact]
    [DisplayName("自定义BaseConnectionString应生效")]
    public void CustomBaseConnectionString_ShouldTakeEffect()
    {
        var factory = new MySqlConnectionFactory
        {
            BaseConnectionString = "Server=remote;Port=3307;User Id=test;Password=pass;"
        };

        var conn = factory.CreateConnection("customdb");
        var builder = new MySqlConnectionStringBuilder(conn.ConnectionString);

        Assert.Equal("remote", builder.Server);
        Assert.Equal(3307, builder.Port);
        Assert.Equal("customdb", builder.Database);
    }
}

/// <summary>EF6 配置测试</summary>
public class MySqlEFConfigurationTests2
{
    [Fact]
    [DisplayName("ProviderInvariantName应为NewLife.MySql.MySqlClient")]
    public void ProviderInvariantName_ShouldBeCorrect()
    {
        Assert.Equal("NewLife.MySql.MySqlClient", MySqlEFConfiguration.ProviderInvariantName);
    }

    [Fact]
    [DisplayName("配置类应能实例化")]
    public void Configuration_ShouldBeInstantiable()
    {
        var config = new MySqlEFConfiguration();

        Assert.NotNull(config);
    }
}

/// <summary>执行策略测试</summary>
public class MySqlExecutionStrategyTests
{
    [Fact]
    [DisplayName("执行策略应能实例化")]
    public void ExecutionStrategy_ShouldBeInstantiable()
    {
        var strategy = new MySqlExecutionStrategy();

        Assert.NotNull(strategy);
    }

    [Fact]
    [DisplayName("自定义重试参数应能实例化")]
    public void ExecutionStrategy_CustomParams_ShouldBeInstantiable()
    {
        var strategy = new MySqlExecutionStrategy(5, TimeSpan.FromSeconds(60));

        Assert.NotNull(strategy);
    }

    [Fact]
    [DisplayName("死锁异常应触发重试")]
    public void ShouldRetryOn_DeadlockException_ShouldReturnTrue()
    {
        var strategy = new TestableMySqlExecutionStrategy();
        var ex = new MySqlException(1213, "Deadlock found");

        Assert.True(strategy.TestShouldRetryOn(ex));
    }

    [Fact]
    [DisplayName("锁等待超时应触发重试")]
    public void ShouldRetryOn_LockWaitTimeout_ShouldReturnTrue()
    {
        var strategy = new TestableMySqlExecutionStrategy();
        var ex = new MySqlException(1205, "Lock wait timeout exceeded");

        Assert.True(strategy.TestShouldRetryOn(ex));
    }

    [Fact]
    [DisplayName("连接过多应触发重试")]
    public void ShouldRetryOn_TooManyConnections_ShouldReturnTrue()
    {
        var strategy = new TestableMySqlExecutionStrategy();
        var ex = new MySqlException(1040, "Too many connections");

        Assert.True(strategy.TestShouldRetryOn(ex));
    }

    [Fact]
    [DisplayName("普通异常不应触发重试")]
    public void ShouldRetryOn_GenericException_ShouldReturnFalse()
    {
        var strategy = new TestableMySqlExecutionStrategy();
        var ex = new InvalidOperationException("some error");

        Assert.False(strategy.TestShouldRetryOn(ex));
    }

    [Fact]
    [DisplayName("非瞬时MySql错误不应触发重试")]
    public void ShouldRetryOn_NonTransientMySqlError_ShouldReturnFalse()
    {
        var strategy = new TestableMySqlExecutionStrategy();
        var ex = new MySqlException(1045, "Access denied");

        Assert.False(strategy.TestShouldRetryOn(ex));
    }

    [Fact]
    [DisplayName("内部异常为瞬时错误应触发重试")]
    public void ShouldRetryOn_InnerException_ShouldCheckRecursively()
    {
        var strategy = new TestableMySqlExecutionStrategy();
        var inner = new MySqlException(1213, "Deadlock found");
        var outer = new InvalidOperationException("wrapper", inner);

        Assert.True(strategy.TestShouldRetryOn(outer));
    }

    /// <summary>可测试的执行策略子类，暴露 ShouldRetryOn 方法</summary>
    private class TestableMySqlExecutionStrategy : MySqlExecutionStrategy
    {
        public Boolean TestShouldRetryOn(Exception ex) => ShouldRetryOn(ex);
    }
}
