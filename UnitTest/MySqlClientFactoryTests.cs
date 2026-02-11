using System.Data.Common;
using NewLife.MySql;

namespace UnitTest;

[Collection(TestCollections.InMemory)]
public class MySqlClientFactoryTests
{
    #region 单例与注册
    [Fact]
    public void Instance_ShouldReturnSameReference()
    {
        var a = MySqlClientFactory.Instance;
        var b = MySqlClientFactory.Instance;

        Assert.NotNull(a);
        Assert.Same(a, b);
    }

    [Fact]
    public void DbProviderFactories_ShouldResolveByInvariantName()
    {
        var factory = DbProviderFactories.GetFactory("NewLife.MySql.MySqlClient");

        Assert.NotNull(factory);
        Assert.IsType<MySqlClientFactory>(factory);
    }
    #endregion

    #region 能力属性
    [Fact]
    public void CanCreateCommandBuilder_ShouldReturnFalse()
    {
        Assert.False(MySqlClientFactory.Instance.CanCreateCommandBuilder);
    }

    [Fact]
    public void CanCreateDataAdapter_ShouldReturnTrue()
    {
        Assert.True(MySqlClientFactory.Instance.CanCreateDataAdapter);
    }

    [Fact]
    public void CanCreateBatch_ShouldReturnTrue()
    {
        Assert.True(MySqlClientFactory.Instance.CanCreateBatch);
    }

    [Fact]
    public void CanCreateDataSourceEnumerator_ShouldReturnFalse()
    {
        Assert.False(MySqlClientFactory.Instance.CanCreateDataSourceEnumerator);
    }
    #endregion

    #region 创建方法
    [Fact]
    public void CreateCommand_ShouldReturnMySqlCommand()
    {
        var command = MySqlClientFactory.Instance.CreateCommand();

        Assert.NotNull(command);
        Assert.IsType<MySqlCommand>(command);
    }

    [Fact]
    public void CreateConnection_ShouldReturnMySqlConnection()
    {
        var connection = MySqlClientFactory.Instance.CreateConnection();

        Assert.NotNull(connection);
        Assert.IsType<MySqlConnection>(connection);
    }

    [Fact]
    public void CreateConnection_ShouldSetFactory()
    {
        var conn = MySqlClientFactory.Instance.CreateConnection() as MySqlConnection;

        Assert.NotNull(conn);
        Assert.Same(MySqlClientFactory.Instance, conn.Factory);
    }

    [Fact]
    public void CreateParameter_ShouldReturnMySqlParameter()
    {
        var parameter = MySqlClientFactory.Instance.CreateParameter();

        Assert.NotNull(parameter);
        Assert.IsType<MySqlParameter>(parameter);
    }

    [Fact]
    public void CreateConnectionStringBuilder_ShouldReturnMySqlConnectionStringBuilder()
    {
        var builder = MySqlClientFactory.Instance.CreateConnectionStringBuilder();

        Assert.NotNull(builder);
        Assert.IsType<MySqlConnectionStringBuilder>(builder);
    }

    [Fact]
    public void CreateDataAdapter_ShouldReturnMySqlDataAdapter()
    {
        var adapter = MySqlClientFactory.Instance.CreateDataAdapter();

        Assert.NotNull(adapter);
        Assert.IsType<MySqlDataAdapter>(adapter);
    }

    [Fact]
    public void CreateBatch_ShouldReturnMySqlBatch()
    {
        var batch = MySqlClientFactory.Instance.CreateBatch();

        Assert.NotNull(batch);
        Assert.IsType<MySqlBatch>(batch);
    }

    [Fact]
    public void CreateBatchCommand_ShouldReturnMySqlBatchCommand()
    {
        var batchCommand = MySqlClientFactory.Instance.CreateBatchCommand();

        Assert.NotNull(batchCommand);
        Assert.IsType<MySqlBatchCommand>(batchCommand);
    }
    #endregion

    #region 多次创建独立实例
    [Fact]
    public void CreateCommand_ShouldReturnNewInstanceEachTime()
    {
        var factory = MySqlClientFactory.Instance;
        var a = factory.CreateCommand();
        var b = factory.CreateCommand();

        Assert.NotSame(a, b);
    }

    [Fact]
    public void CreateConnection_ShouldReturnNewInstanceEachTime()
    {
        var factory = MySqlClientFactory.Instance;
        var a = factory.CreateConnection();
        var b = factory.CreateConnection();

        Assert.NotSame(a, b);
    }

    [Fact]
    public void CreateParameter_ShouldReturnNewInstanceEachTime()
    {
        var factory = MySqlClientFactory.Instance;
        var a = factory.CreateParameter();
        var b = factory.CreateParameter();

        Assert.NotSame(a, b);
    }

    [Fact]
    public void CreateBatch_ShouldReturnNewInstanceEachTime()
    {
        var factory = MySqlClientFactory.Instance;
        var a = factory.CreateBatch();
        var b = factory.CreateBatch();

        Assert.NotSame(a, b);
    }
    #endregion

    #region PoolManager
    [Fact]
    public void PoolManager_ShouldNotBeNull()
    {
        Assert.NotNull(MySqlClientFactory.Instance.PoolManager);
    }

    [Fact]
    public void PoolManager_ShouldBeSettable()
    {
        var factory = new MySqlClientFactory();
        var manager = new MySqlPoolManager();

        factory.PoolManager = manager;

        Assert.Same(manager, factory.PoolManager);
    }
    #endregion

    #region 通过基类接口调用
    [Fact]
    public void AsDbProviderFactory_CreateCommand_ShouldWork()
    {
        DbProviderFactory factory = MySqlClientFactory.Instance;

        var cmd = factory.CreateCommand();

        Assert.IsType<MySqlCommand>(cmd);
    }

    [Fact]
    public void AsDbProviderFactory_CreateConnection_ShouldWork()
    {
        DbProviderFactory factory = MySqlClientFactory.Instance;

        var conn = factory.CreateConnection();

        Assert.IsType<MySqlConnection>(conn);
    }

    [Fact]
    public void AsDbProviderFactory_CreateBatch_ShouldWork()
    {
        DbProviderFactory factory = MySqlClientFactory.Instance;

        var batch = factory.CreateBatch();

        Assert.IsType<MySqlBatch>(batch);
    }
    #endregion
}