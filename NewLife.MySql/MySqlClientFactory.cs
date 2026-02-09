using System.Data.Common;

namespace NewLife.MySql;

/// <summary>客户端工厂</summary>
public sealed partial class MySqlClientFactory : DbProviderFactory
{
    /// <summary>默认实例</summary>
    public static MySqlClientFactory Instance = new();

    static MySqlClientFactory()
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        DbProviderFactories.RegisterFactory("NewLife.MySql.MySqlClient", Instance);
#endif
    }

    #region 属性
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    /// <summary>是否支持创建命令构建器。不支持</summary>
    public override Boolean CanCreateCommandBuilder => false;

    /// <summary>是否支持创建数据适配器</summary>
    public override Boolean CanCreateDataAdapter => true;
#endif

#if NET6_0_OR_GREATER
    /// <summary>是否支持创建批量命令</summary>
    public override Boolean CanCreateBatch => true;
#endif

    /// <summary>不支持创建数据源枚举</summary>
    public override Boolean CanCreateDataSourceEnumerator => false;

    /// <summary>连接池管理器</summary>
    public MySqlPoolManager PoolManager { get; set; } = new();
    #endregion

    #region 方法
    /// <summary>创建命令</summary>
    /// <returns></returns>
    public override DbCommand CreateCommand() => new MySqlCommand();

    /// <summary>创建连接</summary>
    /// <returns></returns>
    public override DbConnection CreateConnection() => new MySqlConnection { Factory = this };

    /// <summary>创建参数</summary>
    /// <returns></returns>
    public override DbParameter CreateParameter() => new MySqlParameter();

    /// <summary>创建连接构造器</summary>
    /// <returns></returns>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new MySqlConnectionStringBuilder();

    /// <summary>创建数据适配器</summary>
    /// <returns></returns>
    public override DbDataAdapter CreateDataAdapter() => new MySqlDataAdapter();

#if NET6_0_OR_GREATER
    /// <summary>创建批量命令</summary>
    /// <returns></returns>
    public override DbBatch CreateBatch() => new MySqlBatch();

    /// <summary>创建批量命令项</summary>
    /// <returns></returns>
    public override DbBatchCommand CreateBatchCommand() => new MySqlBatchCommand();
#endif
    #endregion
}