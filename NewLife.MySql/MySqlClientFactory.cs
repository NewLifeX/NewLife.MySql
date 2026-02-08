using System.Data.Common;

namespace NewLife.MySql;

/// <summary>客户端工厂</summary>
public sealed partial class MySqlClientFactory : DbProviderFactory
{
    /// <summary>默认实例</summary>
    public static MySqlClientFactory Instance = new();

    static MySqlClientFactory()
    {
#if NETSTANDARD2_1_OR_GREATER
        DbProviderFactories.RegisterFactory("NewLife.MySql.MySqlClient", Instance);
#endif
    }

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
    public override DbDataAdapter CreateDataAdapter() => new MySqlDataAdapter();

    /// <summary>不支持创建数据源枚举</summary>
    public override Boolean CanCreateDataSourceEnumerator => false;

    /// <summary>连接池管理器</summary>
    public MySqlPoolManager PoolManager { get; set; } = new();
}