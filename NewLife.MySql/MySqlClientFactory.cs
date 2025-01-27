using System.Data.Common;

namespace NewLife.MySql;

/// <summary>客户端工厂</summary>
public sealed partial class MySqlClientFactory : DbProviderFactory
{
    /// <summary>默认实例</summary>
    public static MySqlClientFactory Instance = new();

    /// <summary>创建命令</summary>
    /// <returns></returns>
    public override DbCommand CreateCommand() => new MySqlCommand();

    /// <summary>创建连接</summary>
    /// <returns></returns>
    public override DbConnection CreateConnection() => new MySqlConnection();

    /// <summary>创建参数</summary>
    /// <returns></returns>
    public override DbParameter CreateParameter() => new MySqlParameter();

    /// <summary>创建连接构造器</summary>
    /// <returns></returns>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new MySqlConnectionStringBuilder();
}