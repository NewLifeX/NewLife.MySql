#if NET7_0_OR_GREATER
using System.Data.Common;

namespace NewLife.MySql;

/// <summary>MySQL 数据源。提供与连接字符串关联的 <see cref="MySqlConnection"/> 工厂，支持 .NET 7+ DbDataSource 模式</summary>
/// <remarks>
/// DbDataSource 是 .NET 7 引入的标准数据源抽象，提供连接池管理和 DI 集成。
/// 通过 <c>new MySqlDataSource(connectionString)</c> 创建实例并注册到 DI 容器。
/// </remarks>
public sealed class MySqlDataSource : DbDataSource
{
    private readonly MySqlConnectionStringBuilder _settings;

    /// <summary>实例化数据源</summary>
    /// <param name="connectionString">连接字符串</param>
    internal MySqlDataSource(String connectionString)
    {
        _settings = new MySqlConnectionStringBuilder { ConnectionString = connectionString };
    }

    /// <summary>连接字符串</summary>
    public override String ConnectionString => _settings.ConnectionString;

    /// <summary>创建新连接</summary>
    /// <returns>MySQL 连接实例</returns>
    protected override DbConnection CreateDbConnection()
    {
        return new MySqlConnection { ConnectionString = ConnectionString };
    }

    /// <summary>异步创建并打开新连接</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>已打开的 MySQL 连接实例</returns>
    protected override async ValueTask<DbConnection> OpenDbConnectionAsync(CancellationToken cancellationToken = default)
    {
        var conn = new MySqlConnection { ConnectionString = ConnectionString };
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        return conn;
    }

    /// <summary>创建命令</summary>
    /// <param name="commandText">SQL 语句</param>
    /// <returns>MySQL 命令实例</returns>
    public new MySqlCommand CreateCommand(String? commandText = null)
    {
        var cmd = (MySqlCommand)base.CreateCommand();
        if (commandText != null) cmd.CommandText = commandText;
        return cmd;
    }
}
#endif
