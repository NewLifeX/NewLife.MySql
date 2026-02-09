#if NET6_0_OR_GREATER
using System.Data;
using System.Data.Common;

namespace NewLife.MySql;

/// <summary>批量命令。支持多条 SQL 语句在一次往返中执行，基于 ADO.NET DbBatch API</summary>
public class MySqlBatch : DbBatch
{
    #region 属性
    private MySqlConnection? _connection;

    /// <summary>连接</summary>
    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = value as MySqlConnection;
    }

    private DbTransaction? _transaction;

    /// <summary>事务</summary>
    protected override DbTransaction? DbTransaction
    {
        get => _transaction;
        set => _transaction = value;
    }

    /// <summary>超时时间（秒）</summary>
    public override Int32 Timeout { get; set; } = 30;

    private readonly MySqlBatchCommandCollection _commands = new();

    /// <summary>命令集合</summary>
    protected override DbBatchCommandCollection DbBatchCommands => _commands;

    /// <summary>命令集合</summary>
    public new MySqlBatchCommandCollection BatchCommands => _commands;
    #endregion

    #region 构造
    /// <summary>实例化</summary>
    public MySqlBatch() { }

    /// <summary>使用连接实例化</summary>
    /// <param name="connection">连接</param>
    public MySqlBatch(MySqlConnection connection) => _connection = connection;
    #endregion

    #region 方法
    /// <summary>创建批量命令项</summary>
    /// <returns></returns>
    protected override DbBatchCommand CreateDbBatchCommand() => new MySqlBatchCommand();

    /// <summary>执行批量命令并返回读取器</summary>
    /// <param name="behavior">命令行为</param>
    /// <returns></returns>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        => ExecuteDbDataReaderAsync(behavior, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>异步执行批量命令并返回读取器</summary>
    /// <param name="behavior">命令行为</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        var combinedSql = BuildCombinedSql();

        using var command = new MySqlCommand(_connection!, combinedSql);
        return await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>执行并返回总影响行数</summary>
    /// <returns></returns>
    public override Int32 ExecuteNonQuery()
        => ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>异步执行并返回总影响行数</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public override async Task<Int32> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        var combinedSql = BuildCombinedSql();

        using var command = new MySqlCommand(_connection!, combinedSql);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>执行并返回第一个结果集的第一行第一列</summary>
    /// <returns></returns>
    public override Object? ExecuteScalar()
        => ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>异步执行并返回第一个结果集的第一行第一列</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public override async Task<Object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        var combinedSql = BuildCombinedSql();

        using var command = new MySqlCommand(_connection!, combinedSql);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>预编译</summary>
    public override void Prepare() { }

    /// <summary>异步预编译</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public override Task PrepareAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <summary>取消</summary>
    public override void Cancel() { }

    /// <summary>释放</summary>
    public override void Dispose() { }
    #endregion

    #region 辅助
    /// <summary>将多条命令拼接为分号分隔的多语句 SQL</summary>
    /// <returns></returns>
    private String BuildCombinedSql()
    {
        if (_connection == null) throw new InvalidOperationException("连接未设置");
        if (_commands.Count == 0) throw new InvalidOperationException("没有命令需要执行");

        var sqls = new List<String>(_commands.Count);
        for (var i = 0; i < _commands.Count; i++)
        {
            var cmd = _commands[i];
            var sql = cmd.CommandText;
            if (sql.IsNullOrEmpty()) continue;

            // 替换参数
            if (cmd.Parameters.Count > 0)
                sql = MySqlCommand.SubstituteParameters(sql, cmd.Parameters);

            sqls.Add(sql);
        }

        return String.Join(";", sqls);
    }
    #endregion
}
#endif
