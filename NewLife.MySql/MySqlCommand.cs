using System.Data;
using System.Data.Common;
using System.Text;
using NewLife.Collections;
using NewLife.Data;
using NewLife.MySql.Messages;

namespace NewLife.MySql;

/// <summary>命令</summary>
public class MySqlCommand : DbCommand
{
    #region 属性
    private MySqlConnection _DbConnection = null!;
    /// <summary>连接</summary>
    protected override DbConnection DbConnection { get => _DbConnection; set => _DbConnection = (value as MySqlConnection)!; }

    /// <summary>命令语句</summary>
    public override String CommandText { get; set; } = null!;

    /// <summary>命令类型</summary>
    public override CommandType CommandType { get; set; }

    /// <summary>事务</summary>
    protected override DbTransaction? DbTransaction { get; set; }

    private readonly MySqlParameterCollection _parameters = new();
    /// <summary>参数集合</summary>
    protected override DbParameterCollection DbParameterCollection => _parameters;

    /// <summary>参数集合</summary>
    public new MySqlParameterCollection Parameters => _parameters;

    /// <summary>命令超时。单位秒</summary>
    public override Int32 CommandTimeout { get; set; }

    /// <summary>设计时可见</summary>
    public override Boolean DesignTimeVisible { get; set; }

    /// <summary>更新行方式</summary>
    public override UpdateRowSource UpdatedRowSource { get; set; }
    #endregion

    #region 构造
    /// <summary>实例化</summary>
    public MySqlCommand() { }

    /// <summary>实例化</summary>
    /// <param name="conn"></param>
    /// <param name="commandText"></param>
    public MySqlCommand(DbConnection conn, String commandText)
    {
        Connection = conn;
        CommandText = commandText;
    }

    public MySqlCommand(String commandText, DbConnection conn)
    {
        Connection = conn;
        CommandText = commandText;
    }
    #endregion

    #region 方法
    /// <summary>创建参数</summary>
    /// <returns></returns>
    protected override DbParameter CreateDbParameter() => new MySqlParameter();

    /// <summary>执行读取器。多语句由服务端拆分，通过NextResult()遍历多结果集</summary>
    /// <param name="behavior">命令行为</param>
    /// <returns></returns>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        => ExecuteDbDataReaderAsync(behavior, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>执行并返回影响行数。多语句累加所有语句的影响行数</summary>
    /// <returns></returns>
    public override Int32 ExecuteNonQuery()
        => ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>执行并返回第一个结果集的第一行第一列</summary>
    /// <returns></returns>
    public override Object? ExecuteScalar()
        => ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>预编译语句</summary>
    public override void Prepare()
    {
        var sql = CommandText;
        if (sql.IsNullOrEmpty()) throw new ArgumentNullException(nameof(CommandText));

        var conn = _DbConnection;
        var client = conn.Client!;

        client.PrepareStatementAsync(sql).ConfigureAwait(false).GetAwaiter().GetResult();

        throw new NotImplementedException();
    }

    /// <summary>取消</summary>
    public override void Cancel() { }

    /// <summary>异步执行读取器。多语句由服务端拆分，通过NextResult()遍历多结果集</summary>
    /// <param name="behavior">命令行为</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        var sql = CommandText;
        if (sql.IsNullOrEmpty()) throw new ArgumentNullException(nameof(CommandText));

        var conn = _DbConnection;

        if (CommandType == CommandType.TableDirect) sql = "Select * From " + sql;

        if (behavior.HasFlag(CommandBehavior.SchemaOnly))
        {
            using var limitCmd = new MySqlCommand(conn, "SET SQL_SELECT_LIMIT=0");
            await limitCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        else if (behavior.HasFlag(CommandBehavior.SingleRow))
        {
            using var limitCmd = new MySqlCommand(conn, "SET SQL_SELECT_LIMIT=1");
            await limitCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 执行读取器，多语句由服务端拆分，通过NextResult()遍历
        var reader = new MySqlDataReader
        {
            Command = this
        };
        await ExecuteAsync(cancellationToken).ConfigureAwait(false);
        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);

        return reader;
    }

    /// <summary>异步执行并返回影响行数。多语句累加所有语句的影响行数</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public override async Task<Int32> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        using var reader = await ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        // 消费所有结果集（RecordsAffected 会在 NextResultAsync 中自动累加）
        while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
        {
            // 仅消费结果集，RecordsAffected 已在 NextResultAsync 中累加
        }

        // RecordsAffected 已经是所有结果的累加值
        return reader.RecordsAffected;
    }

    /// <summary>异步执行并返回第一个有数据的结果集的第一行第一列</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public override async Task<Object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        using var reader = await ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        Object? result = null;
        var found = false;

        // 遍历所有结果集，确保网络流干净
        do
        {
            // 如果当前结果集有列（非 OK 包）且尚未找到结果
            if (!found && reader.FieldCount > 0)
            {
                // 尝试读取第一行第一列
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    result = reader.GetValue(0);
                    found = true;
                }
            }
            // 继续消费剩余结果集，NextResultAsync 内部会消费当前结果集的剩余行
        } while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

        return result;
    }
    #endregion

    #region 执行
    /// <summary>异步执行命令，绑定参数后发送请求</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var ms = Pool.MemoryStream.Get();
        try
        {
            ms.Seek(4, SeekOrigin.Current);

            var client = _DbConnection.Client ?? throw new InvalidOperationException("连接未打开");
            BindParameter(client, ms);

            ms.Position = 4;
            var pk = new ArrayPacket(ms);

            // 重置网络流，清理残留数据。如果连接已断开则抛出异常
            if (!client.Reset())
                throw new InvalidOperationException("数据库连接已断开");

            await client.SendQueryAsync(pk, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.MemoryStream.Return(ms);
        }
    }

    private void BindParameter(SqlClient client, Stream ms)
    {
        // 一个字节的查询类型
        ms.WriteByte(0x00);

        if (client.Capability.Has(ClientFlags.CLIENT_QUERY_ATTRIBUTES))
        {
            // 查询特性
            ms.WriteByte(0x00);
            ms.WriteByte(0x01);
        }

        // 替换参数后的命令文本
        var sql = SubstituteParameters(CommandText, _parameters);
        ms.Write(sql.GetBytes());
    }
    #endregion

    #region 参数替换
    /// <summary>客户端参数替换。将SQL中的@参数名替换为转义后的参数值</summary>
    /// <param name="sql">原始SQL</param>
    /// <param name="parameters">参数集合</param>
    /// <returns></returns>
    internal static String SubstituteParameters(String sql, MySqlParameterCollection parameters)
    {
        if (parameters.Count <= 0) return sql;

        var sb = new StringBuilder(sql.Length + parameters.Count * 16);
        var i = 0;
        while (i < sql.Length)
        {
            var ch = sql[i];

            // 跳过字符串字面量，避免替换字符串内的 @
            if (ch == '\'' || ch == '"')
            {
                var quote = ch;
                sb.Append(ch);
                i++;
                while (i < sql.Length)
                {
                    ch = sql[i];
                    sb.Append(ch);
                    i++;
                    if (ch == '\\' && i < sql.Length)
                    {
                        // 转义字符，跳过下一个字符
                        sb.Append(sql[i]);
                        i++;
                    }
                    else if (ch == quote)
                    {
                        // 双引号转义 '' 或 ""
                        if (i < sql.Length && sql[i] == quote)
                        {
                            sb.Append(sql[i]);
                            i++;
                        }
                        else
                            break;
                    }
                }
                continue;
            }

            // 检测参数标记 @
            if (ch == '@' || ch == '?')
            {
                var start = i;
                i++;
                // 读取参数名：字母、数字、下划线
                while (i < sql.Length && (Char.IsLetterOrDigit(sql[i]) || sql[i] == '_'))
                {
                    i++;
                }

                if (i > start + 1)
                {
                    var name = sql[(start + 1)..i];
                    var idx = parameters.IndexOf(name);
                    // 也尝试带@的全名
                    if (idx < 0) idx = parameters.IndexOf(sql[start..i]);

                    if (idx >= 0)
                    {
                        sb.Append(SerializeValue(((MySqlParameter)parameters[idx]).Value));
                        continue;
                    }
                }

                // 未匹配到参数，原样输出
                sb.Append(sql[start..i]);
                continue;
            }

            sb.Append(ch);
            i++;
        }

        return sb.ToString();
    }

    /// <summary>将参数值序列化为SQL字面量</summary>
    /// <param name="value">参数值</param>
    /// <returns></returns>
    internal static String SerializeValue(Object? value)
    {
        if (value == null || value == DBNull.Value) return "NULL";

        return value switch
        {
            String s => "'" + EscapeString(s) + "'",
            Boolean b => b ? "1" : "0",
            DateTime dt => "'" + dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff").TrimEnd('0').TrimEnd('.') + "'",
            DateTimeOffset dto => "'" + dto.ToString("yyyy-MM-dd HH:mm:ss.ffffff").TrimEnd('0').TrimEnd('.') + "'",
            Byte[] bytes => "X'" + bytes.ToHex() + "'",
            Guid guid => "'" + guid.ToString() + "'",
            Enum e => Convert.ToInt64(e).ToString(),
            Single f => f.ToString("R"),
            Double d => d.ToString("R"),
            Decimal m => m.ToString(),
            _ => value.ToString()!,
        };
    }

    /// <summary>转义SQL字符串中的特殊字符</summary>
    /// <param name="value">原始字符串</param>
    /// <returns></returns>
    internal static String EscapeString(String value)
    {
        var sb = new StringBuilder(value.Length + 8);
        foreach (var ch in value)
        {
            switch (ch)
            {
                case '\0': sb.Append("\\0"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\\': sb.Append("\\\\"); break;
                case '\'': sb.Append("\\'"); break;
                case '"': sb.Append("\\\""); break;
                case '\x1a': sb.Append("\\Z"); break;
                default: sb.Append(ch); break;
            }
        }
        return sb.ToString();
    }
    #endregion
}