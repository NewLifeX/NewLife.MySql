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

    private readonly MySqlParameterCollection _parameters = [];
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

    private Int32 _statementId = -1;
    /// <summary>是否已预编译</summary>
    public Boolean IsPrepared => _statementId >= 0;

    private MySqlColumn[]? _paramColumns;

    /// <summary>预编译参数顺序映射。索引为 SQL 中 ? 出现的位置，值为 _parameters 中的参数索引</summary>
    private Int32[]? _paramOrder;
    #endregion

    #region 构造
    /// <summary>实例化</summary>
    public MySqlCommand() { }

    /// <summary>实例化</summary>
    /// <param name="conn">连接</param>
    /// <param name="commandText">命令语句</param>
    public MySqlCommand(DbConnection conn, String commandText)
    {
        Connection = conn;
        CommandText = commandText;
    }

    /// <summary>实例化</summary>
    /// <param name="commandText">命令语句</param>
    /// <param name="conn">连接</param>
    public MySqlCommand(String commandText, DbConnection conn)
    {
        Connection = conn;
        CommandText = commandText;
    }

    /// <summary>释放资源时关闭预编译语句</summary>
    /// <param name="disposing">是否释放托管资源</param>
    protected override void Dispose(Boolean disposing)
    {
        if (disposing)
            UnprepareAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

        base.Dispose(disposing);
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

    /// <summary>预编译语句。通过 COM_STMT_PREPARE 在服务端编译，后续执行走二进制协议</summary>
    public override void Prepare() => PrepareAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>异步预编译语句</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    public override async Task PrepareAsync(CancellationToken cancellationToken = default)
#else
    public async Task PrepareAsync(CancellationToken cancellationToken = default)
#endif
    {
        if (IsPrepared) return;

        var sql = CommandText;
        if (sql.IsNullOrEmpty()) throw new InvalidOperationException("CommandText 不能为空");

        var client = _DbConnection?.Client ?? throw new InvalidOperationException("连接未打开");

        // 将 @param 替换为 ? 占位符，同时记录参数在 SQL 中的出现顺序
        var paramOrder = new List<Int32>();
        var prepSql = ConvertToPositionalParameters(sql, _parameters, paramOrder);

        var result = await client.PrepareStatementAsync(prepSql, cancellationToken).ConfigureAwait(false);
        _statementId = result.StatementId;
        _paramColumns = result.Columns;
        _paramOrder = paramOrder.Count > 0 ? paramOrder.ToArray() : null;
    }

    /// <summary>释放预编译语句的服务端资源</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public async Task UnprepareAsync(CancellationToken cancellationToken = default)
    {
        if (!IsPrepared) return;

        var client = _DbConnection?.Client;
        if (client != null && client.Active)
        {
            try
            {
                await client.CloseStatementAsync(_statementId, cancellationToken).ConfigureAwait(false);
            }
            catch { /* 忽略关闭过程中的异常 */ }
        }

        _statementId = -1;
        _paramColumns = null;
        _paramOrder = null;
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
        var isBinary = await ExecuteAsync(cancellationToken).ConfigureAwait(false);
        reader.IsBinaryProtocol = isBinary;
        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);

        return reader;
    }

    /// <summary>异步执行并返回影响行数。多语句累加所有语句的影响行数</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public override async Task<Int32> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        using var reader = await ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        // 存储过程需要读取输出参数
        if (CommandType == CommandType.StoredProcedure)
        {
            await ReadOutputParametersAsync(reader, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // 消费所有结果集（RecordsAffected 会在 NextResultAsync 中自动累加）
            while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
            {
                // 仅消费结果集，RecordsAffected 已在 NextResultAsync 中累加
            }
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

    #region 批量执行
    /// <summary>批量执行参数化语句。同一 SQL 绑定多组参数执行，返回总影响行数</summary>
    /// <param name="parameterSets">多组参数值数组。每组是一个字典，键为参数名，值为参数值</param>
    /// <returns>总影响行数</returns>
    public Int32 ExecuteBatch(IList<IDictionary<String, Object?>> parameterSets)
        => ExecuteBatchAsync(parameterSets, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>异步批量执行参数化语句。自动 Prepare + Execute × N，返回总影响行数</summary>
    /// <param name="parameterSets">多组参数值数组。每组是一个字典，键为参数名，值为参数值</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>总影响行数</returns>
    public async Task<Int32> ExecuteBatchAsync(IList<IDictionary<String, Object?>> parameterSets, CancellationToken cancellationToken = default)
    {
        if (parameterSets == null || parameterSets.Count == 0) return 0;

        var sql = CommandText;
        if (sql.IsNullOrEmpty()) throw new InvalidOperationException("CommandText 不能为空");

        var client = _DbConnection?.Client ?? throw new InvalidOperationException("连接未打开");

        // 重置网络流
        if (!client.Reset()) throw new InvalidOperationException("数据库连接已断开");

        // 从第一组参数推断参数定义
        var firstSet = parameterSets[0];
        if (_parameters.Count == 0)
        {
            foreach (var kv in firstSet)
                _parameters.AddWithValue(kv.Key, kv.Value);
        }

        // 确保已预编译
        var needClose = false;
        if (!IsPrepared)
        {
            await PrepareAsync(cancellationToken).ConfigureAwait(false);
            needClose = true;
        }

        try
        {
            // 构建多组参数集合，按 SQL 中 ? 占位符的顺序排列
            var order = _paramOrder;
            var sets = new List<MySqlParameterCollection>(parameterSets.Count);
            foreach (var dict in parameterSets)
            {
                var ps = new MySqlParameterCollection();
                if (order != null)
                {
                    // 按 SQL 中参数出现顺序排列
                    for (var j = 0; j < order.Length; j++)
                    {
                        var p = (MySqlParameter)_parameters[order[j]];
                        var name = p.ParameterName ?? "";
                        var cleanName = name.StartsWith("@") ? name[1..] : name;

                        Object? val = null;
                        if (!dict.TryGetValue(cleanName, out val))
                            dict.TryGetValue("@" + cleanName, out val);

                        ps.AddWithValue(cleanName, val);
                    }
                }
                else
                {
                    foreach (MySqlParameter p in _parameters)
                    {
                        var name = p.ParameterName ?? "";
                        var cleanName = name.StartsWith("@") ? name[1..] : name;

                        Object? val = null;
                        if (!dict.TryGetValue(cleanName, out val))
                            dict.TryGetValue("@" + cleanName, out val);

                        ps.AddWithValue(cleanName, val);
                    }
                }
                sets.Add(ps);
            }

            return await client.ExecuteStatementPipelineAsync(_statementId, sets, _paramColumns, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (needClose)
                await UnprepareAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>使用数组参数批量执行。类似 Oracle 的 ArrayBindCount，每个参数传入数组值</summary>
    /// <param name="count">执行次数（数组长度）</param>
    /// <returns>总影响行数</returns>
    public Int32 ExecuteArrayBatch(Int32 count)
        => ExecuteArrayBatchAsync(count, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>异步使用数组参数批量执行。参数的 Value 设为数组，指定 count 为数组长度</summary>
    /// <param name="count">执行次数（数组长度）</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>总影响行数</returns>
    public async Task<Int32> ExecuteArrayBatchAsync(Int32 count, CancellationToken cancellationToken = default)
    {
        if (count <= 0) return 0;

        var sql = CommandText;
        if (sql.IsNullOrEmpty()) throw new InvalidOperationException("CommandText 不能为空");

        var client = _DbConnection?.Client ?? throw new InvalidOperationException("连接未打开");

        if (!client.Reset()) throw new InvalidOperationException("数据库连接已断开");

        // 确保已预编译
        var needClose = false;
        if (!IsPrepared)
        {
            await PrepareAsync(cancellationToken).ConfigureAwait(false);
            needClose = true;
        }

        try
        {
            // 从参数的数组值中提取每组参数，按 SQL 中 ? 占位符的顺序排列
            var order = _paramOrder;
            var sets = new List<MySqlParameterCollection>(count);
            for (var i = 0; i < count; i++)
            {
                var ps = new MySqlParameterCollection();
                if (order != null)
                {
                    for (var j = 0; j < order.Length; j++)
                    {
                        var p = (MySqlParameter)_parameters[order[j]];
                        var name = p.ParameterName ?? "";
                        var val = ExtractArrayValue(p.Value, i);
                        ps.AddWithValue(name, val);
                    }
                }
                else
                {
                    foreach (MySqlParameter p in _parameters)
                    {
                        var name = p.ParameterName ?? "";
                        var val = ExtractArrayValue(p.Value, i);
                        ps.AddWithValue(name, val);
                    }
                }
                sets.Add(ps);
            }

            return await client.ExecuteStatementPipelineAsync(_statementId, sets, _paramColumns, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (needClose)
                await UnprepareAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>从数组类型的参数值中提取指定索引的元素</summary>
    /// <param name="value">参数值（可能是数组）</param>
    /// <param name="index">索引</param>
    /// <returns></returns>
    private static Object? ExtractArrayValue(Object? value, Int32 index)
    {
        if (value == null) return null;

        // 支持各种数组类型
        if (value is Array arr)
            return index < arr.Length ? arr.GetValue(index) : null;

        if (value is System.Collections.IList list)
            return index < list.Count ? list[index] : null;

        // 非数组值，每次都用同一个值
        return value;
    }
    #endregion

    #region 执行
    /// <summary>异步执行命令。根据是否预编译选择文本协议或二进制协议</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>是否使用了二进制协议（COM_STMT_EXECUTE）</returns>
    private async Task<Boolean> ExecuteAsync(CancellationToken cancellationToken)
    {
        var client = _DbConnection.Client ?? throw new InvalidOperationException("连接未打开");

        // 重置网络流，清理残留数据。如果连接已断开则抛出异常
        if (!client.Reset())
            throw new InvalidOperationException("数据库连接已断开");

        // 预编译路径：使用 COM_STMT_EXECUTE 二进制协议
        if (IsPrepared)
        {
            var orderedParams = ReorderParameters(_parameters, _paramOrder);
            await client.ExecuteStatementAsync(_statementId, orderedParams, _paramColumns, cancellationToken).ConfigureAwait(false);
            return true;
        }

        // 连接字符串配置了服务端预编译且有参数时，自动走预编译路径
        var setting = _DbConnection.Setting;
        if (setting.UseServerPrepare && _parameters.Count > 0 && CommandType != CommandType.StoredProcedure)
        {
            await PrepareAsync(cancellationToken).ConfigureAwait(false);
            var orderedParams = ReorderParameters(_parameters, _paramOrder);
            await client.ExecuteStatementAsync(_statementId, orderedParams, _paramColumns, cancellationToken).ConfigureAwait(false);
            return true;
        }

        // 文本协议路径：客户端参数替换 + COM_QUERY
        var ms = Pool.MemoryStream.Get();
        try
        {
            ms.Seek(4, SeekOrigin.Current);

            BindParameter(client, ms);

            ms.Position = 4;
            var pk = new ArrayPacket(ms);

            await client.SendQueryAsync(pk, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.MemoryStream.Return(ms);
        }

        return false;
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

        // 存储过程使用 CALL 语句，先 SET 输入参数，后读取输出参数
        String sql;
        if (CommandType == CommandType.StoredProcedure)
            sql = BuildStoredProcedureCall();
        else
            sql = SubstituteParameters(CommandText, _parameters);

        ms.Write(sql.GetBytes());
    }

    /// <summary>构建存储过程调用的多语句SQL。
    /// 输入参数通过 SET @p=value 设置为用户变量，
    /// 输出参数通过 CALL 后的 SELECT @p 读取。</summary>
    /// <returns>完整的多语句SQL</returns>
    private String BuildStoredProcedureCall()
    {
        var sb = new StringBuilder();
        var callArgs = new List<String>();
        var outParams = new List<String>();

        foreach (MySqlParameter p in _parameters)
        {
            var name = p.ParameterName;
            if (name.IsNullOrEmpty()) continue;

            // 去掉前缀 @
            var cleanName = name.StartsWith("@") ? name[1..] : name;
            var userVar = "@" + cleanName;

            if (p.Direction == ParameterDirection.Input || p.Direction == ParameterDirection.InputOutput)
            {
                // SET @param = value;
                sb.Append("SET ");
                sb.Append(userVar);
                sb.Append('=');
                sb.Append(SerializeValue(p.Value));
                sb.Append(';');
            }

            callArgs.Add(userVar);

            if (p.Direction == ParameterDirection.Output || p.Direction == ParameterDirection.InputOutput)
                outParams.Add(userVar);
        }

        // CALL proc_name(@p1, @p2, ...);
        sb.Append("CALL ");
        sb.Append(CommandText);
        sb.Append('(');
        for (var i = 0; i < callArgs.Count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(callArgs[i]);
        }
        sb.Append(')');

        // 如果有输出参数，追加 SELECT 读取
        if (outParams.Count > 0)
        {
            sb.Append(";SELECT ");
            for (var i = 0; i < outParams.Count; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append(outParams[i]);
            }
        }

        return sb.ToString();
    }

    /// <summary>读取存储过程的输出参数值</summary>
    /// <param name="reader">数据读取器</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private async Task ReadOutputParametersAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        if (CommandType != CommandType.StoredProcedure) return;

        // 收集输出参数列表
        var outParams = new List<MySqlParameter>();
        foreach (MySqlParameter p in _parameters)
        {
            if (p.Direction == ParameterDirection.Output || p.Direction == ParameterDirection.InputOutput)
                outParams.Add(p);
        }
        if (outParams.Count == 0) return;

        // 遍历剩余结果集，找到输出参数的 SELECT 结果集（最后一个有列的结果集）
        // 存储过程可能返回多个结果集，输出参数的 SELECT 在最后
        Object[]? lastRow = null;
        while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
        {
            if (reader.FieldCount > 0 && reader.FieldCount == outParams.Count)
            {
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    lastRow = new Object[reader.FieldCount];
                    reader.GetValues(lastRow);
                }
            }
        }

        // 赋值输出参数
        if (lastRow != null)
        {
            for (var i = 0; i < outParams.Count && i < lastRow.Length; i++)
            {
                outParams[i].Value = lastRow[i] == DBNull.Value ? null : lastRow[i];
            }
        }
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

        var sb = Pool.StringBuilder.Get();
        sb.EnsureCapacity(sql.Length + parameters.Count * 16);
        var i = 0;
        while (i < sql.Length)
        {
            var ch = sql[i];

            // 跳过字符串字面量，避免替换字符串内的 @
            if (ch == '\'' || ch == '"')
            {
                SkipStringLiteral(sql, ref i, sb);
                continue;
            }

            // 跳过 @@ 系统变量（如 @@version），不做参数替换
            if (ch == '@' && i + 1 < sql.Length && sql[i + 1] == '@')
            {
                sb.Append('@');
                sb.Append('@');
                i += 2;
                // 读取系统变量名
                while (i < sql.Length && (Char.IsLetterOrDigit(sql[i]) || sql[i] == '_'))
                {
                    sb.Append(sql[i]);
                    i++;
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

        return sb.Return(true);
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
            Int32 n => n.ToString(),
            Int64 n => n.ToString(),
            Int16 n => n.ToString(),
            Byte n => n.ToString(),
            UInt32 n => n.ToString(),
            UInt64 n => n.ToString(),
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
        // 快速路径：扫描是否需要转义，无特殊字符时直接返回原串
        var needEscape = false;
        foreach (var c in value)
        {
            if (c == '\0' || c == '\n' || c == '\r' || c == '\\' || c == '\'' || c == '"' || c == '\x1a')
            {
                needEscape = true;
                break;
            }
        }
        if (!needEscape) return value;

        var sb = Pool.StringBuilder.Get();
        sb.EnsureCapacity(value.Length + 8);
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
        return sb.Return(true);
    }

    /// <summary>将 SQL 中的 @参数名 替换为 ? 占位符，用于服务端预编译</summary>
    /// <param name="sql">原始 SQL</param>
    /// <param name="parameters">参数集合，用于匹配参数名</param>
    /// <param name="paramOrder">输出参数顺序映射。记录 ? 占位符对应的参数索引，按 SQL 中出现顺序排列</param>
    /// <returns>替换后的 SQL</returns>
    internal static String ConvertToPositionalParameters(String sql, MySqlParameterCollection parameters, List<Int32>? paramOrder = null)
    {
        if (parameters.Count <= 0) return sql;

        var sb = Pool.StringBuilder.Get();
        sb.EnsureCapacity(sql.Length);
        var i = 0;
        while (i < sql.Length)
        {
            var ch = sql[i];

            // 跳过字符串字面量
            if (ch == '\'' || ch == '"')
            {
                SkipStringLiteral(sql, ref i, sb);
                continue;
            }

            // 跳过 @@ 系统变量
            if (ch == '@' && i + 1 < sql.Length && sql[i + 1] == '@')
            {
                sb.Append('@');
                sb.Append('@');
                i += 2;
                while (i < sql.Length && (Char.IsLetterOrDigit(sql[i]) || sql[i] == '_'))
                {
                    sb.Append(sql[i]);
                    i++;
                }
                continue;
            }

            // 检测参数标记 @
            if (ch == '@' || ch == '?')
            {
                var start = i;
                i++;
                while (i < sql.Length && (Char.IsLetterOrDigit(sql[i]) || sql[i] == '_'))
                    i++;

                if (i > start + 1)
                {
                    var name = sql[(start + 1)..i];
                    var idx = parameters.IndexOf(name);
                    if (idx < 0) idx = parameters.IndexOf(sql[start..i]);

                    if (idx >= 0)
                    {
                        sb.Append('?');
                        paramOrder?.Add(idx);
                        continue;
                    }
                }

                sb.Append(sql[start..i]);
                continue;
            }

            sb.Append(ch);
            i++;
        }

        return sb.Return(true);
    }

    /// <summary>按预编译参数顺序重排参数集合。如果无需重排则返回原集合</summary>
    /// <param name="parameters">原始参数集合</param>
    /// <param name="paramOrder">参数顺序映射</param>
    /// <returns>按 SQL 占位符顺序排列的参数集合</returns>
    private static MySqlParameterCollection ReorderParameters(MySqlParameterCollection parameters, Int32[]? paramOrder)
    {
        if (paramOrder == null || paramOrder.Length == 0) return parameters;

        // 检查是否已经按顺序排列，避免不必要的重排
        var inOrder = true;
        for (var i = 0; i < paramOrder.Length; i++)
        {
            if (paramOrder[i] != i) { inOrder = false; break; }
        }
        if (inOrder) return parameters;

        var reordered = new MySqlParameterCollection();
        for (var i = 0; i < paramOrder.Length; i++)
        {
            var p = (MySqlParameter)parameters[paramOrder[i]];
            reordered.AddWithValue(p.ParameterName ?? "", p.Value);
        }
        return reordered;
    }

    /// <summary>跳过 SQL 中的字符串字面量（单引号或双引号），处理转义和引号重复</summary>
    /// <param name="sql">SQL 字符串</param>
    /// <param name="i">当前索引，方法结束后指向字面量之后</param>
    /// <param name="sb">输出缓冲区</param>
    private static void SkipStringLiteral(String sql, ref Int32 i, StringBuilder sb)
    {
        var quote = sql[i];
        sb.Append(quote);
        i++;
        while (i < sql.Length)
        {
            var ch = sql[i];
            sb.Append(ch);
            i++;
            if (ch == '\\' && i < sql.Length)
            {
                // 反斜杠转义，跳过下一个字符
                sb.Append(sql[i]);
                i++;
            }
            else if (ch == quote)
            {
                // 引号重复转义（'' 或 ""）
                if (i < sql.Length && sql[i] == quote)
                {
                    sb.Append(sql[i]);
                    i++;
                }
                else
                    break;
            }
        }
    }
    #endregion
}