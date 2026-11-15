using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using NewLife.Collections;
using NewLife.Data;
using NewLife.Log;
using NewLife.MySql.Common;
using NewLife.MySql.Messages;
using NewLife;

namespace NewLife.MySql;

/// <summary>命令</summary>
public class MySqlCommand : DbCommand
{
    #region 属性
    private MySqlConnection _DbConnection = null!;
    /// <summary>连接</summary>
    protected override DbConnection DbConnection
    {
        get => _DbConnection;
        set => _DbConnection = (value as MySqlConnection)!;
    }

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

    private Int32 _commandTimeout = -1;
    /// <summary>命令超时。单位秒，0 表示不限制；未显式设置时取连接的命令超时</summary>
    public override Int32 CommandTimeout
    {
        get => _commandTimeout >= 0 ? _commandTimeout : _DbConnection?.Setting.CommandTimeout ?? 30;
        set => _commandTimeout = value;
    }

    /// <summary>设计时可见</summary>
    public override Boolean DesignTimeVisible { get; set; }

    /// <summary>更新行方式</summary>
    public override UpdateRowSource UpdatedRowSource { get; set; }

    private Int32 _statementId = -1;
    /// <summary>是否已预编译</summary>
    public Boolean IsPrepared => _statementId >= 0;

    /// <summary>预编译所在的物理客户端。连接关闭/重连后 statement id 失效，必须重新预编译</summary>
    private SqlClient? _preparedClient;

    /// <summary>预编译时的会话版本。连接池会话重置会释放语句，即使物理客户端对象相同也要重新预编译</summary>
    private Int32 _preparedVersion = -1;

    private MySqlColumn[]? _paramColumns;

    /// <summary>预编译参数顺序映射。索引为 SQL 中 ? 出现的位置，值为 _parameters 中的参数索引</summary>
    private Int32[]? _paramOrder;

    /// <summary>值数组批量按位置绑定时为 true，允许把无参数集合的裸 @name 当作占位符</summary>
    private Boolean _positionalAt;

    /// <summary>是否抑制诊断埋点。驱动内部记账语句（如 SET SQL_SELECT_LIMIT 与事务 COMMIT/ROLLBACK）不产生 Execute 跨度</summary>
    internal Boolean SuppressTracing { get; set; }

    /// <summary>执行前自动打开连接。DbDataSource.CreateCommand 返回的命令按 ADO.NET 约定在首次执行时打开关联连接</summary>
    internal Boolean AutoOpenConnection { get; set; }

    /// <summary>销毁命令时关闭关联连接。与 AutoOpenConnection 配套，由 DbDataSource 创建的命令使用</summary>
    internal Boolean CloseConnectionOnDispose { get; set; }
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
        {
            UnprepareAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

            if (CloseConnectionOnDispose)
            {
                CloseConnectionOnDispose = false;
                _DbConnection?.Close();
            }
        }

        base.Dispose(disposing);
    }

#if NET6_0_OR_GREATER
    /// <summary>异步释放。真正异步关闭预编译语句，避免 Dispose 中同步阻塞网络 IO</summary>
    /// <returns>异步任务</returns>
    public override async ValueTask DisposeAsync()
    {
        await UnprepareAsync(CancellationToken.None).ConfigureAwait(false);

        if (CloseConnectionOnDispose)
        {
            CloseConnectionOnDispose = false;
            if (_DbConnection != null) await _DbConnection.CloseAsync().ConfigureAwait(false);
        }

        await base.DisposeAsync().ConfigureAwait(false);

        GC.SuppressFinalize(this);
    }
#endif
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

    /// <summary>返回适合日志输出的 SQL 文本。参数值可能包含口令、手机号等敏感信息，这里只展示参数名</summary>
    /// <returns></returns>
    public override String ToString()
    {
        if (CommandText.IsNullOrEmpty()) return base.ToString() ?? String.Empty;
        if (_parameters.Count == 0) return CommandText;

        return GetSafeCommandText();
    }

    /// <summary>异步预编译语句</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    public override async Task PrepareAsync(CancellationToken cancellationToken = default)
#else
    public async Task PrepareAsync(CancellationToken cancellationToken = default)
#endif
    {
        if (AutoOpenConnection && _DbConnection != null && _DbConnection.State != ConnectionState.Open)
            await _DbConnection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var client = _DbConnection?.Client ?? throw new InvalidOperationException(L.T("Connection is not open."));

        // 预编译语句只存在于某个物理连接上。连接关闭后重新打开、或断线重连后，旧 statement id 已失效
        if (IsPrepared && (!ReferenceEquals(_preparedClient, client) || _preparedVersion != client.SessionVersion))
        {
            _statementId = -1;
            _paramColumns = null;
            _paramOrder = null;
            _preparedClient = null;
            _preparedVersion = -1;
        }
        if (IsPrepared) return;

        var sql = CommandText;
        if (sql.IsNullOrEmpty()) throw new InvalidOperationException(L.T("CommandText cannot be empty."));

        // 将 @param 替换为 ? 占位符，同时记录参数在 SQL 中的出现顺序
        var paramOrder = new List<Int32>();
        var prepSql = ConvertToPositionalParameters(sql, _parameters, paramOrder, _positionalAt, NoBackslashEscapes);

        var result = await client.PrepareStatementAsync(prepSql, cancellationToken).ConfigureAwait(false);
        _statementId = result.StatementId;
        _paramColumns = result.Columns;
        _paramOrder = paramOrder.Count > 0 ? paramOrder.ToArray() : null;
        _preparedClient = client;
        _preparedVersion = client.SessionVersion;
    }

    /// <summary>释放预编译语句的服务端资源</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public async Task UnprepareAsync(CancellationToken cancellationToken = default)
    {
        if (!IsPrepared) return;

        var client = _DbConnection?.Client;
        if (client != null && client.Active && ReferenceEquals(_preparedClient, client) && _preparedVersion == client.SessionVersion)
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
        _preparedClient = null;
        _preparedVersion = -1;
    }

    /// <summary>取消命令执行。当前驱动未实现，调用即抛出异常，避免静默无操作误导调用方</summary>
    /// <exception cref="NotSupportedException">暂不支持取消</exception>
    public override void Cancel() => throw new NotSupportedException(L.T("MySqlCommand.Cancel is not implemented yet, so the running command cannot be canceled."));

    /// <summary>异步执行读取器。多语句由服务端拆分，通过NextResult()遍历多结果集。
    /// 在 ConnectionTimeout 时间内对网络临时故障自动重连重试；事务内或关闭重试时直接执行</summary>
    /// <param name="behavior">命令行为</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        var conn = _DbConnection ?? throw new InvalidOperationException(L.T("The connection has not been set."));
        // 仅读语句支持断线重连重试：DML 在"服务端已执行、响应丢失"时重连重试会重复执行（非幂等），故不重试。
        // 事务内也不重试（避免 DML 重复执行）；即使用户没有把 Transaction 赋给命令，连接上的活动事务也必须识别。
        // 用户主动关闭重试则直接执行
        if (DbTransaction != null || conn.HasTransaction || !conn.Setting.RetryOnNetworkFailure || !IsReadOnlySql(CommandText, CommandType))
            return await ExecuteDbDataReaderCoreAsync(behavior, cancellationToken).ConfigureAwait(false);

        var connTimeout = conn.Setting.ConnectionTimeout;
        // 以毫秒级 TickCount64 计算 deadline；不配置超时时不限制重试次数
        var deadline = connTimeout > 0 ? Runtime.TickCount64 + (Int64)connTimeout * 1000 : Int64.MaxValue;
        var delay = 500;

        while (true)
        {
            try
            {
                return await ExecuteDbDataReaderCoreAsync(behavior, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (IsNetworkException(ex))
            {
                // 超过最大等待时间，直接抛出让上层感知
                if (Runtime.TickCount64 >= deadline) throw;

                // 计算实际等待时间，不超过剩余 deadline
                var remaining = (Int32)(deadline - Runtime.TickCount64);
                var waitMs = Math.Min(delay, remaining);
                if (waitMs > 0)
                    await Task.Delay(waitMs, cancellationToken).ConfigureAwait(false);

                // 指数退避：500ms → 1000ms → 2000ms → 上限 5000ms
                delay = Math.Min(delay * 2, 5000);

                XTrace.WriteLine("MySQL network connection lost, reconnecting after {0}ms ({1}: {2})", waitMs, ex.GetType().Name, ex.Message);

                // 清除预编译状态：重连后服务端上的 StatementId 已失效
                _statementId = -1;
                _paramColumns = null;
                _paramOrder = null;
                _preparedClient = null;
                _preparedVersion = -1;

                // 重建连接
                await conn.ReconnectAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>执行读取器核心逻辑。每次重试都从 conn.Client 重新获取客户端引用</summary>
    /// <param name="behavior">命令行为</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private async Task<DbDataReader> ExecuteDbDataReaderCoreAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        var sql = CommandText;
        if (sql.IsNullOrEmpty()) throw new ArgumentNullException(nameof(CommandText), L.T("CommandText cannot be empty."));

        var conn = _DbConnection ?? throw new InvalidOperationException(L.T("The connection has not been set."));

        // DbDataSource.CreateCommand 的命令在首次执行时打开关联连接
        if (AutoOpenConnection && conn.State != ConnectionState.Open)
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        var client = conn.Client ?? throw new InvalidOperationException(L.T("Connection is not open."));

        if (CommandType == CommandType.TableDirect) sql = "Select * From " + sql;

        var operationLease = await conn.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        var previousTimeout = client.Timeout;

        // SchemaOnly/SingleRow 借用会话变量 SQL_SELECT_LIMIT 限制返回行数，读取器关闭时恢复。
        // SET 经临时连接复用同一客户端执行，避免在已持有租约时重入连接锁
        var selectLimit = behavior.HasFlag(CommandBehavior.SchemaOnly) ? 0 : behavior.HasFlag(CommandBehavior.SingleRow) ? 1 : -1;
        if (selectLimit >= 0)
        {
            try
            {
                var limitConn = new MySqlConnection(conn.ConnectionString) { Client = client };
                using var limitCmd = new MySqlCommand(limitConn, $"SET SQL_SELECT_LIMIT={selectLimit}") { SuppressTracing = true };
                await limitCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                operationLease.Dispose();
                throw;
            }
        }

        var span = SuppressTracing ? default : MySqlDiagnostics.StartExecute(conn, sql, CommandType);

        // 执行读取器，多语句由服务端拆分，通过NextResult()遍历
        try
        {
            var reader = new MySqlDataReader
            {
                Command = this,
                OperationLease = operationLease,
                OriginalTimeout = previousTimeout,
                RestoreTimeoutOnClose = true,
                CommandPhaseTimeout = CommandTimeout,
                // CommandTimeout=0 表示整条命令（含读取阶段）都不限时；否则读取阶段沿用连接超时
                ReadPhaseTimeout = CommandTimeout > 0 ? previousTimeout : 0,
                // 跨度覆盖命令发送到读取器关闭的完整周期
                Activity = span,
                // 会话变量 SQL_SELECT_LIMIT 已改写，关闭时需恢复，避免污染连接池中的会话
                ResetSelectLimit = selectLimit >= 0,
                // SET sql_mode 可能改变 NO_BACKSLASH_ESCAPES，关闭时重新读取，避免后续文本参数按旧模式转义
                RefreshSqlModeOnClose = IsSetSqlMode(sql),
                // CloseConnection 由读取器在关闭时执行，避免调用方忘记关闭
                CloseConnectionOnClose = behavior.HasFlag(CommandBehavior.CloseConnection)
            };
            var isBinary = await ExecuteAsync(cancellationToken).ConfigureAwait(false);
            reader.IsBinaryProtocol = isBinary;
            await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);

            return reader;
        }
        catch (Exception ex)
        {
            client.Timeout = previousTimeout;

            // 执行失败时读取器不会交给调用方，关闭阶段不会执行 ResetSelectLimit；
            // 这里必须主动恢复，否则会话把 SQL_SELECT_LIMIT=0 带回连接池，后续查询全部被静默截断为空。
            if (selectLimit >= 0)
                await ResetSelectLimitAsync(conn, client).ConfigureAwait(false);

            operationLease.Dispose();

            if (IsNetworkException(ex))
            {
                span.SetError(ex);
                span.Dispose();
                throw;
            }

            var error = CreateCommandException(ex, GetSafeCommandText());
            span.SetError(error);
            span.Dispose();

            throw error;
        }
    }

    /// <summary>恢复会话变量 SQL_SELECT_LIMIT。执行失败路径使用，复用当前连接租约内的客户端</summary>
    /// <param name="conn">原连接</param>
    /// <param name="client">底层客户端</param>
    /// <returns></returns>
    private static async Task ResetSelectLimitAsync(MySqlConnection conn, SqlClient client)
    {
        if (!client.Active) return;

        try
        {
            var resetConn = new MySqlConnection(conn.ConnectionString) { Client = client };
            using var cmd = new MySqlCommand(resetConn, "SET SQL_SELECT_LIMIT=DEFAULT") { SuppressTracing = true };
            await cmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // 恢复失败时会话状态不可信，标记连接失效，避免带着行数限制归还连接池
            client.Active = false;
        }
    }

    /// <summary>判断是否为可能修改 sql_mode 的 SET 语句</summary>
    /// <param name="sql">命令文本</param>
    /// <returns>可能修改时返回 true</returns>
    internal static Boolean IsSetSqlMode(String? sql)
    {
        if (sql.IsNullOrEmpty()) return false;

        var text = sql.TrimStart();
        return text.StartsWith("SET", StringComparison.OrdinalIgnoreCase) &&
               text.IndexOf("sql_mode", StringComparison.OrdinalIgnoreCase) >= 0;
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

    /// <summary>判断异常是否为可重试的网络临时故障</summary>
    /// <param name="ex">待判断异常</param>
    /// <returns>true 表示可重试</returns>
    private static Boolean IsNetworkException(Exception ex)
    {
        // 逐层向内看：驱动会把读包异常包成 MySqlException/InvalidOperationException，只看最外层会漏判。
        // 任意一层出现取消/超时都算“不该重试”（旧框架靠关闭 socket 解除挂起，会以 IOException 形式冒出）
        var retryable = false;
        for (var e = ex; e != null; e = e.InnerException)
        {
            switch (e)
            {
                case OperationCanceledException:
                case TimeoutException:
                    return false;
                case IOException:
                case MySqlException { ErrorCode: 2006 or 2013 }:
                    retryable = true;
                    break;
            }
        }

        return retryable;
    }

    /// <summary>判断命令是否为只读语句。仅读语句支持断线重连重试，DML 重试可能重复执行</summary>
    /// <param name="sql">命令文本</param>
    /// <param name="commandType">命令类型</param>
    /// <returns>true 表示只读语句（可安全重试）</returns>
    internal static Boolean IsReadOnlySql(String? sql, CommandType commandType)
    {
        // TableDirect 由驱动改写为 Select * From 表名，属只读
        if (commandType == CommandType.TableDirect) return true;

        // 存储过程可能包含 DML，不视为只读
        if (commandType != CommandType.Text || sql.IsNullOrEmpty()) return false;

        var head = sql.TrimStart();
        // EXPLAIN ANALYZE 会真实执行语句（MySQL 8.0.18+），不能按只读重试，否则断线重连可能重复执行 DML
        if (head.StartsWith("EXPLAIN", StringComparison.OrdinalIgnoreCase))
        {
            var rest = head[7..].TrimStart();
            return !rest.StartsWith("ANALYZE", StringComparison.OrdinalIgnoreCase);
        }

        if (head.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
        {
            // SELECT ... INTO OUTFILE/DUMPFILE 有文件副作用，断线重试可能覆盖目标文件
            if (head.Contains("INTO OUTFILE", StringComparison.OrdinalIgnoreCase) ||
                head.Contains("INTO DUMPFILE", StringComparison.OrdinalIgnoreCase))
                return false;

            return true;
        }

        return head.StartsWith("SHOW", StringComparison.OrdinalIgnoreCase)
            || head.StartsWith("DESC", StringComparison.OrdinalIgnoreCase)
            || head.StartsWith("DESCRIBE", StringComparison.OrdinalIgnoreCase);
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
        if (sql.IsNullOrEmpty()) throw new InvalidOperationException(L.T("CommandText cannot be empty."));

        var client = _DbConnection?.Client ?? throw new InvalidOperationException(L.T("Connection is not open."));
        using var operationLease = await _DbConnection.EnterOperationAsync(cancellationToken).ConfigureAwait(false);

        // 重置网络流
        if (!client.Reset()) throw new InvalidOperationException(L.T("The database connection has been closed."));

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

        using var span = SuppressTracing ? default : MySqlDiagnostics.StartExecute(_DbConnection, sql, CommandType, parameterSets.Count);

        try
        {
            // 构建多组值数组，按 SQL 中 ? 占位符的顺序排列，避免每行 MySqlParameterCollection/MySqlParameter 分配
            var bindings = GetBatchBindings(_parameters, _paramOrder);
            var sets = new List<Object?[]>(parameterSets.Count);
            foreach (var dict in parameterSets)
            {
                var vals = new Object?[bindings.Length];
                for (var i = 0; i < bindings.Length; i++)
                {
                    Object? val = null;
                    if (!dict.TryGetValue(bindings[i].CleanName, out val) && bindings[i].FullName.Length > 0)
                        dict.TryGetValue(bindings[i].FullName, out val);
                    vals[i] = val;
                }
                sets.Add(vals);
            }

            return await client.ExecuteStatementValuesAsync(_statementId, sets, _paramColumns, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var error = CreateCommandException(ex, GetSafeCommandText(), parameterSets.Count);
            span.SetError(error);
            throw error;
        }
        finally
        {
            if (needClose)
                await UnprepareAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>使用值数组批量执行。同一 SQL 绑定多组参数执行，每组为按参数顺序排列的值数组，避免字典与参数对象分配</summary>
    /// <param name="valueSets">多组参数值数组</param>
    /// <returns>总影响行数</returns>
    public Int32 ExecuteBatchValues(IList<Object?[]> valueSets)
        => ExecuteBatchValuesAsync(valueSets, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>异步使用值数组批量执行。自动 Prepare + Execute × N，返回总影响行数</summary>
    /// <param name="valueSets">多组参数值数组，每组按 SQL 参数顺序排列</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>总影响行数</returns>
    public async Task<Int32> ExecuteBatchValuesAsync(IList<Object?[]> valueSets, CancellationToken cancellationToken = default)
    {
        if (valueSets == null || valueSets.Count == 0) return 0;

        var sql = CommandText;
        if (sql.IsNullOrEmpty()) throw new InvalidOperationException(L.T("CommandText cannot be empty."));

        var client = _DbConnection?.Client ?? throw new InvalidOperationException(L.T("Connection is not open."));
        using var operationLease = await _DbConnection.EnterOperationAsync(cancellationToken).ConfigureAwait(false);

        // 重置网络流
        if (!client.Reset()) throw new InvalidOperationException(L.T("The database connection has been closed."));

        // 值数组版本按位置绑定，未预编译时自动 Prepare（SQL 中占位符顺序需与值数组一致）
        var needClose = false;
        if (!IsPrepared)
        {
            // 没有参数集合，但 SQL 仍是参数化的，需临时允许裸 @name 当作占位符
            _positionalAt = true;
            try
            {
                await PrepareAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _positionalAt = false;
            }
            needClose = true;
        }

        using var span = SuppressTracing ? default : MySqlDiagnostics.StartExecute(_DbConnection, sql, CommandType, valueSets.Count);

        try
        {
            return await client.ExecuteStatementValuesAsync(_statementId, valueSets, _paramColumns, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var error = CreateCommandException(ex, GetSafeCommandText(), valueSets.Count);
            span.SetError(error);
            throw error;
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
        if (sql.IsNullOrEmpty()) throw new InvalidOperationException(L.T("CommandText cannot be empty."));

        var client = _DbConnection?.Client ?? throw new InvalidOperationException(L.T("Connection is not open."));
        using var operationLease = await _DbConnection.EnterOperationAsync(cancellationToken).ConfigureAwait(false);

        if (!client.Reset()) throw new InvalidOperationException(L.T("The database connection has been closed."));

        // 确保已预编译
        var needClose = false;
        if (!IsPrepared)
        {
            await PrepareAsync(cancellationToken).ConfigureAwait(false);
            needClose = true;
        }

        using var span = SuppressTracing ? default : MySqlDiagnostics.StartExecute(_DbConnection, sql, CommandType, count);

        try
        {
            // 从参数的数组值中提取每组值数组，按 SQL 中 ? 占位符的顺序排列，避免每行 MySqlParameterCollection 分配
            var bindings = GetBatchBindings(_parameters, _paramOrder);
            var sets = new List<Object?[]>(count);
            for (var i = 0; i < count; i++)
            {
                var vals = new Object?[bindings.Length];
                for (var j = 0; j < bindings.Length; j++)
                    vals[j] = ExtractArrayValue(bindings[j].Parameter.Value, i);
                sets.Add(vals);
            }

            return await client.ExecuteStatementValuesAsync(_statementId, sets, _paramColumns, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var error = CreateCommandException(ex, GetSafeCommandText(), count);
            span.SetError(error);
            throw error;
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

    private static BatchParameterBinding[] GetBatchBindings(MySqlParameterCollection parameters, Int32[]? paramOrder)
    {
        if (paramOrder == null || paramOrder.Length == 0)
        {
            var bindings = new BatchParameterBinding[parameters.Count];
            for (var i = 0; i < parameters.Count; i++)
            {
                var parameter = (MySqlParameter)parameters[i];
                bindings[i] = new BatchParameterBinding(parameter);
            }
            return bindings;
        }

        var ordered = new BatchParameterBinding[paramOrder.Length];
        for (var i = 0; i < paramOrder.Length; i++)
        {
            var parameter = (MySqlParameter)parameters[paramOrder[i]];
            ordered[i] = new BatchParameterBinding(parameter);
        }

        return ordered;
    }

    private readonly struct BatchParameterBinding(MySqlParameter parameter)
    {
        public MySqlParameter Parameter { get; } = parameter;
        public String ParameterName { get; } = parameter.ParameterName ?? String.Empty;
        public String FullName { get; } = parameter.ParameterName ?? String.Empty;
        public String CleanName { get; } = (parameter.ParameterName ?? String.Empty).TrimStart('@');
    }

    private Exception CreateCommandException(Exception ex, String debugSql, Int32? batchCount = null)
    {
        var sb = Pool.StringBuilder.Get();
        sb.Append(ex.Message);

        if (!debugSql.IsNullOrEmpty())
        {
            sb.AppendLine();
            sb.Append(L.T("SQL: {0}", debugSql));
        }

        if (batchCount > 1)
        {
            sb.AppendLine();
            sb.Append(L.T("BatchCount: {0}", batchCount.Value));
        }

        var message = sb.Return(true);

        return ex switch
        {
            MySqlException mySqlException => new MySqlException(mySqlException.ErrorCode, mySqlException.State, message, mySqlException),
            _ => new InvalidOperationException(message, ex),
        };
    }

    /// <summary>异常信息里展示的 SQL：只保留命令文本与参数名，不把替换后的实际值（密码、手机号等）写进日志与链路追踪</summary>
    /// <returns></returns>
    private String GetSafeCommandText()
    {
        var text = CommandText ?? String.Empty;
        if (_parameters.Count == 0) return text;

        var names = new List<String>(_parameters.Count);
        foreach (MySqlParameter item in _parameters)
        {
            var name = item.ParameterName;
            if (!name.IsNullOrEmpty()) names.Add(name);
        }

        return names.Count == 0 ? text : text + " [" + String.Join(",", names) + "]";
    }

    /// <summary>服务器是否启用 NO_BACKSLASH_ESCAPES。该模式下反斜杠不再是转义符，字符串字面量必须改用双写单引号</summary>
    private Boolean NoBackslashEscapes => _DbConnection?.Client?.NoBackslashEscapes == true;

    /// <summary>连接是否开启多语句能力</summary>
    private Boolean AllowMultiStatements => _DbConnection?.Setting.AllowMultiStatements == true;
    #endregion

    #region 执行
    /// <summary>异步执行命令。根据是否预编译选择文本协议或二进制协议</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>是否使用了二进制协议（COM_STMT_EXECUTE）</returns>
    private async Task<Boolean> ExecuteAsync(CancellationToken cancellationToken)
    {
        var client = _DbConnection.Client ?? throw new InvalidOperationException(L.T("Connection is not open."));

        // 连接关闭后重新打开、或断线重连后，旧的预编译语句已随物理连接失效
        if (IsPrepared && (!ReferenceEquals(_preparedClient, client) || _preparedVersion != client.SessionVersion))
        {
            _statementId = -1;
            _paramColumns = null;
            _paramOrder = null;
            _preparedClient = null;
            _preparedVersion = -1;
        }

        // 重置网络流，清理残留数据。如果连接已断开则抛出异常
        if (!client.Reset())
            throw new InvalidOperationException(L.T("The database connection has been closed."));

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
            sql = SubstituteParameters(CommandText, _parameters, NoBackslashEscapes);

        ms.Write(sql.GetBytes());
    }

    /// <summary>构建存储过程调用的SQL。
    /// IN 参数直接嵌入值，避免 SET 用户变量产生额外的 OK 结果集干扰 reader 定位；
    /// OUT 参数通过用户变量传递，CALL 后追加 SELECT 读回；
    /// INOUT 参数先 SET 用户变量，再传递并 SELECT 读回。</summary>
    /// <returns>完整的存储过程调用SQL</returns>
    private String BuildStoredProcedureCall()
    {
        // OUT/INOUT 参数需要 SET 变量 → CALL → SELECT 多段语句，多语句关闭时无法生成合法调用
        if (!AllowMultiStatements)
        {
            foreach (MySqlParameter item in _parameters)
            {
                if (item.Direction != ParameterDirection.Input && !item.ParameterName.IsNullOrEmpty())
                    throw new InvalidOperationException(L.T("Stored procedure with OUT/INOUT parameters requires AllowMultiStatements=true in the connection string."));
            }
        }

        var sb = Pool.StringBuilder.Get();
        var callArgs = new List<String>();
        var outParams = new List<String>();

        foreach (MySqlParameter p in _parameters)
        {
            var name = p.ParameterName;
            if (name.IsNullOrEmpty()) continue;

            // 去掉前缀 @
            var cleanName = name.StartsWith("@") ? name[1..] : name;
            if (cleanName.IsNullOrEmpty() || !IsValidUserVariableName(cleanName))
                throw new ArgumentException(L.T("Invalid parameter name: {0}.", name), nameof(p));

            var userVar = "@" + cleanName;

            switch (p.Direction)
            {
                case ParameterDirection.Output:
                    // OUT 参数：传入用户变量，CALL 后 SELECT 读取输出值
                    callArgs.Add(userVar);
                    outParams.Add(userVar);
                    break;

                case ParameterDirection.InputOutput:
                    // INOUT 参数：先 SET 用户变量传入值，再传递用户变量，CALL 后 SELECT 读取
                    sb.Append("SET ");
                    sb.Append(userVar);
                    sb.Append('=');
                    sb.Append(SerializeValue(p.Value, NoBackslashEscapes));
                    sb.Append(';');
                    callArgs.Add(userVar);
                    outParams.Add(userVar);
                    break;

                default: // Input
                    // IN 参数：直接嵌入值，无需 SET 用户变量，避免额外的 OK 结果集
                    callArgs.Add(SerializeValue(p.Value, NoBackslashEscapes));
                    break;
            }
        }

        // CALL proc_name(val1, val2, @out1, ...)
        sb.Append("CALL ");
        sb.Append(CommandText);
        sb.Append('(');
        for (var i = 0; i < callArgs.Count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(callArgs[i]);
        }
        sb.Append(')');

        // 有输出参数则追加 SELECT 读取输出值
        if (outParams.Count > 0)
        {
            sb.Append(";SELECT ");
            for (var i = 0; i < outParams.Count; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append(outParams[i]);
            }
        }

        return sb.Return(true);
    }

    /// <summary>判断参数名是否可安全用作 MySQL 用户变量（字母、数字、下划线）</summary>
    /// <param name="name">去掉 @ 前缀后的参数名</param>
    /// <returns>合法时返回 true</returns>
    private static Boolean IsValidUserVariableName(String name)
    {
        foreach (var ch in name)
        {
            if (!(ch is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '_')) return false;
        }

        return true;
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
    /// <param name="noBackslashEscapes">服务器是否启用 NO_BACKSLASH_ESCAPES</param>
    /// <returns></returns>
    internal static String SubstituteParameters(String sql, MySqlParameterCollection parameters, Boolean noBackslashEscapes = false)
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
                SkipStringLiteral(sql, ref i, sb, noBackslashEscapes);
                continue;
            }

            // 跳过反引号标识符：其中的 @ 是列名/表名的一部分
            if (ch == '`')
            {
                SkipQuotedIdentifier(sql, ref i, sb);
                continue;
            }

            // 跳过注释：注释里的 @ 不是参数；若不跳过，参数值中的 */ 可提前结束块注释形成注入
            if (ch == '#')
            {
                SkipLineComment(sql, ref i, sb);
                continue;
            }
            if (ch == '-' && i + 1 < sql.Length && sql[i + 1] == '-' && (i + 2 >= sql.Length || Char.IsWhiteSpace(sql[i + 2])))
            {
                SkipLineComment(sql, ref i, sb);
                continue;
            }
            if (ch == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                SkipBlockComment(sql, ref i, sb);
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

                // @@version 等系统变量不是参数占位符，避免参数名恰好为 version 时被替换
                if (ch == '@' && start > 0 && sql[start - 1] == '@')
                {
                    sb.Append(ch);
                    continue;
                }

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
                        sb.Append(SerializeValue(((MySqlParameter)parameters[idx]).Value, noBackslashEscapes));
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
    /// <param name="noBackslashEscapes">服务器是否启用 NO_BACKSLASH_ESCAPES</param>
    /// <returns></returns>
    internal static String SerializeValue(Object? value, Boolean noBackslashEscapes = false)
    {
        if (value == null || value == DBNull.Value) return "NULL";

        return value switch
        {
            String s => "'" + EscapeString(s, noBackslashEscapes) + "'",
            Char c => "'" + EscapeString(c.ToString(), noBackslashEscapes) + "'",
            Boolean b => b ? "1" : "0",
            Int32 n => n.ToString(CultureInfo.InvariantCulture),
            Int64 n => n.ToString(CultureInfo.InvariantCulture),
            Int16 n => n.ToString(CultureInfo.InvariantCulture),
            Byte n => n.ToString(CultureInfo.InvariantCulture),
            UInt16 n => n.ToString(CultureInfo.InvariantCulture),
            SByte n => n.ToString(CultureInfo.InvariantCulture),
            UInt32 n => n.ToString(CultureInfo.InvariantCulture),
            UInt64 n => n.ToString(CultureInfo.InvariantCulture),
            // 使用 InvariantCulture 固定小数分隔符与日期时间分隔符，避免区域性（如 de-DE 小数点为逗号）生成非法 SQL 字面量
            DateTime dt => "'" + dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture).TrimEnd('0').TrimEnd('.') + "'",
            DateTimeOffset dto => "'" + dto.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture).TrimEnd('0').TrimEnd('.') + "'",
#if NET6_0_OR_GREATER
            DateOnly d => "'" + d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "'",
            TimeOnly t => "'" + t.ToString("HH:mm:ss.ffffff", CultureInfo.InvariantCulture).TrimEnd('0').TrimEnd('.') + "'",
#endif
            TimeSpan ts => "'" + ts.ToString("c", CultureInfo.InvariantCulture) + "'",
            Byte[] bytes => "X'" + bytes.ToHex() + "'",
            MySqlGeometry geom => "X'" + geom.Value.ToHex() + "'",
            Guid guid => "'" + guid.ToString() + "'",
            Enum e => Convert.ToInt64(e).ToString(CultureInfo.InvariantCulture),
            Single f => f.ToString("R", CultureInfo.InvariantCulture),
            Double d => d.ToString("R", CultureInfo.InvariantCulture),
            Decimal m => m.ToString(CultureInfo.InvariantCulture),
            // 未知类型统一按字符串字面量加引号，避免裸拼接破坏 SQL 结构（也曾把 Char/TimeSpan 输出成裸值）
            _ => "'" + EscapeString(value.ToString() ?? String.Empty, noBackslashEscapes) + "'",
        };
    }

    /// <summary>转义SQL字符串中的特殊字符</summary>
    /// <param name="value">原始字符串</param>
    /// <param name="noBackslashEscapes">服务器是否启用 NO_BACKSLASH_ESCAPES</param>
    /// <returns></returns>
    internal static String EscapeString(String value, Boolean noBackslashEscapes = false)
    {
        // NO_BACKSLASH_ESCAPES 模式下反斜杠是普通字符，\' 不会闭合引号，必须改用双写单引号；
        // 继续用反斜杠转义会让单引号直接裸露，形成注入
        if (noBackslashEscapes)
        {
            return value.Contains('\'') ? value.Replace("'", "''") : value;
        }

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
    /// <param name="positionalAt">无参数集合时是否把裸 @name 当作占位符。仅值数组批量按位置绑定才需要，其余场景保留用户变量</param>
    /// <param name="noBackslashEscapes">服务器是否启用 NO_BACKSLASH_ESCAPES</param>
    /// <returns>替换后的 SQL</returns>
    internal static String ConvertToPositionalParameters(String sql, MySqlParameterCollection parameters, List<Int32>? paramOrder = null, Boolean positionalAt = false, Boolean noBackslashEscapes = false)
    {
        // 值数组批量执行时参数集合为空，但 SQL 仍是参数化的，需要把 @name 当作位置占位符；
        // 普通命令没有参数时，@name 是 MySQL 用户变量（如 SET @x = 1），替换成 ? 会直接语法错误
        var hasParams = parameters.Count > 0;
        if (!hasParams && !sql.Contains('@')) return sql;

        var sb = Pool.StringBuilder.Get();
        sb.EnsureCapacity(sql.Length);
        var i = 0;
        while (i < sql.Length)
        {
            var ch = sql[i];

            // 跳过字符串字面量
            if (ch == '\'' || ch == '"')
            {
                SkipStringLiteral(sql, ref i, sb, noBackslashEscapes);
                continue;
            }

            // 跳过反引号标识符与注释
            if (ch == '`')
            {
                SkipQuotedIdentifier(sql, ref i, sb);
                continue;
            }
            if (ch == '#')
            {
                SkipLineComment(sql, ref i, sb);
                continue;
            }
            if (ch == '-' && i + 1 < sql.Length && sql[i + 1] == '-' && (i + 2 >= sql.Length || Char.IsWhiteSpace(sql[i + 2])))
            {
                SkipLineComment(sql, ref i, sb);
                continue;
            }
            if (ch == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                SkipBlockComment(sql, ref i, sb);
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
                    var idx = -1;
                    if (hasParams)
                    {
                        idx = parameters.IndexOf(name);
                        if (idx < 0) idx = parameters.IndexOf(sql[start..i]);
                    }

                    if (idx >= 0)
                    {
                        sb.Append('?');
                        paramOrder?.Add(idx);
                        continue;
                    }

                    // 值数组批量：@name 视为位置参数占位符，替换为 ?
                    if (!hasParams && positionalAt && ch == '@')
                    {
                        sb.Append('?');
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
    /// <param name="noBackslashEscapes">服务器是否启用 NO_BACKSLASH_ESCAPES</param>
    private static void SkipStringLiteral(String sql, ref Int32 i, StringBuilder sb, Boolean noBackslashEscapes)
    {
        var quote = sql[i];
        sb.Append(quote);
        i++;
        while (i < sql.Length)
        {
            var ch = sql[i];
            sb.Append(ch);
            i++;
            if (ch == '\\' && !noBackslashEscapes && i < sql.Length)
            {
                // 反斜杠转义，跳过下一个字符；NO_BACKSLASH_ESCAPES 下反斜杠是普通字符
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

    /// <summary>跳过反引号标识符，处理 `` 双写</summary>
    /// <param name="sql">SQL 字符串</param>
    /// <param name="i">当前索引，方法结束后指向标识符之后</param>
    /// <param name="sb">输出缓冲区</param>
    private static void SkipQuotedIdentifier(String sql, ref Int32 i, StringBuilder sb)
    {
        sb.Append('`');
        i++;
        while (i < sql.Length)
        {
            var ch = sql[i];
            sb.Append(ch);
            i++;
            if (ch == '`')
            {
                if (i < sql.Length && sql[i] == '`')
                {
                    sb.Append('`');
                    i++;
                }
                else
                    break;
            }
        }
    }

    /// <summary>跳过行注释（# 或 -- ），保留换行符</summary>
    /// <param name="sql">SQL 字符串</param>
    /// <param name="i">当前索引，方法结束后指向行尾</param>
    /// <param name="sb">输出缓冲区</param>
    private static void SkipLineComment(String sql, ref Int32 i, StringBuilder sb)
    {
        while (i < sql.Length)
        {
            var ch = sql[i];
            sb.Append(ch);
            i++;
            if (ch == '\n' || ch == '\r') break;
        }
    }

    /// <summary>跳过块注释，保留注释定界符；未闭合时消费到 SQL 末尾</summary>
    /// <param name="sql">SQL 字符串</param>
    /// <param name="i">当前索引，方法结束后指向注释之后</param>
    /// <param name="sb">输出缓冲区</param>
    private static void SkipBlockComment(String sql, ref Int32 i, StringBuilder sb)
    {
        sb.Append("/*");
        i += 2;
        while (i < sql.Length)
        {
            var ch = sql[i];
            sb.Append(ch);
            i++;
            if (ch == '*' && i < sql.Length && sql[i] == '/')
            {
                sb.Append('/');
                i++;
                break;
            }
        }
    }
    #endregion
}