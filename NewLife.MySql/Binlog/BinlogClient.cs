using System.Text;
using NewLife.Buffers;
using NewLife.Log;
using NewLife.MySql.Common;
using NewLife;

namespace NewLife.MySql.Binlog;

/// <summary>Binlog 客户端。高级封装，提供完整的 MySQL binlog 订阅能力</summary>
/// <remarks>
/// 使用流程：
/// 1. 创建 BinlogClient 实例，传入连接字符串
/// 2. 设置 OnEvent 回调或使用 SubscribeAsync 异步枚举
/// 3. 调用 StartAsync 开始订阅
/// 
/// 示例：
/// <code>
/// var client = new BinlogClient("Server=127.0.0.1;Port=3306;UserID=root;Password=root");
/// client.ServerId = 65535;
/// client.OnEvent += ev =>
/// {
///     if (ev.TableName == "user")
///         Console.WriteLine($"{ev.EventType} {ev.DatabaseName}.{ev.TableName}");
/// };
/// await client.StartAsync();
/// </code>
/// </remarks>
public class BinlogClient : DisposeBase
{
    #region 属性
    /// <summary>连接字符串</summary>
    public String ConnectionString { get; set; } = "";

    /// <summary>从属服务器 ID。需在主库中唯一，默认随机生成</summary>
    public Int32 ServerId { get; set; }

    /// <summary>起始 Binlog 位置。为空时从当前位置开始</summary>
    public BinlogPosition? Position { get; set; }

    /// <summary>需要监听的数据库名集合。为空时监听所有数据库</summary>
    public IList<String> DatabaseNames { get; set; } = [];

    /// <summary>需要监听的表名集合。为空时监听所有表。格式：表名（不含数据库前缀）</summary>
    public IList<String> TableNames { get; set; } = [];

    /// <summary>是否自动重连。默认 true</summary>
    public Boolean AutoReconnect { get; set; } = true;

    /// <summary>重连间隔。单位毫秒，默认 3000</summary>
    public Int32 ReconnectInterval { get; set; } = 3000;

    /// <summary>是否正在运行</summary>
    public Boolean Running { get; private set; }

    /// <summary>性能跟踪器</summary>
    public ITracer? Tracer { get; set; }

    /// <summary>日志</summary>
    public ILog Log { get; set; } = Logger.Null;

    private String? _logPrefix;
    /// <summary>日志前缀。前缀在翻译之后拼接，不参与本地化查表</summary>
    public String LogPrefix { get => _logPrefix ??= "[Binlog] "; set => _logPrefix = value; }

    private SqlClient? _client;
    private CancellationTokenSource? _cts;
    private readonly Random _rnd = new();

    /// <summary>会话启动闸门。0 空闲，1 已有启动中/运行中的会话，保证 StartAsync 与 SubscribeAsync 并发首次调用也互斥</summary>
    private Int32 _sessionGate;

    /// <summary>表映射缓存。tableId → BinlogEvent（TABLE_MAP 事件）</summary>
    private readonly Dictionary<UInt64, BinlogEvent> _tableMap = [];
    #endregion

    #region 构造
    /// <summary>实例化 Binlog 客户端</summary>
    public BinlogClient() => ServerId = _rnd.Next(100000, 999999);

    /// <summary>实例化 Binlog 客户端</summary>
    /// <param name="connectionString">连接字符串</param>
    public BinlogClient(String connectionString) : this() => ConnectionString = connectionString;

    /// <summary>销毁并释放资源</summary>
    /// <param name="disposing">是否正在释放托管资源</param>
    protected override void Dispose(Boolean disposing)
    {
        base.Dispose(disposing);

        if (disposing) Stop();
    }
    #endregion

    #region 事件
    /// <summary>收到 Binlog 事件时触发</summary>
    public event Action<BinlogEvent>? OnEvent;

    /// <summary>发生错误时触发</summary>
    public event Action<Exception>? OnError;
    #endregion

    #region 方法
    /// <summary>启动 Binlog 订阅</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (Running) return;

        // 并发首次调用时只有一个能拿到闸门；拿不到说明另一个 Start/Subscribe 正在启动
        if (Interlocked.CompareExchange(ref _sessionGate, 1, 0) != 0) return;

        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _cts = cts;

        try
        {
            await ConnectAndDumpAsync(cts.Token).ConfigureAwait(false);
        }
        catch
        {
            // 启动失败时释放令牌，否则再次 Start 会丢掉旧引用。
            // 只有仍持有当前会话时才复位闸门，避免清掉并发新会话的占用
            if (ReferenceEquals(_cts, cts))
            {
                _cts = null;
                Interlocked.Exchange(ref _sessionGate, 0);
            }
            cts.TryDispose();
            throw;
        }

        // 启动期间已被 Stop 取消：不再置运行态，避免留下一个永远 Running 的壳。
        // 必须与异常路径一样清空 _cts 并复位闸门，否则后续 StartAsync 会静默 no-op、SubscribeAsync 永远报“已在运行”，
        // Stop 还会对已释放的令牌调 Cancel 抛 ObjectDisposedException
        if (cts.IsCancellationRequested || !ReferenceEquals(_cts, cts))
        {
            if (ReferenceEquals(_cts, cts))
            {
                _cts = null;
                Interlocked.Exchange(ref _sessionGate, 0);
            }
            cts.TryDispose();
            return;
        }

        Running = true;

        // 启动后台事件读取循环。把本轮令牌一并交给循环，清理时只回收自己那一份
        _ = Task.Run(() => EventLoopAsync(cts), cts.Token);
    }

    /// <summary>停止 Binlog 订阅</summary>
    public void Stop()
    {
        Running = false;
        Interlocked.Exchange(ref _sessionGate, 0);

        _cts?.Cancel();
        _cts.TryDispose();
        _cts = null;

        // 发送 COM_QUIT 注销从属服务器并关闭连接
        var client = _client;
        if (client != null)
        {
            try
            {
                client.StopBinlogDumpAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch { /* 忽略关闭过程中的异常 */ }
        }
        _client.TryDispose();
        _client = null;

        lock (_tableMap) _tableMap.Clear();
    }

    /// <summary>异步停止 Binlog 订阅。优先使用此方法避免阻塞</summary>
    /// <returns>异步任务</returns>
    public async Task StopAsync()
    {
        Running = false;
        Interlocked.Exchange(ref _sessionGate, 0);

        _cts?.Cancel();
        _cts.TryDispose();
        _cts = null;

        // 发送 COM_QUIT 注销从属服务器并关闭连接
        var client = _client;
        if (client != null)
        {
            try
            {
                await client.StopBinlogDumpAsync().ConfigureAwait(false);
            }
            catch { /* 忽略关闭过程中的异常 */ }
        }
        _client.TryDispose();
        _client = null;

        lock (_tableMap) _tableMap.Clear();
    }

    /// <summary>异步枚举 Binlog 事件。替代 OnEvent 回调的消费模式</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步事件枚举</returns>
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    public async IAsyncEnumerable<BinlogEvent> SubscribeAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // StartAsync 与 SubscribeAsync 互斥：同时开会留下无人 Stop 的第二个会话，事件重复投递且连接泄漏
        if (Running) throw new InvalidOperationException(L.T("A binlog subscription is already running."));
        if (Interlocked.CompareExchange(ref _sessionGate, 1, 0) != 0)
            throw new InvalidOperationException(L.T("A binlog subscription is already running."));

        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _cts = cts;
        var client = _client;

        try
        {
            await ConnectAndDumpAsync(cts.Token).ConfigureAwait(false);
            // 启动期间已被 Stop 取消：直接结束，finally 负责释放令牌
            if (!ReferenceEquals(_cts, cts)) yield break;

            Running = true;
            client = _client;

            while (!cts.Token.IsCancellationRequested)
            {
                // 会话已被 Stop 或新一次订阅替换：立即退出，避免旧连接的事件覆盖新会话的 Position/表映射
                if (!ReferenceEquals(_cts, cts)) break;

                BinlogEvent? ev;
                try
                {
                    ev = await ReadAndParseEventAsync(client, cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    RaiseError(ex);
                    if (!AutoReconnect) break;

                    Log.ErrorPrefixed(LogPrefix, "Binlog read error; reconnecting in {0}ms: {1}", ReconnectInterval, ex.Message);
                    await Task.Delay(ReconnectInterval, cts.Token).ConfigureAwait(false);
                    await ReconnectAsync(client, cts.Token).ConfigureAwait(false);
                    client = _client;
                    continue;
                }

                if (ev == null) continue;
                if (!FilterEvent(ev)) continue;

                yield return ev;
            }
        }
        finally
        {
            // 只回收本轮持有的连接与令牌：Stop 后立即重新 Subscribe 时 _client/_cts 已指向新会话，
            // 无条件清理会把新连接和令牌一起释放，新订阅随后空转
            if (ReferenceEquals(_client, client))
            {
                _client.TryDispose();
                _client = null;
                lock (_tableMap) _tableMap.Clear();
            }

            if (ReferenceEquals(_cts, cts))
            {
                _cts = null;
                Running = false;
                Interlocked.Exchange(ref _sessionGate, 0);
            }
            cts.TryDispose();
        }
    }
#endif

    /// <summary>连接并发送 Binlog 转储请求</summary>
    private async Task ConnectAndDumpAsync(CancellationToken cancellationToken)
    {
        var setting = new MySqlConnectionStringBuilder(ConnectionString);

        var client = new SqlClient(setting)
        {
            Tracer = Tracer,
        };
        try
        {
            await client.OpenAsync(cancellationToken).ConfigureAwait(false);
            await client.ConfigureAsync(cancellationToken).ConfigureAwait(false);

            // 会话已被 Stop 取消时不再发布连接，避免旧会话的重连覆盖新会话的 _client
            cancellationToken.ThrowIfCancellationRequested();
            _client = client;

            // 设置 binlog 校验和为 NONE，避免 CRC32 校验影响解析
            await ExecuteSimpleAsync(client, "SET @master_binlog_checksum = 'NONE'", cancellationToken).ConfigureAwait(false);

            // 如果没有指定起始位置，查询当前 binlog 位置。先放局部变量，取消检查通过后再写回共享 Position，
            // 避免已停止的旧会话把新会话的续传位点覆盖成旧值
            var position = Position;
            if (position == null || position.FileName.IsNullOrEmpty())
                position = await GetCurrentPositionAsync(client, cancellationToken).ConfigureAwait(false);

            cancellationToken.ThrowIfCancellationRequested();
            Position = position;

            Log.InfoPrefixed(LogPrefix, "Started subscribing to binlog: {0} ServerId={1}", position, ServerId);

            // 注册从属服务器
            var host = setting.Server ?? "";
            var port = setting.Port;
            if (port == 0) port = 3306;
            await client.RegisterSlaveAsync(ServerId, host, setting.UserID ?? "", setting.Password ?? "", (Int16)port, cancellationToken).ConfigureAwait(false);

            // 发送 BINLOG_DUMP 请求
            await client.BinlogDumpAsync(position, ServerId, 0, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // 启动失败时自己回收半初始化连接，避免 StartAsync/SubscribeAsync 调用方只看到异常却留下打开的 socket
            if (ReferenceEquals(_client, client)) _client = null;
            client.TryDispose();
            throw;
        }
    }

    /// <summary>查询当前 binlog 文件和位置</summary>
    /// <param name="client">本轮会话的物理连接</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private static async Task<BinlogPosition> GetCurrentPositionAsync(SqlClient client, CancellationToken cancellationToken)
    {
        var conn = new MySqlConnection { Client = client };

        // MySQL 8.4 移除了 SHOW MASTER STATUS，改用 SHOW BINARY LOG STATUS；
        // 8.0 及更早版本只认旧语句，因此先试新语句，仅当服务端报“语句不存在/语法错误”时回退。
        foreach (var sql in new[] { "SHOW BINARY LOG STATUS", "SHOW MASTER STATUS" })
        {
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;

                using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var file = reader.GetString(0);
                    var pos = reader.GetValue(1).ToLong();
                    return new BinlogPosition(file, (UInt32)pos);
                }

                break;
            }
            catch (MySqlException ex) when (ex.ErrorCode is 1064 or 1146)
            {
                // 旧版本不识别 SHOW BINARY LOG STATUS，继续尝试 SHOW MASTER STATUS
            }
        }

        throw new InvalidOperationException(L.T("Unable to obtain the current binlog position; make sure binlog is enabled."));
    }

    /// <summary>执行简单 SQL 语句</summary>
    /// <param name="client">本轮会话的物理连接</param>
    /// <param name="sql">SQL 语句</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private static async Task ExecuteSimpleAsync(SqlClient client, String sql, CancellationToken cancellationToken)
    {
        var conn = new MySqlConnection { Client = client };
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>后台事件读取循环</summary>
    private async Task EventLoopAsync(CancellationTokenSource cts)
    {
        var cancellationToken = cts.Token;
        var client = _client;

        // 清理必须放在 finally：OnError 处理器由用户提供，抛异常会让循环异常退出，
        // 连接泄漏且 Running/闸门永久占住（此后 StartAsync 静默 no-op）
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // 会话已被 Stop 或新一次订阅替换：立即退出，避免旧连接的事件覆盖新会话的 Position/表映射
                if (!ReferenceEquals(_cts, cts)) break;

                BinlogEvent? ev;
                try
                {
                    ev = await ReadAndParseEventAsync(client, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    RaiseError(ex);
                    if (!AutoReconnect) break;

                    Log.ErrorPrefixed(LogPrefix, "Binlog read error; reconnecting in {0}ms: {1}", ReconnectInterval, ex.Message);
                    try
                    {
                        await Task.Delay(ReconnectInterval, cancellationToken).ConfigureAwait(false);
                        await ReconnectAsync(client, cancellationToken).ConfigureAwait(false);
                        client = _client;
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Exception ex2)
                    {
                        RaiseError(ex2);

                        // 重连本身失败时 _client 仍为空，立即 continue 会空转占满 CPU；退避后再重试
                        try
                        {
                            await Task.Delay(ReconnectInterval, cancellationToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) { break; }
                    }
                    continue;
                }

                if (ev == null) continue;
                if (!FilterEvent(ev)) continue;

                try
                {
                    OnEvent?.Invoke(ev);
                }
                catch (Exception ex)
                {
                    Log.ErrorPrefixed(LogPrefix, "Event handler error: {0}", ex.Message);
                }
            }
        }
        finally
        {
            // 事件循环结束（取消、关闭重试或 AutoReconnect=false）时释放物理连接，
            // 否则再次 Start 会覆盖 _client，旧连接泄漏。
            // 只回收本轮持有的连接与令牌：Stop 后立即 Start 的新会话可能已替换 _client/_cts，
            // 旧循环若无条件清理会把新连接和令牌一起释放，新订阅随后空转。
            if (ReferenceEquals(_client, client))
            {
                _client.TryDispose();
                _client = null;
                lock (_tableMap) _tableMap.Clear();
            }

            if (ReferenceEquals(_cts, cts))
            {
                _cts = null;
                Running = false;
                Interlocked.Exchange(ref _sessionGate, 0);
            }
            cts.TryDispose();
        }
    }

    /// <summary>触发错误事件。用户处理器抛出的异常只记日志，不能打断事件循环的清理流程</summary>
    /// <param name="ex">错误异常</param>
    private void RaiseError(Exception ex)
    {
        try
        {
            OnError?.Invoke(ex);
        }
        catch (Exception handlerError)
        {
            Log.ErrorPrefixed(LogPrefix, "Error handler error: {0}", handlerError.Message);
        }
    }

    /// <summary>读取并解析下一个 Binlog 事件</summary>
    /// <param name="client">本轮订阅独占的物理连接</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private async Task<BinlogEvent?> ReadAndParseEventAsync(SqlClient? client, CancellationToken cancellationToken)
    {
        if (client == null) return null;

        var ev = await client.ReadBinlogEventAsync(cancellationToken).ConfigureAwait(false);
        if (ev == null) return null;

        // 连接已被 Stop 或新会话替换：不再更新共享 Position/表映射，也不投递事件，
        // 避免旧会话在停止瞬间覆盖新会话的续传位点
        if (!ReferenceEquals(_client, client)) return null;

        // 更新位置
        if (ev.NextPosition > 0)
            Position = new BinlogPosition(Position?.FileName ?? "", ev.NextPosition);

        // 解析特定事件类型
        ParseEvent(ev);

        return ev;
    }

    /// <summary>重连</summary>
    /// <param name="client">本轮会话持有的连接。不能读取 _client，Stop 后立即 Start 时它可能已属于新会话</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private async Task ReconnectAsync(SqlClient? client, CancellationToken cancellationToken)
    {
        if (client != null)
        {
            try
            {
                await client.StopBinlogDumpAsync(cancellationToken).ConfigureAwait(false);
            }
            catch { /* 忽略关闭异常 */ }
        }
        client.TryDispose();
        if (ReferenceEquals(_client, client))
        {
            _client = null;
            lock (_tableMap) _tableMap.Clear();
        }

        // 会话已被 Stop 取消时不再重连，避免旧循环给新会话建出第二条连接
        cancellationToken.ThrowIfCancellationRequested();
        await ConnectAndDumpAsync(cancellationToken).ConfigureAwait(false);

        Log.InfoPrefixed(LogPrefix, "Binlog reconnected: {0}", Position);
    }

    /// <summary>根据数据库和表名过滤事件</summary>
    internal Boolean FilterEvent(BinlogEvent ev)
    {
        // 只过滤行相关事件
        if (!IsRowEvent(ev.EventType) && ev.EventType != BinlogEventType.TABLE_MAP_EVENT)
            return true;

        // 数据库过滤
        if (DatabaseNames.Count > 0 && !ev.DatabaseName.IsNullOrEmpty())
        {
            var matched = false;
            for (var i = 0; i < DatabaseNames.Count; i++)
            {
                if (ev.DatabaseName.EqualIgnoreCase(DatabaseNames[i]))
                {
                    matched = true;
                    break;
                }
            }
            if (!matched) return false;
        }

        // 表名过滤
        if (TableNames.Count > 0 && !ev.TableName.IsNullOrEmpty())
        {
            var matched = false;
            for (var i = 0; i < TableNames.Count; i++)
            {
                if (ev.TableName.EqualIgnoreCase(TableNames[i]))
                {
                    matched = true;
                    break;
                }
            }
            if (!matched) return false;
        }

        return true;
    }

    /// <summary>判断是否为行事件</summary>
    private static Boolean IsRowEvent(BinlogEventType type) => type is
        BinlogEventType.WRITE_ROWS_EVENT or BinlogEventType.WRITE_ROWS_EVENT_V1 or
        BinlogEventType.UPDATE_ROWS_EVENT or BinlogEventType.UPDATE_ROWS_EVENT_V1 or
        BinlogEventType.DELETE_ROWS_EVENT or BinlogEventType.DELETE_ROWS_EVENT_V1;
    #endregion

    #region 事件解析
    /// <summary>解析 Binlog 事件内容</summary>
    internal void ParseEvent(BinlogEvent ev)
    {
        switch (ev.EventType)
        {
            case BinlogEventType.ROTATE_EVENT:
                ParseRotateEvent(ev);
                break;
            case BinlogEventType.QUERY_EVENT:
                ParseQueryEvent(ev);
                break;
            case BinlogEventType.TABLE_MAP_EVENT:
                ParseTableMapEvent(ev);
                break;
            case BinlogEventType.WRITE_ROWS_EVENT:
            case BinlogEventType.WRITE_ROWS_EVENT_V1:
            case BinlogEventType.UPDATE_ROWS_EVENT:
            case BinlogEventType.UPDATE_ROWS_EVENT_V1:
            case BinlogEventType.DELETE_ROWS_EVENT:
            case BinlogEventType.DELETE_ROWS_EVENT_V1:
                ParseRowsEvent(ev);
                break;
        }
    }

    /// <summary>解析旋转事件。更新当前 binlog 文件名</summary>
    private void ParseRotateEvent(BinlogEvent ev)
    {
        var data = ev.Data;
        if (data == null || data.Length < 8) return;

        var reader = new SpanReader(data) { IsLittleEndian = true };

        // position (8 bytes LE)
        ev.NextBinlogPosition = reader.ReadUInt64();

        // binlog filename (剩余字节)
        if (reader.Available > 0)
            ev.NextBinlogFile = Encoding.UTF8.GetString(data, reader.Position, reader.Available);

        // 更新位置
        if (!ev.NextBinlogFile.IsNullOrEmpty())
            Position = new BinlogPosition(ev.NextBinlogFile, (UInt32)ev.NextBinlogPosition);

        // 新 binlog 文件的 table_id 映射从头开始，旧映射必须清空
        lock (_tableMap) _tableMap.Clear();
    }

    /// <summary>解析查询事件</summary>
    private static void ParseQueryEvent(BinlogEvent ev)
    {
        var data = ev.Data;
        if (data == null || data.Length < 13) return;

        var reader = new SpanReader(data) { IsLittleEndian = true };

        // thread_id(4) + exec_time(4)
        reader.Advance(8);
        var dbNameLen = reader.ReadByte();
        var errorCode = reader.ReadUInt16();
        var statusVarsLen = reader.ReadUInt16();

        // 跳过状态变量
        if (reader.Available < statusVarsLen + dbNameLen + 1) return;
        reader.Advance(statusVarsLen);

        // 数据库名（零结尾）
        ev.QueryDatabase = Encoding.UTF8.GetString(data, reader.Position, dbNameLen);
        reader.Advance(dbNameLen + 1); // 跳过零结尾

        // SQL 语句
        if (reader.Available > 0)
            ev.Query = Encoding.UTF8.GetString(data, reader.Position, reader.Available);
    }

    /// <summary>解析表映射事件</summary>
    private void ParseTableMapEvent(BinlogEvent ev)
    {
        var data = ev.Data;
        if (data == null || data.Length < 8) return;

        var reader = new SpanReader(data) { IsLittleEndian = true };

        // table_id (6 bytes LE)
        ev.TableId = ReadTableId(ref reader);

        // flags (2 bytes)
        reader.Advance(2);

        // 数据库名 (1 byte len + string + NUL)
        if (reader.Available < 1) return;
        var dbLen = reader.ReadByte();
        if (reader.Available < dbLen + 1) return;
        ev.DatabaseName = Encoding.UTF8.GetString(data, reader.Position, dbLen);
        reader.Advance(dbLen + 1); // 跳过零结尾

        // 表名 (1 byte len + string + NUL)
        if (reader.Available < 1) return;
        var tblLen = reader.ReadByte();
        if (reader.Available < tblLen + 1) return;
        ev.TableName = Encoding.UTF8.GetString(data, reader.Position, tblLen);
        reader.Advance(tblLen + 1); // 跳过零结尾

        // 列数量 (length-encoded integer)
        if (reader.Available < 1) return;
        var colCount = (Int32)reader.ReadLength();
        ev.ColumnCount = colCount;

        // 列类型数组
        if (reader.Available >= colCount)
            ev.ColumnTypes = reader.ReadBytes(colCount).ToArray();

        // 缓存表映射
        lock (_tableMap) _tableMap[ev.TableId] = ev;
    }

    /// <summary>解析行事件。从数据体中提取 tableId 并关联表映射</summary>
    private void ParseRowsEvent(BinlogEvent ev)
    {
        var data = ev.Data;
        if (data == null || data.Length < 8) return;

        var reader = new SpanReader(data) { IsLittleEndian = true };

        // table_id (6 bytes LE)
        ev.TableId = ReadTableId(ref reader);

        // 从表映射中获取数据库名和表名
        BinlogEvent? tableMap;
        lock (_tableMap) _tableMap.TryGetValue(ev.TableId, out tableMap);
        if (tableMap != null)
        {
            ev.DatabaseName = tableMap.DatabaseName;
            ev.TableName = tableMap.TableName;
            ev.ColumnCount = tableMap.ColumnCount;
            ev.ColumnTypes = tableMap.ColumnTypes;
        }

        // flags (2 bytes)
        reader.Advance(2);

        // v2 行事件有额外的头部（extra_data_length + extra_data）
        if (ev.EventType is BinlogEventType.WRITE_ROWS_EVENT or BinlogEventType.UPDATE_ROWS_EVENT or BinlogEventType.DELETE_ROWS_EVENT)
        {
            if (reader.Available < 2) return;
            var extraLen = reader.ReadUInt16();
            // extra_data_length 包含自身的 2 字节，已读取 2 字节
            if (extraLen > 2) reader.Advance(extraLen - 2);
        }

        // 列数量 (length-encoded)
        if (reader.Available < 1) return;
        var colCount = (Int32)reader.ReadLength();

        // columns-present-bitmap（标记本事件包含哪些列）
        var bitmapLen = (colCount + 7) / 8;
        if (reader.Available < bitmapLen) return;
        reader.Advance(bitmapLen);

        // UPDATE 事件有第二个 columns-present-bitmap（更新后的列）
        var isUpdate = ev.EventType is BinlogEventType.UPDATE_ROWS_EVENT or BinlogEventType.UPDATE_ROWS_EVENT_V1;
        if (isUpdate)
        {
            if (reader.Available < bitmapLen) return;
            reader.Advance(bitmapLen);
        }

        // 行数据在 reader.Position 之后，存储原始引用供高级用户解析
        // 行数据的精确解析依赖列类型元数据，这里提供基础的行计数
        ev.Rows ??= [];
        if (isUpdate) ev.BeforeRows ??= [];
    }

    /// <summary>读取 6 字节小端 table_id</summary>
    private static UInt64 ReadTableId(ref SpanReader reader)
    {
        var bytes = reader.ReadBytes(6);
        return (UInt64)bytes[0] | ((UInt64)bytes[1] << 8) | ((UInt64)bytes[2] << 16) |
               ((UInt64)bytes[3] << 24) | ((UInt64)bytes[4] << 32) | ((UInt64)bytes[5] << 40);
    }
    #endregion

    #region 日志
    /// <summary>写日志</summary>
    /// <param name="format">格式化字符串</param>
    /// <param name="args">参数</param>
    public void WriteLog(String format, params Object[] args) => Log?.InfoPrefixed(LogPrefix, format, args);
    #endregion
}
