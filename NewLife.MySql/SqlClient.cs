using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using NewLife.Buffers;
using NewLife.Collections;
using NewLife.Data;
using NewLife.MySql.Common;
using NewLife.MySql.Messages;
using NewLife.Threading;

namespace NewLife.MySql;

/// <summary>客户端</summary>
public class SqlClient : DisposeBase
{
    #region 属性
    /// <summary>连接字符串配置</summary>
    public MySqlConnectionStringBuilder Setting { get; } = [];

    /// <summary>连接是否活动</summary>
    public Boolean Active { get; private set; }

    /// <summary>服务器支持的最大数据包大小</summary>
    public Int64 MaxPacketSize { get; private set; }

    /// <summary>客户端支持的特性标志</summary>
    public ClientFlags Capability { get; set; }

    /// <summary>服务器变量集合，键值对形式存储</summary>
    public IDictionary<String, String>? Variables { get; set; }

    private Stream? _stream;
    /// <summary>底层网络数据流</summary>
    public Stream? BaseStream { get => _stream; set => _stream = value; }

    /// <summary>服务器握手欢迎消息</summary>
    public WelcomeMessage? Welcome { get; set; }

    /// <summary>字符编码，默认 UTF-8</summary>
    public Encoding Encoding { get; set; } = Encoding.UTF8;

    /// <summary>读取超时。单位秒，默认15秒，0表示不超时</summary>
    public Int32 Timeout { get; set; } = 15;

    /// <summary>最后活跃时间。最后一次发送指令的时间</summary>
    public DateTime LastActive { get; set; }

    private TcpClient? _client;
    private Byte _seq = 1;
    private TimerX? _timer;
    #endregion

    #region 构造
    /// <summary>实例化客户端</summary>
    public SqlClient() { }

    /// <summary>实例化客户端</summary>
    /// <param name="setting"></param>
    public SqlClient(MySqlConnectionStringBuilder setting) => Setting = setting;

    /// <summary>销毁并释放所有资源</summary>
    /// <param name="disposing">是否正在释放托管资源</param>
    protected override void Dispose(Boolean disposing)
    {
        base.Dispose(disposing);

        if (disposing)
            Close();
    }
    #endregion

    #region 打开关闭
    /// <summary>异步打开到 MySQL 服务器的连接</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task OpenAsync(CancellationToken cancellationToken = default)
    {
        if (Active) return;
        if (_stream != null) return;
        cancellationToken.ThrowIfCancellationRequested();

        var set = Setting;
        var server = set.Server;
        if (server.IsNullOrEmpty()) throw new InvalidOperationException("未指定服务器地址");

        var port = set.Port;
        if (port == 0) port = 3306;

        var msTimeout = set.ConnectionTimeout * 1000;
        if (msTimeout <= 0) msTimeout = 15000;

        // 异步连接网络
        var client = new TcpClient();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(msTimeout);

        try
        {
#if NET5_0_OR_GREATER
            await client.ConnectAsync(server, port, cts.Token).ConfigureAwait(false);
#else
            // .NET Framework 4.5 不支持 ConnectAsync 的 CancellationToken 参数
            var connectTask = client.ConnectAsync(server, port);
            var completedTask = await Task.WhenAny(connectTask, Task.Delay(msTimeout, cts.Token)).ConfigureAwait(false);
            if (completedTask != connectTask)
            {
                client.Close();
                throw new TimeoutException($"连接 {server}:{port} 超时({msTimeout}ms)");
            }
            await connectTask.ConfigureAwait(false);
#endif
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            client.Close();
            throw new TimeoutException($"连接 {server}:{port} 超时({msTimeout}ms)");
        }
        catch
        {
            client.Close();
            throw;
        }

        // 设置读写超时
        client.ReceiveTimeout = msTimeout;
        client.SendTimeout = msTimeout;

        _client = client;
        _stream = client.GetStream();

        try
        {
            // 异步读取欢迎信息
            var welcome = await GetWelcomeAsync(cancellationToken).ConfigureAwait(false);
            Welcome = welcome;
            Capability = welcome.Capability;

            // SSL/TLS 握手
            var sslMode = set.SslMode;
            if (!sslMode.IsNullOrEmpty() && !sslMode.EqualIgnoreCase("None", "Disabled"))
            {
                if (Capability.Has(ClientFlags.SSL))
                    await StartSslAsync(server!, cancellationToken).ConfigureAwait(false);
                else if (sslMode.EqualIgnoreCase("Required"))
                    throw new NotSupportedException("服务器不支持 SSL 连接");
            }

            // 验证方法
            var method = welcome.AuthMethod;
            if (!method.IsNullOrEmpty() && !method.EqualIgnoreCase("mysql_native_password", "caching_sha2_password"))
                throw new NotSupportedException("不支持验证方式 " + method);

            // 异步验证
            var auth = new Authentication(this);
            await auth.AuthenticateAsync(welcome, false, cancellationToken).ConfigureAwait(false);

            // 认证成功后，将 Capability 更新为实际协商的客户端标志
            Capability = auth.GetFlags(welcome.Capability);

            // 认证成功后才标记为活动状态
            Active = true;
        }
        catch
        {
            // 认证/握手失败时清理资源，避免半初始化状态
            _client.TryDispose();
            _client = null;
            _stream = null;
            Welcome = null;
            Active = false;
            throw;
        }

        //_timer = new TimerX(s => { _ = PingAsync(); }, null, 10_000, 30_000) { Async = true };
    }

    /// <summary>异步发送SSL请求并升级为TLS连接</summary>
    /// <param name="server">服务器地址</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private async Task StartSslAsync(String server, CancellationToken cancellationToken)
    {
        // 发送 SSL 请求包
        var flags = Capability | ClientFlags.SSL;
        var buf = new Byte[32];
        var writer = new SpanWriter(buf);
        writer.Write((UInt32)flags);
        writer.Write(0xFF_FFFF); // MaxPacket
        writer.Write((Byte)33); // UTF-8
        writer.Write(new Byte[23]);

        _seq = 1;
        await SendPacketAsync((ArrayPacket)buf, cancellationToken).ConfigureAwait(false);

        // 升级为 SslStream
        var sslStream = new SslStream(_stream!, false, (sender, certificate, chain, errors) => true);
#if NET5_0_OR_GREATER
        await sslStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
        {
            TargetHost = server,
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
        }, cancellationToken).ConfigureAwait(false);
#else
        await sslStream.AuthenticateAsClientAsync(server, null, SslProtocols.Tls12, false).ConfigureAwait(false);
#endif
        _stream = sslStream;
    }

    /// <summary>关闭与服务器的连接并释放资源</summary>
    public void Close()
    {
        // 停止定时器
        _timer.TryDispose();
        _timer = null;

        // 尝试发送退出命令
        if (_stream != null)
        {
            _seq = 0;

            try
            {
                SendCommandAsync(DbCmd.QUIT).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch { /* 忽略关闭过程中的异常 */ }
        }

        // 释放网络资源
        _client.TryDispose();
        _client = null;
        _stream = null;

        Active = false;
    }

    /// <summary>异步加载并配置服务器变量和参数</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public virtual async Task ConfigureAsync(CancellationToken cancellationToken = default)
    {
        var vs = Variables ??= await LoadVariablesAsync(cancellationToken).ConfigureAwait(false);

        if (vs.TryGetValue("max_allowed_packet", out var str)) MaxPacketSize = str.ToLong();
        //vs.TryGetValue("character_set_client", out var clientCharSet);
        //vs.TryGetValue("character_set_connection", out var connCharSet);
    }
    #endregion

    #region 方法
    /// <summary>异步读取服务器握手欢迎消息。从中得知服务器验证方式等一系列参数信息</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>欢迎消息对象</returns>
    private async Task<WelcomeMessage> GetWelcomeAsync(CancellationToken cancellationToken)
    {
        var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

        var msg = new WelcomeMessage();
        msg.Read(rs.Data.GetSpan());

        return msg;
    }

    /// <summary>异步加载服务器变量</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>服务器变量字典</returns>
    private async Task<IDictionary<String, String>> LoadVariablesAsync(CancellationToken cancellationToken)
    {
        var dic = new NullableDictionary<String, String>();
        var conn = new MySqlConnection { Client = this };
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SHOW VARIABLES";

        using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var key = reader.GetString(0);
            var value = reader.GetString(1);
            dic[key] = value;
        }

        return dic;
    }
    #endregion

    #region 网络操作
    /// <summary>重置连接状态，清除网络流中的残留数据</summary>
    /// <returns>连接是否仍然可用</returns>
    public Boolean Reset()
    {
        var ns = _stream;
        if (ns == null || !Active) return false;

        try
        {
            // 清除网络流中的残留数据
            if (ns is NetworkStream { DataAvailable: true } nss)
            {
                var buf = Pool.Shared.Rent(1024);
                try
                {
                    while (nss.DataAvailable && ns.Read(buf, 0, buf.Length) > 0)
                    {
                        // 持续读取直到没有可用数据
                    }
                }
                finally
                {
                    Pool.Shared.Return(buf);
                }
            }

            return true;
        }
        catch (ObjectDisposedException)
        {
            Active = false;
            return false;
        }
        catch (IOException)
        {
            Active = false;
            return false;
        }
    }

    /// <summary>异步从网络流读取一个完整的 MySQL 协议数据包</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>响应数据包</returns>
    public async Task<Response> ReadPacketAsync(CancellationToken cancellationToken = default)
    {
        var ms = _stream ?? throw new InvalidOperationException("未打开连接");

        // 根据 Timeout 属性创建超时令牌，确保不会无限等待
        var timeout = Timeout;
        using var cts = timeout > 0
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;
        cts?.CancelAfter(timeout * 1000);
        var token = cts?.Token ?? cancellationToken;

        try
        {
            // 3字节长度 + 1字节序列号
            var buf = Pool.Shared.Rent(4);
            var count = await ms.ReadExactlyAsync(buf, 0, 4, token).ConfigureAwait(false);
            if (count < 4) throw new InvalidDataException($"读取数据包头部失败，可用{count}字节");

            var rs = new Response(ms)
            {
                Length = buf[0] + (buf[1] << 8) + (buf[2] << 16),
                Sequence = buf[3],
            };
            _seq = (Byte)(rs.Sequence + 1);
            Pool.Shared.Return(buf);

            // 读取数据。长度必须刚好，因为可能有多帧数据包
            var len = rs.Length;
            var pk = new OwnerPacket(len);
            count = await ms.ReadExactlyAsync(pk.Buffer, pk.Offset, len, token).ConfigureAwait(false);

            pk.Resize(count);
            rs.Set(pk);

            // 错误包
            if (rs.IsError)
            {
                var reader = new SpanReader(pk.Slice(1, -1));
                var code = reader.ReadUInt16();
                var msg = reader.ReadZeroString();

                // 前面有6字符错误码
                if (!msg.IsNullOrEmpty() && msg[0] == '#')
                    throw new MySqlException(code, msg[..6], msg[6..]);
                else
                    throw new MySqlException(code, msg);
            }

            return rs;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // 超时导致的取消，标记连接不可用
            Active = false;
            throw new TimeoutException($"读取数据包超时({timeout}s)");
        }
    }

    /// <summary>异步发送 MySQL 协议数据包。建议数据包头部预留4字节空间以填充帧头</summary>
    /// <param name="pk">待发送的数据包</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task SendPacketAsync(IPacket pk, CancellationToken cancellationToken = default)
    {
        var ms = _stream ?? throw new InvalidOperationException("未打开连接");

        var len = pk.Total;
        var pk2 = pk.ExpandHeader(4);

        pk2[0] = (Byte)(len & 0xFF);
        pk2[1] = (Byte)((len >> 8) & 0xFF);
        pk2[2] = (Byte)((len >> 16) & 0xFF);
        pk2[3] = _seq++;

        //var data = pk2.ReadBytes();
        //await ms.WriteAsync(data, 0, data.Length, cancellationToken).ConfigureAwait(false);
        await pk2.CopyToAsync(ms, cancellationToken).ConfigureAwait(false);
        await ms.FlushAsync(cancellationToken).ConfigureAwait(false);

        LastActive = DateTime.Now;
    }

    /// <summary>异步发送命令请求</summary>
    /// <param name="command">命令类型</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public async Task SendCommandAsync(DbCmd command, CancellationToken cancellationToken = default)
    {
        var buf = Pool.Shared.Rent(4 + 1);
        try
        {
            buf[4] = (Byte)command;

            // 每一次查询请求，序列号都要重置为0
            _seq = 0;

            // 偏移4字节为帧头预留空间，避免 ExpandHeader 额外分配
            await SendPacketAsync(new ArrayPacket(buf, 4, 1), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }
    }
    #endregion

    #region 逻辑命令
    /// <summary>异步切换当前数据库。使用 COM_INIT_DB 二进制命令</summary>
    /// <param name="databaseName">目标数据库名</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    /// <remarks>
    /// 使用 MySQL COM_INIT_DB 协议命令切换数据库，等效于 USE database 语句。
    /// 优点：无需 SQL 解析，性能略优于文本协议。
    /// 注意：不会修改连接字符串，仅切换服务器端的当前数据库。
    /// </remarks>
    public async Task SetDatabaseAsync(String databaseName, CancellationToken cancellationToken = default)
    {
        if (databaseName.IsNullOrEmpty())
            throw new ArgumentNullException(nameof(databaseName));

        var bytes = Encoding.GetBytes(databaseName);
        var len = 1 + bytes.Length;
        var buf = Pool.Shared.Rent(4 + len);

        try
        {
            // 预留4字节帧头
            buf[4] = (Byte)DbCmd.INIT_DB;
            Array.Copy(bytes, 0, buf, 5, bytes.Length);

            // 每一次命令请求，序列号都要重置为0
            _seq = 0;

            await SendPacketAsync(new ArrayPacket(buf, 4, len), cancellationToken).ConfigureAwait(false);

            // 读取 OK 响应
            var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
            if (!rs.IsOK)
                throw new MySqlException("切换数据库失败");
        }
        finally
        {
            Pool.Shared.Return(buf);
        }
    }

    /// <summary>异步发送 SQL 查询请求到服务器</summary>
    /// <param name="pk">包含查询语句的数据包</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task SendQueryAsync(IPacket pk, CancellationToken cancellationToken = default)
    {
        pk[0] = (Byte)DbCmd.QUERY;

        // 每一次查询请求，序列号都要重置为0
        _seq = 0;

        await SendPacketAsync(pk, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>解析服务器响应获取查询结果。解析 OK 包获取影响行数和最后插入ID，或返回结果集列数</summary>
    /// <param name="rs">服务器响应数据包</param>
    /// <returns>查询结果对象</returns>
    public QueryResult GetResult(Response rs)
    {
        var reader = rs.CreateReader(0);

        if (rs.IsOK)
        {
            // OK_Packet: header(0x00) + affected_rows(length-encoded) + last_insert_id(length-encoded) + status(2) + warnings(2)
            reader.Advance(1);
            var affectedRows = reader.ReadLength();
            var insertedId = reader.ReadLength();

            ServerStatusFlags statusFlags = 0;
            UInt16 warnings = 0;
            if (Capability.Has(ClientFlags.PROTOCOL_41) && reader.Available >= 4)
            {
                statusFlags = (ServerStatusFlags)reader.ReadUInt16();
                warnings = reader.ReadUInt16();
            }

            return new QueryResult(0, (Int32)affectedRows, insertedId, statusFlags, warnings);
        }
        else
        {
            // 读取列信息
            var fieldCount = (Int32)reader.ReadLength();
            return new QueryResult(fieldCount, 0, 0, 0, 0);
        }
    }

    /// <summary>异步读取结果集的列定义信息</summary>
    /// <param name="count">列数量</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>列信息数组</returns>
    public async Task<MySqlColumn[]> GetColumnsAsync(Int32 count, CancellationToken cancellationToken = default)
    {
        var list = new List<MySqlColumn>(count);
        for (var i = 0; i < count; i++)
        {
            var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
            if (rs.IsEOF) break;

            var reader = new SpanReader(rs.Data);

            var dc = new MySqlColumn
            {
                Catalog = reader.ReadString(),
                Database = reader.ReadString(),
                Table = reader.ReadString(),
                RealTable = reader.ReadString(),
                Name = reader.ReadString(),
                OriginalName = reader.ReadString(),
                Flag = reader.ReadByte(),
                Charset = reader.ReadInt16(),
                Length = reader.ReadInt32(),
                Type = (MySqlDbType)reader.ReadByte(),
                ColumnFlags = reader.ReadInt16(),
                Scale = reader.ReadByte()
            };

            if (reader.Available >= 2) reader.ReadInt16();

            list.Add(dc);
        }

        // 读取 EOF 包
        var pk = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (pk.IsEOF) { }

        return [.. list];
    }

    /// <summary>异步读取结果集的下一行数据</summary>
    /// <param name="values">用于存储行数据的数组</param>
    /// <param name="columns">列信息数组</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>行读取结果，包含是否成功及状态信息</returns>
    public async Task<RowResult> NextRowAsync(Object[] values, MySqlColumn[] columns, CancellationToken cancellationToken = default)
    {
        var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (rs.IsEOF)
        {
            // EOF_Packet: header(0xFE) + warnings(2) + status_flags(2)
            var eofReader = new Buffers.SpanReader(rs.Data.Slice(1));
            ServerStatusFlags statusFlags = 0;
            UInt16 warnings = 0;
            if (Capability.Has(ClientFlags.PROTOCOL_41) && eofReader.Available >= 4)
            {
                warnings = eofReader.ReadUInt16();
                statusFlags = (ServerStatusFlags)eofReader.ReadUInt16();
            }
            return new RowResult(false, statusFlags, warnings);
        }

        // 读取行数据
        var reader = rs.CreateReader(0);
        for (var i = 0; i < values.Length; i++)
        {
            var len = (Int32)reader.ReadLength();
            if (len == -1)
            {
                values[i] = DBNull.Value;
                continue;
            }

            var buf = reader.ReadBytes(len);
            values[i] = columns[i].Type switch
            {
                MySqlDbType.Decimal or MySqlDbType.NewDecimal => Decimal.Parse(buf.ToStr()),
                MySqlDbType.Byte => (SByte)Int64.Parse(buf.ToStr()),
                MySqlDbType.Int16 or MySqlDbType.UInt16 => (Int16)Int64.Parse(buf.ToStr()),
                MySqlDbType.Int24 or MySqlDbType.UInt24 or MySqlDbType.Int32 or MySqlDbType.UInt32 => (Int32)Int64.Parse(buf.ToStr()),
                MySqlDbType.Int64 or MySqlDbType.UInt64 => Int64.Parse(buf.ToStr()),
                MySqlDbType.Float => Single.Parse(buf.ToStr()),
                MySqlDbType.Double => Double.Parse(buf.ToStr()),
                MySqlDbType.DateTime or MySqlDbType.Timestamp or MySqlDbType.Date or MySqlDbType.Time => buf.ToStr().ToDateTime(),
                MySqlDbType.VarChar or MySqlDbType.String or MySqlDbType.TinyText or MySqlDbType.MediumText or MySqlDbType.LongText or MySqlDbType.Text or MySqlDbType.VarString or MySqlDbType.Enum => buf.ToStr(),
                MySqlDbType.Blob or MySqlDbType.TinyBlob or MySqlDbType.MediumBlob or MySqlDbType.LongBlob => buf.ToArray(),
                MySqlDbType.Bit => buf[0] != 0,
                MySqlDbType.Json => buf.ToStr(),
                MySqlDbType.Guid => buf.ToStr(),
                _ => buf.ToArray(),
            };
        }

        return new RowResult(true, 0, 0);
    }

    /// <summary>异步读取二进制协议结果行（COM_STMT_EXECUTE 返回的行数据）</summary>
    /// <param name="values">用于存储行数据的数组</param>
    /// <param name="columns">列信息数组</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>行读取结果，包含是否成功及状态信息</returns>
    public async Task<RowResult> NextBinaryRowAsync(Object[] values, MySqlColumn[] columns, CancellationToken cancellationToken = default)
    {
        var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (rs.IsEOF)
        {
            var eofReader = new Buffers.SpanReader(rs.Data.Slice(1));
            ServerStatusFlags statusFlags = 0;
            UInt16 warnings = 0;
            if (Capability.Has(ClientFlags.PROTOCOL_41) && eofReader.Available >= 4)
            {
                warnings = eofReader.ReadUInt16();
                statusFlags = (ServerStatusFlags)eofReader.ReadUInt16();
            }
            return new RowResult(false, statusFlags, warnings);
        }

        var numCols = columns.Length;
        var reader = rs.CreateReader(0);

        // 二进制行格式：header(0x00) + null_bitmap + values
        reader.Advance(1); // 跳过 0x00 header

        // null_bitmap: (num_columns + 7 + 2) / 8 字节，位偏移量为 2
        var nullBitmapLen = (numCols + 7 + 2) / 8;
        var nullBitmap = reader.ReadBytes(nullBitmapLen);

        for (var i = 0; i < numCols; i++)
        {
            // 检查是否为 NULL（位偏移 +2）
            var bitIndex = i + 2;
            if ((nullBitmap[bitIndex / 8] & (1 << (bitIndex % 8))) != 0)
            {
                values[i] = DBNull.Value;
                continue;
            }

            // SpanReader 是值类型，必须 ref 传递才能保留 Position 前进
            values[i] = ReadBinaryColumnValue(ref reader, columns[i]);
        }

        return new RowResult(true, 0, 0);
    }

    /// <summary>从二进制行中读取单列的值</summary>
    /// <param name="reader">数据读取器</param>
    /// <param name="column">列定义</param>
    /// <returns>列值</returns>
    private Object ReadBinaryColumnValue(ref Buffers.SpanReader reader, MySqlColumn column)
    {
        switch (column.Type)
        {
            case MySqlDbType.Byte:
                return (SByte)reader.ReadByte();
            case MySqlDbType.Int16:
                return reader.ReadInt16();
            case MySqlDbType.UInt16:
                return (UInt16)reader.ReadInt16();
            case MySqlDbType.Int24:
            case MySqlDbType.Int32:
                return reader.ReadInt32();
            case MySqlDbType.UInt24:
            case MySqlDbType.UInt32:
                return (UInt32)reader.ReadInt32();
            case MySqlDbType.Int64:
                return reader.ReadInt64();
            case MySqlDbType.UInt64:
                return (UInt64)reader.ReadInt64();
            case MySqlDbType.Float:
                {
                    var bytes = reader.ReadBytes(4);
                    return BitConverter.ToSingle(bytes.ToArray(), 0);
                }
            case MySqlDbType.Double:
                {
                    var bytes = reader.ReadBytes(8);
                    return BitConverter.ToDouble(bytes.ToArray(), 0);
                }
            case MySqlDbType.Decimal:
            case MySqlDbType.NewDecimal:
                {
                    var buf = reader.ReadString();
                    return Decimal.Parse(buf);
                }
            case MySqlDbType.DateTime:
            case MySqlDbType.Timestamp:
            case MySqlDbType.Date:
                return ReadBinaryDateTimeValue(ref reader);
            case MySqlDbType.Time:
                return ReadBinaryTimeValue(ref reader);
            case MySqlDbType.Bit:
                {
                    var len = (Int32)reader.ReadLength();
                    var buf = reader.ReadBytes(len);
                    return buf[0] != 0;
                }
            case MySqlDbType.Blob:
            case MySqlDbType.TinyBlob:
            case MySqlDbType.MediumBlob:
            case MySqlDbType.LongBlob:
                {
                    var len = (Int32)reader.ReadLength();
                    return reader.ReadBytes(len).ToArray();
                }
            default:
                // 字符串类型：VarChar, String, Text, Json, Guid, Enum 等
                return reader.ReadString();
        }
    }

    /// <summary>读取二进制 DATETIME 值</summary>
    private static Object ReadBinaryDateTimeValue(ref Buffers.SpanReader reader)
    {
        var len = reader.ReadByte();
        if (len == 0) return DateTime.MinValue;

        var year = (Int32)reader.ReadUInt16();
        var month = reader.ReadByte();
        var day = reader.ReadByte();

        var hour = 0;
        var minute = 0;
        var second = 0;
        var microsecond = 0;

        if (len >= 7)
        {
            hour = reader.ReadByte();
            minute = reader.ReadByte();
            second = reader.ReadByte();
        }
        if (len >= 11)
        {
            microsecond = reader.ReadInt32();
        }

        return new DateTime(year, month, day, hour, minute, second, microsecond / 1000);
    }

    /// <summary>读取二进制 TIME 值</summary>
    private static Object ReadBinaryTimeValue(ref Buffers.SpanReader reader)
    {
        var len = reader.ReadByte();
        if (len == 0) return TimeSpan.Zero;

        var isNeg = reader.ReadByte() != 0;
        var days = reader.ReadInt32();
        var hours = reader.ReadByte();
        var minutes = reader.ReadByte();
        var seconds = reader.ReadByte();

        var microseconds = 0;
        if (len >= 12)
            microseconds = reader.ReadInt32();

        var ts = new TimeSpan(days, hours, minutes, seconds, microseconds / 1000);
        return isNeg ? ts.Negate() : ts;
    }

    /// <summary>异步准备预编译语句</summary>
    /// <param name="sql">SQL 语句</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>预编译结果，包含语句 ID 和参数列信息</returns>
    public async Task<PrepareResult> PrepareStatementAsync(String sql, CancellationToken cancellationToken = default)
    {
        var len = 1 + Encoding.GetByteCount(sql);
        var pk = new OwnerPacket(len);
        pk[0] = (Byte)DbCmd.PREPARE;
        Encoding.GetBytes(sql, 0, sql.Length, pk.Buffer, pk.Offset + 1);

        // 每一次查询请求，序列号都要重置为0
        _seq = 0;

        await SendPacketAsync(pk, cancellationToken).ConfigureAwait(false);

        var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

        // COM_STMT_PREPARE_OK: status(1) + statement_id(4) + num_columns(2) + num_params(2) + filler(1) + warning_count(2)
        // 跳过 status byte (0x00)
        var reader = rs.CreateReader(1);

        var statementId = reader.ReadInt32();
        var numCols = reader.ReadInt16();
        var num = reader.ReadInt16();
        reader.Advance(3);

        MySqlColumn[]? columns = null;
        if (num > 0)
        {
            columns = await GetColumnsAsync(num, cancellationToken).ConfigureAwait(false);
        }
        if (numCols > 0)
        {
            while (numCols-- > 0)
            {
                await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
            }
            await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        }
        return new PrepareResult(statementId, columns!);
    }

    /// <summary>异步发送心跳检测连接可用性</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>连接是否正常</returns>
    public async Task<Boolean> PingAsync(CancellationToken cancellationToken = default)
    {
        if (!Active || _stream == null) return false;

        try
        {
            await SendCommandAsync(DbCmd.PING, cancellationToken).ConfigureAwait(false);
            await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

            return true;
        }
        catch
        {
            // 心跳失败，标记为不可用，停止定时器
            Active = false;

            _timer.TryDispose();
            _timer = null;

            return false;
        }
    }

    /// <summary>异步执行已准备的预编译语句，支持二进制参数绑定</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="parameters">参数集合，可为空</param>
    /// <param name="paramColumns">Prepare 返回的参数列信息</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task ExecuteStatementAsync(Int32 statementId, MySqlParameterCollection? parameters, MySqlColumn[]? paramColumns, CancellationToken cancellationToken = default)
    {
        var numParams = parameters?.Count ?? 0;
        var hasQueryAttrs = Capability.Has(ClientFlags.CLIENT_QUERY_ATTRIBUTES);

        // 构建 COM_STMT_EXECUTE 数据包
        var ms = Pool.MemoryStream.Get();
        try
        {
            // 预留帧头 4 字节
            ms.Seek(4, SeekOrigin.Current);

            ms.WriteByte((Byte)DbCmd.EXECUTE);

            // statement_id (4 bytes LE)
            ms.WriteByte((Byte)(statementId & 0xFF));
            ms.WriteByte((Byte)((statementId >> 8) & 0xFF));
            ms.WriteByte((Byte)((statementId >> 16) & 0xFF));
            ms.WriteByte((Byte)((statementId >> 24) & 0xFF));

            // flags (cursor_type): 1 字节，0x00 表示不使用游标
            ms.WriteByte(0x00);

            // iteration_count: 固定 1
            ms.WriteByte(0x01);
            ms.WriteByte(0x00);
            ms.WriteByte(0x00);
            ms.WriteByte(0x00);

            // CLIENT_QUERY_ATTRIBUTES 模式下需要发送 parameter_count（含查询属性参数）
            if (hasQueryAttrs)
                WriteLengthEncodedInteger(ms, numParams);

            if (numParams > 0)
            {
                // null_bitmap: (numParams + 7) / 8 bytes
                var nullBitmapLen = (numParams + 7) / 8;
                var nullBitmap = new Byte[nullBitmapLen];
                for (var i = 0; i < numParams; i++)
                {
                    var val = ((MySqlParameter)parameters![i]).Value;
                    if (val == null || val == DBNull.Value)
                        nullBitmap[i / 8] |= (Byte)(1 << (i % 8));
                }
                ms.Write(nullBitmap, 0, nullBitmapLen);

                // new_params_bound_flag = 1（始终重新发送类型信息）
                ms.WriteByte(0x01);

                // param_type × N (每个2字节: type + unsigned_flag)
                // CLIENT_QUERY_ATTRIBUTES 模式下，每个参数类型后还需附带参数名（length-encoded string）
                for (var i = 0; i < numParams; i++)
                {
                    var val = ((MySqlParameter)parameters![i]).Value;
                    var (typeId, unsigned) = GetMySqlTypeForValue(val);
                    ms.WriteByte(typeId);
                    ms.WriteByte(unsigned ? (Byte)0x80 : (Byte)0x00);

                    if (hasQueryAttrs)
                    {
                        // 参数名为空字符串（预编译语句按位置绑定，不需要名称）
                        WriteLengthEncodedInteger(ms, 0);
                    }
                }

                // param_value × N（仅非 NULL 参数）
                for (var i = 0; i < numParams; i++)
                {
                    var val = ((MySqlParameter)parameters![i]).Value;
                    if (val != null && val != DBNull.Value)
                        WriteBinaryValue(ms, val);
                }
            }

            ms.Position = 4;
            var pk = new ArrayPacket(ms);

            _seq = 0;
            await SendPacketAsync(pk, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.MemoryStream.Return(ms);
        }
    }

    /// <summary>根据 C# 值推断 MySQL 二进制协议类型 ID</summary>
    /// <param name="value">参数值</param>
    /// <returns>MySQL type ID 和是否无符号</returns>
    private static (Byte typeId, Boolean unsigned) GetMySqlTypeForValue(Object? value) => value switch
    {
        null or DBNull => (0x06, false),        // MYSQL_TYPE_NULL
        SByte => (0x01, false),                 // MYSQL_TYPE_TINY
        Byte => (0x01, true),                   // MYSQL_TYPE_TINY unsigned
        Int16 => (0x02, false),                 // MYSQL_TYPE_SHORT
        UInt16 => (0x02, true),                 // MYSQL_TYPE_SHORT unsigned
        Int32 => (0x03, false),                 // MYSQL_TYPE_LONG
        UInt32 => (0x03, true),                 // MYSQL_TYPE_LONG unsigned
        Int64 => (0x08, false),                 // MYSQL_TYPE_LONGLONG
        UInt64 => (0x08, true),                 // MYSQL_TYPE_LONGLONG unsigned
        Single => (0x04, false),                // MYSQL_TYPE_FLOAT
        Double => (0x05, false),                // MYSQL_TYPE_DOUBLE
        Decimal => (0xF6, false),               // MYSQL_TYPE_NEWDECIMAL → length-encoded string
        Boolean => (0x01, false),               // MYSQL_TYPE_TINY
        DateTime => (0x0C, false),              // MYSQL_TYPE_DATETIME
        DateTimeOffset dto => (0x0C, false),    // MYSQL_TYPE_DATETIME
        TimeSpan => (0x0B, false),              // MYSQL_TYPE_TIME
        Byte[] => (0xFC, false),                // MYSQL_TYPE_BLOB
        Guid => (0xFE, false),                  // MYSQL_TYPE_STRING
        String => (0xFE, false),                // MYSQL_TYPE_STRING
        Enum => (0x08, false),                  // MYSQL_TYPE_LONGLONG
        _ => (0xFE, false),                     // MYSQL_TYPE_STRING (fallback: ToString)
    };

    /// <summary>将 C# 值以 MySQL 二进制协议格式写入流</summary>
    /// <param name="ms">目标流</param>
    /// <param name="value">参数值</param>
    private void WriteBinaryValue(Stream ms, Object value)
    {
        switch (value)
        {
            case SByte v:
                ms.WriteByte(unchecked((Byte)v));
                break;
            case Byte v:
                ms.WriteByte(v);
                break;
            case Boolean v:
                ms.WriteByte(v ? (Byte)1 : (Byte)0);
                break;
            case Int16 v:
                ms.WriteByte((Byte)(v & 0xFF));
                ms.WriteByte((Byte)((v >> 8) & 0xFF));
                break;
            case UInt16 v:
                ms.WriteByte((Byte)(v & 0xFF));
                ms.WriteByte((Byte)((v >> 8) & 0xFF));
                break;
            case Int32 v:
                WriteInt32(ms, v);
                break;
            case UInt32 v:
                WriteInt32(ms, unchecked((Int32)v));
                break;
            case Int64 v:
                WriteInt64(ms, v);
                break;
            case UInt64 v:
                WriteInt64(ms, unchecked((Int64)v));
                break;
            case Single v:
                {
                    var bytes = BitConverter.GetBytes(v);
                    ms.Write(bytes, 0, 4);
                    break;
                }
            case Double v:
                {
                    var bytes = BitConverter.GetBytes(v);
                    ms.Write(bytes, 0, 8);
                    break;
                }
            case Decimal v:
                WriteLengthEncodedString(ms, v.ToString());
                break;
            case DateTime v:
                WriteBinaryDateTime(ms, v);
                break;
            case DateTimeOffset v:
                WriteBinaryDateTime(ms, v.DateTime);
                break;
            case TimeSpan v:
                WriteBinaryTime(ms, v);
                break;
            case Byte[] v:
                WriteLengthEncodedBytes(ms, v);
                break;
            case Guid v:
                WriteLengthEncodedString(ms, v.ToString());
                break;
            case String v:
                WriteLengthEncodedString(ms, v);
                break;
            case Enum v:
                WriteInt64(ms, Convert.ToInt64(v));
                break;
            default:
                WriteLengthEncodedString(ms, value.ToString()!);
                break;
        }
    }

    /// <summary>写入 4 字节小端整数</summary>
    private static void WriteInt32(Stream ms, Int32 v)
    {
        ms.WriteByte((Byte)(v & 0xFF));
        ms.WriteByte((Byte)((v >> 8) & 0xFF));
        ms.WriteByte((Byte)((v >> 16) & 0xFF));
        ms.WriteByte((Byte)((v >> 24) & 0xFF));
    }

    /// <summary>写入 8 字节小端整数</summary>
    private static void WriteInt64(Stream ms, Int64 v)
    {
        ms.WriteByte((Byte)(v & 0xFF));
        ms.WriteByte((Byte)((v >> 8) & 0xFF));
        ms.WriteByte((Byte)((v >> 16) & 0xFF));
        ms.WriteByte((Byte)((v >> 24) & 0xFF));
        ms.WriteByte((Byte)((v >> 32) & 0xFF));
        ms.WriteByte((Byte)((v >> 40) & 0xFF));
        ms.WriteByte((Byte)((v >> 48) & 0xFF));
        ms.WriteByte((Byte)((v >> 56) & 0xFF));
    }

    /// <summary>写入 length-encoded 字符串</summary>
    private void WriteLengthEncodedString(Stream ms, String value)
    {
        var bytes = Encoding.GetBytes(value);
        WriteLengthEncodedBytes(ms, bytes);
    }

    /// <summary>写入 length-encoded 字节数组</summary>
    private static void WriteLengthEncodedBytes(Stream ms, Byte[] bytes)
    {
        WriteLengthEncodedInteger(ms, bytes.Length);
        ms.Write(bytes, 0, bytes.Length);
    }

    /// <summary>写入 length-encoded 整数</summary>
    private static void WriteLengthEncodedInteger(Stream ms, Int64 length)
    {
        if (length < 0xFB)
        {
            ms.WriteByte((Byte)length);
        }
        else if (length < 0xFFFF)
        {
            ms.WriteByte(0xFC);
            ms.WriteByte((Byte)(length & 0xFF));
            ms.WriteByte((Byte)((length >> 8) & 0xFF));
        }
        else if (length <= 0xFF_FFFF)
        {
            ms.WriteByte(0xFD);
            ms.WriteByte((Byte)(length & 0xFF));
            ms.WriteByte((Byte)((length >> 8) & 0xFF));
            ms.WriteByte((Byte)((length >> 16) & 0xFF));
        }
        else
        {
            ms.WriteByte(0xFE);
            for (var i = 0; i < 8; i++)
                ms.WriteByte((Byte)((length >> (i * 8)) & 0xFF));
        }
    }

    /// <summary>写入 MySQL 二进制 DATETIME 格式</summary>
    private static void WriteBinaryDateTime(Stream ms, DateTime dt)
    {
        // MySQL binary datetime: length + year(2) + month(1) + day(1) + [hour(1) + min(1) + sec(1)] + [microsec(4)]
        var hasTime = dt.Hour != 0 || dt.Minute != 0 || dt.Second != 0 || dt.Millisecond != 0;
        var hasMicro = dt.Millisecond != 0;

        if (!hasTime)
        {
            // 仅日期: 4 bytes
            ms.WriteByte(4);
            ms.WriteByte((Byte)(dt.Year & 0xFF));
            ms.WriteByte((Byte)((dt.Year >> 8) & 0xFF));
            ms.WriteByte((Byte)dt.Month);
            ms.WriteByte((Byte)dt.Day);
        }
        else if (!hasMicro)
        {
            // 日期 + 时间: 7 bytes
            ms.WriteByte(7);
            ms.WriteByte((Byte)(dt.Year & 0xFF));
            ms.WriteByte((Byte)((dt.Year >> 8) & 0xFF));
            ms.WriteByte((Byte)dt.Month);
            ms.WriteByte((Byte)dt.Day);
            ms.WriteByte((Byte)dt.Hour);
            ms.WriteByte((Byte)dt.Minute);
            ms.WriteByte((Byte)dt.Second);
        }
        else
        {
            // 日期 + 时间 + 微秒: 11 bytes
            ms.WriteByte(11);
            ms.WriteByte((Byte)(dt.Year & 0xFF));
            ms.WriteByte((Byte)((dt.Year >> 8) & 0xFF));
            ms.WriteByte((Byte)dt.Month);
            ms.WriteByte((Byte)dt.Day);
            ms.WriteByte((Byte)dt.Hour);
            ms.WriteByte((Byte)dt.Minute);
            ms.WriteByte((Byte)dt.Second);
            var micro = dt.Millisecond * 1000;
            WriteInt32(ms, micro);
        }
    }

    /// <summary>写入 MySQL 二进制 TIME 格式</summary>
    private static void WriteBinaryTime(Stream ms, TimeSpan ts)
    {
        var isNegative = ts < TimeSpan.Zero;
        if (isNegative) ts = ts.Negate();

        var hasMicro = ts.Milliseconds != 0;

        if (!hasMicro)
        {
            // 8 bytes: is_negative(1) + days(4) + hours(1) + minutes(1) + seconds(1)
            ms.WriteByte(8);
            ms.WriteByte(isNegative ? (Byte)1 : (Byte)0);
            WriteInt32(ms, ts.Days);
            ms.WriteByte((Byte)ts.Hours);
            ms.WriteByte((Byte)ts.Minutes);
            ms.WriteByte((Byte)ts.Seconds);
        }
        else
        {
            // 12 bytes: 包含微秒
            ms.WriteByte(12);
            ms.WriteByte(isNegative ? (Byte)1 : (Byte)0);
            WriteInt32(ms, ts.Days);
            ms.WriteByte((Byte)ts.Hours);
            ms.WriteByte((Byte)ts.Minutes);
            ms.WriteByte((Byte)ts.Seconds);
            var micro = ts.Milliseconds * 1000;
            WriteInt32(ms, micro);
        }
    }

    /// <summary>异步关闭预编译语句并释放服务器资源。COM_STMT_CLOSE 无响应包</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task CloseStatementAsync(Int32 statementId, CancellationToken cancellationToken = default)
    {
        var buf = Pool.Shared.Rent(4 + 1 + 4);
        try
        {
            // 预留4字节帧头
            buf[4] = (Byte)DbCmd.CLOSE_STMT;
            buf[5] = (Byte)(statementId & 0xFF);
            buf[6] = (Byte)((statementId >> 8) & 0xFF);
            buf[7] = (Byte)((statementId >> 16) & 0xFF);
            buf[8] = (Byte)((statementId >> 24) & 0xFF);

            _seq = 0;
            await SendPacketAsync(new ArrayPacket(buf, 4, 5), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }
        // COM_STMT_CLOSE 不会返回响应包
    }

    /// <summary>异步重置预编译语句的参数和状态</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task ResetStatementAsync(Int32 statementId, CancellationToken cancellationToken = default)
    {
        var buf = Pool.Shared.Rent(4 + 1 + 4);
        try
        {
            buf[4] = (Byte)DbCmd.RESET_STMT;
            buf[5] = (Byte)(statementId & 0xFF);
            buf[6] = (Byte)((statementId >> 8) & 0xFF);
            buf[7] = (Byte)((statementId >> 16) & 0xFF);
            buf[8] = (Byte)((statementId >> 24) & 0xFF);

            _seq = 0;
            await SendPacketAsync(new ArrayPacket(buf, 4, 5), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }

        // COM_STMT_RESET 返回 OK 包
        await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>管道化批量执行预编译语句。连续发送多组 EXECUTE 包，然后批量读取响应</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="parameterSets">多组参数集合</param>
    /// <param name="paramColumns">Prepare 返回的参数列信息</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>总影响行数</returns>
    public async Task<Int32> ExecuteStatementPipelineAsync(Int32 statementId, IList<MySqlParameterCollection> parameterSets, MySqlColumn[]? paramColumns, CancellationToken cancellationToken = default)
    {
        if (parameterSets == null || parameterSets.Count == 0) return 0;

        // 管道化：逐条发送 EXECUTE + 读取响应
        // MySQL 协议是严格的请求-响应模式，每个 COM_STMT_EXECUTE 都需要独立的序列号
        // 因此管道化实际上是：发送→读响应→发送→读响应... 的快速循环
        // 相比普通模式的优势在于：跳过了 MySqlCommand 层的开销，直接在协议层循环
        var totalAffected = 0;
        for (var i = 0; i < parameterSets.Count; i++)
        {
            await ExecuteStatementAsync(statementId, parameterSets[i], paramColumns, cancellationToken).ConfigureAwait(false);
            var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
            var qr = GetResult(rs);
            totalAffected += qr.AffectedRows;
        }

        return totalAffected;
    }
    #endregion
}