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
    /// <summary>设置</summary>
    public MySqlConnectionStringBuilder Setting { get; } = [];

    /// <summary>是否活动</summary>
    public Boolean Active { get; private set; }

    /// <summary>最大包大小</summary>
    public Int64 MaxPacketSize { get; private set; }

    /// <summary>服务器特性</summary>
    public ClientFlags Capability { get; set; }

    /// <summary>服务器变量</summary>
    public IDictionary<String, String>? Variables { get; set; }

    private Stream? _stream;
    /// <summary>基础数据流</summary>
    public Stream? BaseStream { get => _stream; set => _stream = value; }

    /// <summary>欢迎信息</summary>
    public WelcomeMessage? Welcome { get; set; }

    /// <summary>编码</summary>
    public Encoding Encoding { get; set; } = Encoding.UTF8;

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

    /// <summary>销毁</summary>
    /// <param name="disposing"></param>
    protected override void Dispose(Boolean disposing)
    {
        base.Dispose(disposing);

        Close();
    }
    #endregion

    #region 打开关闭
    /// <summary>异步打开连接</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public async Task OpenAsync(CancellationToken cancellationToken = default)
    {
        if (Active) return;
        if (_stream != null) return;

        var set = Setting;
        var server = set.Server;
        if (server.IsNullOrEmpty()) throw new InvalidOperationException("未指定服务器地址");

        var port = set.Port;
        if (port == 0) port = 3306;

        var msTimeout = set.ConnectionTimeout * 1000;
        if (msTimeout <= 0) msTimeout = 15000;

        // 异步连接网络
        var client = new TcpClient
        {
            ReceiveTimeout = msTimeout,
            SendTimeout = msTimeout,
        };

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(msTimeout);

        try
        {
            await client.ConnectAsync(server, port).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            client.Close();
            throw new TimeoutException($"连接 {server}:{port} 超时({set.ConnectionTimeout}s)");
        }
        catch
        {
            client.Close();
            throw;
        }

        _client = client;
        _stream = client.GetStream();

        // 异步读取欢迎信息
        var welcome = await GetWelcomeAsync(cancellationToken).ConfigureAwait(false);
        Welcome = welcome;
        Capability = welcome.Capability;

        Active = true;

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

        _timer = new TimerX(s => { _ = PingAsync(); }, null, 10_000, 30_000) { Async = true };
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

    /// <summary>关闭</summary>
    public void Close()
    {
        _timer.TryDispose();
        _timer = null;

        if (_stream != null)
        {
            _seq = 0;

            try
            {
                SendCommandAsync(DbCmd.QUIT).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch { }
        }

        _client.TryDispose();
        _client = null;
        _stream = null;

        Active = false;
    }

    /// <summary>异步配置</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public virtual async Task ConfigureAsync(CancellationToken cancellationToken = default)
    {
        var vs = Variables ??= await LoadVariablesAsync(cancellationToken).ConfigureAwait(false);

        if (vs.TryGetValue("max_allowed_packet", out var str)) MaxPacketSize = str.ToLong();
        vs.TryGetValue("character_set_client", out var clientCharSet);
        vs.TryGetValue("character_set_connection", out var connCharSet);
    }
    #endregion

    #region 方法
    /// <summary>异步握手欢迎消息。从中得知服务器验证方式等一系列参数信息</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private async Task<WelcomeMessage> GetWelcomeAsync(CancellationToken cancellationToken)
    {
        var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

        var msg = new WelcomeMessage();
        msg.Read(rs.Data.GetSpan());

        return msg;
    }

    /// <summary>异步加载服务器变量</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private async Task<IDictionary<String, String>> LoadVariablesAsync(CancellationToken cancellationToken)
    {
        var dic = new Dictionary<String, String>();
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
    /// <summary>重置。干掉历史残留数据</summary>
    /// <returns>连接是否仍然可用</returns>
    public Boolean Reset()
    {
        var ns = _stream;
        if (ns == null || !Active) return false;

        try
        {
            // 干掉历史残留数据
            if (ns is NetworkStream { DataAvailable: true } nss)
            {
                var buf = Pool.Shared.Rent(1024);
                try
                {
                    Int32 count;
                    do
                    {
                        count = ns.Read(buf, 0, buf.Length);
                    } while (count > 0 && nss.DataAvailable);
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

    /// <summary>异步读取精确字节数</summary>
    /// <param name="stream">数据流</param>
    /// <param name="buffer">缓冲区</param>
    /// <param name="offset">偏移</param>
    /// <param name="count">字节数</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private static async Task<Int32> ReadExactlyAsync(Stream stream, Byte[] buffer, Int32 offset, Int32 count, CancellationToken cancellationToken)
    {
        var totalRead = 0;
        while (totalRead < count)
        {
            var n = await stream.ReadAsync(buffer, offset + totalRead, count - totalRead, cancellationToken).ConfigureAwait(false);
            if (n <= 0) break;
            totalRead += n;
        }
        return totalRead;
    }

    /// <summary>异步读取数据包</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public async Task<Response> ReadPacketAsync(CancellationToken cancellationToken = default)
    {
        var ms = _stream ?? throw new InvalidOperationException("未打开连接");

        // 3字节长度 + 1字节序列号
        var buf = Pool.Shared.Rent(4);
        var count = await ReadExactlyAsync(ms, buf, 0, 4, cancellationToken).ConfigureAwait(false);
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
        count = await ReadExactlyAsync(ms, pk.Buffer, pk.Offset, len, cancellationToken).ConfigureAwait(false);

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

    /// <summary>异步发送数据包。建议数据包头部预留4字节空间以填充帧头</summary>
    /// <param name="pk">数据包</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
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
    /// <summary>异步发送查询请求</summary>
    /// <param name="pk">数据包</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public async Task SendQueryAsync(IPacket pk, CancellationToken cancellationToken = default)
    {
        pk[0] = (Byte)DbCmd.QUERY;

        // 每一次查询请求，序列号都要重置为0
        _seq = 0;

        await SendPacketAsync(pk, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>获取结果。解析OK包获取影响行数和最后插入ID，或返回结果集列数</summary>
    /// <param name="affectedRow">影响行数</param>
    /// <param name="insertedId">最后插入ID</param>
    /// <returns>结果集列数，0表示无结果集（如INSERT/UPDATE/DELETE）</returns>
    public Int32 GetResult(Response rs, ref Int32 affectedRow, ref Int64 insertedId)
    {
        var reader = rs.CreateReader(0);

        if (rs.IsOK)
        {
            // OK_Packet: header(0x00) + affected_rows(length-encoded) + last_insert_id(length-encoded) + status(2) + warnings(2)
            reader.Advance(1);
            affectedRow += reader.ReadLength();
            insertedId = reader.ReadLength();

            return 0;
        }
        else
        {
            // 读取列信息
            return reader.ReadLength();
        }
    }

    /// <summary>异步读取列信息</summary>
    /// <param name="count">列数</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
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

        {
            var pk = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
            if (pk.IsEOF) { }
        }

        return list.ToArray();
    }

    /// <summary>异步读取下一行</summary>
    /// <param name="values">行值数组</param>
    /// <param name="columns">列信息</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public async Task<Boolean> NextRowAsync(Object[] values, MySqlColumn[] columns, CancellationToken cancellationToken = default)
    {
        var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (rs.IsEOF) return false;

        // 第一回读取长度
        var reader = rs.CreateReader(0);
        for (var i = 0; i < values.Length; i++)
        {
            var len = reader.ReadLength();
            if (len == -1)
            {
                values[i] = DBNull.Value;
                continue;
            }

            var buf = reader.ReadBytes(len);
            values[i] = columns[i].Type switch
            {
                MySqlDbType.Decimal or MySqlDbType.NewDecimal => Decimal.Parse(buf.ToStr()),
                MySqlDbType.Byte or MySqlDbType.Int16 or MySqlDbType.Int24 or MySqlDbType.Int32 or MySqlDbType.Int64 or MySqlDbType.UInt16 or MySqlDbType.UInt24 or MySqlDbType.UInt32 or MySqlDbType.UInt64 => Int64.Parse(buf.ToStr()),
                MySqlDbType.Float or MySqlDbType.Double => Double.Parse(buf.ToStr()),
                MySqlDbType.DateTime or MySqlDbType.Timestamp or MySqlDbType.Date or MySqlDbType.Time => buf.ToStr().ToDateTime(),
                MySqlDbType.VarChar or MySqlDbType.String or MySqlDbType.TinyText or MySqlDbType.MediumText or MySqlDbType.LongText or MySqlDbType.Text or MySqlDbType.VarString or MySqlDbType.Enum => buf.ToStr(),
                MySqlDbType.Blob or MySqlDbType.TinyBlob or MySqlDbType.MediumBlob or MySqlDbType.LongBlob => buf.ToArray(),
                MySqlDbType.Bit => buf[0] != 0,
                MySqlDbType.Json => buf.ToStr(),
                MySqlDbType.Guid => buf.ToStr(),
                _ => buf.ToArray(),
            };

            len = -1;
        }

        return true;
    }

    /// <summary>异步准备语句</summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public async Task<Tuple<Int32, MySqlColumn[]>> PrepareStatementAsync(String sql, CancellationToken cancellationToken = default)
    {
        var len = 1 + Encoding.GetByteCount(sql);
        var pk = new OwnerPacket(len);
        pk[0] = (Byte)DbCmd.PREPARE;
        Encoding.GetBytes(sql, 0, sql.Length, pk.Buffer, pk.Offset + 1);

        // 每一次查询请求，序列号都要重置为0
        _seq = 0;

        await SendPacketAsync(pk, cancellationToken).ConfigureAwait(false);

        var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        var reader = rs.CreateReader(0);

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
        return new Tuple<Int32, MySqlColumn[]>(statementId, columns!);
    }

    /// <summary>异步心跳</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
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

    /// <summary>异步执行预编译语句</summary>
    /// <param name="statementId">语句ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public async Task ExecuteStatementAsync(Int32 statementId, CancellationToken cancellationToken = default)
    {
        var buf = Pool.Shared.Rent(1 + 4);
        try
        {
            var writer = new SpanWriter(buf);
            writer.Write((Byte)DbCmd.EXECUTE);
            writer.Write(statementId);

            await SendPacketAsync(new ArrayPacket(buf, 0, 5), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }

        await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>异步关闭预编译语句</summary>
    /// <param name="statementId">语句ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public async Task CloseStatementAsync(Int32 statementId, CancellationToken cancellationToken = default)
    {
        var buf = Pool.Shared.Rent(1 + 4);
        try
        {
            var writer = new SpanWriter(buf);
            writer.Write((Byte)DbCmd.CLOSE_STMT);
            writer.Write(statementId);

            await SendPacketAsync(new ArrayPacket(buf, 0, 5), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }

        await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
    }
    #endregion
}