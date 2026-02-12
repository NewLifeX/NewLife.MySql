using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using NewLife.Buffers;
using NewLife.Collections;
using NewLife.Data;
using NewLife.Log;
using NewLife.MySql.Binlog;
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

    /// <summary>当前数据库名。记录正在使用的数据库，首次打开连接时赋值，调用 SetDatabaseAsync 后会更新</summary>
    public String Database { get; set; } = null!;

    /// <summary>性能跟踪器</summary>
    public ITracer? Tracer { get; set; } = MySqlClientFactory.Instance.Tracer;

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

        using var span = Tracer?.NewSpan($"db:{set.Database}:Open", new { server, port, set.UserID, set.SslMode, set.UseServerPrepare, set.Pipeline });

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
        catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            span?.SetError(ex);
            client.Close();
            throw new TimeoutException($"连接 {server}:{port} 超时({msTimeout}ms)");
        }
        catch (Exception ex)
        {
            span?.SetError(ex);
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

            // 记录当前使用的数据库
            Database = set.Database ?? "";

            // 认证成功后才标记为活动状态
            Active = true;
        }
        catch (Exception ex)
        {
            span?.SetError(ex);

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

            using var span = Tracer?.NewSpan($"db:{Database}:Close");
            try
            {
                SendCommandAsync(DbCmd.QUIT).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                span?.SetError(ex);
                /* 忽略关闭过程中的异常 */
            }
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
        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

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
                var buf = Pool.Shared.Rent(8192);
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
    /// <returns>服务器数据包</returns>
    /// <remarks>
    /// 读取流程：先读取固定的 4 字节帧头（3 字节长度 + 1 字节序列号），从中解析出后续数据体的长度；
    /// 然后精确读取该长度的数据体，既不能多读也不能少读。MySQL 协议可能会在网络流中连续发送多个响应包（例如在 GetColumns 场景中每个列定义对应一个包），
    /// 如果在读取当前包时多读到下一个包的数据，将导致下一个包解包失败或数据错位。因此必须严格按照帧头给出的长度逐包读取。
    /// </remarks>
    public async Task<ServerPacket> ReadPacketAsync(CancellationToken cancellationToken = default)
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

            var rs = new ServerPacket(ms)
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
                var reader = new SpanReader(pk.Slice(1));
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
    /// <param name="flush">是否立即刷新网络流。管道化批量发送时设为 false，最后统一 Flush</param>
    /// <returns>异步任务</returns>
    public async Task SendPacketAsync(IPacket pk, CancellationToken cancellationToken = default, Boolean flush = true)
    {
        var ms = _stream ?? throw new InvalidOperationException("未打开连接");

        var len = pk.Total;
        var pk2 = pk.ExpandHeader(4);

        pk2[0] = (Byte)(len & 0xFF);
        pk2[1] = (Byte)((len >> 8) & 0xFF);
        pk2[2] = (Byte)((len >> 16) & 0xFF);
        pk2[3] = _seq++;

        await pk2.CopyToAsync(ms, cancellationToken).ConfigureAwait(false);
        if (flush) await ms.FlushAsync(cancellationToken).ConfigureAwait(false);

        LastActive = DateTime.Now;
    }

    /// <summary>异步刷新网络流，确保所有缓冲数据发送到服务器</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        var ms = _stream ?? throw new InvalidOperationException("未打开连接");
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

    /// <summary>异步发送带 Int32 参数的命令请求。适用于 command(1) + value(4) 格式的协议命令</summary>
    /// <param name="command">命令类型</param>
    /// <param name="value">命令参数值（如 statement_id、connection_id 等）</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task SendCommandAsync(DbCmd command, Int32 value, CancellationToken cancellationToken = default)
    {
        var buf = Pool.Shared.Rent(4 + 5);
        try
        {
            var writer = new SpanWriter(buf);
            writer.Advance(4);
            writer.Write((Byte)command);
            writer.Write(value);

            _seq = 0;
            await SendPacketAsync(new ArrayPacket(buf, 4, 5), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }
    }

    /// <summary>异步发送带字符串参数的命令请求。适用于 command(1) + string_payload 格式的协议命令</summary>
    /// <param name="command">命令类型</param>
    /// <param name="payload">字符串负载（按当前 Encoding 编码）</param>
    /// <param name="nulTerminated">是否在字符串末尾添加 NUL 终止符（用于 COM_FIELD_LIST 等需要分隔多段数据的命令）</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task SendCommandAsync(DbCmd command, String payload, Boolean nulTerminated = false, CancellationToken cancellationToken = default)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));

        var byteCount = Encoding.GetByteCount(payload);
        var extra = nulTerminated ? 1 : 0;
        var len = 1 + byteCount + extra;
        var buf = Pool.Shared.Rent(4 + len);

        try
        {
            // 写命令字节
            buf[4] = (Byte)command;

            // 直接编码到租借缓冲，避免中间分配。payload 从 offset 5 开始
            if (byteCount > 0)
                Encoding.GetBytes(payload, 0, payload.Length, buf, 5);

            if (nulTerminated)
                buf[5 + byteCount] = 0;

            _seq = 0;
            await SendPacketAsync(new ArrayPacket(buf, 4, len), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }
    }
    #endregion

    #region 基础命令
    /// <summary>异步发送心跳检测连接可用性</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>连接是否正常</returns>
    public async Task<Boolean> PingAsync(CancellationToken cancellationToken = default)
    {
        if (!Active || _stream == null) return false;

        /*
         PING 协议格式（文本/二进制命令都使用 COM_PING）
         请求（客户端 -> 服务端）:
           COM_PING (1) : 单字节命令，命令码为 0x0E（DbCmd.PING）
           MySQL 协议帧：4 字节帧头(3 bytes length + 1 byte sequence) + payload
           payload[0] = 0x0E

         响应（服务端 -> 客户端）:
           OK_Packet: header(0x00) + affected_rows(length-encoded) + last_insert_id(length-encoded) + status(2) + warnings(2)
           或者可能出现 ERR_Packet 描述错误

         实现要点：
         - 每次发送命令前将内部序列号 _seq 重置为 0，以确保帧序号正确
         - 发送后读取一个完整的响应包并检查是否为 OK 或 Error
         - 使用 Tracer 记录埋点，尊重传入的 CancellationToken 和 Timeout 属性
        */

        try
        {
            // 发送 PING 命令
            await SendCommandAsync(DbCmd.PING, cancellationToken).ConfigureAwait(false);
            using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

            // 如果是错误包，ReadPacketAsync 会抛出 MySqlException；到这里代表收到 OK 或其他合法包
            return rs.IsOK || !rs.IsError;
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

    /// <summary>异步切换当前数据库。使用 COM_INIT_DB 二进制命令</summary>
    /// <param name="databaseName">目标数据库名</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    /// <remarks>
    /// 使用 MySQL COM_INIT_DB 协议命令切换数据库，等效于 USE database 语句。
    /// 优点：无需 SQL 解析，性能略优于文本协议。
    /// 注意：不会修改连接字符串，仅切换服务器端的当前数据库。
    /// 
    /// 连接池安全性：
    /// - 调用此方法后，Database 会被更新为新的数据库名
    /// - 连接归还连接池时，会检查 Database 是否等于原始数据库名
    /// - 如果不等于（未切回原始数据库），连接将被销毁而不是归还连接池
    /// - 如果等于（已切回原始数据库），连接可以安全归还连接池复用
    /// </remarks>
    public async Task SetDatabaseAsync(String databaseName, CancellationToken cancellationToken = default)
    {
        if (databaseName.IsNullOrEmpty())
            throw new ArgumentNullException(nameof(databaseName));
        await SendCommandAsync(DbCmd.INIT_DB, databaseName, false, cancellationToken).ConfigureAwait(false);

        // 读取 OK 响应
        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (!rs.IsOK) throw new MySqlException("切换数据库失败");

        // 更新当前数据库名
        Database = databaseName;
    }

    /// <summary>异步创建数据库。使用 COM_CREATE_DB 二进制命令</summary>
    /// <param name="databaseName">要创建的数据库名</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    /// <remarks>
    /// COM_CREATE_DB 协议格式：
    /// 请求：command(1) + schema_name(EOF-terminated)
    /// 响应：OK 包或 ERR 包
    /// 注意：此命令已被弃用，建议使用 CREATE DATABASE 语句替代
    /// </remarks>
    public async Task CreateDatabaseAsync(String databaseName, CancellationToken cancellationToken = default)
    {
        if (databaseName.IsNullOrEmpty())
            throw new ArgumentNullException(nameof(databaseName));
        await SendCommandAsync(DbCmd.CREATE_DB, databaseName, false, cancellationToken).ConfigureAwait(false);

        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (!rs.IsOK) throw new MySqlException("创建数据库失败");
    }

    /// <summary>异步删除数据库。使用 COM_DROP_DB 二进制命令</summary>
    /// <param name="databaseName">要删除的数据库名</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    /// <remarks>
    /// COM_DROP_DB 协议格式：
    /// 请求：command(1) + schema_name(EOF-terminated)
    /// 响应：OK 包或 ERR 包
    /// 注意：此命令已被弃用，建议使用 DROP DATABASE 语句替代
    /// </remarks>
    public async Task DropDatabaseAsync(String databaseName, CancellationToken cancellationToken = default)
    {
        if (databaseName.IsNullOrEmpty())
            throw new ArgumentNullException(nameof(databaseName));
        await SendCommandAsync(DbCmd.DROP_DB, databaseName, false, cancellationToken).ConfigureAwait(false);

        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (!rs.IsOK) throw new MySqlException("删除数据库失败");
    }

    #endregion

    #region 查询与结果集
    /// <summary>异步发送 SQL 查询请求到服务器。</summary>
    /// <remarks>构造查询包时通常应预留帧头或使用 `ExpandHeader(4)`/在偏移 4 处写入命令字节，以便本方法把 `DbCmd.QUERY` 写入第一字节。</remarks>
    /// <param name="pk">包含查询语句的数据包。注意：数据包的第一个字节必须留作命令字节（DbCmd），构造查询数据包时不要占用该字节。</param>
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
    /// <param name="response">服务器数据包</param>
    /// <returns>查询结果对象</returns>
    public QueryResult GetResult(ServerPacket response)
    {
        var reader = response.CreateReader(0);

        if (response.IsOK)
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
            using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
            if (rs.IsEOF) break;

            var reader = new SpanReader(rs.Data);
            var dc = new MySqlColumn();
            dc.Read(ref reader);

            if (reader.Available >= 2) reader.ReadInt16();

            list.Add(dc);
        }

        // 读取 EOF 包
        using var rs2 = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (rs2.IsEOF) { }

        return [.. list];
    }

    /// <summary>异步读取结果集的下一行数据</summary>
    /// <param name="values">用于存储行数据的数组</param>
    /// <param name="columns">列信息数组</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>行读取结果，包含是否成功及状态信息</returns>
    public async Task<RowResult> NextRowAsync(Object[] values, MySqlColumn[] columns, CancellationToken cancellationToken = default)
    {
        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (rs.IsEOF) return ReadEofRowResult(rs);

        // 读取行数据（文本协议：所有值以 UTF-8 字符串传输）
        var reader = rs.CreateReader(0);
        for (var i = 0; i < values.Length; i++)
        {
            var len = (Int32)reader.ReadLength();
            if (len == -1)
            {
                values[i] = DBNull.Value;
                continue;
            }

            values[i] = MySqlFieldCodec.ReadTextValue(ref reader, columns[i], len);
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
        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (rs.IsEOF) return ReadEofRowResult(rs);

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
            values[i] = MySqlFieldCodec.ReadBinaryValue(ref reader, columns[i]);
        }

        return new RowResult(true, 0, 0);
    }

    /// <summary>读取 EOF 包并返回行结束结果</summary>
    /// <param name="rs">服务器包</param>
    /// <returns>行读取结果</returns>
    private RowResult ReadEofRowResult(ServerPacket rs)
    {
        // EOF_Packet: header(0xFE) + warnings(2) + status_flags(2)
        var eofReader = rs.CreateReader(1);
        ServerStatusFlags statusFlags = 0;
        UInt16 warnings = 0;
        if (Capability.Has(ClientFlags.PROTOCOL_41) && eofReader.Available >= 4)
        {
            warnings = eofReader.ReadUInt16();
            statusFlags = (ServerStatusFlags)eofReader.ReadUInt16();
        }
        return new RowResult(false, statusFlags, warnings);
    }

    /// <summary>异步获取指定表的字段列表。使用 COM_FIELD_LIST 二进制命令</summary>
    /// <param name="table">表名</param>
    /// <param name="wildcard">列名通配符模式，可为空表示获取所有列</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>列信息数组</returns>
    /// <remarks>
    /// COM_FIELD_LIST 协议格式：
    /// 请求：command(1) + table(NUL-terminated) + wildcard(optional, EOF-terminated)
    /// 响应：多个列定义包 + EOF 包，或 ERR 包
    /// 注意：此命令在 MySQL 5.7.11 后被标记为已弃用，建议使用 SHOW COLUMNS 替代
    /// </remarks>
    public async Task<MySqlColumn[]> FieldListAsync(String table, String? wildcard = null, CancellationToken cancellationToken = default)
    {
        if (table.IsNullOrEmpty())
            throw new ArgumentNullException(nameof(table));

        var tableByteCount = Encoding.GetByteCount(table);
        var wildcardByteCount = wildcard.IsNullOrEmpty() ? 0 : Encoding.GetByteCount(wildcard);
        // command(1) + table + NUL(1) + wildcard
        var len = 1 + tableByteCount + 1 + wildcardByteCount;
        var buf = Pool.Shared.Rent(4 + len);

        try
        {
            buf[4] = (Byte)DbCmd.FIELD_LIST;
            Encoding.GetBytes(table, 0, table.Length, buf, 5);
            buf[5 + tableByteCount] = 0; // NUL 终止符
            if (wildcardByteCount > 0)
                Encoding.GetBytes(wildcard!, 0, wildcard!.Length, buf, 6 + tableByteCount);

            _seq = 0;
            await SendPacketAsync(new ArrayPacket(buf, 4, len), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }

        // 读取列定义包直到 EOF
        var list = new List<MySqlColumn>();
        while (true)
        {
            using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
            if (rs.IsEOF) break;

            var reader = new SpanReader(rs.Data);
            var dc = new MySqlColumn();
            dc.Read(ref reader);
            list.Add(dc);
        }

        return [.. list];
    }

    #endregion

    #region 预编译语句
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

        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
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
            // 读取列定义包 + EOF 包
            for (var j = 0; j <= numCols; j++)
            {
                using var _ = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        return new PrepareResult(statementId, columns!);
    }

    /// <summary>异步执行已准备的预编译语句，支持二进制参数绑定</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="parameters">参数集合，可为空</param>
    /// <param name="paramColumns">Prepare 返回的参数列信息</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task ExecuteStatementAsync(Int32 statementId, MySqlParameterCollection? parameters, MySqlColumn[]? paramColumns, CancellationToken cancellationToken = default)
    {
        var sendPk = BuildExecutePacket(statementId, parameters);
        _seq = 0;
        await SendPacketAsync(sendPk, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>构建 COM_STMT_EXECUTE 数据包。仅构建包内容，不发送</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="parameters">参数集合，可为空</param>
    /// <returns>待发送的数据包（不含帧头）</returns>
    private IPacket BuildExecutePacket(Int32 statementId, MySqlParameterCollection? parameters)
    {
        var numParams = parameters?.Count ?? 0;
        var hasQueryAttrs = Capability.Has(ClientFlags.CLIENT_QUERY_ATTRIBUTES);

        // 构建 COM_STMT_EXECUTE 数据包
        var pk = new OwnerPacket(8192);
        var writer = new SpanWriter(pk);
        writer.Advance(4); // 预留帧头

        writer.Write((Byte)DbCmd.EXECUTE);
        writer.Write(statementId);  // statement_id (4 bytes LE)
        writer.Write((Byte)0x00);   // flags (cursor_type): 不使用游标
        writer.Write((Int32)1);     // iteration_count: 固定 1

        // CLIENT_QUERY_ATTRIBUTES 模式下需要发送 parameter_count（含查询属性参数）
        if (hasQueryAttrs)
            writer.WriteLength(numParams);

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
            writer.Write(nullBitmap);

            // new_params_bound_flag = 1（始终重新发送类型信息）
            writer.Write((Byte)0x01);

            // param_type × N (每个2字节: type + unsigned_flag)
            // CLIENT_QUERY_ATTRIBUTES 模式下，每个参数类型后还需附带参数名（length-encoded string）
            for (var i = 0; i < numParams; i++)
            {
                var val = ((MySqlParameter)parameters![i]).Value;
                var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue(val);
                writer.Write(typeId);
                writer.Write(unsigned ? (Byte)0x80 : (Byte)0x00);

                if (hasQueryAttrs)
                {
                    // 参数名为空字符串（预编译语句按位置绑定，不需要名称）
                    writer.WriteLength(0);
                }
            }

            // param_value × N（仅非 NULL 参数）
            for (var i = 0; i < numParams; i++)
            {
                var val = ((MySqlParameter)parameters![i]).Value;
                if (val != null && val != DBNull.Value)
                    MySqlFieldCodec.WriteBinaryValue(ref writer, val, Encoding);
            }
        }

        return pk.Slice(4, writer.Position - 4);
    }

    /// <summary>异步关闭预编译语句并释放服务器资源。COM_STMT_CLOSE 无响应包</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task CloseStatementAsync(Int32 statementId, CancellationToken cancellationToken = default)
    {
        await SendCommandAsync(DbCmd.CLOSE_STMT, statementId, cancellationToken).ConfigureAwait(false);
        // COM_STMT_CLOSE 不会返回响应包
    }

    /// <summary>异步重置预编译语句的参数和状态</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task ResetStatementAsync(Int32 statementId, CancellationToken cancellationToken = default)
    {
        await SendCommandAsync(DbCmd.RESET_STMT, statementId, cancellationToken).ConfigureAwait(false);

        // COM_STMT_RESET 返回 OK 包
        using var _ = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>异步发送预编译语句的大数据参数。使用 COM_STMT_SEND_LONG_DATA 二进制命令</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="paramIndex">参数索引（从 0 开始）</param>
    /// <param name="data">要发送的数据块</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    /// <remarks>
    /// COM_STMT_SEND_LONG_DATA 协议格式：
    /// 请求：command(1) + statement_id(4) + param_id(2) + data(EOF-terminated)
    /// 响应：无响应包。可多次调用同一参数发送大数据分片，服务端自动拼接
    /// 用途：用于发送 BLOB/TEXT 等大字段数据，避免一次性将大数据放入 EXECUTE 包
    /// </remarks>
    public async Task SendLongDataAsync(Int32 statementId, Int16 paramIndex, Byte[] data, CancellationToken cancellationToken = default)
    {
        if (data == null)
            throw new ArgumentNullException(nameof(data));

        // command(1) + statement_id(4) + param_id(2) + data
        var len = 1 + 4 + 2 + data.Length;
        var buf = Pool.Shared.Rent(4 + len);

        try
        {
            var writer = new SpanWriter(buf);
            writer.Advance(4);
            writer.Write((Byte)DbCmd.LONG_DATA);
            writer.Write(statementId);
            writer.Write(paramIndex);
            writer.Write(data);

            _seq = 0;
            await SendPacketAsync(new ArrayPacket(buf, 4, len), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }

        // COM_STMT_SEND_LONG_DATA 无响应包
    }

    /// <summary>异步获取预编译语句的游标结果行。使用 COM_STMT_FETCH 二进制命令</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="numRows">要获取的行数</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    /// <remarks>
    /// COM_STMT_FETCH 协议格式：
    /// 请求：command(1) + statement_id(4) + num_rows(4, little-endian)
    /// 响应：多个二进制行数据包 + EOF 包
    /// 前提：EXECUTE 时需指定 CURSOR_TYPE_READ_ONLY 游标标志，否则服务端返回错误
    /// 用途：配合服务端游标逐批拉取数据行，适合大结果集场景
    /// </remarks>
    public async Task FetchAsync(Int32 statementId, Int32 numRows, CancellationToken cancellationToken = default)
    {
        var buf = Pool.Shared.Rent(4 + 1 + 4 + 4);
        try
        {
            var writer = new SpanWriter(buf);
            writer.Advance(4);
            writer.Write((Byte)DbCmd.FETCH);
            writer.Write(statementId);
            writer.Write(numRows);

            _seq = 0;
            await SendPacketAsync(new ArrayPacket(buf, 4, 9), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }

        // 响应的行数据和 EOF 包由调用方通过 NextBinaryRowAsync 逐行读取
    }

    #endregion

    #region 管道化执行
    /// <summary>管道化批量执行预编译语句。根据 Pipeline 设置选择真管道化或串行模式</summary>
    /// <remarks>
    /// 真管道化模式（Pipeline=true）：批量构建并发送所有 EXECUTE 包到网络缓冲区，最后一次性 Flush，
    /// 然后按顺序逐个读取响应。网络延迟仅发生一次，适合大批量 DML 操作。
    /// 串行模式（Pipeline=false）：逐条发送并读取响应，跳过 MySqlCommand 层的开销，直接在协议层循环。
    /// </remarks>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="parameterSets">多组参数集合</param>
    /// <param name="paramColumns">Prepare 返回的参数列信息</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>总影响行数</returns>
    public async Task<Int32> ExecuteStatementPipelineAsync(Int32 statementId, IList<MySqlParameterCollection> parameterSets, MySqlColumn[]? paramColumns, CancellationToken cancellationToken = default)
    {
        if (parameterSets == null || parameterSets.Count == 0) return 0;

        // 真管道化：批量发送所有请求包，最后一次性 Flush，再批量读取响应
        if (Setting.Pipeline)
            return await ExecutePipelineCoreAsync(statementId, parameterSets, cancellationToken).ConfigureAwait(false);

        // 串行模式：逐条发送 EXECUTE + 读取响应的快速循环
        var totalAffected = 0;
        for (var i = 0; i < parameterSets.Count; i++)
        {
            await ExecuteStatementAsync(statementId, parameterSets[i], paramColumns, cancellationToken).ConfigureAwait(false);
            using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
            var qr = GetResult(rs);
            totalAffected += qr.AffectedRows;
        }

        return totalAffected;
    }

    /// <summary>真管道化执行核心。并发发送 COM_STMT_EXECUTE 包和读取响应</summary>
    /// <remarks>
    /// 利用 TCP 全双工特性，发送和接收并发执行，避免大批量管道化时 TCP 缓冲区双向填满导致死锁。
    /// 当批量数较大时（如万行 UPDATE），若先发完所有请求再读响应，客户端发送缓冲区和服务端发送缓冲区
    /// 可能同时填满，双方均阻塞在写操作上形成死锁，最终触发读取超时。
    /// 并发模式下，读取任务在 ReadExactlyAsync 处让出执行权等待数据，发送任务持续推送数据包，
    /// 服务端处理后返回的响应立即被读取任务消费，保持双向数据流畅通。
    /// </remarks>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="parameterSets">多组参数集合</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>总影响行数</returns>
    private async Task<Int32> ExecutePipelineCoreAsync(Int32 statementId, IList<MySqlParameterCollection> parameterSets, CancellationToken cancellationToken)
    {
        var count = parameterSets.Count;
        var ms = _stream ?? throw new InvalidOperationException("未打开连接");

        // 先启动读取任务，它会在 ReadExactlyAsync 处让出执行权等待数据到达
        var readTask = ReadPipelineResponsesAsync(count, cancellationToken);

        // 发送所有 EXECUTE 包，直接写入 seq=0 帧头，避免与读取任务竞争 _seq 字段
        for (var i = 0; i < count; i++)
        {
            var pk = BuildExecutePacket(statementId, parameterSets[i]);

            // 管道化每个 EXECUTE 独立使用 seq=0，直接构造帧头避免并发读写 _seq 竞态
            var len = pk.Total;
            var pk2 = pk.ExpandHeader(4);
            pk2[0] = (Byte)(len & 0xFF);
            pk2[1] = (Byte)((len >> 8) & 0xFF);
            pk2[2] = (Byte)((len >> 16) & 0xFF);
            pk2[3] = 0;

            await pk2.CopyToAsync(ms, cancellationToken).ConfigureAwait(false);

            // 定期 Flush 推送数据到服务端（SslStream 有内部缓冲，必须 Flush 才能发出）
            if (i == count - 1 || (i + 1) % 100 == 0)
                await ms.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        // 等待所有响应读取完成
        return await readTask.ConfigureAwait(false);
    }

    /// <summary>批量读取管道化响应，累加影响行数</summary>
    /// <param name="count">需要读取的响应数量</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>总影响行数</returns>
    private async Task<Int32> ReadPipelineResponsesAsync(Int32 count, CancellationToken cancellationToken)
    {
        var totalAffected = 0;
        MySqlException? firstError = null;
        for (var i = 0; i < count; i++)
        {
            try
            {
                using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
                var qr = GetResult(rs);
                totalAffected += qr.AffectedRows;
            }
            catch (MySqlException ex)
            {
                // 记录第一个错误，继续读取后续响应以保持连接状态干净
                firstError ??= ex;
            }
        }

        // 如果管道中有命令执行失败，抛出第一个错误
        if (firstError != null) throw firstError;

        return totalAffected;
    }
    #endregion

    #region 服务器管理
    /// <summary>异步获取服务器统计信息。使用 COM_STATISTICS 二进制命令</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>服务器统计信息字符串，包含 Uptime、Threads、Queries 等</returns>
    /// <remarks>
    /// COM_STATISTICS 协议格式：
    /// 请求：command(1)
    /// 响应：非标准 OK 包，直接返回一个人类可读的统计信息字符串（不以 0x00 开头）
    /// 示例响应：Uptime: 123  Threads: 1  Questions: 456  Slow queries: 0  Opens: 78  ...
    /// </remarks>
    public async Task<String> StatisticsAsync(CancellationToken cancellationToken = default)
    {
        await SendCommandAsync(DbCmd.STATISTICS, cancellationToken).ConfigureAwait(false);
        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

        // COM_STATISTICS 返回的是纯文本字符串，不是标准的 OK/ERR/EOF 包
        return Encoding.GetString(rs.Data.GetSpan());
    }

    /// <summary>异步获取当前活动线程的进程列表。使用 COM_PROCESS_INFO 二进制命令</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>查询结果对象，包含列数信息用于后续读取结果集</returns>
    /// <remarks>
    /// COM_PROCESS_INFO 协议格式：
    /// 请求：command(1)
    /// 响应：与 SELECT 查询相同的结果集格式（列数 + 列定义 + 行数据 + EOF）
    /// 返回的列包含：Id, User, Host, db, Command, Time, State, Info
    /// 注意：此命令已被弃用，建议使用 SHOW PROCESSLIST 语句替代
    /// </remarks>
    public async Task<QueryResult> ProcessInfoAsync(CancellationToken cancellationToken = default)
    {
        await SendCommandAsync(DbCmd.PROCESS_INFO, cancellationToken).ConfigureAwait(false);
        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

        return GetResult(rs);
    }

    /// <summary>异步终止指定的服务器线程。使用 COM_PROCESS_KILL 二进制命令</summary>
    /// <param name="processId">要终止的线程 ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    /// <remarks>
    /// COM_PROCESS_KILL 协议格式：
    /// 请求：command(1) + connection_id(4, little-endian)
    /// 响应：OK 包或 ERR 包
    /// 注意：此命令已被弃用，建议使用 KILL 语句替代
    /// </remarks>
    public async Task ProcessKillAsync(Int32 processId, CancellationToken cancellationToken = default)
    {
        await SendCommandAsync(DbCmd.PROCESS_KILL, processId, cancellationToken).ConfigureAwait(false);

        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (!rs.IsOK)
            throw new MySqlException("终止线程失败");
    }

    /// <summary>异步设置连接选项。使用 COM_SET_OPTION 二进制命令</summary>
    /// <param name="option">选项值。0 = MYSQL_OPTION_MULTI_STATEMENTS_ON，1 = MYSQL_OPTION_MULTI_STATEMENTS_OFF</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    /// <remarks>
    /// COM_SET_OPTION 协议格式：
    /// 请求：command(1) + option(2, little-endian)
    /// 响应：EOF 包（成功）或 ERR 包
    /// 选项值：
    ///   0 = MYSQL_OPTION_MULTI_STATEMENTS_ON  - 启用多语句支持
    ///   1 = MYSQL_OPTION_MULTI_STATEMENTS_OFF - 禁用多语句支持
    /// </remarks>
    public async Task SetOptionAsync(Int16 option, CancellationToken cancellationToken = default)
    {
        var buf = Pool.Shared.Rent(4 + 1 + 2);
        try
        {
            var writer = new SpanWriter(buf);
            writer.Advance(4);
            writer.Write((Byte)DbCmd.SET_OPTION);
            writer.Write(option);

            _seq = 0;
            await SendPacketAsync(new ArrayPacket(buf, 4, 3), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }

        // COM_SET_OPTION 返回 EOF 包表示成功
        using var _ = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>异步获取服务器时间戳。使用 COM_TIME 二进制命令</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    /// <remarks>
    /// COM_TIME 协议格式：
    /// 请求：command(1)
    /// 响应：时间字符串或 ERR 包
    /// 注意：此命令在 MySQL 协议中已不推荐使用，服务器可能返回错误
    /// </remarks>
    public async Task TimeAsync(CancellationToken cancellationToken = default)
    {
        await SendCommandAsync(DbCmd.TIME, cancellationToken).ConfigureAwait(false);
        using var _ = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region Binlog 复制
    /// <summary>异步注册从属服务器。使用 COM_REGISTER_SLAVE 二进制命令</summary>
    /// <param name="serverId">从属服务器 ID</param>
    /// <param name="host">从属服务器主机名</param>
    /// <param name="user">复制用户名</param>
    /// <param name="password">复制密码</param>
    /// <param name="port">从属服务器端口</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    /// <remarks>
    /// COM_REGISTER_SLAVE 协议格式：
    /// 请求：command(1) + server_id(4) + hostname_length(1) + hostname + user_length(1) + user
    ///       + password_length(1) + password + port(2) + replication_rank(4) + master_id(4)
    /// 响应：OK 包或 ERR 包
    /// 用途：将当前连接注册为 MySQL 复制的从属节点，配合 COM_BINLOG_DUMP 使用
    /// </remarks>
    public async Task RegisterSlaveAsync(Int32 serverId, String host, String user, String password, Int16 port, CancellationToken cancellationToken = default)
    {
        var hostStr = host ?? "";
        var userStr = user ?? "";
        var pwdStr = password ?? "";
        var hostByteCount = Encoding.GetByteCount(hostStr);
        var userByteCount = Encoding.GetByteCount(userStr);
        var pwdByteCount = Encoding.GetByteCount(pwdStr);

        // command(1) + server_id(4) + hostname_len(1) + hostname + user_len(1) + user
        // + password_len(1) + password + port(2) + replication_rank(4) + master_id(4)
        var len = 1 + 4 + 1 + hostByteCount + 1 + userByteCount + 1 + pwdByteCount + 2 + 4 + 4;
        var buf = Pool.Shared.Rent(4 + len);

        try
        {
            var writer = new SpanWriter(buf);
            writer.Advance(4);
            writer.Write((Byte)DbCmd.REGISTER_SLAVE);
            writer.Write(serverId);

            // hostname（长度前缀 + 内容直接编码到缓冲区）
            writer.Write(hostStr, 0, Encoding);
            writer.Write(userStr, 0, Encoding);
            writer.Write(pwdStr, 0, Encoding);

            writer.Write(port);
            writer.Write((Int32)0); // replication_rank
            writer.Write((Int32)0); // master_id

            _seq = 0;
            await SendPacketAsync(new ArrayPacket(buf, 4, len), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Pool.Shared.Return(buf);
        }

        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (!rs.IsOK)
            throw new MySqlException("注册从属服务器失败");
    }

    /// <summary>异步停止 Binlog 转储并注销从属服务器。使用 COM_QUIT 关闭连接</summary>
    /// <remarks>
    /// MySQL 协议没有专门的"注销从属"命令。停止 binlog 订阅的标准方式是：
    /// 1. 发送 COM_QUIT 关闭连接，服务器会自动清除从属注册信息
    /// 2. 或者通过 KILL 命令终止 binlog dump 线程
    /// 连接关闭后，服务器端会自动释放该 slave 的注册记录和 binlog 读取线程。
    /// </remarks>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task StopBinlogDumpAsync(CancellationToken cancellationToken = default)
    {
        if (!Active || _stream == null) return;

        try
        {
            await SendCommandAsync(DbCmd.QUIT, cancellationToken).ConfigureAwait(false);
        }
        catch { /* 忽略退出过程中的异常 */ }

        // 释放网络资源
        _client.TryDispose();
        _client = null;
        _stream = null;
        Active = false;
    }

    /// <summary>异步发送 Binlog 转储请求。使用 COM_BINLOG_DUMP 二进制命令，开始接收 binlog 事件流</summary>
    /// <param name="position">Binlog 起始位置（文件名+偏移）</param>
    /// <param name="serverId">从属服务器 ID，需唯一</param>
    /// <param name="flags">转储标志。1 = BINLOG_DUMP_NON_BLOCK（非阻塞模式，到达末尾立即返回 EOF）</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    /// <remarks>
    /// COM_BINLOG_DUMP 协议格式：
    /// 请求：command(1) + binlog_pos(4) + flags(2) + server_id(4) + binlog_filename(EOF)
    /// 响应：持续发送 binlog 事件包，每个包以 0x00 状态字节开头，后跟完整的事件数据
    /// 用途：将当前连接变为 binlog 监听模式，持续接收 MySQL 主库的 binlog 事件流
    /// </remarks>
    public async Task BinlogDumpAsync(BinlogPosition position, Int32 serverId, UInt16 flags = 0, CancellationToken cancellationToken = default)
    {
        if (position == null) throw new ArgumentNullException(nameof(position));

        var fileName = position.FileName ?? "";
        var fileBytes = Encoding.GetBytes(fileName);

        // command(1) + binlog_pos(4) + flags(2) + server_id(4) + binlog_filename
        var len = 1 + 4 + 2 + 4 + fileBytes.Length;
        using var pk = new OwnerPacket(4 + len);
        var writer = new SpanWriter(pk);
        writer.Advance(4); // 预留帧头

        writer.Write((Byte)DbCmd.BINLOG_DUMP);
        writer.Write(position.Position);
        writer.Write(flags);
        writer.Write(serverId);
        writer.Write(fileBytes);

        _seq = 0;
        await SendPacketAsync(pk.Slice(4, len), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>异步读取下一个 Binlog 事件。在 BinlogDumpAsync 之后持续调用以接收事件流</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>Binlog 事件，EOF 时返回 null</returns>
    /// <remarks>
    /// Binlog 事件包格式：
    /// OK 标志(1字节, 0x00) + 事件头(19字节) + 事件数据体
    /// 事件头：timestamp(4) + type_code(1) + server_id(4) + event_length(4) + next_position(4) + flags(2)
    /// 当收到 EOF 包（0xFE）或 ERR 包（0xFF）时表示流结束
    /// </remarks>
    public async Task<BinlogEvent?> ReadBinlogEventAsync(CancellationToken cancellationToken = default)
    {
        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

        // EOF 表示 binlog 流结束（非阻塞模式）
        if (rs.IsEOF) return null;

        var span = rs.Data.GetSpan();

        // 第一个字节是 OK 标志 0x00，跳过；至少需要 1 + 19 字节
        if (span.Length < 20) return null;

        var reader = new SpanReader(span[1..]) { IsLittleEndian = true };
        var ev = new BinlogEvent
        {
            Timestamp = reader.ReadUInt32(),
            EventType = (BinlogEventType)reader.ReadByte(),
            ServerId = reader.ReadUInt32(),
            EventLength = reader.ReadUInt32(),
            NextPosition = reader.ReadUInt32(),
            Flags = reader.ReadUInt16(),
        };

        // 事件数据体（跳过 1 字节 OK 标志 + 19 字节事件头）
        if (span.Length > 20)
            ev.Data = span[20..].ToArray();

        return ev;
    }
    #endregion
}