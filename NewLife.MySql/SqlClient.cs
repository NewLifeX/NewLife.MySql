using System.IO.Compression;
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
    private const Int32 PacketTraceBytes = 512;
    private const Int32 CompressedHeaderSize = 7;
    private Boolean _resetUnsupported;

    // 压缩协议相关
    private Boolean _useCompression;
    private Byte[]? _compressHeaderBuf;
    // 解压缓存：一个压缩包可能包含多个普通包，缓存未消费的普通包数据（含各包 4 字节普通帧头）
    private Byte[]? _compressCache;
    private Int32 _compressCacheOffset;
    private Int32 _compressCacheLength;
    // 是否存在跨压缩包的不完整普通包数据，为 true 时强制读取下一个压缩包拼接
    private Boolean _compressPending;

    #region 属性
    /// <summary>连接字符串配置</summary>
    public MySqlConnectionStringBuilder Setting { get; } = [];

    /// <summary>是否启用压缩协议。认证流程在发送认证包后、读取认证响应前启用（服务器收到 COMPRESS flag 后即开始压缩）</summary>
    internal Boolean UseCompression { get => _useCompression; set => _useCompression = value; }

    /// <summary>单帧载荷上限（协议帧头 3 字节长度字段）</summary>
    private const Int32 MaxPayload = 0xFF_FFFF;

    /// <summary>连接是否活动</summary>
    public Boolean Active { get; internal set; }

    /// <summary>服务器支持的最大数据包大小</summary>
    public Int64 MaxPacketSize { get; private set; }

    /// <summary>客户端支持的特性标志</summary>
    public ClientFlags Capability { get; set; }

    /// <summary>服务器变量集合，键值对形式存储</summary>
    public IDictionary<String, String>? Variables { get; set; }

    /// <summary>服务器是否启用 NO_BACKSLASH_ESCAPES。启用时反斜杠是普通字符，字符串字面量只能用双写单引号转义；变量未加载时按未启用处理</summary>
    public Boolean NoBackslashEscapes
    {
        get
        {
            if (Variables == null || !Variables.TryGetValue("sql_mode", out var mode) || mode.IsNullOrEmpty()) return false;

            return mode.Contains("NO_BACKSLASH_ESCAPES", StringComparison.OrdinalIgnoreCase);
        }
    }

    /// <summary>会话版本。会话被重置（COM_RESET_CONNECTION）时递增，用于识别预编译语句是否已失效</summary>
    internal Int32 SessionVersion { get; private set; }

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

    /// <summary>连接创建时间。认证成功打开连接时赋值，用于连接最大存活期判定</summary>
    public DateTime CreatedTime { get; internal set; }

    /// <summary>当前数据库名。记录正在使用的数据库，首次打开连接时赋值，调用 SetDatabaseAsync 后会更新</summary>
    public String Database { get; set; } = null!;

    /// <summary>性能跟踪器</summary>
    public ITracer? Tracer { get; set; } = MySqlClientFactory.Instance.Tracer;

    private TcpClient? _client;
    private Socket? _rawSocket;
    /// <summary>底层 TCP 客户端。供连接池探活与单元测试注入</summary>
    internal TcpClient? Tcp { get => _client; set => _client = value; }

    private Byte _seq = 1;
    private TimerX? _timer;

    // 行接收缓冲区：跨行复用，避免每行分配 OwnerPacket + ArrayPool
    private Byte[]? _rowBuffer;
    // 二进制行 null_bitmap 缓冲区：跨行复用，避免每行分配 byte[]
    private Byte[]? _nullBitmap;
    // 帧头缓冲区：4 字节固定大小，同一连接串行读包，可安全复用
    private readonly Byte[] _headerBuf = new Byte[4];
    // 读前置缓冲区：合并多次小读取为一次大的底层读取，减少系统调用次数
    private readonly ReadBuffer _reader = new();
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
        await OpenAsync(true, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>异步打开到 MySQL 服务器的连接</summary>
    /// <param name="allowSsl">是否允许尝试 SSL/TLS 握手</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    private async Task OpenAsync(Boolean allowSsl, CancellationToken cancellationToken)
    {
        if (Active) return;
        if (_stream != null) return;
        cancellationToken.ThrowIfCancellationRequested();

        // 同一个 SqlClient 可能被连接池复用；新连接不能继承上一条连接的压缩缓存/序列状态
        ResetCompressionState();

        var set = Setting;
        var server = set.Server;
        var socketPath = set.Socket;
        if (server.IsNullOrEmpty() && socketPath.IsNullOrEmpty())
            throw new InvalidOperationException(L.T("Server address is not specified (Server or Socket)."));

        // SslMode 拼错时旧实现会静默按“加密但不校验证书”处理，服务端不支持 SSL 时甚至静默降级明文。
        // 在建立网络连接前拒绝未知取值，避免安全配置失效。
        var sslMode = set.SslMode;
        if (!sslMode.IsNullOrEmpty() && !IsSupportedSslMode(sslMode))
            throw new ArgumentException(L.T("Unsupported SslMode: {0}.", sslMode), nameof(set.SslMode));

        // 握手 charset 只声明编号，驱动收发始终按 UTF-8 编码。GBK/GB2312/Latin1 等会造成双向乱码，
        // 且可能让转义后的字节被服务器按另一字符集解释，直接拒绝而不是静默使用错误编码。
        if (set.CharSet is not (MySqlCharSet.Utf8 or MySqlCharSet.Utf8Mb4 or MySqlCharSet.Utf8Mb4Unicode))
            throw new NotSupportedException(L.T("Character set {0} is not supported; use utf8, utf8mb4, or utf8mb4_unicode_ci.", set.CharSet));

        var port = set.Port;
        if (port == 0) port = 3306;
        var isUnixSocket = !socketPath.IsNullOrEmpty();
        if (isUnixSocket) server = socketPath!;

        var msTimeout = set.ConnectionTimeout * 1000;
        if (msTimeout <= 0) msTimeout = 15000;

        using var span = Tracer?.NewSpan($"db:{set.Database}:Open", new { server, port, set.UserID, set.SslMode, set.UseServerPrepare, set.Pipeline });

        // 异步连接网络
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(msTimeout);

        if (isUnixSocket)
        {
            // Unix 域套接字连接（仅 .NET Core 5.0+ 且 Linux/macOS 生效）
#if NET5_0_OR_GREATER
            var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            socket.ReceiveTimeout = msTimeout;
            socket.SendTimeout = msTimeout;
            try
            {
                await socket.ConnectAsync(new UnixDomainSocketEndPoint(socketPath!), cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
            {
                span?.SetError(ex);
                socket.Dispose();
                throw new TimeoutException(L.T("Unix socket connection to {0} timed out ({1}ms).", socketPath, msTimeout));
            }
            catch (Exception ex)
            {
                span?.SetError(ex);
                socket.Dispose();
                throw;
            }
            _rawSocket = socket;
            _stream = new NetworkStream(socket, ownsSocket: true);
#else
            throw new PlatformNotSupportedException(L.T("Unix domain sockets are only supported on .NET 5.0+ platforms (Linux/macOS)."));
#endif
        }
        else
        {
            var client = new TcpClient();
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
                    throw new TimeoutException(L.T("Connection to {0}:{1} timed out ({2}ms).", server, port, msTimeout));
                }
                await connectTask.ConfigureAwait(false);
#endif
            }
            catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
            {
                span?.SetError(ex);
                client.Close();
                throw new TimeoutException(L.T("Connection to {0}:{1} timed out ({2}ms).", server, port, msTimeout));
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
        }

        try
        {
            // 异步读取欢迎信息。使用连接超时令牌，保证整个打开过程（含握手/认证）受 ConnectionTimeout 约束
            var welcome = await GetWelcomeAsync(cts.Token).ConfigureAwait(false);
            Welcome = welcome;
            Capability = welcome.Capability;

            // SSL/TLS 握手
            if (allowSsl && !sslMode.IsNullOrEmpty() && !sslMode.EqualIgnoreCase("None", "Disabled"))
            {
                if (Capability.Has(ClientFlags.SSL))
                {
                    try
                    {
                        await StartSslAsync(server!, cts.Token).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (sslMode.EqualIgnoreCase("Preferred"))
                    {
                        XTrace.WriteLine("[MySqlSsl] db={0} server={1}:{2} SslMode=Preferred fallback to plain text: {3}: {4}",
                            set.Database, server, port, ex.GetType().Name, ex.Message);

                        _client.TryDispose();
                        _client = null;
                        _stream = null;
                        _reader.Reset();
                        Welcome = null;
                        Variables = null;
                        MaxPacketSize = 0;
                        Database = null!;
                        Capability = 0;
                        Active = false;
                        _seq = 1;

                        await OpenAsync(false, cts.Token).ConfigureAwait(false);
                        return;
                    }
                }
                else if (sslMode.EqualIgnoreCase("Required") || VerifyCertificate)
                    throw new NotSupportedException(L.T("The server does not support SSL connections."));
            }

            // 验证方法
            var method = welcome.AuthMethod;
            if (!method.IsNullOrEmpty() && !method.EqualIgnoreCase("mysql_native_password", "caching_sha2_password", "authentication_webauthn_client"))
                throw new NotSupportedException(L.T("Unsupported authentication method: {0}.", method));

            // 异步验证
            var auth = new Authentication(this);
            await auth.AuthenticateAsync(welcome, false, cts.Token).ConfigureAwait(false);

            // 认证成功后，将 Capability 更新为实际协商的客户端标志
            Capability = auth.GetFlags(welcome.Capability);

            // 压缩协议：如果客户端和服务器都支持 COMPRESS 且连接字符串启用，则开启压缩
            if (Capability.Has(ClientFlags.COMPRESS) && set.UseCompression)
                _useCompression = true;

            // 记录当前使用的数据库
            Database = set.Database ?? "";

            // 认证成功后才标记为活动状态
            Active = true;
            CreatedTime = DateTime.Now;
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
#if !NET5_0_OR_GREATER
            // TLS 握手等阶段不响应取消令牌，超时/取消通过关闭 socket 解除挂起，这里还原为超时/取消语义
            if (cts.IsCancellationRequested)
            {
                if (cancellationToken.IsCancellationRequested) throw new OperationCanceledException(cancellationToken);
                throw new TimeoutException(L.T("Connection to {0}:{1} timed out ({2}ms).", server, port, msTimeout));
            }
#endif
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
        var auth = new Authentication(this);
        var flags = auth.GetFlags(Capability) | ClientFlags.SSL;
        var buf = new Byte[32];
        var writer = new SpanWriter(buf);
        writer.Write((UInt32)flags);
        writer.Write(0xFF_FFFF); // MaxPacket
        writer.Write(Setting.GetCharSetNumber()); // 字符集编号，默认utf8mb4
        writer.Write(new Byte[23]);

        _seq = 1;
        await SendPacketAsync((ArrayPacket)buf, cancellationToken).ConfigureAwait(false);

        // 升级为 SslStream。VerifyFull/VerifyCA/VerifyIdentity 必须校验证书链与主机名，
        // Required/Preferred 保持历史行为（不校验），便于自签证书环境继续可用
        var verifyCertificate = VerifyCertificate;
        var sslErrors = SslPolicyErrors.None;
        var sslStream = new SslStream(_stream!, false, (sender, certificate, chain, errors) =>
        {
            var accepted = AcceptServerCertificate(verifyCertificate, errors);
            if (!accepted) sslErrors = errors;

            return accepted;
        });
#if NET5_0_OR_GREATER
        try
        {
            await sslStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = server,
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            }, cancellationToken).ConfigureAwait(false);
        }
#else
        // 旧框架的 TLS 握手同样不响应取消令牌，超时/取消时关闭 socket 解除挂起
        using var abort = cancellationToken.Register(static s => ((SqlClient)s!).Abort(), this);
        try
        {
            await sslStream.AuthenticateAsClientAsync(server, null, SslProtocols.Tls12, false).ConfigureAwait(false);
        }
#endif
        catch (AuthenticationException ex) when (sslErrors != SslPolicyErrors.None)
        {
            throw new AuthenticationException(
                L.T("Server certificate validation failed ({0}). Set SslMode=Required to skip certificate validation.", sslErrors), ex);
        }
        _reader.Reset();
        _stream = sslStream;
    }

    /// <summary>当前 SslMode 是否要求校验证书链与主机名</summary>
    private Boolean VerifyCertificate => Setting.SslMode.EqualIgnoreCase("VerifyFull", "VerifyCA", "VerifyIdentity");

    /// <summary>判断 SslMode 是否为受支持取值</summary>
    /// <param name="sslMode">连接字符串中的 SslMode</param>
    /// <returns>是否受支持</returns>
    internal static Boolean IsSupportedSslMode(String? sslMode)
        => sslMode.IsNullOrEmpty() || sslMode.EqualIgnoreCase("None", "Disabled", "Preferred", "Required", "VerifyCA", "VerifyFull", "VerifyIdentity");

    /// <summary>判断是否接受服务端证书</summary>
    /// <param name="verifyCertificate">是否要求校验证书</param>
    /// <param name="errors">证书链/主机名校验结果</param>
    /// <returns>是否接受</returns>
    internal static Boolean AcceptServerCertificate(Boolean verifyCertificate, SslPolicyErrors errors) => !verifyCertificate || errors == SslPolicyErrors.None;

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
        _reader.Reset();
        ResetCompressionState();

        Active = false;
        SessionVersion++;
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
    /// <summary>重置连接状态。探测底层 socket 是否仍可复用</summary>
    /// <returns>连接是否仍然可用</returns>
    public Boolean Reset()
    {
        if (!Active) return false;

        if (IsSocketAlive() != SocketHealth.Alive)
        {
            // 对端已关闭、连接重置、或存在残留脏数据，标记失效不再复用
            Active = false;
            return false;
        }

        return true;
    }

    /// <summary>重置连接会话状态。探测底层 socket 健康状态，并通过 COM_RESET_CONNECTION 清理会话变量、临时表与用户变量</summary>
    /// <returns>连接是否仍然可用</returns>
    /// <remarks>探测职责完全由 <see cref="IsSocketAlive"/> 接管，对 NetworkStream 与 SslStream 同样有效（探测底层 socket），
    /// 不再像旧实现那样仅排空 NetworkStream 残留而漏过 SSL 连接与半开 socket。
    /// COM_RESET_CONNECTION 需要 MySQL 5.7.3+ / MariaDB 10.2.4+；更早版本跳过会话重置。</remarks>
    public Boolean ResetSession()
    {
        if (!Reset()) return false;

        // 未加载服务器变量说明这不是一个完成配置的会话（如测试桩），没有会话状态需要清理
        if (Variables == null) return true;

        if (!SupportsResetConnection) return true;

        var timeout = Timeout;
        try
        {
            // 归还是同步路径，必须给重置命令设置上限，避免网络黑洞把连接池槽位永久占住
            if (Setting.ConnectionTimeout > 0) Timeout = Setting.ConnectionTimeout;

            _seq = 0;
            SendCommandAsync(DbCmd.RESET_CONNECTION).ConfigureAwait(false).GetAwaiter().GetResult();
            using var rs = ReadPacketAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
            if (!rs.IsOK)
            {
                Active = false;
                return false;
            }

            // 重置后当前数据库回到连接字符串指定的初始库
            Database = Setting.Database!;
            // 会话变量已恢复默认值，丢弃当前会话缓存；下次借出时由连接池缓存或 SHOW VARIABLES 重新填充
            Variables = null;
            // COM_RESET_CONNECTION 会释放服务端预编译语句，递增版本让持有旧 statement id 的命令重新预编译
            SessionVersion++;
            return true;
        }
        catch (MySqlException ex) when (ex.ErrorCode == 1047)
        {
            // 服务器不认识 COM_RESET_CONNECTION（如部分兼容产品）：ERR 包已完整读走，连接仍可复用，
            // 记录后跳过后续重置，避免每次归还都白白丢弃连接
            _resetUnsupported = true;
            return true;
        }
        catch
        {
            // 重置失败说明协议/会话状态不可信，丢弃连接而不是带着未知状态回池
            Active = false;
            return false;
        }
        finally
        {
            Timeout = timeout;
        }
    }

    /// <summary>服务器是否支持 COM_RESET_CONNECTION</summary>
    private Boolean SupportsResetConnection
    {
        get
        {
            if (_resetUnsupported) return false;

            var version = Welcome?.ServerVersion;
            if (version.IsNullOrEmpty()) return false;

            var parts = version.Split('.', '-');
            if (parts.Length < 2) return false;

            var major = parts[0].ToInt();
            var minor = parts[1].ToInt();
            var patch = parts.Length > 2 ? parts[2].ToInt() : 0;

            if (version.Contains("MariaDB", StringComparison.OrdinalIgnoreCase))
                return major > 10 || (major == 10 && (minor > 2 || (minor == 2 && patch >= 4)));

            return major > 5 || (major == 5 && (minor > 7 || (minor == 7 && patch >= 3)));
        }
    }

    /// <summary>中止底层网络。旧框架（net45~netstandard2.1）的 NetworkStream 异步读写不响应取消令牌，
    /// 只能关闭 socket 解除挂起，否则黑洞网络会让读操作永久占用连接池槽位</summary>
    private void Abort()
    {
        Active = false;
        _client.TryDispose();
        _client = null;
        _stream = null;
        _reader.Reset();
        ResetCompressionState();
    }

    /// <summary>探测底层 socket 健康状态。非阻塞、微秒级，取代旧 Reset 的排残留逻辑</summary>
    /// <returns>socket 健康状态</returns>
    /// <remarks>
    /// 基于 <see cref="Socket.Poll(Int32, SelectMode)"/> + <see cref="Socket.Available"/> 判定：
    /// 串行请求-响应模型下，连接借出时不应有任何可读数据。
    /// Poll(SelectRead) 为 true 表示有数据可读或连接已关闭/重置：
    /// Available==0 说明对端已关闭/重置 socket；Available&gt;0 说明残留了上一次未读尽的响应（协议错位脏连接）。
    /// 对 SSL 连接同样有效，因为探测的是底层 socket 而非加密流。
    /// 注意：无 FIN 的黑洞型断连（NAT/防火墙静默丢弃）此探测无法发现，由空闲 PING 与最大存活期兜底。
    /// </remarks>
    public SocketHealth IsSocketAlive()
    {
        var socket = _rawSocket ?? _client?.Client;
        if (socket == null || !socket.Connected) return SocketHealth.Closed;

        try
        {
            if (socket.Poll(0, SelectMode.SelectRead))
                return socket.Available == 0 ? SocketHealth.Closed : SocketHealth.HasResidualData;

            return SocketHealth.Alive;
        }
        catch (ObjectDisposedException)
        {
            return SocketHealth.Closed;
        }
        catch (SocketException)
        {
            return SocketHealth.Closed;
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
        var ms = _stream ?? throw new InvalidOperationException(L.T("Connection is not open."));

        // 根据 Timeout 属性创建超时令牌，确保不会无限等待
        var timeout = Timeout;
        using var cts = timeout > 0
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;
        cts?.CancelAfter(timeout * 1000);
        var token = cts?.Token ?? cancellationToken;

#if !NET5_0_OR_GREATER
        // 旧框架的 NetworkStream 异步读取忽略取消令牌，超时/取消时关闭 socket 以解除阻塞
        using var abort = token.Register(static s => ((SqlClient)s!).Abort(), this);
#endif

        try
        {
            // 压缩协议
            if (_useCompression)
                return await ReadCompressedPacketAsync(ms, token).ConfigureAwait(false);

            // 3字节长度 + 1字节序列号，复用实例字段避免池租借开销（同一连接串行读包，无并发风险）
            var buf = _headerBuf;
            await _reader.ReadAsync(ms, buf, 0, 4, token).ConfigureAwait(false);

            var rs = new ServerPacket(ms)
            {
                Length = buf[0] + (buf[1] << 8) + (buf[2] << 16),
                Sequence = buf[3],
            };
            _seq = (Byte)(rs.Sequence + 1);

            // 读取数据。长度必须刚好，因为可能有多帧数据包
            var len = rs.Length;

            // 数据包大小上限检测，防止恶意超大包导致 OOM
            var maxPacketSize = MaxPacketSize > 0 ? MaxPacketSize : 64 * 1024 * 1024;
            if (len > maxPacketSize)
            {
                Active = false;
                throw new InvalidOperationException(L.T("Packet size ({0}) exceeds the maximum allowed ({1}).", len, maxPacketSize));
            }

            OwnerPacket pk;
            if (len < MaxPayload)
            {
                pk = new OwnerPacket(len);
                await _reader.ReadAsync(ms, pk.Buffer, pk.Offset, len, token).ConfigureAwait(false);
                pk.Resize(len);
            }
            else
            {
                // 大报文：服务端按 0xFFFFFF 分帧连续发送，单帧满长时必须续读后续帧拼接，
                // 收尾帧长度不足 0xFFFFFF（恰好整倍数时为 0）
                var parts = new List<Byte[]>();
                var total = 0;
                var frameLen = len;
                while (true)
                {
                    var part = new Byte[frameLen];
                    await _reader.ReadAsync(ms, part, 0, frameLen, token).ConfigureAwait(false);
                    parts.Add(part);
                    total += frameLen;

                    if (total > maxPacketSize)
                    {
                        Active = false;
                        throw new InvalidOperationException(L.T("Packet size ({0}) exceeds the maximum allowed ({1}).", total, maxPacketSize));
                    }

                    if (frameLen < MaxPayload) break;

                    // 续读下一帧帧头
                    await _reader.ReadAsync(ms, buf, 0, 4, token).ConfigureAwait(false);
                    frameLen = buf[0] + (buf[1] << 8) + (buf[2] << 16);
                    rs.Sequence = buf[3];
                    _seq = (Byte)(rs.Sequence + 1);
                }

                pk = new OwnerPacket(total);
                var offset = pk.Offset;
                foreach (var part in parts)
                {
                    Buffer.BlockCopy(part, 0, pk.Buffer, offset, part.Length);
                    offset += part.Length;
                }
                pk.Resize(total);
                rs.Length = total;
            }

            rs.Set(pk);
            WritePacketLog("<=", rs.Sequence, pk.GetSpan(), pk.Length);

            // 错误包
            if (rs.IsError)
            {
                var reader = new SpanReader(pk.Slice(1));
                var code = reader.ReadUInt16();
                var msg = reader.ReadZeroString();

                // 前面有 '#' + 5 字符 SQLSTATE；State 按 ADO.NET 惯例不包含 '#'
                if (msg.Length >= 6 && msg[0] == '#')
                    throw new MySqlException(code, msg[1..6], msg[6..]);
                else
                    throw new MySqlException(code, msg);
            }

            return rs;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // 内部读超时导致的取消，标记连接不可用
            Active = false;
            throw new TimeoutException(L.T("Timed out reading a packet ({0}s).", timeout));
        }
        catch (OperationCanceledException)
        {
            // 调用方主动取消：半截响应残留在 socket 上，连接已不可复用，标记失效避免带毒归还连接池后 seq 错位
            Active = false;
            throw;
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
        {
            // 连接断开/EOF/IO 错误（EndOfStreamException 派生自 IOException）：标记失效，
            // 避免脏连接归还连接池后被后续查询复用导致协议错位、读取超时
            Active = false;
#if !NET5_0_OR_GREATER
            // 超时/取消通过关闭 socket 解除挂起，读操作以 ODE/IOException 结束，这里还原为调用方期望的超时/取消语义
            if (token.IsCancellationRequested)
            {
                if (cancellationToken.IsCancellationRequested) throw new OperationCanceledException(cancellationToken);
                throw new TimeoutException(L.T("Timed out reading a packet ({0}s).", timeout));
            }
#endif
            throw;
        }
    }

    private void ResetCompressionState()
    {
        _useCompression = false;
        _compressCache = null;
        _compressCacheOffset = 0;
        _compressCacheLength = 0;
        _compressPending = false;
    }

    private void WritePacketLog(String direction, Byte sequence, ReadOnlySpan<Byte> payload, Int32 length)
    {
        if (!Setting.TracePackets) return;

        // 认证阶段（含握手响应与密码校验包）直接跳过载荷，避免密码哈希/明文口令进日志
        if (!Active)
        {
            XTrace.WriteLine("[MySqlPacket] {0} db={1} seq={2} len={3} kind=Auth payload=skipped",
                direction, Database, sequence, length);
            return;
        }

        try
        {
            var available = Math.Min(Math.Max(length, 0), payload.Length);
            var kind = available > 0 ? GetPacketKind(direction, payload[0]) : "Empty";

            // 默认只记录包类型和长度。SQL、参数值等业务数据可能含口令/个人信息，必须显式开启载荷日志
            if (!Setting.TracePacketPayload)
            {
                XTrace.WriteLine("[MySqlPacket] {0} db={1} seq={2} len={3} kind={4} data=skipped",
                    direction, Database, sequence, length, kind);
                return;
            }

            var size = Math.Min(available, PacketTraceBytes);
            var data = size > 0 ? payload[..size].ToArray().ToHex() : String.Empty;
            var suffix = size < available ? "..." : String.Empty;

            XTrace.WriteLine("[MySqlPacket] {0} db={1} seq={2} len={3} kind={4} data={5}{6}",
                direction, Database, sequence, length, kind, data, suffix);
        }
        catch (Exception ex)
        {
            XTrace.WriteLine("[MySqlPacket] {0} db={1} seq={2} len={3} logging-failed={4}: {5}",
                direction, Database, sequence, length, ex.GetType().Name, ex.Message);
        }
    }

    private static String GetPacketKind(String direction, Byte firstByte)
    {
        if (direction == "=>")
            return Enum.IsDefined(typeof(DbCmd), firstByte) ? ((DbCmd)firstByte).ToString() : $"0x{firstByte:X2}";

        return firstByte switch
        {
            0x00 => "OK",
            0xFF => "ERROR",
            0xFE => "EOF/OK_END",
            _ => $"0x{firstByte:X2}"
        };
    }

    /// <summary>为协议帧头准备前置空间。已预留则零拷贝借位，未预留则新建链头挂接负载</summary>
    /// <param name="pk">负载数据包</param>
    /// <param name="size">帧头字节数</param>
    /// <returns>带头部空间的数据包，调用方负责 Dispose</returns>
    /// <remarks>
    /// v12 起函数库把原 ExpandHeader 的降级语义拆分为：ExpandHeader 严格借位（未预留直接抛异常）、PrepareHeader 自动降级。
    /// 此处组合两者，保持 v11「有前置空间零拷贝借位、否则新建链头」的行为，避免热路径额外分配。
    /// </remarks>
    private static IPacket PrepareFrameHeader(IPacket pk, Int32 size) => pk switch
    {
        ArrayPacket ap when ap.FreeHeader >= size => ap.ExpandHeader(size),
        OwnerPacket op when op.RefCount > 0 && op.FreeHeader >= size => op.ExpandHeader(size),
        _ => pk.PrepareHeader(size),
    };

    /// <summary>异步发送 MySQL 协议数据包。建议数据包头部预留4字节空间以填充帧头</summary>
    /// <param name="pk">待发送的数据包</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <param name="flush">是否立即刷新网络流。管道化批量发送时设为 false，最后统一 Flush</param>
    /// <returns>异步任务</returns>
    public async Task SendPacketAsync(IPacket pk, CancellationToken cancellationToken = default, Boolean flush = true)
    {
        var ms = _stream ?? throw new InvalidOperationException(L.T("Connection is not open."));

        // 写方向同样受 Timeout 约束：对端不再收取数据时写入会一直挂起，命令与连接池都被拖死
        // 超时/取消一律视为连接失效，避免半截请求残留在协议流里
        var timeout = Timeout;
        using var cts = timeout > 0
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;
        cts?.CancelAfter(timeout * 1000);
        var token = cts?.Token ?? cancellationToken;
#if !NET5_0_OR_GREATER
        // 旧框架的 NetworkStream 异步写入忽略取消令牌，超时/取消时关闭 socket 以解除阻塞
        using var abort = token.Register(static s => ((SqlClient)s!).Abort(), this);
#endif

        // 压缩协议
        if (_useCompression)
        {
            await SendCompressedPacketAsync(pk, ms, flush, token).ConfigureAwait(false);
            return;
        }

        var len = pk.Total;
        var pk2 = PrepareFrameHeader(pk, 4);

        var payload = pk2.GetSpan();
        if (payload.Length >= len + 4)
            payload = payload.Slice(4, len);
        else if (payload.Length > len)
            payload = payload.Slice(payload.Length - len, len);

        try
        {
            if (len < MaxPayload)
            {
                pk2[0] = (Byte)(len & 0xFF);
                pk2[1] = (Byte)((len >> 8) & 0xFF);
                pk2[2] = (Byte)((len >> 16) & 0xFF);
                pk2[3] = _seq++;

                WritePacketLog("=>", pk2[3], payload, len);

                await pk2.CopyToAsync(ms, token).ConfigureAwait(false);
            }
            else
            {
                // 超过单帧上限：按 0xFFFFFF 切帧，每帧自带 4 字节帧头；
                // 旧实现只把低 3 字节写入长度，超长报文长度回绕，服务端按错误长度解包导致协议错位
                await SendChunkedAsync(pk2, len, ms, token).ConfigureAwait(false);
                WritePacketLog("=>", (Byte)(_seq - 1), ReadOnlySpan<Byte>.Empty, len);
            }

            if (flush) await ms.FlushAsync(token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // 内部写超时：请求可能已发出半截，连接不能再用
            Active = false;
            throw new TimeoutException(L.T("Timed out sending a packet ({0}s).", timeout));
        }
        catch
        {
            // 发送中断（如批量写入时网络抛 IOException）服务端可能只收到半截请求，连接协议状态已破坏。
            // 标记连接失效，避免脏连接归还连接池后被后续查询复用，导致响应错位、查询端读取超时。
            Active = false;
            throw;
        }
        finally
        {
            // 发送即消费：归还链头缓冲。v12 的 PrepareHeader 不改动原句柄所有权（借位共享/链式引用），
            // 原包若为拥有句柄需在此一并释放，避免池化缓冲泄漏（v11 由 ExpandHeader 级联释放）
            if (pk2 is IOwnerPacket owner) owner.Dispose();
            if (pk is IOwnerPacket pkOwner) pkOwner.Dispose();
        }

        LastActive = DateTime.Now;
    }

    /// <summary>发送超过单帧上限的报文。按 0xFFFFFF 切帧，每帧独立 4 字节帧头</summary>
    /// <param name="pk">已预留帧头的报文（可能是链式）</param>
    /// <param name="len">载荷长度</param>
    /// <param name="ms">目标流</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    private async Task SendChunkedAsync(IPacket pk, Int32 len, Stream ms, CancellationToken cancellationToken)
    {
        // 先归一到连续缓冲：链式报文的 GetSpan 只返回单段，切帧需要连续视图
        var body = new Byte[len];
        var pos = 0;
        var skip = 4;
        for (var node = (IPacket?)pk; node != null; node = node.Next)
        {
            var span = node.GetSpan();
            if (skip > 0)
            {
                if (span.Length <= skip)
                {
                    skip -= span.Length;
                    continue;
                }

                span = span[skip..];
                skip = 0;
            }
            if (pos + span.Length > len) span = span[..(len - pos)];
            span.CopyTo(new Span<Byte>(body, pos, span.Length));
            pos += span.Length;
        }

        var header = new Byte[4];
        var sent = 0;
        while (sent < len)
        {
            var size = Math.Min(MaxPayload, len - sent);
            WriteFrameHeader(header, size, _seq++);
            await ms.WriteAsync(header, 0, 4, cancellationToken).ConfigureAwait(false);
            await ms.WriteAsync(body, sent, size, cancellationToken).ConfigureAwait(false);
            sent += size;
        }

        // 载荷恰好是单帧上限的整数倍时，必须补一个长度 0 的帧表示报文结束
        if (len % MaxPayload == 0)
        {
            WriteFrameHeader(header, 0, _seq++);
            await ms.WriteAsync(header, 0, 4, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>写入 4 字节协议帧头（3 字节长度 + 1 字节序列号）</summary>
    /// <param name="header">目标缓冲</param>
    /// <param name="length">载荷长度</param>
    /// <param name="sequence">序列号</param>
    private static void WriteFrameHeader(Byte[] header, Int32 length, Byte sequence)
    {
        header[0] = (Byte)(length & 0xFF);
        header[1] = (Byte)((length >> 8) & 0xFF);
        header[2] = (Byte)((length >> 16) & 0xFF);
        header[3] = sequence;
    }

    #region 压缩协议
    /// <summary>读取压缩协议数据包</summary>
    /// <remarks>
    /// 一个压缩包可包含多个普通包，且普通包可跨压缩包分片（大结果集数据行）。
    /// 解压后缓存未消费的普通包数据，不完整包保留剩余并跨包拼接。
    /// </remarks>
    private async Task<ServerPacket> ReadCompressedPacketAsync(Stream ms, CancellationToken cancellationToken)
    {
        var timeout = Timeout;
        using var cts = timeout > 0
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;
        cts?.CancelAfter(timeout * 1000);
        var token = cts?.Token ?? cancellationToken;

        try
        {
            var (data, seq) = await ReadPlainPacketAsync(ms, token).ConfigureAwait(false);

            // 普通包满长时，后续普通包是同一条报文的续帧（超过 16MB 的结果行），必须拼接后再返回
            if (data.Length < MaxPayload) return BuildServerPacket(ms, data, seq);

            var parts = new List<Byte[]> { data };
            var total = data.Length;
            var lastSeq = seq;
            var maxPacketSize = MaxPacketSize > 0 ? MaxPacketSize : 64 * 1024 * 1024;
            while (true)
            {
                var (next, nextSeq) = await ReadPlainPacketAsync(ms, token).ConfigureAwait(false);
                parts.Add(next);
                total += next.Length;
                lastSeq = nextSeq;

                if (total > maxPacketSize)
                {
                    Active = false;
                    throw new InvalidOperationException(L.T("Packet size ({0}) exceeds the maximum allowed ({1}).", total, maxPacketSize));
                }

                if (next.Length < MaxPayload) break;
            }

            var buf = new Byte[total];
            var pos = 0;
            foreach (var part in parts)
            {
                Buffer.BlockCopy(part, 0, buf, pos, part.Length);
                pos += part.Length;
            }

            return BuildServerPacket(ms, buf, lastSeq);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            Active = false;
            throw new TimeoutException(L.T("Timed out reading a packet ({0}s).", timeout));
        }
        catch (OperationCanceledException)
        {
            Active = false;
            throw;
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
        {
            Active = false;
            throw;
        }
    }

    /// <summary>从压缩流中读出一个普通包的数据体（不含 4 字节帧头）。普通包跨压缩包分片时自动拼接</summary>
    /// <param name="ms">网络流</param>
    /// <param name="token">取消令牌</param>
    /// <returns>数据体与序列号</returns>
    private async Task<(Byte[] Data, Byte Sequence)> ReadPlainPacketAsync(Stream ms, CancellationToken token)
    {
        while (true)
        {
            // 缓存不足一个完整普通包头或存在跨包待拼接数据时，读取并解压下一个压缩包（前置拼接保留的不完整包数据）
            if (_compressCache == null || _compressCacheOffset + 4 > _compressCacheLength || _compressPending)
            {
                await FillCompressedCacheAsync(ms, token).ConfigureAwait(false);
            }

            var cache = _compressCache!;
            var offset = _compressCacheOffset;
            if (offset + 4 > _compressCacheLength) continue; // 数据不足，循环读取下一个压缩包

            var len = cache[offset] + (cache[offset + 1] << 8) + (cache[offset + 2] << 16);
            var seq = cache[offset + 3];
            _seq = (Byte)(seq + 1);

            // 普通包不完整（跨压缩包分片）：保留未消费部分并标记，循环强制读取下一个压缩包拼接
            if (offset + 4 + len > _compressCacheLength)
            {
                var remain = _compressCacheLength - offset;
                if (remain > 0 && offset > 0)
                    Buffer.BlockCopy(cache, offset, cache, 0, remain);
                _compressCacheOffset = 0;
                _compressCacheLength = remain;
                _compressPending = true;
                continue;
            }

            _compressCacheOffset = offset + 4 + len;

            var data = new Byte[len];
            if (len > 0) Buffer.BlockCopy(cache, offset + 4, data, 0, len);

            return (data, seq);
        }
    }

    /// <summary>读取并解压一个压缩包，前置拼接上次未消费的不完整普通包数据</summary>
    /// <param name="ms">网络流</param>
    /// <param name="token">取消令牌</param>
    private async Task FillCompressedCacheAsync(Stream ms, CancellationToken token)
    {
        // 7字节压缩包头: 3字节压缩长度 + 1字节序列号 + 3字节未压缩长度
        _compressHeaderBuf ??= new Byte[CompressedHeaderSize];
        var hdr = _compressHeaderBuf;
        await _reader.ReadAsync(ms, hdr, 0, CompressedHeaderSize, token).ConfigureAwait(false);

        var compressedLen = hdr[0] + (hdr[1] << 8) + (hdr[2] << 16);
        var seq = hdr[3];
        var uncompressedLen = hdr[4] + (hdr[5] << 8) + (hdr[6] << 16);
        _seq = (Byte)(seq + 1);

        // 压缩/解压后数据包大小上限检测
        var maxPacketSize = MaxPacketSize > 0 ? MaxPacketSize : 64 * 1024 * 1024;
        if (uncompressedLen > maxPacketSize)
        {
            Active = false;
            throw new InvalidOperationException(L.T("Decompressed packet size ({0}) exceeds the maximum allowed ({1}).", uncompressedLen, maxPacketSize));
        }
        if (compressedLen > maxPacketSize)
        {
            Active = false;
            throw new InvalidOperationException(L.T("Compressed packet size ({0}) exceeds the maximum allowed ({1}).", compressedLen, maxPacketSize));
        }

        // 保留尚未消费的跨包数据（不完整普通包尾部）
        var remain = _compressCacheLength - _compressCacheOffset;
        if (remain < 0) remain = 0;

        Byte[] payload;
        Int32 payloadLen;
        if (uncompressedLen == 0)
        {
            // 未压缩小包：payload 直接是带普通帧头的标准包
            payload = new Byte[remain + compressedLen];
            if (remain > 0) Buffer.BlockCopy(_compressCache!, _compressCacheOffset, payload, 0, remain);
            await _reader.ReadAsync(ms, payload, remain, compressedLen, token).ConfigureAwait(false);
            payloadLen = remain + compressedLen;
        }
        else
        {
            // 压缩包：读取压缩数据后 zlib 解压，前置拼接保留数据
            var compressed = new Byte[compressedLen];
            await _reader.ReadAsync(ms, compressed, 0, compressedLen, token).ConfigureAwait(false);

            payload = new Byte[remain + uncompressedLen];
            if (remain > 0) Buffer.BlockCopy(_compressCache!, _compressCacheOffset, payload, 0, remain);
            var n = ZlibDecompress(compressed, 0, compressedLen, payload, remain, uncompressedLen);
            payloadLen = remain + n;
        }
        _compressCache = payload;
        _compressCacheOffset = 0;
        _compressCacheLength = payloadLen;
        _compressPending = false;
    }

    /// <summary>用普通包数据体构建服务端数据包，并记录日志、检测错误包</summary>
    /// <param name="ms">网络流</param>
    /// <param name="data">普通包数据体</param>
    /// <param name="sequence">序列号</param>
    /// <returns>服务端数据包</returns>
    private ServerPacket BuildServerPacket(Stream ms, Byte[] data, Byte sequence)
    {
        var len = data.Length;
        var pk = new OwnerPacket(len);
        if (len > 0) Buffer.BlockCopy(data, 0, pk.Buffer, pk.Offset, len);

        var rs = new ServerPacket(ms) { Length = len, Sequence = sequence };
        rs.Set(pk);
        WritePacketLog("<=", sequence, pk.GetSpan(), len);

        // 错误包检测
        if (rs.IsError)
        {
            var reader = new SpanReader(rs.Data.Slice(1));
            var code = reader.ReadUInt16();
            var msg = reader.ReadZeroString();
            if (msg.Length >= 6 && msg[0] == '#')
                throw new MySqlException(code, msg[1..6], msg[6..]);
            else
                throw new MySqlException(code, msg);
        }

        return rs;
    }

    /// <summary>发送压缩协议数据包</summary>
    private async Task SendCompressedPacketAsync(IPacket pk, Stream ms, Boolean flush, CancellationToken cancellationToken)
    {
        var len = pk.Total;
        var pk2 = PrepareFrameHeader(pk, 4);

        try
        {
            // 普通包长度与压缩包长度字段都只有 3 字节，超过 16MB 需要压缩层分帧（各服务端实现差异大），
            // 这里明确拒绝，避免发出长度回绕的包把协议流搞乱
            if (len > MaxPayload)
                throw new NotSupportedException(L.T("Packets larger than 16MB require UseCompression=false in the connection string."));

            // 链式数据（PrepareFrameHeader 可能生成 { 帧头, Next=原包 } 链，GetSpan 仅返回单段）合并到连续缓冲，
            // 跳过预留的 4 字节帧头，帧头由普通包自行补齐
            var body = new Byte[len];
            var pos = 0;
            var skip = 4;
            for (var node = (IPacket?)pk2; node != null; node = node.Next)
            {
                var span = node.GetSpan();
                if (skip > 0)
                {
                    if (span.Length <= skip)
                    {
                        skip -= span.Length;
                        continue;
                    }

                    span = span[skip..];
                    skip = 0;
                }
                if (pos + span.Length > len) span = span[..(len - pos)];
                span.CopyTo(new Span<Byte>(body, pos, span.Length));
                pos += span.Length;
            }

            var raw = BuildPlainFrames(body);
            await WriteCompressedChunkAsync(ms, raw, 0, raw.Length, cancellationToken).ConfigureAwait(false);

            WritePacketLog("=>", _seq, pk.GetSpan(), len);

            if (flush) await ms.FlushAsync(cancellationToken).ConfigureAwait(false);

            LastActive = DateTime.Now;
        }
        finally
        {
            // 发送即消费：归还链头与原包缓冲（v12 不再级联转移原包所有权）
            if (pk2 is IOwnerPacket owner) owner.Dispose();
            if (pk is IOwnerPacket pkOwner) pkOwner.Dispose();
        }
    }

    /// <summary>把数据体切成普通包序列（4 字节帧头 + 数据）</summary>
    /// <param name="body">数据体</param>
    /// <returns>普通包序列字节</returns>
    /// <remarks>数据体不超过 16MB 时只产生一个普通包；数据体为空时产生一个 0 长度普通包</remarks>
    private static Byte[] BuildPlainFrames(Byte[] body)
    {
        var len = body.Length;
        var frames = new Byte[4 + len];
        frames[0] = (Byte)(len & 0xFF);
        frames[1] = (Byte)((len >> 8) & 0xFF);
        frames[2] = (Byte)((len >> 16) & 0xFF);
        frames[3] = 0;
        if (len > 0) Buffer.BlockCopy(body, 0, frames, 4, len);

        return frames;
    }

    /// <summary>发送一个压缩包。数据体可能落在普通帧中间，读取端会跨包拼接</summary>
    /// <param name="ms">网络流</param>
    /// <param name="buffer">普通包序列缓冲</param>
    /// <param name="offset">本压缩包起始位置</param>
    /// <param name="count">本压缩包长度</param>
    /// <param name="cancellationToken">取消令牌</param>
    private async Task WriteCompressedChunkAsync(Stream ms, Byte[] buffer, Int32 offset, Int32 count, CancellationToken cancellationToken)
    {
        var seq = _seq++;

        // zlib 压缩：MySQL 压缩协议使用 zlib 格式（带 2 字节头 + adler32），与 .NET DeflateStream 的 DEFLATE 格式不兼容
        var compressed = ZlibCompress(new ReadOnlySpan<Byte>(buffer, offset, count));

        // 压缩后更大则直接发原文（未压缩长度字段填 0），两者都保证长度字段不溢出
        var sendUncompressed = compressed.Length >= count;
        var clen = sendUncompressed ? count : compressed.Length;

        using var pk = new OwnerPacket(CompressedHeaderSize + clen);
        var dst = pk.GetSpan();
        dst[0] = (Byte)(clen & 0xFF);
        dst[1] = (Byte)((clen >> 8) & 0xFF);
        dst[2] = (Byte)((clen >> 16) & 0xFF);
        dst[3] = seq;
        dst[4] = (Byte)(sendUncompressed ? 0 : (count & 0xFF));
        dst[5] = (Byte)(sendUncompressed ? 0 : ((count >> 8) & 0xFF));
        dst[6] = (Byte)(sendUncompressed ? 0 : ((count >> 16) & 0xFF));

        if (sendUncompressed)
            new ReadOnlySpan<Byte>(buffer, offset, count).CopyTo(dst[CompressedHeaderSize..]);
        else
            compressed.AsSpan(0, clen).CopyTo(dst[CompressedHeaderSize..]);

        try
        {
            await pk.CopyToAsync(ms, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            Active = false;
            throw;
        }
    }
    #endregion

    #region zlib 压缩辅助
    /// <summary>zlib 压缩。MySQL 压缩协议使用 zlib 格式（带 2 字节头 + adler32），与 .NET DeflateStream 的 DEFLATE 格式不兼容</summary>
    /// <param name="data">待压缩数据</param>
    /// <returns>zlib 压缩字节</returns>
    internal static Byte[] ZlibCompress(Byte[] data) => ZlibCompress(new ReadOnlySpan<Byte>(data));

    /// <summary>zlib 压缩。MySQL 压缩协议使用 zlib 格式（带 2 字节头 + adler32），与 .NET DeflateStream 的 DEFLATE 格式不兼容</summary>
    /// <param name="data">待压缩数据</param>
    /// <returns>zlib 压缩字节</returns>
    private static Byte[] ZlibCompress(ReadOnlySpan<Byte> data)
    {
#if NET6_0_OR_GREATER
        using var ms = new MemoryStream();
        using (var zlib = new ZLibStream(ms, CompressionLevel.Optimal, leaveOpen: true))
            zlib.Write(data);
        return ms.ToArray();
#else
        // 旧框架无 ZLibStream：2 字节 zlib 头 + DEFLATE 数据 + adler32（大端）
        using var ms = new MemoryStream();
        ms.WriteByte(0x78);
        ms.WriteByte(0x9C);
        using (var deflate = new DeflateStream(ms, CompressionLevel.Optimal, leaveOpen: true))
        {
            var arr = data.ToArray();
            deflate.Write(arr, 0, arr.Length);
        }
        var adler = Adler32(data);
        ms.WriteByte((Byte)((adler >> 24) & 0xFF));
        ms.WriteByte((Byte)((adler >> 16) & 0xFF));
        ms.WriteByte((Byte)((adler >> 8) & 0xFF));
        ms.WriteByte((Byte)(adler & 0xFF));
        return ms.ToArray();
#endif
    }

    /// <summary>zlib 解压到目标缓冲。MySQL 压缩协议使用 zlib 格式</summary>
    /// <param name="compressed">压缩数据源</param>
    /// <param name="offset">源偏移</param>
    /// <param name="count">压缩数据长度</param>
    /// <param name="output">目标缓冲</param>
    /// <param name="outputOffset">目标偏移</param>
    /// <param name="outputCount">目标容量</param>
    /// <returns>实际解压字节数</returns>
    internal static Int32 ZlibDecompress(Byte[] compressed, Int32 offset, Int32 count, Byte[] output, Int32 outputOffset, Int32 outputCount)
    {
#if NET6_0_OR_GREATER
        using var cs = new MemoryStream(compressed, offset, count);
        using var zlib = new ZLibStream(cs, CompressionMode.Decompress);
        return ReadAll(zlib, output, outputOffset, outputCount);
#else
        // 旧框架无 ZLibStream：跳过 2 字节 zlib 头后用 DeflateStream（忽略尾部 adler32）
        using var cs = new MemoryStream(compressed, offset + 2, count - 2);
        using var deflate = new DeflateStream(cs, CompressionMode.Decompress);
        return ReadAll(deflate, output, outputOffset, outputCount);
#endif
    }

    /// <summary>从解压流读取全部数据到目标缓冲</summary>
    /// <param name="stream">解压流</param>
    /// <param name="output">目标缓冲</param>
    /// <param name="outputOffset">目标偏移</param>
    /// <param name="outputCount">目标容量</param>
    /// <returns>实际读取字节数</returns>
    private static Int32 ReadAll(Stream stream, Byte[] output, Int32 outputOffset, Int32 outputCount)
    {
        var total = 0;
        while (total < outputCount)
        {
            var n = stream.Read(output, outputOffset + total, outputCount - total);
            if (n == 0) break;
            total += n;
        }
        return total;
    }

    /// <summary>计算 Adler-32 校验（zlib 格式尾部校验，旧框架手动构造时需要）</summary>
    /// <param name="data">数据</param>
    /// <returns>Adler-32 值</returns>
    private static UInt32 Adler32(ReadOnlySpan<Byte> data)
    {
        UInt32 a = 1, b = 0;
        for (var i = 0; i < data.Length; i++)
        {
            a = (a + data[i]) % 65521;
            b = (b + a) % 65521;
        }
        return (b << 16) | a;
    }
    #endregion

    /// <summary>异步刷新网络流，确保所有缓冲数据发送到服务器</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        var ms = _stream ?? throw new InvalidOperationException(L.T("Connection is not open."));
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

            // 偏移4字节为帧头预留空间，避免 PrepareFrameHeader 额外分配
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

        using var span = Tracer?.NewSpan($"db:{Database}:Ping");
        try
        {
            // 发送 PING 命令
            await SendCommandAsync(DbCmd.PING, cancellationToken).ConfigureAwait(false);
            using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

            // 如果是错误包，ReadPacketAsync 会抛出 MySqlException；到这里代表收到 OK 或其他合法包
            return rs.IsOK || !rs.IsError;
        }
        catch (Exception ex)
        {
            span?.SetError(ex);

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
        if (!rs.IsOK) throw new MySqlException(L.T("Failed to change the current database."));

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
        if (!rs.IsOK) throw new MySqlException(L.T("Failed to create the database."));
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
        if (!rs.IsOK) throw new MySqlException(L.T("Failed to drop the database."));
    }

    #endregion

    #region 查询与结果集
    /// <summary>异步发送 SQL 查询请求到服务器。</summary>
    /// <remarks>构造查询包时通常应预留帧头或在偏移 4 处写入命令字节，以便本方法把 `DbCmd.QUERY` 写入第一字节；发送路径的 PrepareFrameHeader 会自动为未预留的包挂接链头。</remarks>
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

        // 读取并消费 EOF 包
        using var rs2 = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

        return [.. list];
    }

    /// <summary>读取行数据包到复用缓冲区（含帧头解析、错误处理、超时管理）</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>正数为行数据长度（数据在 _rowBuffer[0..len]）；负数或零表示 EOF（取反为 EOF 包长度）</returns>
    private async Task<Int32> ReadRowPacketAsync(CancellationToken cancellationToken)
    {
        var ms = _stream ?? throw new InvalidOperationException(L.T("Connection is not open."));

        var timeout = Timeout;
        using var cts = timeout > 0
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;
        cts?.CancelAfter(timeout * 1000);
        var token = cts?.Token ?? cancellationToken;

#if !NET5_0_OR_GREATER
        // 旧框架的 NetworkStream 异步读取忽略取消令牌，超时/取消时关闭 socket 以解除阻塞
        using var abort = token.Register(static s => ((SqlClient)s!).Abort(), this);
#endif

        try
        {
            // 压缩协议：行数据包同样在压缩包内（含 4 字节普通帧头），必须走统一压缩读取路径
            if (_useCompression)
            {
                using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);

                var clen = rs.Length;
                if (_rowBuffer == null || _rowBuffer.Length < clen)
                    _rowBuffer = new Byte[Math.Max(clen, 16384)];
                // EOF 数据也复制，供 ParseEofRowResult 解析 warnings/status
                rs.Data.GetSpan().CopyTo(new Span<Byte>(_rowBuffer, 0, clen));

                // EOF 包：返回负数，调用方取反得到 EOF 包长度
                if (rs.IsEOF) return -clen;
                return clen;
            }

            if (_rowBuffer == null)
                _rowBuffer = new Byte[16384];

            // 帧头使用独立的 4 字节缓冲，避免覆盖已读到的行数据（大行续帧时需要）
            var header = _headerBuf;
            await _reader.ReadAsync(ms, header, 0, 4, token).ConfigureAwait(false);
            var len = header[0] + (header[1] << 8) + (header[2] << 16);
            _seq = (Byte)(header[3] + 1);

            // 行数据包大小上限检测
            var maxPacketSize = MaxPacketSize > 0 ? MaxPacketSize : 64 * 1024 * 1024;
            if (len > maxPacketSize)
            {
                Active = false;
                throw new InvalidOperationException(L.T("Row packet size ({0}) exceeds the maximum allowed ({1}).", len, maxPacketSize));
            }
            if (_rowBuffer.Length < len)
                _rowBuffer = new Byte[Math.Max(len, 16384)];

            await _reader.ReadAsync(ms, _rowBuffer, 0, len, token).ConfigureAwait(false);

            // 大行：服务端按 0xFFFFFF 分帧，需续读后续帧拼接后再解析
            if (len == MaxPayload)
            {
                var total = len;
                while (true)
                {
                    await _reader.ReadAsync(ms, header, 0, 4, token).ConfigureAwait(false);
                    var next = header[0] + (header[1] << 8) + (header[2] << 16);
                    _seq = (Byte)(header[3] + 1);

                    if (total + next > maxPacketSize)
                    {
                        Active = false;
                        throw new InvalidOperationException(L.T("Row packet size ({0}) exceeds the maximum allowed ({1}).", total + next, maxPacketSize));
                    }

                    if (_rowBuffer.Length < total + next)
                    {
                        var bigger = new Byte[Math.Max(total + next, _rowBuffer.Length * 2)];
                        Buffer.BlockCopy(_rowBuffer, 0, bigger, 0, total);
                        _rowBuffer = bigger;
                    }

                    if (next > 0)
                        await _reader.ReadAsync(ms, _rowBuffer, total, next, token).ConfigureAwait(false);

                    total += next;
                    if (next < MaxPayload) break;
                }
                len = total;
            }

            WritePacketLog("<=", (Byte)(_seq - 1), new ReadOnlySpan<Byte>(_rowBuffer, 0, len), len);

            // 错误包
            if (_rowBuffer[0] == 0xFF)
            {
                var errReader = new SpanReader(new ReadOnlySpan<Byte>(_rowBuffer, 1, len - 1));
                var code = errReader.ReadUInt16();
                var msg = errReader.ReadZeroString();
                if (msg.Length >= 6 && msg[0] == '#')
                    throw new MySqlException(code, msg[1..6], msg[6..]);
                else
                    throw new MySqlException(code, msg);
            }

            // EOF 包：返回负数，调用方取反得到 EOF 包长度
            if (_rowBuffer[0] == 0xFE && len < 9)
                return -len;

            return len;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            Active = false;
            throw new TimeoutException(L.T("Timed out reading a packet ({0}s).", timeout));
        }
        catch (OperationCanceledException)
        {
            // 调用方主动取消：半截响应残留在 socket 上，连接已不可复用
            Active = false;
            throw;
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
        {
            // 连接断开/EOF/IO 错误，标记失效，避免脏连接归还连接池后协议错位
            Active = false;
#if !NET5_0_OR_GREATER
            // 超时/取消通过关闭 socket 解除挂起，读操作以 ODE/IOException 结束，这里还原为调用方期望的超时/取消语义
            if (token.IsCancellationRequested)
            {
                if (cancellationToken.IsCancellationRequested) throw new OperationCanceledException(cancellationToken);
                throw new TimeoutException(L.T("Timed out reading a packet ({0}s).", timeout));
            }
#endif
            throw;
        }
    }

    /// <summary>从 _rowBuffer 解析 EOF 包为行结束结果</summary>
    /// <param name="bodyLen">EOF 包体长度</param>
    /// <returns>行读取结果</returns>
    private RowResult ParseEofRowResult(Int32 bodyLen)
    {
        ServerStatusFlags statusFlags = 0;
        UInt16 warnings = 0;
        if (Capability.Has(ClientFlags.PROTOCOL_41) && bodyLen >= 5)
        {
            var reader = new SpanReader(new ReadOnlySpan<Byte>(_rowBuffer!, 1, bodyLen - 1));
            warnings = reader.ReadUInt16();
            statusFlags = (ServerStatusFlags)reader.ReadUInt16();
        }
        return new RowResult(false, statusFlags, warnings);
    }

    /// <summary>异步读取结果集的下一行数据</summary>
    /// <param name="values">用于存储行数据的数组</param>
    /// <param name="columns">列信息数组</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>行读取结果，包含是否成功及状态信息</returns>
    public async Task<RowResult> NextRowAsync(Object[] values, MySqlColumn[] columns, CancellationToken cancellationToken = default)
    {
        var len = await ReadRowPacketAsync(cancellationToken).ConfigureAwait(false);
        if (len <= 0) return ParseEofRowResult(-len);

        // 读取行数据（文本协议：所有值以 UTF-8 字符串传输）
        var reader = new SpanReader(new ReadOnlySpan<Byte>(_rowBuffer!, 0, len));
        for (var i = 0; i < values.Length; i++)
        {
            var fieldLen = (Int32)reader.ReadLength();
            if (fieldLen == -1)
            {
                values[i] = DBNull.Value;
                continue;
            }

            values[i] = MySqlFieldCodec.ReadTextValue(ref reader, columns[i], fieldLen);
        }

        return RowResult.DataRow;
    }

    /// <summary>异步读取二进制协议结果行（COM_STMT_EXECUTE 返回的行数据）</summary>
    /// <param name="values">用于存储行数据的数组</param>
    /// <param name="columns">列信息数组</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>行读取结果，包含是否成功及状态信息</returns>
    public async Task<RowResult> NextBinaryRowAsync(Object[] values, MySqlColumn[] columns, CancellationToken cancellationToken = default)
    {
        var len = await ReadRowPacketAsync(cancellationToken).ConfigureAwait(false);
        if (len <= 0) return ParseEofRowResult(-len);

        var numCols = columns.Length;
        var reader = new SpanReader(new ReadOnlySpan<Byte>(_rowBuffer!, 0, len));

        // 二进制行格式：header(0x00) + null_bitmap + values
        reader.Advance(1);

        // null_bitmap: (num_columns + 7 + 2) / 8 字节，位偏移量为 2
        // 复用实例缓冲避免每行分配 byte[]（与 _rowBuffer 同思路）
        var nullBitmapLen = (numCols + 7 + 2) / 8;
        if (_nullBitmap == null || _nullBitmap.Length < nullBitmapLen)
            _nullBitmap = new Byte[nullBitmapLen];
        reader.ReadBytes(nullBitmapLen).CopyTo(new Span<Byte>(_nullBitmap, 0, nullBitmapLen));
        var nullBitmap = _nullBitmap;

        for (var i = 0; i < numCols; i++)
        {
            var bitIndex = i + 2;
            if ((nullBitmap[bitIndex / 8] & (1 << (bitIndex % 8))) != 0)
            {
                values[i] = DBNull.Value;
                continue;
            }

            values[i] = MySqlFieldCodec.ReadBinaryValue(ref reader, columns[i]);
        }

        return RowResult.DataRow;
    }

    /// <summary>跳过当前结果集中的一行数据（不解析内容），用于快速消费剩余行</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>行读取结果，包含是否到达 EOF 及状态信息</returns>
    public async Task<RowResult> SkipRowAsync(CancellationToken cancellationToken = default)
    {
        using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (rs.IsEOF) return ReadEofRowResult(rs);

        return RowResult.DataRow;
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
        using var pk = new OwnerPacket(len);
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
        // 转值数组走统一核心，避免每行 MySqlParameter 对象分配
        await ExecuteStatementAsync(statementId, ToValues(parameters), paramColumns, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>异步执行已准备的预编译语句（值数组版本），支持二进制参数绑定</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="values">参数值数组，按参数顺序排列，可为空</param>
    /// <param name="paramColumns">Prepare 返回的参数列信息</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task ExecuteStatementAsync(Int32 statementId, Object?[]? values, MySqlColumn[]? paramColumns, CancellationToken cancellationToken = default)
    {
        var sendPk = BuildExecutePacket(statementId, values);
        _seq = 0;
        try
        {
            await SendPacketAsync(sendPk, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // 覆盖压缩协议路径：SendPacketAsync 非压缩路径已释放链头产物（Dispose 幂等），此处统一归还
            if (sendPk is IOwnerPacket owner) owner.Dispose();
        }
    }

    /// <summary>构建 COM_STMT_EXECUTE 数据包。仅构建包内容，不发送</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="parameters">参数集合，可为空</param>
    /// <returns>待发送的数据包（不含帧头）</returns>
    private IPacket BuildExecutePacket(Int32 statementId, MySqlParameterCollection? parameters)
        => BuildExecutePacket(statementId, ToValues(parameters));

    /// <summary>构建 COM_STMT_EXECUTE 数据包（值数组版本）。仅构建包内容，不发送</summary>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="values">参数值数组，按参数顺序排列，可为空</param>
    /// <returns>待发送的数据包（不含帧头）</returns>
    private IPacket BuildExecutePacket(Int32 statementId, Object?[]? values)
    {
        var numParams = values?.Length ?? 0;
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
                var val = values![i];
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
                var val = values![i];
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
                var val = values![i];
                if (val != null && val != DBNull.Value)
                    MySqlFieldCodec.WriteBinaryValue(ref writer, val, Encoding);
            }
        }

        return pk.Slice(4, writer.Position - 4);
    }

    /// <summary>提取参数集合的值数组，按参数顺序排列。批量执行统一走值数组核心，避免每行 MySqlParameterCollection/MySqlParameter 分配</summary>
    /// <param name="parameters">参数集合，可为空</param>
    /// <returns>值数组，无参数时返回 null</returns>
    private static Object?[]? ToValues(MySqlParameterCollection? parameters)
    {
        if (parameters == null || parameters.Count == 0) return null;

        var vals = new Object?[parameters.Count];
        for (var i = 0; i < parameters.Count; i++)
            vals[i] = ((MySqlParameter)parameters[i]).Value;
        return vals;
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

        // 转值数组走统一核心，避免每行 MySqlParameterCollection/MySqlParameter 分配
        var sets = new List<Object?[]>(parameterSets.Count);
        foreach (var ps in parameterSets)
            sets.Add(ToValues(ps)!);

        return await ExecuteStatementValuesAsync(statementId, sets, paramColumns, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>管道化批量执行预编译语句（值数组版本）。parameterSets 每行为按参数顺序排列的值数组</summary>
    /// <remarks>
    /// 真管道化模式（Pipeline=true）：批量构建并发送所有 EXECUTE 包到网络缓冲区，最后一次性 Flush，
    /// 然后按顺序逐个读取响应。网络延迟仅发生一次，适合大批量 DML 操作。
    /// 串行模式（Pipeline=false）：逐条发送并读取响应，跳过 MySqlCommand 层的开销，直接在协议层循环。
    /// </remarks>
    /// <param name="statementId">预编译语句 ID</param>
    /// <param name="parameterSets">多组参数值数组，每组按参数顺序排列</param>
    /// <param name="paramColumns">Prepare 返回的参数列信息</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>总影响行数</returns>
    public async Task<Int32> ExecuteStatementValuesAsync(Int32 statementId, IList<Object?[]> parameterSets, MySqlColumn[]? paramColumns, CancellationToken cancellationToken = default)
    {
        if (parameterSets == null || parameterSets.Count == 0) return 0;

        // 真管道化：批量发送所有请求包，最后一次性 Flush，再批量读取响应。
        // 压缩协议下请求帧需要经过压缩层封装，当前管道化发送路径直接写普通帧，必须回退到串行模式避免协议流损坏。
        if (Setting.Pipeline && !_useCompression)
            return await ExecutePipelineValuesCoreAsync(statementId, parameterSets, cancellationToken).ConfigureAwait(false);

        // 串行模式：逐条发送 EXECUTE + 读取响应的快速循环
        var totalAffected = 0;
        for (var i = 0; i < parameterSets.Count; i++)
        {
            try
            {
                await ExecuteStatementAsync(statementId, parameterSets[i], paramColumns, cancellationToken).ConfigureAwait(false);
                using var rs = await ReadPacketAsync(cancellationToken).ConfigureAwait(false);
                var qr = GetResult(rs);
                totalAffected += qr.AffectedRows;
            }
            catch (Exception ex) when (ex is not MySqlException)
            {
                // 网络/协议错误（如发送中途断线、读取超时）会使连接残留半截请求或响应，协议状态不可信。
                // 标记连接失效，避免脏连接归还连接池后被后续查询复用导致读取超时。
                // 业务异常 MySqlException（如唯一键冲突）连接仍干净，保持可复用。
                Active = false;
                throw;
            }
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
    /// <param name="parameterSets">多组参数值数组，每组按参数顺序排列</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>总影响行数</returns>
    private async Task<Int32> ExecutePipelineValuesCoreAsync(Int32 statementId, IList<Object?[]> parameterSets, CancellationToken cancellationToken)
    {
        var count = parameterSets.Count;
        var ms = _stream ?? throw new InvalidOperationException(L.T("Connection is not open."));

        // 读取任务使用独立取消源：发送方向一旦失败，可主动取消后台读取，避免它成为野任务在后台继续读取网络流
        using var readCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // 先启动读取任务，它会在 ReadExactlyAsync 处让出执行权等待数据到达
        var readTask = ReadPipelineResponsesAsync(count, readCts.Token);

        // 标记发送循环是否全部成功完成，用于区分“发送失败（连接脏）”与“读取返回业务错误（连接干净）”
        var sendCompleted = false;

        try
        {
            // 发送所有 EXECUTE 包，直接写入 seq=0 帧头，避免与读取任务竞争 _seq 字段
            for (var i = 0; i < count; i++)
            {
                var pk = BuildExecutePacket(statementId, parameterSets[i]);

                // 管道化每个 EXECUTE 独立使用 seq=0，直接构造帧头避免并发读写 _seq 竞态
                var len = pk.Total;
                var pk2 = PrepareFrameHeader(pk, 4);
                try
                {
                    pk2[0] = (Byte)(len & 0xFF);
                    pk2[1] = (Byte)((len >> 8) & 0xFF);
                    pk2[2] = (Byte)((len >> 16) & 0xFF);
                    pk2[3] = 0;

                    await pk2.CopyToAsync(ms, cancellationToken).ConfigureAwait(false);

                    // 定期 Flush 推送数据到服务端（SslStream 有内部缓冲，必须 Flush 才能发出）
                    if (i == count - 1 || (i + 1) % 100 == 0)
                        await ms.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    // 发送即消费：归还链头与 BuildExecutePacket 返回的池化缓冲，避免万行批量泄漏 8KB×N
                    if (pk2 is IOwnerPacket owner) owner.Dispose();
                    if (pk is IOwnerPacket pkOwner) pkOwner.Dispose();
                }
            }
            sendCompleted = true;

            // 等待所有响应读取完成
            return await readTask.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (!sendCompleted)
            {
                // 发送方向失败（如批量写入 5000 条时发送抛 IOException），此时后台读取任务可能仍在运行。
                // 连接的协议状态已不可信，必须标记失效，防止脏连接归还连接池后被后续查询复用，
                // 否则新查询的读取会与残留响应错位、帧序号混乱，最终表现为查询端“读取数据包超时”。
                Active = false;

                // 取消并等待后台读取任务结束，避免它成为野任务在后台继续读取网络流，
                // 与归还连接池后被复用的新连接争抢同一个底层流。
                readCts.Cancel();
                try
                {
                    await readTask.ConfigureAwait(false);
                }
                catch { /* 吞掉读取任务异常，向上抛出原始发送失败异常 */ }
            }
            else if (ex is not MySqlException)
            {
                // 发送已全部完成，但读取阶段遭遇网络/协议错误（如 IOException、读取超时），连接状态不可信，标记失效。
                // 业务异常 MySqlException（如唯一键冲突）说明所有响应已正常读完、连接干净，无需失效，可正常归还复用。
                Active = false;
            }

            throw;
        }
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
            throw new MySqlException(L.T("Failed to kill the thread."));
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
    /// <exception cref="InvalidOperationException">连接未使用 TLS 且提供了非空口令</exception>
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
        if (!pwdStr.IsNullOrEmpty() && _stream is not SslStream)
            throw new InvalidOperationException(L.T("RegisterSlave requires a TLS connection when a password is supplied."));

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
            throw new MySqlException(L.T("Failed to register the replica."));
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