using System.Collections.Concurrent;
using NewLife.Collections;
using NewLife.Log;
using NewLife.MySql.Common;

namespace NewLife.MySql;

/// <summary>连接池。每个连接字符串一个连接池，管理多个可重用连接</summary>
public class MySqlPool : ObjectPool<SqlClient>
{
    /// <summary>设置</summary>
    public MySqlConnectionStringBuilder? Setting { get; set; }

    private IDictionary<String, String>? _Variables;
    private DateTime _nextTime;
    private String? _serverVersion;
    private String? _serverVersionComment;
    private DatabaseType _databaseType;
    private DateTime _serverInfoExpire;
    /// <summary>服务器变量。缓存10分钟后自动过期</summary>
    public IDictionary<String, String>? Variables
    {
        get
        {
            if (_Variables == null || _nextTime < DateTime.UtcNow) return null;

            return _Variables;
        }
        set
        {
            _Variables = value;
            _nextTime = DateTime.UtcNow.AddMinutes(10);
        }
    }

    /// <summary>尝试获取缓存的服务器信息</summary>
    /// <param name="serverVersion">服务器版本</param>
    /// <param name="serverVersionComment">版本注释</param>
    /// <param name="databaseType">数据库类型</param>
    /// <returns>是否命中缓存</returns>
    public Boolean TryGetServerInfo(out String? serverVersion, out String? serverVersionComment, out DatabaseType databaseType)
    {
        if (_serverVersion.IsNullOrEmpty() || _serverInfoExpire < DateTime.UtcNow)
        {
            serverVersion = null;
            serverVersionComment = null;
            databaseType = DatabaseType.MySQL;
            return false;
        }

        serverVersion = _serverVersion;
        serverVersionComment = _serverVersionComment;
        databaseType = _databaseType;
        return true;
    }

    /// <summary>缓存服务器信息</summary>
    /// <param name="serverVersion">服务器版本</param>
    /// <param name="serverVersionComment">版本注释</param>
    /// <param name="databaseType">数据库类型</param>
    public void SetServerInfo(String serverVersion, String? serverVersionComment, DatabaseType databaseType)
    {
        _serverVersion = serverVersion;
        _serverVersionComment = serverVersionComment;
        _databaseType = databaseType;
        _serverInfoExpire = DateTime.UtcNow.AddMinutes(10);
    }

    /// <summary>创建连接</summary>
    protected override SqlClient OnCreate()
    {
        var set = Setting ?? throw new ArgumentNullException(nameof(Setting));
        var connStr = set.ConnectionString;
        if (connStr.IsNullOrEmpty()) throw new InvalidOperationException("连接字符串不能为空");

        return new SqlClient(set);
    }

    /// <summary>借出对象前的同步校验。失效/到龄/socket 异常的连接返回 false，
    /// 交由基类丢弃并自动重取，从而修正 BusyCount 记账，避免旧实现裸 TryDispose 泄漏 busy 槽</summary>
    /// <param name="value">待校验连接</param>
    /// <returns>是否可借出</returns>
    protected override Boolean OnGet(SqlClient value)
    {
        // 新建连接尚未打开，交由调用方打开
        if (value.Welcome == null) return true;

        var set = Setting;
        var decision = ConnectionHealth.Evaluate(value.Active, value.CreatedTime, value.LastActive, DateTime.Now,
            set?.ConnectionLifeTime ?? 0, set?.ConnectionIdlePingTime ?? 0);

        // 已失效或超过最大存活期，丢弃（基类随后调用 OnDispose 关闭连接）
        if (decision == ConnectionDecision.Discard) return false;

        // 廉价非阻塞 socket 探活：每次借出都做，不再被空闲时间窗口门控。对端已关或残留脏数据一律丢弃
        if (value.IsSocketAlive() != SocketHealth.Alive) return false;

        // Reusable 直接借出；NeedPing 由异步 GetAsync 补一次 PING
        return true;
    }

    /// <summary>归还对象前的校验。失效/到龄/socket 异常的连接不再入池，主动销毁释放 socket</summary>
    /// <param name="value">待归还连接</param>
    /// <returns>是否可归还入池</returns>
    protected override Boolean OnReturn(SqlClient value)
    {
        var set = Setting;
        var decision = ConnectionHealth.Evaluate(value.Active, value.CreatedTime, value.LastActive, DateTime.Now,
            set?.ConnectionLifeTime ?? 0, set?.ConnectionIdlePingTime ?? 0);

        var reusable = decision != ConnectionDecision.Discard && value.IsSocketAlive() == SocketHealth.Alive;

        // 基类在 OnReturn 返回 false 时不会调用 OnDispose，这里主动释放避免 socket 泄漏
        if (!reusable) value.TryDispose();

        return reusable;
    }

    /// <summary>异步获取连接。基类 OnGet 已完成同步校验（活性/存活期/socket 探活）；
    /// 此处仅对空闲较久的连接补一次 PING 验活，兜底无 FIN 的黑洞型断连</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>可用的数据库连接</returns>
    public new async Task<SqlClient> GetAsync(CancellationToken cancellationToken = default)
    {
        var set = Setting;
        var lifetime = set?.ConnectionLifeTime ?? 0;
        var idlePing = set?.ConnectionIdlePingTime ?? 0;

        var retryCount = 0;
        while (true)
        {
            // base.Get() 内部已通过 OnGet 过滤失效/到龄/socket 异常连接，并正确维护 _busy 记账
            var client = base.Get();

            // 新建连接尚未打开，直接返回由调用方打开
            if (client.Welcome == null) return client;

            // 仅空闲超阈值的连接需要补一次 PING；其余直接复用
            var decision = ConnectionHealth.Evaluate(client.Active, client.CreatedTime, client.LastActive, DateTime.Now, lifetime, idlePing);
            if (decision != ConnectionDecision.NeedPing) return client;

            if (await PingWithTimeoutAsync(client, cancellationToken).ConfigureAwait(false)) return client;

            // PING 失败：PingAsync 已标记 Active=false，归还走 OnReturn 丢弃并修正 BusyCount 记账（不再裸 TryDispose 泄漏 busy 槽）
            Return(client);
            if (retryCount++ > 10) throw new InvalidOperationException("无法从连接池获取可用连接");
        }
    }

    /// <summary>带短超时的异步 Ping 检测</summary>
    private static async Task<Boolean> PingWithTimeoutAsync(SqlClient client, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(3));
        return await client.PingAsync(cts.Token).ConfigureAwait(false);
    }
}

/// <summary>连接池管理。根据连接字符串，换取对应连接池</summary>
public class MySqlPoolManager
{
    private readonly ConcurrentDictionary<String, MySqlPool> _pools = new();
    /// <summary>获取连接池。连接字符串相同时共用连接池</summary>
    /// <param name="setting"></param>
    /// <returns></returns>
    public MySqlPool GetPool(MySqlConnectionStringBuilder setting) => _pools.GetOrAdd(setting.ConnectionString, k => CreatePool(setting));

    /// <summary>创建连接池</summary>
    /// <param name="setting"></param>
    /// <returns></returns>
    protected virtual MySqlPool CreatePool(MySqlConnectionStringBuilder setting)
    {
        using var span = DefaultTracer.Instance?.NewSpan("db:mysql:CreatePool", new { setting.Server, setting.Database });

        var pool = new MySqlPool
        {
            //Name = Name + "Pool",
            //Instance = this,
            Setting = setting,
            Min = setting.MinPoolSize > 0 ? setting.MinPoolSize : 0,
            Max = setting.MaxPoolSize > 0 ? setting.MaxPoolSize : 100,
            IdleTime = 30,
            AllIdleTime = 300,
            //Log = ClientLog,

            //Callback = OnCreate,
        };

        return pool;
    }
}