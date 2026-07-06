using System.Collections.Concurrent;
using NewLife.Collections;
using NewLife.Log;
using NewLife.MySql.Common;

namespace NewLife.MySql;

/// <summary>连接池。每个连接字符串一个连接池，管理多个可重用连接</summary>
public class MySqlPool : ObjectPool<SqlClient>
{
    /// <summary>连接池 PING 阈值系数。空闲超过 IdlePoolTime×PingRatio 的连接借出前补一次 PING 验活
    /// （0.8 表示闲置达 80% 清理阈值时触发 PING，剩余 20% 窗口防误清理）</summary>
    private const Double PingRatio = 0.8;

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
        var idlePing = set != null && set.IdlePoolTime > 0 ? (Int32)(set.IdlePoolTime * PingRatio) : 0;
        var decision = ConnectionHealth.Evaluate(value.Active, value.CreatedTime, value.LastActive, DateTime.Now,
            set?.ConnectionLifeTime ?? 0, idlePing);

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
        var idlePing = set != null && set.IdlePoolTime > 0 ? (Int32)(set.IdlePoolTime * PingRatio) : 0;
        var decision = ConnectionHealth.Evaluate(value.Active, value.CreatedTime, value.LastActive, DateTime.Now,
            set?.ConnectionLifeTime ?? 0, idlePing);

        var reusable = decision != ConnectionDecision.Discard && value.IsSocketAlive() == SocketHealth.Alive;

        // 基类在 OnReturn 返回 false 时不会调用 OnDispose，这里主动释放避免 socket 泄漏
        if (!reusable) value.TryDispose();

        return reusable;
    }

    /// <summary>异步检查借出时资源是否可用。对空闲较久的连接补一次 PING 验活；
    /// 任何异常视为连接不可用，由基类自动丢弃并重试。</summary>
    /// <param name="value">待检查连接</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>是否可用</returns>
    protected override async Task<Boolean> OnGetAsync(SqlClient value, CancellationToken cancellationToken)
    {
        try
        {
            // 新建连接尚未打开，交由调用方打开
            if (value.Welcome == null) return true;

            // 评估连接健康度：是否已失效、是否到达存活期、是否需要 PING
            var set = Setting;
            var lifetime = set?.ConnectionLifeTime ?? 0;
            var idlePing = set != null && set.IdlePoolTime > 0 ? (Int32)(set.IdlePoolTime * PingRatio) : 0;

            var decision = ConnectionHealth.Evaluate(value.Active, value.CreatedTime, value.LastActive, DateTime.Now, lifetime, idlePing);
            if (decision == ConnectionDecision.Discard) return false;

            // 廉价非阻塞 socket 探活：对端已关或残留脏数据一律丢弃（OnGet 同步路径同样做此检查）
            if (value.IsSocketAlive() != SocketHealth.Alive) return false;

            if (decision == ConnectionDecision.Reusable) return true;

            // decision == NeedPing：空闲较久，补一次异步 PING 验活
            if (await PingWithTimeoutAsync(value, cancellationToken).ConfigureAwait(false))
                return true;

            return false;
        }
        catch
        {
            // 任何异常（Evaluate AddSeconds 越界/Ping 未捕获异常）视为连接不可用
            return false;
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
            Setting = setting,
            Min = setting.MinPoolSize > 0 ? setting.MinPoolSize : 0,
            Max = setting.MaxPoolSize > 0 ? setting.MaxPoolSize : 100,
            IdleTime = setting.IdlePoolTime > 0 ? setting.IdlePoolTime : 60,
            // MaxLifetime 对接 ConnectionLifeTime，超龄连接由基类自动回收（>0 时生效）
            MaxLifetime = setting.ConnectionLifeTime > 0 ? setting.ConnectionLifeTime : 0,
        };

        return pool;
    }
}