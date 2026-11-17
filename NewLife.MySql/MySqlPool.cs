using System.Collections.Concurrent;
using NewLife.Collections;
using NewLife.Log;
using NewLife.MySql.Common;
using NewLife.Threading;

namespace NewLife.MySql;

/// <summary>连接池。每个连接字符串一个连接池，管理多个可重用连接</summary>
public class MySqlPool : ObjectPool<SqlClient>
{
    /// <summary>连接池 PING 阈值系数。空闲超过 IdlePoolTime×PingRatio 的连接借出前补一次 PING 验活
    /// （0.8 表示闲置达 80% 清理阈值时触发 PING，剩余 20% 窗口防误清理）</summary>
    private const Double PingRatio = 0.8;

    /// <summary>设置</summary>
    public MySqlConnectionStringBuilder? Setting { get; set; }

    private readonly Object _lifecycleLock = new();
    private DateTime _lastUsed = DateTime.UtcNow;
    private Int32 _renting;
    private Int32 _returning;
    private Boolean _retired;

    /// <summary>最后使用时间。用于连接池管理按空闲时间淘汰整个池</summary>
    public DateTime LastUsed
    {
        get
        {
            lock (_lifecycleLock) return _lastUsed;
        }
    }

    /// <summary>连接池是否已退役。退役后不再接受新的借出请求</summary>
    public Boolean IsRetired => Volatile.Read(ref _retired);

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

    /// <summary>借出连接。与退役流程互斥：退役检查通过后，本次借出计入 _renting，直到借出完成</summary>
    /// <returns>连接</returns>
    public override SqlClient Get()
    {
        EnterRent();
        try
        {
            return base.Get();
        }
        finally
        {
            ExitRent();
        }
    }

    /// <summary>异步借出连接。与退役流程互斥：退役检查通过后，本次借出计入 _renting，直到借出完成</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>连接</returns>
    public override async Task<SqlClient> GetAsync(CancellationToken cancellationToken = default)
    {
        EnterRent();
        try
        {
            return await base.GetAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ExitRent();
        }
    }

    /// <summary>归还连接。归还完成前不允许退役，避免清理线程把正在入池的连接丢掉</summary>
    /// <param name="value">连接</param>
    /// <returns>是否成功归入池</returns>
    public override Boolean Return(SqlClient value)
    {
        lock (_lifecycleLock)
        {
            if (_retired)
            {
                value.TryDispose();
                return false;
            }

            _returning++;
            _lastUsed = DateTime.UtcNow;
        }

        try
        {
            return base.Return(value);
        }
        finally
        {
            lock (_lifecycleLock)
            {
                _returning--;
                _lastUsed = DateTime.UtcNow;
            }
        }
    }

    private void EnterRent()
    {
        lock (_lifecycleLock)
        {
            if (_retired) throw new ObjectDisposedException(nameof(MySqlPool));

            _renting++;
            _lastUsed = DateTime.UtcNow;
        }
    }

    private void ExitRent()
    {
        lock (_lifecycleLock)
        {
            _renting--;
            _lastUsed = DateTime.UtcNow;
        }
    }

    /// <summary>尝试退役。只有在没有借出、没有归还、没有繁忙连接时才允许退役</summary>
    /// <param name="idleBefore">空闲截止时间，最后使用时间必须早于该时间</param>
    /// <returns>退役成功返回 true</returns>
    internal Boolean TryRetire(DateTime idleBefore)
    {
        lock (_lifecycleLock)
        {
            if (_retired || _renting > 0 || _returning > 0 || BusyCount > 0) return false;
            if (_lastUsed > idleBefore) return false;

            _retired = true;
            return true;
        }
    }

    /// <summary>创建连接</summary>
    protected override SqlClient OnCreate()
    {
        var set = Setting ?? throw new ArgumentNullException(nameof(Setting));
        var connStr = set.ConnectionString;
        if (connStr.IsNullOrEmpty()) throw new InvalidOperationException(L.T("Connection string cannot be empty."));

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

        // NeedPing 连接在同步借出路径无法异步验活，直接丢弃重建，避免借出可能已断开的空闲连接；
        // 异步 GetAsync 路径仍通过 PING 保留连接
        if (decision == ConnectionDecision.NeedPing) return false;

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

        // 归还前重置会话：清理用户变量、临时表、SET sql_mode 等会话状态，避免下一位使用者继承
        if (reusable) reusable = value.ResetSession();

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
public class MySqlPoolManager : DisposeBase
{
    private readonly ConcurrentDictionary<String, MySqlPool> _pools = new();
    private readonly TimerX _timer;

    /// <summary>连接池数量。用于诊断动态连接字符串是否造成池对象累积</summary>
    internal Int32 PoolCount => _pools.Count;

    /// <summary>实例化连接池管理，并启动空闲池清理定时器</summary>
    public MySqlPoolManager()
    {
        // 每 60 秒扫描一次。只有超过 PoolExpireTime 且没有任何借出/归还的池才会被移除
        _timer = new TimerX(ScanPools, null, 60_000, 60_000) { Async = false };
    }

    /// <summary>获取连接池。连接字符串相同时共用连接池</summary>
    /// <param name="setting"></param>
    /// <returns></returns>
    public MySqlPool GetPool(MySqlConnectionStringBuilder setting)
    {
        if (Disposed) throw new ObjectDisposedException(nameof(MySqlPoolManager));

        var key = setting.GetPoolKey();
        while (true)
        {
            var pool = _pools.GetOrAdd(key, _ => CreatePool(setting));

            // 清理线程可能刚好把旧池标记为退役。此时重新创建，避免把已释放的池交给调用方
            if (!pool.IsRetired) return pool;

            RemovePool(key, pool);
        }
    }

    private Boolean RemovePool(String key, MySqlPool pool)
        => ((ICollection<KeyValuePair<String, MySqlPool>>)_pools).Remove(new KeyValuePair<String, MySqlPool>(key, pool));

    /// <summary>创建连接池</summary>
    /// <param name="setting"></param>
    /// <returns></returns>
    protected virtual MySqlPool CreatePool(MySqlConnectionStringBuilder setting)
    {
        using var span = DefaultTracer.Instance?.NewSpan("db:mysql:CreatePool", new { setting.Server, setting.Database });

        // 克隆 Setting 快照，避免与调用方共享可变实例。
        // 否则创建池的连接调用 ChangeDatabase 修改 Setting.Database 时，会篡改池配置，导致后续新连接连到错误数据库
        var pool = new MySqlPool
        {
            Setting = setting.Clone(),
            Min = setting.MinPoolSize > 0 ? setting.MinPoolSize : 0,
            Max = setting.MaxPoolSize > 0 ? setting.MaxPoolSize : 100,
            IdleTime = setting.IdlePoolTime > 0 ? setting.IdlePoolTime : 60,
            // MaxLifetime 对接 ConnectionLifeTime，超龄连接由基类自动回收（>0 时生效）
            MaxLifetime = setting.ConnectionLifeTime > 0 ? setting.ConnectionLifeTime : 0,
        };

        return pool;
    }

    /// <summary>扫描并移除长时间未使用的空连接池</summary>
    /// <param name="state">定时器状态</param>
    private void ScanPools(Object? state) => SweepPools(DateTime.UtcNow);

    /// <summary>按最后使用时间清理空闲连接池。返回移除数量，便于测试和诊断</summary>
    /// <param name="now">当前时间</param>
    /// <returns>移除的连接池数量</returns>
    internal Int32 SweepPools(DateTime now)
    {
        var count = 0;
        foreach (var item in _pools)
        {
            var pool = item.Value;
            var expire = pool.Setting?.PoolExpireTime ?? 0;
            if (expire <= 0) continue;

            var idleBefore = now.AddSeconds(-expire);
            if (!pool.TryRetire(idleBefore)) continue;

            RemovePool(item.Key, pool);
            pool.Dispose();
            count++;
        }

        return count;
    }

    /// <summary>销毁管理器，停止定时器并释放所有连接池</summary>
    /// <param name="disposing">是否释放托管资源</param>
    protected override void Dispose(Boolean disposing)
    {
        if (disposing)
        {
            _timer.TryDispose();

            foreach (var item in _pools)
            {
                if (_pools.TryRemove(item.Key, out var pool)) pool.TryDispose();
            }
        }

        base.Dispose(disposing);
    }
}
