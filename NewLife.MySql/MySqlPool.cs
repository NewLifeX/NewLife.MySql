using System.Collections.Concurrent;
using NewLife.Collections;
using NewLife.Log;
using NewLife.MySql.Common;

namespace NewLife.MySql;

/// <summary>连接池。每个连接字符串一个连接池，管理多个可重用连接</summary>
public class MySqlPool : ObjectPool<SqlClient>
{
    private static readonly TimeSpan PingIdleTime = TimeSpan.FromSeconds(10);

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

    /// <summary>异步获取连接。剔除无效连接</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>可用的数据库连接</returns>
    public new async Task<SqlClient> GetAsync(CancellationToken cancellationToken = default)
    {
        var retryCount = 0;
        while (true)
        {
            var client = base.Get();

            // 新创建的连接尚未打开，直接返回由调用方打开
            if (client.Welcome == null) return client;

            // 已打开的连接借出前强制做一次轻量验活，避免半断开的连接在首个命令时才暴露失败
            if (!client.Active || !client.Reset() ||
                NeedPing(client) && !await PingWithTimeoutAsync(client, cancellationToken).ConfigureAwait(false))
            {
                // 连接已失效，丢弃后重试
                client.TryDispose();
                if (retryCount++ > 10) throw new InvalidOperationException("无法从连接池获取可用连接");
                continue;
            }

            return client;
        }
    }

    /// <summary>带短超时的异步 Ping 检测</summary>
    private static async Task<Boolean> PingWithTimeoutAsync(SqlClient client, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(3));
        return await client.PingAsync(cts.Token).ConfigureAwait(false);
    }

    private static Boolean NeedPing(SqlClient client)
    {
        if (client.LastActive == DateTime.MinValue) return true;

        return client.LastActive.Add(PingIdleTime) <= DateTime.Now;
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
        using var span = DefaultTracer.Instance?.NewSpan("db:mysql:CreatePool", setting.ConnectionString);

        var pool = new MySqlPool
        {
            //Name = Name + "Pool",
            //Instance = this,
            Setting = setting,
            Min = 10,
            Max = 100000,
            IdleTime = 30,
            AllIdleTime = 300,
            //Log = ClientLog,

            //Callback = OnCreate,
        };

        return pool;
    }
}