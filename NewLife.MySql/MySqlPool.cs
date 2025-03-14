using System.Collections.Concurrent;
using NewLife.Collections;
using NewLife.Log;

namespace NewLife.MySql;

/// <summary>连接池。每个连接字符串一个连接池，管理多个可重用连接</summary>
public class MySqlPool : ObjectPool<SqlClient>
{
    /// <summary>设置</summary>
    public MySqlConnectionStringBuilder? Setting { get; set; }

    private IDictionary<String, String>? _Variables;
    private DateTime _nextTime;
    /// <summary>服务器变量</summary>
    public IDictionary<String, String>? Variables
    {
        get
        {
            if (_Variables == null || _nextTime < DateTime.Now) return null;

            return _Variables;
        }
        set
        {
            _Variables = value;
            _nextTime = DateTime.Now.AddMinutes(10);
        }
    }

    /// <summary>创建连接</summary>
    protected override SqlClient OnCreate()
    {
        var set = Setting ?? throw new ArgumentNullException(nameof(Setting));
        var connStr = set.ConnectionString;
        if (connStr.IsNullOrEmpty()) throw new ArgumentNullException(nameof(Setting));

        var client = new SqlClient(set);

        return client;
    }

    /// <summary>获取连接。剔除无效连接</summary>
    public override SqlClient Get()
    {
        while (true)
        {
            var client = base.Get();

            // 重置网络数据流，清理上次未处理完的数据。如果网络已经断开，这里会重新从池里获取一个新的连接
            try
            {
                client.Reset();
                return client;
            }
            catch { }
        }
    }
}

/// <summary>连接池管理。根据连接字符串，换取对应连接池</summary>
public class MySqlPoolManager
{
    private ConcurrentDictionary<String, MySqlPool> _pools = new();
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