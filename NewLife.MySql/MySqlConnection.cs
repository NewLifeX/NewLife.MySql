using System.Data;
using System.Data.Common;

namespace NewLife.MySql;

/// <summary>数据库连接</summary>
public sealed partial class MySqlConnection : DbConnection
{
    #region 属性
    /// <summary>设置</summary>
    public MySqlConnectionStringBuilder Setting { get; } = [];

    /// <summary>连接字符串</summary>
    public override String ConnectionString { get => Setting.ConnectionString; set => Setting.ConnectionString = value; }

    /// <summary>数据库</summary>
    public override String? Database => Setting.Database;

    /// <summary>服务器</summary>
    public override String? DataSource => Setting.Server;

    /// <summary>连接超时</summary>
    public override Int32 ConnectionTimeout => Setting.ConnectionTimeout;

    private String _Version = null!;
    /// <summary>版本</summary>
    public override String ServerVersion => _Version;

    private ConnectionState _State;
    /// <summary>连接状态</summary>
    public override ConnectionState State => _State;

    /// <summary>工厂</summary>
    public MySqlClientFactory Factory { get; set; } = MySqlClientFactory.Instance;

    /// <summary>提供者工厂</summary>
    protected override DbProviderFactory DbProviderFactory => Factory;

    /// <summary>客户端连接</summary>
    public SqlClient? Client { get; set; }

    private MySqlPool? _pool;
    private SchemaProvider? _schemaProvider;
    #endregion

    #region 构造
    /// <summary>实例化</summary>
    public MySqlConnection() { }

    /// <summary>使用连接字符串实例化</summary>
    /// <param name="connStr"></param>
    public MySqlConnection(String connStr) : this() => ConnectionString = connStr;

    /// <summary>销毁</summary>
    protected override void Dispose(Boolean disposing)
    {
        base.Dispose(disposing);

        Close();
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    /// <summary>销毁</summary>
    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync().ConfigureAwait(false);

        await CloseAsync().ConfigureAwait(false);
    }
#endif
    #endregion

    #region 打开关闭
    /// <summary>打开</summary>
    public override void Open() => OpenAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>关闭</summary>
    public override void Close()
    {
        if (State == ConnectionState.Closed) return;

        // 关闭附属对象

        var client = Client;
        if (client != null)
        {
            // 检查当前数据库是否与原始数据库一致
            // 如果不一致，说明调用过 SetDatabaseAsync 且未切回，连接状态被污染，直接销毁
            // 如果一致，说明要么未切换过数据库，要么切换后又切回来了，可以安全归还连接池
            if (client.Database != Setting.Database)
            {
                // 数据库状态被污染，销毁连接不归还连接池
                client.TryDispose();
            }
            else if (_pool != null)
            {
                // 数据库状态正常，归还连接池
                _pool.Return(client);
            }
            else
            {
                client.TryDispose();
            }

            Client = null;
            _pool = null;
        }

        SetState(ConnectionState.Closed);
    }

    private void SetState(ConnectionState newState)
    {
        if (newState == State) return;

        var oldState = _State;
        _State = newState;

        OnStateChange(new StateChangeEventArgs(oldState, newState));
    }

    /// <summary>异步打开连接</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (State == ConnectionState.Open) return;

        SetState(ConnectionState.Connecting);

        try
        {
            var client = Client;
            if (client == null)
            {
                // 根据连接字符串创建连接池,然后从连接池获取连接
                _pool = Factory?.PoolManager?.GetPool(Setting);

                client = _pool?.Get() ?? new SqlClient(Setting);

                cancellationToken.ThrowIfCancellationRequested();

                if (client.Welcome == null)
                    await client.OpenAsync(cancellationToken).ConfigureAwait(false);

                var welcome = client.Welcome;
                if (welcome != null)
                {
                    _Version = welcome.ServerVersion!;
                }

                Client = client;

                // 设置读取超时
                var cmdTimeout = Setting.CommandTimeout;
                if (cmdTimeout > 0) client.Timeout = cmdTimeout;

                // 配置参数，优先从连接池获取缓存的变量
                var vs = _pool?.Variables;
                if (vs != null) client.Variables = vs;

                await client.ConfigureAsync(cancellationToken).ConfigureAwait(false);
                _pool?.Variables = client.Variables;
            }
        }
        catch (Exception)
        {
            SetState(ConnectionState.Closed);
            throw;
        }

        SetState(ConnectionState.Open);
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    /// <summary>异步关闭连接</summary>
    /// <returns></returns>
    public override Task CloseAsync()
    {
        Close();
        return Task.CompletedTask;
    }
#endif
    #endregion

    #region 执行命令
    /// <summary>执行SQL语句</summary>
    /// <param name="sql">SQL语句</param>
    /// <returns>影响行数</returns>
    public Int32 ExecuteNonQuery(String sql)
    {
        using var cmd = CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteNonQuery();
    }

    // 批量操作请使用 MySqlCommand.ExecuteBatchAsync，支持预编译语句 + 数组参数绑定
    #endregion

    #region 接口方法
    /// <summary>开始事务</summary>
    /// <param name="isolationLevel"></param>
    /// <returns></returns>
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        var sql = "SET TRANSACTION ISOLATION LEVEL ";
        sql += isolationLevel switch
        {
            IsolationLevel.ReadCommitted => "READ COMMITTED",
            IsolationLevel.ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel.Unspecified or IsolationLevel.RepeatableRead => "REPEATABLE READ",
            IsolationLevel.Serializable => "SERIALIZABLE",
            _ => throw new NotSupportedException(isolationLevel + ""),
        };

        ExecuteNonQuery(sql);
        ExecuteNonQuery("BEGIN");

        return new MySqlTransaction(this, isolationLevel);
    }

    /// <summary>改变数据库。不支持在事务中途切换数据库</summary>
    /// <param name="databaseName">目标数据库名</param>
    /// <remarks>
    /// 本方法通过关闭连接、修改连接字符串、重新打开连接的方式实现数据库切换。
    /// 优点：实现简单，连接池映射正确，避免连接池污染。
    /// 缺点：有连接关闭/打开的性能开销。
    /// 
    /// 使用约束：
    /// 1. 不支持在事务中途切换数据库（事务会丢失）
    /// 2. 不建议频繁调用（有性能开销）
    /// 3. 如需频繁切换数据库，建议使用多个连接对象
    /// </remarks>
    public override void ChangeDatabase(String databaseName)
    {
        // 场景1：未打开连接，直接修改设置，清空连接池引用
        if (State != ConnectionState.Open)
        {
            Setting.Database = databaseName;
            _pool = null;
            return;
        }

        // 场景2：已打开连接，关闭后修改设置再重新打开
        // 这样确保连接池映射正确（连接字符串变化后会映射到不同的连接池）
        Close();
        Setting.Database = databaseName;
        _pool = null;
        Open();
    }

    /// <summary>创建命令</summary>
    /// <returns></returns>
    protected override DbCommand CreateDbCommand()
    {
        var cmd = Factory.CreateCommand();
        cmd.Connection = this;

        return cmd;
    }

#if NET6_0_OR_GREATER
    /// <summary>创建批量命令</summary>
    /// <returns></returns>
    protected override DbBatch CreateDbBatch() => new MySqlBatch(this);
#endif

    /// <summary>获取架构信息</summary>
    public override DataTable GetSchema() => GetSchema(null, null);

    /// <summary>获取架构信息</summary>
    public override DataTable GetSchema(String collectionName) => GetSchema(collectionName, null);

    /// <summary>获取架构信息</summary>
    public override DataTable GetSchema(String? collectionName, String?[]? restrictionValues)
    {
        var provider = _schemaProvider ??= new SchemaProvider(this);
        return provider.GetSchema(collectionName, restrictionValues).AsDataTable();
    }
    #endregion
}