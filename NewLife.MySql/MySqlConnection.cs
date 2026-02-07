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

#if NETSTANDARD2_1_OR_GREATER
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
    public override void Open()
    {
        if (State == ConnectionState.Open) return;

        SetState(ConnectionState.Connecting);

        // 打开网络连接
        try
        {
            var client = Client;
            if (client == null)
            {
                // 根据连接字符串创建连接池，然后从连接池获取连接
                _pool = Factory?.PoolManager?.GetPool(Setting);

                client = _pool?.Get() ?? new SqlClient(Setting);

                if (client.Welcome == null)
                    client.Open();

                var welcome = client.Welcome;
                if (welcome != null)
                {
                    _Version = welcome.ServerVersion!;
                }

                Client = client;

                // 配置参数，优先从连接池获取缓存的变量
                var vs = _pool?.Variables;
                if (vs != null) client.Variables = vs;

                client.Configure();
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

    /// <summary>关闭</summary>
    public override void Close()
    {
        if (State == ConnectionState.Closed) return;

        // 关闭附属对象

        var client = Client;
        if (client != null)
        {
            // 关闭网络连接，归还连接池
            if (_pool != null)
                _pool.Return(client);
            else
                client.TryDispose();
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
    public override Task OpenAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Open();
#if NET45
        return Task.FromResult(0);
#else
        return Task.CompletedTask;
#endif
    }

#if NETSTANDARD2_1_OR_GREATER
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
    /// <param name="sql"></param>
    /// <returns></returns>
    public Int32 ExecuteNonQuery(String sql)
    {
        using var cmd = CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteNonQuery();
    }
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

    /// <summary>改变数据库</summary>
    /// <param name="databaseName"></param>
    public override void ChangeDatabase(String databaseName)
    {
        var opened = State == ConnectionState.Open;
        if (opened) Close();

        Setting.Database = databaseName;

        if (opened) Open();
    }

    /// <summary>创建命令</summary>
    /// <returns></returns>
    protected override DbCommand CreateDbCommand()
    {
        var cmd = Factory.CreateCommand();
        cmd.Connection = this;

        return cmd;
    }

    /// <summary>获取架构信息</summary>
    public override DataTable GetSchema() => GetSchema(null, null);

    /// <summary>获取架构信息</summary>
    public override DataTable GetSchema(String collectionName) => GetSchema(collectionName, null);

    /// <summary>获取架构信息</summary>
    public override DataTable GetSchema(String? collectionName, String[]? restrictionValues)
    {
        var provider = _schemaProvider ??= new SchemaProvider(this);
        return provider.GetSchema(collectionName, restrictionValues).AsDataTable();
    }
    #endregion
}