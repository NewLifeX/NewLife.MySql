using System.Data;
#if NET6_0_OR_GREATER
using System.Diagnostics;
#endif

namespace NewLife.MySql;

/// <summary>MySQL 客户端诊断埋点。基于 System.Diagnostics.ActivitySource 输出 OpenTelemetry 兼容链路</summary>
/// <remarks>
/// 源名称为 <see cref="SourceName"/>，订阅方式为 <c>AddSource("NewLife.MySql")</c>。
/// 产出 Open（连接）、Execute（命令，覆盖读取器生命周期）、Commit / Rollback（事务）四类 Client 跨度，
/// 属性遵循 OpenTelemetry 稳定数据库语义约定（db.system.name / db.namespace / server.address / db.query.text / error.type 等）。
/// net6.0 以下目标框架或未订阅时为空实现，仅保留极小的判断开销。
/// </remarks>
internal static class MySqlDiagnostics
{
    #region 属性
    /// <summary>ActivitySource 名称。OpenTelemetry 通过 AddSource 订阅该名称</summary>
    public const String SourceName = "NewLife.MySql";

#if NET6_0_OR_GREATER
    private static readonly ActivitySource _source = new(SourceName, typeof(MySqlDiagnostics).Assembly.GetName().Version?.ToString());
#endif
    #endregion

    #region 方法
    /// <summary>开始连接打开跨度</summary>
    /// <param name="setting">连接设置</param>
    /// <returns>跨度句柄，未订阅时为空句柄</returns>
    public static MySqlActivity StartOpen(MySqlConnectionStringBuilder setting)
    {
#if NET6_0_OR_GREATER
        var activity = _source.StartActivity("Open", ActivityKind.Client);
        if (activity != null) SetConnectionTags(activity, setting);

        return new MySqlActivity(activity);
#else
        _ = setting;
        return default;
#endif
    }

    /// <summary>开始命令执行跨度</summary>
    /// <param name="connection">连接，可为空</param>
    /// <param name="sql">SQL 文本</param>
    /// <param name="commandType">命令类型</param>
    /// <param name="batchSize">批量条数，大于 0 时按 BATCH 标注</param>
    /// <returns>跨度句柄，未订阅时为空句柄</returns>
    public static MySqlActivity StartExecute(MySqlConnection? connection, String? sql, CommandType commandType, Int32 batchSize = 0)
    {
#if NET6_0_OR_GREATER
        var activity = _source.StartActivity("Execute", ActivityKind.Client);
        if (activity == null) return default;

        var setting = connection?.Setting;
        if (setting != null) SetConnectionTags(activity, setting);

        var threadId = connection?.Client?.Welcome?.ThreadID;
        if (threadId is > 0) activity.SetTag("db.connection_id", threadId.Value.ToString());

        if (sql is not null and not "") activity.SetTag("db.query.text", sql);

        // 仅在无需解析 SQL 文本即可确定时标注操作名（与 MySqlConnector 约定一致）
        if (batchSize > 0)
        {
            activity.SetTag("db.operation.name", "BATCH");
            activity.SetTag("db.operation.batch.size", batchSize);
        }
        else if (commandType == CommandType.StoredProcedure)
        {
            activity.SetTag("db.operation.name", "CALL");
            if (sql is not null and not "") activity.SetTag("db.stored_procedure.name", sql);
        }
        else if (commandType == CommandType.TableDirect)
            activity.SetTag("db.operation.name", "SELECT");

        return new MySqlActivity(activity);
#else
        _ = connection;
        _ = sql;
        _ = commandType;
        _ = batchSize;
        return default;
#endif
    }

    /// <summary>开始事务跨度</summary>
    /// <param name="connection">连接</param>
    /// <param name="name">跨度名称（Commit / Rollback）</param>
    /// <returns>跨度句柄，未订阅时为空句柄</returns>
    public static MySqlActivity StartTransaction(MySqlConnection connection, String name)
    {
#if NET6_0_OR_GREATER
        var activity = _source.StartActivity(name, ActivityKind.Client);
        if (activity != null) SetConnectionTags(activity, connection.Setting);

        return new MySqlActivity(activity);
#else
        _ = connection;
        _ = name;
        return default;
#endif
    }

#if NET6_0_OR_GREATER
    private static void SetConnectionTags(Activity activity, MySqlConnectionStringBuilder setting)
    {
        activity.SetTag("db.system.name", "mysql");

        // Unix 套接字以套接字路径作为地址，便于与 TCP 场景区分
        var address = setting.Socket;
        if (String.IsNullOrEmpty(address)) address = setting.Server;
        if (!String.IsNullOrEmpty(address)) activity.SetTag("server.address", address);

        if (setting.Port is > 0 and not 3306) activity.SetTag("server.port", setting.Port);
        if (!String.IsNullOrEmpty(setting.Database)) activity.SetTag("db.namespace", setting.Database);
    }
#endif
    #endregion
}

/// <summary>跨度句柄。net6.0 以下目标框架为空实现，调用方无需条件编译</summary>
internal readonly struct MySqlActivity : IDisposable
{
#if NET6_0_OR_GREATER
    private readonly Activity? _activity;

    /// <summary>实例化</summary>
    /// <param name="activity">底层活动对象，可为空</param>
    public MySqlActivity(Activity? activity) => _activity = activity;

    /// <summary>是否已采集</summary>
    public Boolean IsActive => _activity != null;

    /// <summary>设置标签</summary>
    /// <param name="key">标签名</param>
    /// <param name="value">标签值</param>
    public void SetTag(String key, Object? value) => _activity?.SetTag(key, value);

    /// <summary>标记异常。设置错误标签、异常事件与错误状态</summary>
    /// <param name="ex">异常</param>
    public void SetError(Exception ex)
    {
        var activity = _activity;
        if (activity == null) return;

        // MySqlException 记录 MySQL 错误号，其余记录异常类型全名（与 MySqlConnector 约定一致）
        if (ex is MySqlException my && my.ErrorCode > 0)
        {
            var code = my.ErrorCode.ToString();
            activity.SetTag("error.type", code);
            activity.SetTag("db.response.status_code", code);
        }
        else
            activity.SetTag("error.type", ex.GetType().FullName);

        activity.SetStatus(ActivityStatusCode.Error, ex.Message);
        activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
        {
            ["exception.type"] = ex.GetType().FullName,
            ["exception.message"] = ex.Message,
            ["exception.stacktrace"] = ex.ToString(),
        }));
    }

    /// <summary>结束跨度</summary>
    public void Dispose() => _activity?.Dispose();
#else
    /// <summary>是否已采集</summary>
    public Boolean IsActive => false;

    /// <summary>设置标签</summary>
    /// <param name="key">标签名</param>
    /// <param name="value">标签值</param>
    public void SetTag(String key, Object? value) { }

    /// <summary>标记异常</summary>
    /// <param name="ex">异常</param>
    public void SetError(Exception ex) { }

    /// <summary>结束跨度</summary>
    public void Dispose() { }
#endif
}
