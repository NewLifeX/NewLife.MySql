using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>链路追踪测试。验证 ActivitySource 跨度的产生时机与 OpenTelemetry 稳定语义属性</summary>
[Collection(TestCollections.TableOperations)]
public class TracingTests : IDisposable
{
    private static readonly String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public TracingTests()
    {
        _table = "trace_test_" + Rand.Next(10000);
        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();
        _conn.ExecuteNonQuery($@"CREATE TEMPORARY TABLE IF NOT EXISTS `{_table}` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `name` VARCHAR(50) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TEMPORARY TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    [Fact]
    [DisplayName("命令执行产生 Execute 跨度并记录SQL文本")]
    public void ExecuteSpan_CapturesQuery()
    {
        using var collector = new SpanCollector();

        var marker = NewMarker();
        using var cmd = new MySqlCommand(_conn, $"SELECT 1 /* {marker} */");
        Assert.Equal(1, cmd.ExecuteScalar().ToInt());

        // 并行测试共享全局监听器，用 SQL 中的唯一标记定位本次产生的跨度
        var span = Assert.Single(collector.OfQuery(marker));
        Assert.Equal("mysql", span.Tags["db.system.name"]);
        Assert.Equal($"SELECT 1 /* {marker} */", span.Tags["db.query.text"]);
        Assert.True(span.Tags.ContainsKey("db.connection_id"));
        Assert.Equal(ActivityStatusCode.Unset, span.Status);
    }

    [Fact]
    [DisplayName("读取器关闭前跨度不结束，关闭后结束")]
    public void ExecuteSpan_EndsWhenReaderClosed()
    {
        using var collector = new SpanCollector();

        var marker = NewMarker();
        using var cmd = new MySqlCommand(_conn, $"SELECT 1 UNION SELECT 2 UNION SELECT 3 /* {marker} */");
        var reader = cmd.ExecuteReader();

        // 跨度覆盖结果读取周期：读取器未关闭时尚未结束
        Assert.Empty(collector.OfQuery(marker));

        reader.Dispose();
        Assert.Single(collector.OfQuery(marker));
    }

    [Fact]
    [DisplayName("打开连接产生 Open 跨度并记录连接属性")]
    public void OpenSpan_CapturesConnectionTags()
    {
        using var collector = new SpanCollector();

        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        var spans = collector.OfName("Open");
        Assert.NotEmpty(spans);
        foreach (var span in spans)
        {
            Assert.Equal("mysql", span.Tags["db.system.name"]);
            Assert.True(span.Tags.ContainsKey("server.address"));
        }
        Assert.Contains(spans, e => e.Tags.ContainsKey("db.connection_id"));
    }

    [Fact]
    [DisplayName("执行失败时跨度标记错误状态与MySQL错误号")]
    public void ExecuteSpan_OnError_SetsErrorStatus()
    {
        using var collector = new SpanCollector();

        var table = "no_such_table_" + Rand.Next(10000);
        using var cmd = new MySqlCommand(_conn, $"SELECT * FROM {table}");
        var ex = Assert.Throws<MySqlException>(() => cmd.ExecuteNonQuery());

        var span = Assert.Single(collector.OfQuery(table));
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal(ex.ErrorCode.ToString(), span.Tags["error.type"]);
        Assert.Equal(ex.ErrorCode.ToString(), span.Tags["db.response.status_code"]);
        Assert.Contains("exception", span.Events);
    }

    [Fact]
    [DisplayName("事务提交与回滚产生独立跨度且内部语句不重复计数")]
    public void TransactionSpans_CommitAndRollback()
    {
        using var collector = new SpanCollector();

        using (var tx = _conn.BeginTransaction())
            tx.Commit();

        using (var tx = _conn.BeginTransaction())
            tx.Rollback();

        var commits = collector.OfName("Commit");
        var rollbacks = collector.OfName("Rollback");
        Assert.NotEmpty(commits);
        Assert.NotEmpty(rollbacks);
        Assert.Equal("mysql", commits[0].Tags["db.system.name"]);

        // 驱动内部记账语句（BEGIN / SET TRANSACTION / COMMIT / ROLLBACK）不产生 Execute 跨度
        var texts = collector.OfName("Execute")
            .Select(e => e.Tags.TryGetValue("db.query.text", out var v) ? v as String : null)
            .Where(e => !String.IsNullOrEmpty(e))
            .ToArray();
        Assert.DoesNotContain(texts, e => e!.StartsWith("SET TRANSACTION") || e == "BEGIN" || e == "COMMIT" || e == "ROLLBACK");
    }

    [Fact]
    [DisplayName("批量执行产生 BATCH 跨度并记录批量条数")]
    public void BatchSpan_RecordsBatchSize()
    {
        using var collector = new SpanCollector();

        var marker = NewMarker();
        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}`(`name`) VALUES(@name) /* {marker} */");
        var affected = cmd.ExecuteBatchValues([
            ["a"],
            ["b"],
            ["c"],
        ]);

        Assert.Equal(3, affected);

        var span = Assert.Single(collector.OfQuery(marker));
        Assert.Equal("BATCH", span.Tags["db.operation.name"]);
        Assert.Equal(3, span.Tags["db.operation.batch.size"]);
    }

    [Fact]
    [DisplayName("数组绑定批量产生 BATCH 跨度并记录批量条数")]
    public void ArrayBatchSpan_RecordsBatchSize()
    {
        using var collector = new SpanCollector();

        var marker = NewMarker();
        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}`(`name`) VALUES(@name) /* {marker} */");
        cmd.Parameters.AddWithValue("name", new String[] { "a", "b", "c", "d" });

        Assert.Equal(4, cmd.ExecuteArrayBatch(4));

        var span = Assert.Single(collector.OfQuery(marker));
        Assert.Equal("BATCH", span.Tags["db.operation.name"]);
        Assert.Equal(4, span.Tags["db.operation.batch.size"]);
    }

    [Fact]
    [DisplayName("存储过程跨度标注 CALL 与过程名")]
    public void StoredProcedureSpan_RecordsProcedureName()
    {
        using var collector = new SpanCollector();

        var proc = "nl_trace_proc_" + Rand.Next(10000);
        try
        {
            _conn.ExecuteNonQuery($"CREATE PROCEDURE `{proc}`() SELECT 42");

            using var cmd = new MySqlCommand(_conn, proc) { CommandType = CommandType.StoredProcedure };
            Assert.Equal(42, cmd.ExecuteScalar().ToInt());

            var span = Assert.Single(collector.OfProcedure(proc));
            Assert.Equal("CALL", span.Tags["db.operation.name"]);
            Assert.Equal(proc, span.Tags["db.query.text"]);
        }
        finally
        {
            _conn.ExecuteNonQuery($"DROP PROCEDURE IF EXISTS `{proc}`");
        }
    }

    #region 辅助
    /// <summary>生成唯一测试标记，避免并行测试的跨度相互干扰</summary>
    /// <returns>标记文本</returns>
    private static String NewMarker() => "nl-trace-" + Guid.NewGuid().ToString("N").Substring(0, 8);

    /// <summary>跨度采集器。订阅驱动 ActivitySource，在跨度结束时快照关键数据，避免持有 Activity 引用</summary>
    private sealed class SpanCollector : IDisposable
    {
        private readonly ActivityListener _listener;
        private readonly List<SpanInfo> _spans = [];

        public SpanCollector()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "NewLife.MySql",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => { lock (_spans) _spans.Add(SpanInfo.From(activity)); }
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public SpanInfo[] Spans
        {
            get { lock (_spans) return [.. _spans]; }
        }

        public SpanInfo[] OfName(String name) => Spans.Where(e => e.Name == name).ToArray();

        /// <summary>按 SQL 文本中的唯一标记筛选 Execute 跨度</summary>
        /// <param name="marker">标记文本</param>
        /// <returns>匹配的跨度集合</returns>
        public SpanInfo[] OfQuery(String marker) => Spans
            .Where(e => e.Tags.TryGetValue("db.query.text", out var v) && v is String s && s.Contains(marker))
            .ToArray();

        /// <summary>按存储过程名筛选 Execute 跨度</summary>
        /// <param name="name">过程名</param>
        /// <returns>匹配的跨度集合</returns>
        public SpanInfo[] OfProcedure(String name) => Spans
            .Where(e => e.Tags.TryGetValue("db.stored_procedure.name", out var v) && (v as String) == name)
            .ToArray();

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>跨度快照</summary>
    /// <param name="Name">跨度名称</param>
    /// <param name="Tags">标签集合</param>
    /// <param name="Status">状态</param>
    /// <param name="Description">状态描述</param>
    /// <param name="Events">事件名称集合</param>
    private sealed record SpanInfo(String Name, Dictionary<String, Object> Tags, ActivityStatusCode Status, String Description, String[] Events)
    {
        public static SpanInfo From(Activity activity) => new(
            activity.OperationName,
            activity.TagObjects.ToDictionary(e => e.Key, e => e.Value),
            activity.Status,
            activity.StatusDescription,
            activity.Events.Select(e => e.Name).ToArray());
    }
    #endregion
}
