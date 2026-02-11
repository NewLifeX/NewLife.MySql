using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Order;
using NewLife.MySql;

namespace Benchmark;

/// <summary>批量 INSERT 性能基准测试。
/// 对比 NewLife.MySql 的五种批量操作方案在不同数据量级下的性能差异。</summary>
/// <remarks>
/// 测试前须确保 MySQL 可访问，并已创建测试表：
/// <code>
/// CREATE TABLE IF NOT EXISTS bench_users (
///     id BIGINT AUTO_INCREMENT PRIMARY KEY,
///     name VARCHAR(64) NOT NULL,
///     age INT NOT NULL,
///     email VARCHAR(128),
///     score DOUBLE,
///     created DATETIME NOT NULL
/// ) ENGINE=InnoDB;
/// </code>
/// 
/// 连接字符串通过环境变量 MYSQL_CONNSTR 配置，默认：
/// Server=localhost;Port=3306;Database=benchmark;User Id=root;Password=root;
/// </remarks>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[Config(typeof(StyleConfig))]
public class BatchInsertBenchmark
{
    /// <summary>数据量级：100 / 1000 / 10000</summary>
    [Params(100, 1000, 10000)]
    public Int32 RowCount { get; set; }

    private String _connStr = null!;
    private String _pipelineConnStr = null!;
    private String[] _names = null!;
    private Int32[] _ages = null!;
    private String[] _emails = null!;
    private Double[] _scores = null!;
    private DateTime[] _createds = null!;

    [GlobalSetup]
    public void Setup()
    {
        _connStr = Environment.GetEnvironmentVariable("MYSQL_CONNSTR")
            ?? "Server=localhost;Port=3306;Database=benchmark;User Id=root;Password=root;";
        _pipelineConnStr = _connStr + ";Pipeline=true";

        // 准备测试数据
        var rnd = new Random(42);
        _names = new String[RowCount];
        _ages = new Int32[RowCount];
        _emails = new String[RowCount];
        _scores = new Double[RowCount];
        _createds = new DateTime[RowCount];

        for (var i = 0; i < RowCount; i++)
        {
            _names[i] = $"User_{i:D6}";
            _ages[i] = rnd.Next(18, 65);
            _emails[i] = $"user{i}@test.com";
            _scores[i] = Math.Round(rnd.NextDouble() * 100, 2);
            _createds[i] = DateTime.Now.AddMinutes(-rnd.Next(0, 525600));
        }

        // 确保测试表存在
        using var conn = new MySqlConnection(_connStr);
        conn.Open();
        conn.ExecuteNonQuery("""
            CREATE TABLE IF NOT EXISTS bench_users (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(64) NOT NULL,
                age INT NOT NULL,
                email VARCHAR(128),
                score DOUBLE,
                created DATETIME NOT NULL
            ) ENGINE=InnoDB
            """);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // 每次迭代前清空表
        using var conn = new MySqlConnection(_connStr);
        conn.Open();
        conn.ExecuteNonQuery("TRUNCATE TABLE bench_users");
    }

    /// <summary>方案 A：字典参数集 ExecuteBatch（串行模式）</summary>
    [Benchmark(Description = "ExecuteBatch（字典参数集）")]
    [BenchmarkCategory("串行")]
    public Int32 DictBatch()
    {
        using var conn = new MySqlConnection(_connStr);
        conn.Open();

        using var cmd = new MySqlCommand(conn, "INSERT INTO bench_users(name, age, email, score, created) VALUES(@name, @age, @email, @score, @created)");
        cmd.Parameters.AddWithValue("name", "");
        cmd.Parameters.AddWithValue("age", 0);
        cmd.Parameters.AddWithValue("email", "");
        cmd.Parameters.AddWithValue("score", 0.0);
        cmd.Parameters.AddWithValue("created", DateTime.Now);

        var paramSets = new List<IDictionary<String, Object?>>(RowCount);
        for (var i = 0; i < RowCount; i++)
        {
            paramSets.Add(new Dictionary<String, Object?>
            {
                ["name"] = _names[i],
                ["age"] = _ages[i],
                ["email"] = _emails[i],
                ["score"] = _scores[i],
                ["created"] = _createds[i],
            });
        }

        return cmd.ExecuteBatch(paramSets);
    }

    /// <summary>方案 B：数组绑定 ExecuteArrayBatch（串行模式）</summary>
    [Benchmark(Description = "ExecuteArrayBatch（数组绑定）")]
    [BenchmarkCategory("串行")]
    public Int32 ArrayBatch()
    {
        using var conn = new MySqlConnection(_connStr);
        conn.Open();

        using var cmd = new MySqlCommand(conn, "INSERT INTO bench_users(name, age, email, score, created) VALUES(@name, @age, @email, @score, @created)");
        cmd.Parameters.AddWithValue("name", _names);
        cmd.Parameters.AddWithValue("age", _ages);
        cmd.Parameters.AddWithValue("email", _emails);
        cmd.Parameters.AddWithValue("score", _scores);
        cmd.Parameters.AddWithValue("created", _createds);

        return cmd.ExecuteArrayBatch(RowCount);
    }

    /// <summary>方案 C-1：字典参数集 + 管道化 Pipeline</summary>
    [Benchmark(Description = "ExecuteBatch + Pipeline（字典+管道化）")]
    [BenchmarkCategory("管道化")]
    public Int32 DictBatchPipeline()
    {
        using var conn = new MySqlConnection(_pipelineConnStr);
        conn.Open();

        using var cmd = new MySqlCommand(conn, "INSERT INTO bench_users(name, age, email, score, created) VALUES(@name, @age, @email, @score, @created)");
        cmd.Parameters.AddWithValue("name", "");
        cmd.Parameters.AddWithValue("age", 0);
        cmd.Parameters.AddWithValue("email", "");
        cmd.Parameters.AddWithValue("score", 0.0);
        cmd.Parameters.AddWithValue("created", DateTime.Now);

        var paramSets = new List<IDictionary<String, Object?>>(RowCount);
        for (var i = 0; i < RowCount; i++)
        {
            paramSets.Add(new Dictionary<String, Object?>
            {
                ["name"] = _names[i],
                ["age"] = _ages[i],
                ["email"] = _emails[i],
                ["score"] = _scores[i],
                ["created"] = _createds[i],
            });
        }

        return cmd.ExecuteBatch(paramSets);
    }

    /// <summary>方案 C-2：数组绑定 + 管道化 Pipeline</summary>
    [Benchmark(Description = "ExecuteArrayBatch + Pipeline（数组+管道化）")]
    [BenchmarkCategory("管道化")]
    public Int32 ArrayBatchPipeline()
    {
        using var conn = new MySqlConnection(_pipelineConnStr);
        conn.Open();

        using var cmd = new MySqlCommand(conn, "INSERT INTO bench_users(name, age, email, score, created) VALUES(@name, @age, @email, @score, @created)");
        cmd.Parameters.AddWithValue("name", _names);
        cmd.Parameters.AddWithValue("age", _ages);
        cmd.Parameters.AddWithValue("email", _emails);
        cmd.Parameters.AddWithValue("score", _scores);
        cmd.Parameters.AddWithValue("created", _createds);

        return cmd.ExecuteArrayBatch(RowCount);
    }

    /// <summary>方案 D：多行 INSERT VALUES（文本协议）</summary>
    [Benchmark(Description = "多行 INSERT VALUES（文本拼接）")]
    [BenchmarkCategory("文本")]
    public Int32 MultiRowValues()
    {
        using var conn = new MySqlConnection(_connStr);
        conn.Open();

        // 分批发送，每批最多 1000 行，避免超过 max_allowed_packet
        var batchSize = 1000;
        var totalAffected = 0;

        for (var offset = 0; offset < RowCount; offset += batchSize)
        {
            var count = Math.Min(batchSize, RowCount - offset);
            var sb = new StringBuilder();
            sb.Append("INSERT INTO bench_users(name, age, email, score, created) VALUES");

            for (var i = 0; i < count; i++)
            {
                var idx = offset + i;
                if (i > 0) sb.Append(',');
                sb.Append($"('{_names[idx]}',{_ages[idx]},'{_emails[idx]}',{_scores[idx]},'{_createds[idx]:yyyy-MM-dd HH:mm:ss}')");
            }

            totalAffected += conn.ExecuteNonQuery(sb.ToString());
        }

        return totalAffected;
    }

    /// <summary>方案 E：逐行 INSERT（基线对照）</summary>
    [Benchmark(Baseline = true, Description = "逐行 INSERT（基线）")]
    [BenchmarkCategory("基线")]
    public Int32 SingleRowInsert()
    {
        using var conn = new MySqlConnection(_connStr);
        conn.Open();

        var totalAffected = 0;
        for (var i = 0; i < RowCount; i++)
        {
            using var cmd = new MySqlCommand(conn, "INSERT INTO bench_users(name, age, email, score, created) VALUES(@name, @age, @email, @score, @created)");
            cmd.Parameters.AddWithValue("name", _names[i]);
            cmd.Parameters.AddWithValue("age", _ages[i]);
            cmd.Parameters.AddWithValue("email", _emails[i]);
            cmd.Parameters.AddWithValue("score", _scores[i]);
            cmd.Parameters.AddWithValue("created", _createds[i]);
            totalAffected += cmd.ExecuteNonQuery();
        }

        return totalAffected;
    }

    /// <summary>方案 F：DbBatch API（.NET 6+）</summary>
    [Benchmark(Description = "DbBatch API（.NET 6+ 标准）")]
    [BenchmarkCategory("文本")]
    public Int32 DbBatchInsert()
    {
        using var conn = new MySqlConnection(_connStr);
        conn.Open();

        // DbBatch 内部合并为多语句，分批避免过长
        var batchSize = 500;
        var totalAffected = 0;

        for (var offset = 0; offset < RowCount; offset += batchSize)
        {
            var count = Math.Min(batchSize, RowCount - offset);
            var batch = conn.CreateBatch();

            for (var i = 0; i < count; i++)
            {
                var idx = offset + i;
                var batchCmd = new MySqlBatchCommand(
                    $"INSERT INTO bench_users(name, age, email, score, created) VALUES('{_names[idx]}',{_ages[idx]},'{_emails[idx]}',{_scores[idx]},'{_createds[idx]:yyyy-MM-dd HH:mm:ss}')");
                batch.BatchCommands.Add(batchCmd);
            }

            totalAffected += batch.ExecuteNonQuery();
        }

        return totalAffected;
    }

    /// <summary>自定义样式配置</summary>
    private class StyleConfig : ManualConfig
    {
        public StyleConfig()
        {
            AddColumn(StatisticColumn.OperationsPerSecond);
        }
    }
}
