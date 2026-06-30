using System.Data;
using System.Diagnostics;
using System.Text;
using NewLife.MySql;
using NL = NewLife.MySql;

namespace Benchmark;

/// <summary>新功能专项性能测试。覆盖压缩协议、MySqlBulkCopy、连接池压力、参数化查询开销</summary>
/// <remarks>
/// 运行方式：dotnet run -c Release --project Benchmark/Benchmark.csproj -- --feature
/// 连接字符串通过环境变量 MYSQL_CONNSTR 配置。
/// </remarks>
public static class FeatureBenchmark
{
    private const Int32 Warmup = 1;
    private const Int32 Rounds = 3;
    private static String _connStr = null!;

    public static async Task RunAsync(String[] args)
    {
        _connStr = Environment.GetEnvironmentVariable("MYSQL_CONNSTR")
            ?? "Server=127.0.0.1;Port=3306;Database=benchmark;User Id=root;Password=root;";

        Console.WriteLine("=== NewLife.MySql Feature Benchmark ===");
        Console.WriteLine();

        // 确保数据库和表存在
        try
        {
            var builder = new MySqlConnectionStringBuilder(_connStr);
            var dbName = builder.Database;
            builder.Database = "mysql";
            using (var c = new MySqlConnection(builder.ConnectionString))
            {
                c.Open();
                c.ExecuteNonQuery("CREATE DATABASE IF NOT EXISTS `" + dbName + "`");
            }

            using var conn = new MySqlConnection(_connStr);
            conn.Open();
            Console.WriteLine("MySQL: " + conn.ServerVersion);
            Console.WriteLine("DatabaseType: " + conn.DatabaseType);
            conn.ExecuteNonQuery("DROP TABLE IF EXISTS bench_feature");
            conn.ExecuteNonQuery("""
                CREATE TABLE bench_feature (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(64) NOT NULL,
                    age INT NOT NULL,
                    email VARCHAR(128),
                    score DOUBLE,
                    data LONGBLOB,
                    created DATETIME NOT NULL,
                    INDEX idx_age(age)
                ) ENGINE=InnoDB
                """);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Connect failed: " + ex.Message);
            return;
        }

        Console.WriteLine();

        // ========== 1. 压缩协议对比 ==========
        await BenchmarkCompressionAsync();

        // ========== 2. MySqlBulkCopy 大批量导入 ==========
        await BenchmarkBulkCopyAsync();

        // ========== 3. 连接池压力测试 ==========
        await BenchmarkConnectionPoolAsync();

        // ========== 4. 参数化查询 vs 文本拼接 ==========
        await BenchmarkParameterizationAsync();

        // ========== 5. DataReader 读取性能 ==========
        await BenchmarkDataReaderAsync();
    }

    #region 压缩协议
    private static async Task BenchmarkCompressionAsync()
    {
        Console.WriteLine("## 1. 压缩协议对比（10,000 行批量 INSERT）");
        Console.WriteLine();

        var rowCount = 10000;
        var (names, ages, emails, scores, dates, blobs) = MakeFeatureData(rowCount);
        var compressedCs = _connStr.TrimEnd(';') + ";UseCompression=true;Pipeline=true";

        var all = new List<BenchmarkResult>();

        // 无压缩 + Pipeline + 事务
        await RunBenchmark(all, "Pipeline(tx) 无压缩", rowCount,
            () => DoPipelineInsert(_connStr, rowCount, names, ages, emails, scores, dates));

        // 压缩 + Pipeline + 事务
        await RunBenchmark(all, "Pipeline(tx) 压缩", rowCount,
            () => DoPipelineInsert(compressedCs, rowCount, names, ages, emails, scores, dates));

        // 压缩 + Pipeline + 事务 + BLOB 数据
        await RunBenchmark(all, "Pipeline(tx) 压缩+BLOB", rowCount,
            () => DoPipelineInsertWithBlob(compressedCs, rowCount, names, ages, emails, scores, dates, blobs));

        PrintResults(all, "压缩协议对比");
        Console.WriteLine();
    }
    #endregion

    #region MySqlBulkCopy
    private static async Task BenchmarkBulkCopyAsync()
    {
        Console.WriteLine("## 2. MySqlBulkCopy 大批量导入");
        Console.WriteLine();

        var rowCounts = new[] { 1000, 10000, 100000 };
        var all = new List<BenchmarkResult>();

        foreach (var n in rowCounts)
        {
            Console.WriteLine($"--- {n} 行 ---");
            var (names, ages, emails, scores, dates, _) = MakeFeatureData(n);

            // Pipeline(tx) 作为对比基准
            await RunBenchmark(all, $"Pipeline(tx) {n}行", n,
                () => DoPipelineInsert(_connStr, n, names, ages, emails, scores, dates));

            // MySqlBulkCopy
            await RunBenchmark(all, $"BulkCopy {n}行", n,
                () => DoBulkCopyInsert(n, names, ages, emails, scores, dates));

            Console.WriteLine();
        }

        PrintResults(all, "MySqlBulkCopy vs Pipeline");
        Console.WriteLine();
    }
    #endregion

    #region 连接池
    private static async Task BenchmarkConnectionPoolAsync()
    {
        Console.WriteLine("## 3. 连接池压力测试（100 并发 × 10 次查询）");
        Console.WriteLine();

        var all = new List<BenchmarkResult>();

        // 预热连接池
        using (var c = new MySqlConnection(_connStr)) { c.Open(); }

        // 单线程顺序
        await RunBenchmark(all, "单线程 1000 次查询", 1000,
            () => DoSequentialQueries(1000));

        // 10 并发
        await RunBenchmark(all, "10 并发 × 100 次查询", 1000,
            () => DoConcurrentQueries(10, 100));

        // 50 并发
        await RunBenchmark(all, "50 并发 × 20 次查询", 1000,
            () => DoConcurrentQueries(50, 20));

        PrintResults(all, "连接池压力测试");
        Console.WriteLine();
    }
    #endregion

    #region 参数化查询
    private static async Task BenchmarkParameterizationAsync()
    {
        Console.WriteLine("## 4. 参数化查询 vs 文本拼接（10,000 次 SELECT）");
        Console.WriteLine();

        var rowCount = 10000;
        var all = new List<BenchmarkResult>();

        // 参数化查询
        await RunBenchmark(all, "参数化 SELECT(客户端替换)", rowCount,
            () => DoParameterizedSelect(rowCount));

        // 纯文本 SQL
        await RunBenchmark(all, "纯文本 SQL", rowCount,
            () => DoTextSelect(rowCount));

        PrintResults(all, "参数化查询开销");
        Console.WriteLine();
    }
    #endregion

    #region DataReader
    private static async Task BenchmarkDataReaderAsync()
    {
        Console.WriteLine("## 5. DataReader 读取性能（10,000 行 × 6 列）");
        Console.WriteLine();

        var rowCount = 10000;
        var all = new List<BenchmarkResult>();

        // 按索引读取
        await RunBenchmark(all, "GetByIndex（序号访问）", rowCount,
            () => DoReaderByIndex(rowCount));

        // 按名称读取
        await RunBenchmark(all, "GetByName（名称访问）", rowCount,
            () => DoReaderByName(rowCount));

        // 强类型读取
        await RunBenchmark(all, "GetInt64/String 等强类型", rowCount,
            () => DoReaderTyped(rowCount));

        PrintResults(all, "DataReader 读取方式");
        Console.WriteLine();
    }
    #endregion

    #region 实现
    private static Int32 DoPipelineInsert(String cs, Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new MySqlConnection(cs);
        conn.Open();
        using var tr = conn.BeginTransaction();
        using var cmd = new MySqlCommand(conn, "INSERT INTO bench_feature(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)");
        cmd.Parameters.AddWithValue("name", names);
        cmd.Parameters.AddWithValue("age", ages);
        cmd.Parameters.AddWithValue("email", emails);
        cmd.Parameters.AddWithValue("score", scores);
        cmd.Parameters.AddWithValue("created", dates);
        var result = cmd.ExecuteArrayBatch(count);
        tr.Commit();
        return result;
    }

    private static Int32 DoPipelineInsertWithBlob(String cs, Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates, Byte[][] blobs)
    {
        using var conn = new MySqlConnection(cs);
        conn.Open();
        using var tr = conn.BeginTransaction();
        using var cmd = new MySqlCommand(conn, "INSERT INTO bench_feature(name,age,email,score,data,created) VALUES(@name,@age,@email,@score,@data,@created)");
        cmd.Parameters.AddWithValue("name", names);
        cmd.Parameters.AddWithValue("age", ages);
        cmd.Parameters.AddWithValue("email", emails);
        cmd.Parameters.AddWithValue("score", scores);
        cmd.Parameters.AddWithValue("data", blobs);
        cmd.Parameters.AddWithValue("created", dates);
        var result = cmd.ExecuteArrayBatch(count);
        tr.Commit();
        return result;
    }

    private static Int32 DoBulkCopyInsert(Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new MySqlConnection(_connStr);
        conn.Open();

        // 构造 DataTable
        var dt = new DataTable("bench_feature");
        dt.Columns.Add("name", typeof(String));
        dt.Columns.Add("age", typeof(Int32));
        dt.Columns.Add("email", typeof(String));
        dt.Columns.Add("score", typeof(Double));
        dt.Columns.Add("created", typeof(DateTime));

        for (var i = 0; i < count; i++)
        {
            dt.Rows.Add(names[i], ages[i], emails[i], scores[i], dates[i]);
        }

        var bulkCopy = new MySqlBulkCopy((MySqlConnection)conn)
        {
            DestinationTableName = "bench_feature",
            BatchSize = 1000,
        };
        bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping("name", "name"));
        bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping("age", "age"));
        bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping("email", "email"));
        bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping("score", "score"));
        bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping("created", "created"));
        bulkCopy.WriteToServer(dt);

        return count;
    }

    private static Int32 DoSequentialQueries(Int32 count)
    {
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var conn = new MySqlConnection(_connStr);
            conn.Open();
            using var cmd = new MySqlCommand(conn, "SELECT 1");
            total += (Int32)(Int64)cmd.ExecuteScalar()!;
        }
        return total;
    }

    private static Int32 DoConcurrentQueries(Int32 concurrency, Int32 perThread)
    {
        var total = 0;
        var tasks = new Task<Int32>[concurrency];
        for (var t = 0; t < concurrency; t++)
        {
            tasks[t] = Task.Run(() =>
            {
                var local = 0;
                for (var i = 0; i < perThread; i++)
                {
                    using var conn = new MySqlConnection(_connStr);
                    conn.Open();
                    using var cmd = new MySqlCommand(conn, "SELECT 1");
                    local += (Int32)(Int64)cmd.ExecuteScalar()!;
                }
                return local;
            });
        }
        Task.WaitAll(tasks);
        foreach (var t in tasks)
            total += t.Result;
        return total;
    }

    private static Int32 DoParameterizedSelect(Int32 count)
    {
        using var conn = new MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = new MySqlCommand(conn, "SELECT id,name,age,email,score,created FROM bench_feature WHERE id=@id");
            cmd.Parameters.AddWithValue("id", (Int64)(i + 1));
            using var reader = cmd.ExecuteReader();
            if (reader.Read()) total++;
        }
        return total;
    }

    private static Int32 DoTextSelect(Int32 count)
    {
        using var conn = new MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = new MySqlCommand(conn, $"SELECT id,name,age,email,score,created FROM bench_feature WHERE id={i + 1}");
            using var reader = cmd.ExecuteReader();
            if (reader.Read()) total++;
        }
        return total;
    }

    private static Int32 DoReaderByIndex(Int32 count)
    {
        using var conn = new MySqlConnection(_connStr);
        conn.Open();
        using var cmd = new MySqlCommand(conn, $"SELECT id,name,age,email,score,created FROM bench_feature LIMIT {count}");
        using var reader = cmd.ExecuteReader();
        var total = 0;
        while (reader.Read())
        {
            _ = reader[0]; _ = reader[1]; _ = reader[2];
            _ = reader[3]; _ = reader[4]; _ = reader[5];
            total++;
        }
        return total;
    }

    private static Int32 DoReaderByName(Int32 count)
    {
        using var conn = new MySqlConnection(_connStr);
        conn.Open();
        using var cmd = new MySqlCommand(conn, $"SELECT id,name,age,email,score,created FROM bench_feature LIMIT {count}");
        using var reader = cmd.ExecuteReader();
        var total = 0;
        while (reader.Read())
        {
            _ = reader["id"]; _ = reader["name"]; _ = reader["age"];
            _ = reader["email"]; _ = reader["score"]; _ = reader["created"];
            total++;
        }
        return total;
    }

    private static Int32 DoReaderTyped(Int32 count)
    {
        using var conn = new MySqlConnection(_connStr);
        conn.Open();
        using var cmd = new MySqlCommand(conn, $"SELECT id,name,age,email,score,created FROM bench_feature LIMIT {count}");
        using var reader = cmd.ExecuteReader();
        var total = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetInt32(2);
            _ = reader.GetString(3);
            _ = reader.GetDouble(4);
            _ = reader.GetDateTime(5);
            total++;
        }
        return total;
    }
    #endregion

    #region 辅助
    private static void Truncate()
    {
        using var c = new MySqlConnection(_connStr);
        c.Open();
        c.ExecuteNonQuery("TRUNCATE TABLE bench_feature");
    }

    private static (String[], Int32[], String[], Double[], DateTime[], Byte[][]) MakeFeatureData(Int32 count)
    {
        var rnd = new Random(42);
        var names = new String[count];
        var ages = new Int32[count];
        var emails = new String[count];
        var scores = new Double[count];
        var dates = new DateTime[count];
        var blobs = new Byte[count][];
        var blob = new Byte[1024]; // 1KB BLOB
        rnd.NextBytes(blob);

        for (var i = 0; i < count; i++)
        {
            names[i] = $"User_{i:D6}";
            ages[i] = rnd.Next(18, 65);
            emails[i] = $"user{i}@test.com";
            scores[i] = Math.Round(rnd.NextDouble() * 100, 2);
            dates[i] = DateTime.Now.AddMinutes(-rnd.Next(0, 525600));
            blobs[i] = blob;
        }
        return (names, ages, emails, scores, dates, blobs);
    }

    private static async Task RunBenchmark(List<BenchmarkResult> all, String name, Int32 rowCount, Func<Int32> action)
    {
        // 预热
        for (var w = 0; w < Warmup; w++)
        {
            Truncate();
            try { action(); } catch { }
        }

        var times = new List<Double>();
        var affected = 0;
        String? err = null;
        for (var m = 0; m < Rounds; m++)
        {
            Truncate();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var sw = Stopwatch.StartNew();
            try
            {
                affected = action();
                sw.Stop();
                times.Add(sw.Elapsed.TotalMilliseconds);
            }
            catch (Exception ex)
            {
                sw.Stop();
                err = ex.Message;
                break;
            }
        }

        if (err != null)
        {
            Console.WriteLine($"  {name,-40} ERROR: {err}");
            all.Add(new BenchmarkResult(name, rowCount, -1, -1, -1, affected, err));
        }
        else
        {
            times.Sort();
            var med = times[times.Count / 2];
            var mn = times[0];
            var mx = times[^1];
            var ops = rowCount / (med / 1000.0);
            Console.WriteLine($"  {name,-40} median={med,10:F2}ms  min={mn,10:F2}ms  max={mx,10:F2}ms  rows/s={ops,12:F0}  n={affected}");
            all.Add(new BenchmarkResult(name, rowCount, med, mn, mx, affected, null));
        }
    }

    private static void PrintResults(List<BenchmarkResult> all, String title)
    {
        Console.WriteLine($"### {title}");
        Console.WriteLine();
        Console.WriteLine("| 方案 | 行数 | 中位数(ms) | 最小值(ms) | 最大值(ms) | 行/s |");
        Console.WriteLine("|------|------|------:|------:|------:|------:|");
        foreach (var r in all)
        {
            if (r.Err != null)
                Console.WriteLine($"| {r.Name} | {r.Rows} | ERROR: {r.Err} | - | - | - |");
            else
                Console.WriteLine($"| {r.Name} | {r.Rows} | {r.Med:F2} | {r.Min:F2} | {r.Max:F2} | {(r.Rows / (r.Med / 1000.0)):F0} |");
        }
        Console.WriteLine();
    }
    #endregion
}

record BenchmarkResult(String Name, Int32 Rows, Double Med, Double Min, Double Max, Int32 Affected, String? Err);
