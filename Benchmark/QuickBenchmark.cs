using System.Diagnostics;
using System.Text;
using NewLife.MySql;

namespace Benchmark;

/// <summary>快速性能测试。Stopwatch 计时，每方案多轮取中位数</summary>
public static class QuickBenchmark
{
    private const Int32 Warmup = 1;
    private const Int32 Rounds = 3;
    private static String _connStr = null!;
    private static String _pipeConnStr = null!;

    public static async Task RunAsync(String[] args)
    {
        _connStr = Environment.GetEnvironmentVariable("MYSQL_CONNSTR")
            ?? "Server=127.0.0.1;Port=3306;Database=benchmark;User Id=root;Password=root;";
        _pipeConnStr = _connStr.TrimEnd(';') + ";Pipeline=true";

        Console.WriteLine("=== NewLife.MySql Quick Benchmark ===");

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
            conn.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS bench_users (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64) NOT NULL, age INT NOT NULL, email VARCHAR(128), score DOUBLE, created DATETIME NOT NULL) ENGINE=InnoDB");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Connect failed: " + ex.Message);
            return;
        }

        Console.WriteLine();
        var rowCounts = new[] { 100, 1000, 10000 };
        var all = new List<R>();

        foreach (var n in rowCounts)
        {
            Console.WriteLine("--- " + n + " rows ---");
            var (names, ages, emails, scores, dates) = MakeData(n);

            await RunOne(all, "SingleRow INSERT", "baseline", n, () => DoSingleRow(_connStr, n, names, ages, emails, scores, dates));
            await RunOne(all, "ExecuteBatch", "serial", n, () => DoDictBatch(_connStr, n, names, ages, emails, scores, dates));
            await RunOne(all, "ExecuteArrayBatch", "serial", n, () => DoArrayBatch(_connStr, n, names, ages, emails, scores, dates));
            await RunOne(all, "ExecuteBatch+Pipeline", "pipeline", n, () => DoDictBatch(_pipeConnStr, n, names, ages, emails, scores, dates));
            await RunOne(all, "ExecuteArrayBatch+Pipeline", "pipeline", n, () => DoArrayBatch(_pipeConnStr, n, names, ages, emails, scores, dates));
            await RunOne(all, "Multi-row VALUES", "text", n, () => DoMultiRow(_connStr, n, names, ages, emails, scores, dates));
            await RunOne(all, "DbBatch API", "text", n, () => DoDbBatch(_connStr, n, names, ages, emails, scores, dates));
            await RunOne(all, "SingleRow INSERT (tx)", "tx", n, () => DoSingleRowTx(_connStr, n, names, ages, emails, scores, dates));
            await RunOne(all, "ExecuteBatch (tx)", "tx", n, () => DoDictBatchTx(_connStr, n, names, ages, emails, scores, dates));
            await RunOne(all, "ExecuteArrayBatch (tx)", "tx", n, () => DoArrayBatchTx(_connStr, n, names, ages, emails, scores, dates));
            await RunOne(all, "ExecuteBatch+Pipeline (tx)", "tx+pipe", n, () => DoDictBatchTx(_pipeConnStr, n, names, ages, emails, scores, dates));
            await RunOne(all, "ExecuteArrayBatch+Pipeline (tx)", "tx+pipe", n, () => DoArrayBatchTx(_pipeConnStr, n, names, ages, emails, scores, dates));

            Console.WriteLine();
        }

        Console.WriteLine();
        Console.WriteLine("=== Markdown ===");
        Console.WriteLine();
        PrintTable(all, rowCounts);
        PrintSpeedup(all, rowCounts);
    }

    private static async Task RunOne(List<R> all, String name, String cat, Int32 rowCount, Func<Int32> action)
    {
        // 预热
        for (var w = 0; w < Warmup; w++)
        {
            Truncate();
            try { action(); } catch { }
        }

        // 测量
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
            Console.WriteLine("  " + name.PadRight(40) + " ERROR: " + err);
            all.Add(new R(name, cat, rowCount, -1, -1, -1, affected, err));
        }
        else
        {
            times.Sort();
            var med = times[times.Count / 2];
            var mn = times[0];
            var mx = times[^1];
            var ops = rowCount / (med / 1000.0);
            Console.WriteLine("  " + name.PadRight(40) + " median=" + med.ToString("F2").PadLeft(10) + "ms  min=" + mn.ToString("F2").PadLeft(10) + "ms  max=" + mx.ToString("F2").PadLeft(10) + "ms  rows/s=" + ops.ToString("F0").PadLeft(12) + "  n=" + affected);
            all.Add(new R(name, cat, rowCount, med, mn, mx, affected, null));
        }

        await Task.CompletedTask;
    }

    private static (String[], Int32[], String[], Double[], DateTime[]) MakeData(Int32 count)
    {
        var rnd = new Random(42);
        var names = new String[count];
        var ages = new Int32[count];
        var emails = new String[count];
        var scores = new Double[count];
        var dates = new DateTime[count];
        for (var i = 0; i < count; i++)
        {
            names[i] = "User_" + i.ToString("D6");
            ages[i] = rnd.Next(18, 65);
            emails[i] = "user" + i + "@test.com";
            scores[i] = Math.Round(rnd.NextDouble() * 100, 2);
            dates[i] = DateTime.Now.AddMinutes(-rnd.Next(0, 525600));
        }
        return (names, ages, emails, scores, dates);
    }

    private static void Truncate()
    {
        using var c = new MySqlConnection(_connStr);
        c.Open();
        c.ExecuteNonQuery("TRUNCATE TABLE bench_users");
    }

    #region Autocommit
    private static Int32 DoSingleRow(String cs, Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new MySqlConnection(cs);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = new MySqlCommand(conn, "INSERT INTO bench_users(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)");
            cmd.Parameters.AddWithValue("name", names[i]);
            cmd.Parameters.AddWithValue("age", ages[i]);
            cmd.Parameters.AddWithValue("email", emails[i]);
            cmd.Parameters.AddWithValue("score", scores[i]);
            cmd.Parameters.AddWithValue("created", dates[i]);
            total += cmd.ExecuteNonQuery();
        }
        return total;
    }

    private static Int32 DoDictBatch(String cs, Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new MySqlConnection(cs);
        conn.Open();
        using var cmd = new MySqlCommand(conn, "INSERT INTO bench_users(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)");
        cmd.Parameters.AddWithValue("name", "");
        cmd.Parameters.AddWithValue("age", 0);
        cmd.Parameters.AddWithValue("email", "");
        cmd.Parameters.AddWithValue("score", 0.0);
        cmd.Parameters.AddWithValue("created", DateTime.Now);
        var ps = new List<IDictionary<String, Object?>>(count);
        for (var i = 0; i < count; i++)
        {
            ps.Add(new Dictionary<String, Object?>
            {
                ["name"] = names[i],
                ["age"] = ages[i],
                ["email"] = emails[i],
                ["score"] = scores[i],
                ["created"] = dates[i],
            });
        }
        return cmd.ExecuteBatch(ps);
    }

    private static Int32 DoArrayBatch(String cs, Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new MySqlConnection(cs);
        conn.Open();
        using var cmd = new MySqlCommand(conn, "INSERT INTO bench_users(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)");
        cmd.Parameters.AddWithValue("name", names);
        cmd.Parameters.AddWithValue("age", ages);
        cmd.Parameters.AddWithValue("email", emails);
        cmd.Parameters.AddWithValue("score", scores);
        cmd.Parameters.AddWithValue("created", dates);
        return cmd.ExecuteArrayBatch(count);
    }

    private static Int32 DoMultiRow(String cs, Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new MySqlConnection(cs);
        conn.Open();
        var batchSize = 1000;
        var total = 0;
        for (var offset = 0; offset < count; offset += batchSize)
        {
            var n = Math.Min(batchSize, count - offset);
            var sb = new StringBuilder("INSERT INTO bench_users(name,age,email,score,created) VALUES");
            for (var i = 0; i < n; i++)
            {
                var idx = offset + i;
                if (i > 0) sb.Append(',');
                sb.Append("('").Append(names[idx]).Append("',")
                  .Append(ages[idx]).Append(",'")
                  .Append(emails[idx]).Append("',")
                  .Append(scores[idx]).Append(",'")
                  .Append(dates[idx].ToString("yyyy-MM-dd HH:mm:ss")).Append("')");
            }
            total += conn.ExecuteNonQuery(sb.ToString());
        }
        return total;
    }

    private static Int32 DoDbBatch(String cs, Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new MySqlConnection(cs);
        conn.Open();
        var batchSize = 500;
        var total = 0;
        for (var offset = 0; offset < count; offset += batchSize)
        {
            var n = Math.Min(batchSize, count - offset);
            var batch = conn.CreateBatch();
            for (var i = 0; i < n; i++)
            {
                var idx = offset + i;
                batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO bench_users(name,age,email,score,created) VALUES('" + names[idx] + "'," + ages[idx] + ",'" + emails[idx] + "'," + scores[idx] + ",'" + dates[idx].ToString("yyyy-MM-dd HH:mm:ss") + "')"));
            }
            total += batch.ExecuteNonQuery();
        }
        return total;
    }
    #endregion

    #region Transaction
    private static Int32 DoSingleRowTx(String cs, Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new MySqlConnection(cs);
        conn.Open();
        using var tr = conn.BeginTransaction();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = new MySqlCommand(conn, "INSERT INTO bench_users(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)");
            cmd.Parameters.AddWithValue("name", names[i]);
            cmd.Parameters.AddWithValue("age", ages[i]);
            cmd.Parameters.AddWithValue("email", emails[i]);
            cmd.Parameters.AddWithValue("score", scores[i]);
            cmd.Parameters.AddWithValue("created", dates[i]);
            total += cmd.ExecuteNonQuery();
        }
        tr.Commit();
        return total;
    }

    private static Int32 DoDictBatchTx(String cs, Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new MySqlConnection(cs);
        conn.Open();
        using var tr = conn.BeginTransaction();
        using var cmd = new MySqlCommand(conn, "INSERT INTO bench_users(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)");
        cmd.Parameters.AddWithValue("name", "");
        cmd.Parameters.AddWithValue("age", 0);
        cmd.Parameters.AddWithValue("email", "");
        cmd.Parameters.AddWithValue("score", 0.0);
        cmd.Parameters.AddWithValue("created", DateTime.Now);
        var ps = new List<IDictionary<String, Object?>>(count);
        for (var i = 0; i < count; i++)
        {
            ps.Add(new Dictionary<String, Object?>
            {
                ["name"] = names[i],
                ["age"] = ages[i],
                ["email"] = emails[i],
                ["score"] = scores[i],
                ["created"] = dates[i],
            });
        }
        var result = cmd.ExecuteBatch(ps);
        tr.Commit();
        return result;
    }

    private static Int32 DoArrayBatchTx(String cs, Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new MySqlConnection(cs);
        conn.Open();
        using var tr = conn.BeginTransaction();
        using var cmd = new MySqlCommand(conn, "INSERT INTO bench_users(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)");
        cmd.Parameters.AddWithValue("name", names);
        cmd.Parameters.AddWithValue("age", ages);
        cmd.Parameters.AddWithValue("email", emails);
        cmd.Parameters.AddWithValue("score", scores);
        cmd.Parameters.AddWithValue("created", dates);
        var result = cmd.ExecuteArrayBatch(count);
        tr.Commit();
        return result;
    }
    #endregion

    #region Output
    private static void PrintTable(List<R> all, Int32[] rowCounts)
    {
        Console.WriteLine("### Results (median ms)");
        Console.WriteLine();
        var sb = new StringBuilder("| Method | Category |");
        foreach (var rc in rowCounts) sb.Append(" " + rc + " rows |");
        Console.WriteLine(sb.ToString());
        sb.Clear();
        sb.Append("|------|------|");
        foreach (var _ in rowCounts) sb.Append("------:|");
        Console.WriteLine(sb.ToString());
        var ns = all.Select(r => r.Name).Distinct().ToList();
        foreach (var name in ns)
        {
            var first = all.First(r => r.Name == name);
            sb.Clear();
            sb.Append("| " + name + " | " + first.Cat + " |");
            foreach (var rc in rowCounts)
            {
                var r = all.FirstOrDefault(x => x.Name == name && x.Rows == rc);
                if (r == null || r.Err != null)
                    sb.Append(" - |");
                else
                    sb.Append(" " + r.Med.ToString("F2") + " |");
            }
            Console.WriteLine(sb.ToString());
        }
        Console.WriteLine();
    }

    private static void PrintSpeedup(List<R> all, Int32[] rowCounts)
    {
        Console.WriteLine("### Speedup vs SingleRow INSERT");
        Console.WriteLine();
        var sb = new StringBuilder("| Method |");
        foreach (var rc in rowCounts) sb.Append(" " + rc + " rows |");
        Console.WriteLine(sb.ToString());
        sb.Clear();
        sb.Append("|------|");
        foreach (var _ in rowCounts) sb.Append("------:|");
        Console.WriteLine(sb.ToString());
        var ns = all.Select(r => r.Name).Distinct().ToList();
        foreach (var name in ns)
        {
            sb.Clear();
            sb.Append("| " + name + " |");
            foreach (var rc in rowCounts)
            {
                var bl = all.FirstOrDefault(x => x.Name == "SingleRow INSERT" && x.Rows == rc);
                var cur = all.FirstOrDefault(x => x.Name == name && x.Rows == rc);
                if (bl == null || cur == null || bl.Err != null || cur.Err != null || cur.Med <= 0)
                    sb.Append(" - |");
                else
                    sb.Append(" " + (bl.Med / cur.Med).ToString("F1") + "x |");
            }
            Console.WriteLine(sb.ToString());
        }
        Console.WriteLine();
    }
    #endregion

    private record R(String Name, String Cat, Int32 Rows, Double Med, Double Min, Double Max, Int32 Affected, String? Err);
}
