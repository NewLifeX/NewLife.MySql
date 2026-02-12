using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using NL = NewLife.MySql;
using Official = MySql.Data.MySqlClient;
using Connector = MySqlConnector;

namespace Benchmark;

/// <summary>三驱动对比性能测试。
/// 对比 NewLife.MySql、MySql.Data（Oracle官方）、MySqlConnector 在增删改查四种操作中的性能差异。
/// 每种操作测试两种模式：逐行操作 和 参数化管道/批量操作。</summary>
public static class DriverBenchmark
{
    private const Int32 Warmup = 1;
    private const Int32 Rounds = 3;
    private static String _connStr = null!;

    public static async Task RunAsync(String[] args)
    {
        _connStr = Environment.GetEnvironmentVariable("MYSQL_CONNSTR")
            ?? "Server=127.0.0.1;Port=3306;Database=benchmark;User Id=root;Password=root;";

        Console.WriteLine("=== Driver Comparison Benchmark ===");

        // 确保数据库和表存在
        try
        {
            var builder = new NL.MySqlConnectionStringBuilder(_connStr);
            var dbName = builder.Database;
            builder.Database = "mysql";
            using (var c = new NL.MySqlConnection(builder.ConnectionString))
            {
                c.Open();
                c.ExecuteNonQuery("CREATE DATABASE IF NOT EXISTS `" + dbName + "`");
            }

            using var conn = new NL.MySqlConnection(_connStr);
            conn.Open();
            Console.WriteLine("MySQL: " + conn.ServerVersion);
            conn.ExecuteNonQuery("DROP TABLE IF EXISTS bench_driver");
            conn.ExecuteNonQuery("CREATE TABLE bench_driver (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64) NOT NULL, age INT NOT NULL, email VARCHAR(128), score DOUBLE, created DATETIME NOT NULL) ENGINE=InnoDB");
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
            Console.WriteLine("======== " + n + " rows ========");
            var (names, ages, emails, scores, dates) = MakeData(n);

            // --- INSERT ---
            Console.WriteLine("--- INSERT ---");
            await RunOne(all, "INSERT", "NewLife SingleRow", n, () => NL_SingleInsert(n, names, ages, emails, scores, dates));
            await RunOne(all, "INSERT", "NewLife Pipeline(tx)", n, () => NL_PipelineInsert(n, names, ages, emails, scores, dates));
            await RunOne(all, "INSERT", "Official SingleRow", n, () => Off_SingleInsert(n, names, ages, emails, scores, dates));
            await RunOne(all, "INSERT", "Official Batch(tx)", n, () => Off_BatchInsert(n, names, ages, emails, scores, dates));
            await RunOne(all, "INSERT", "Connector SingleRow", n, () => Conn_SingleInsert(n, names, ages, emails, scores, dates));
            await RunOne(all, "INSERT", "Connector Batch(tx)", n, () => Conn_BatchInsert(n, names, ages, emails, scores, dates));

            // --- SELECT ---
            Console.WriteLine("--- SELECT ---");
            SeedData(n);
            await RunOne(all, "SELECT", "NewLife SingleRow", n, () => NL_SingleSelect(n), () => SeedData(n));
            await RunOne(all, "SELECT", "Official SingleRow", n, () => Off_SingleSelect(n), () => SeedData(n));
            await RunOne(all, "SELECT", "Connector SingleRow", n, () => Conn_SingleSelect(n), () => SeedData(n));

            // --- UPDATE ---
            Console.WriteLine("--- UPDATE ---");
            var newAges = MakeUpdateData(n);
            await RunOne(all, "UPDATE", "NewLife SingleRow", n, () => NL_SingleUpdate(n, newAges), () => SeedData(n));
            await RunOne(all, "UPDATE", "NewLife Pipeline(tx)", n, () => NL_PipelineUpdate(n, newAges), () => SeedData(n));
            await RunOne(all, "UPDATE", "Official SingleRow", n, () => Off_SingleUpdate(n, newAges), () => SeedData(n));
            await RunOne(all, "UPDATE", "Official Batch(tx)", n, () => Off_BatchUpdate(n, newAges), () => SeedData(n));
            await RunOne(all, "UPDATE", "Connector SingleRow", n, () => Conn_SingleUpdate(n, newAges), () => SeedData(n));
            await RunOne(all, "UPDATE", "Connector Batch(tx)", n, () => Conn_BatchUpdate(n, newAges), () => SeedData(n));

            // --- DELETE ---
            Console.WriteLine("--- DELETE ---");
            await RunOne(all, "DELETE", "NewLife SingleRow", n, () => NL_SingleDelete(n), () => SeedData(n));
            await RunOne(all, "DELETE", "NewLife Pipeline(tx)", n, () => NL_PipelineDelete(n), () => SeedData(n));
            await RunOne(all, "DELETE", "Official SingleRow", n, () => Off_SingleDelete(n), () => SeedData(n));
            await RunOne(all, "DELETE", "Official Batch(tx)", n, () => Off_BatchDelete(n), () => SeedData(n));
            await RunOne(all, "DELETE", "Connector SingleRow", n, () => Conn_SingleDelete(n), () => SeedData(n));
            await RunOne(all, "DELETE", "Connector Batch(tx)", n, () => Conn_BatchDelete(n), () => SeedData(n));

            Console.WriteLine();
        }

        // 输出 Markdown
        Console.WriteLine();
        Console.WriteLine("=== Markdown ===");
        Console.WriteLine();
        foreach (var op in new[] { "INSERT", "SELECT", "UPDATE", "DELETE" })
        {
            PrintDriverTable(all, rowCounts, op);
        }
    }

    #region 辅助
    private static async Task RunOne(List<R> all, String op, String driver, Int32 rowCount, Func<Int32> action, Action? setup = null)
    {
        // 预热
        for (var w = 0; w < Warmup; w++)
        {
            if (setup != null) setup(); else Truncate();
            try { action(); } catch { }
        }

        var times = new List<Double>();
        var affected = 0;
        String? err = null;
        for (var m = 0; m < Rounds; m++)
        {
            if (setup != null) setup(); else Truncate();
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
            Console.WriteLine("  " + (driver + " " + op).PadRight(40) + " ERROR: " + err);
            all.Add(new R(op, driver, rowCount, -1, -1, -1, affected, err));
        }
        else
        {
            times.Sort();
            var med = times[times.Count / 2];
            var mn = times[0];
            var mx = times[^1];
            var ops = rowCount / (med / 1000.0);
            Console.WriteLine("  " + driver.PadRight(30) + " median=" + med.ToString("F2").PadLeft(10) + "ms  min=" + mn.ToString("F2").PadLeft(10) + "ms  max=" + mx.ToString("F2").PadLeft(10) + "ms  rows/s=" + ops.ToString("F0").PadLeft(10) + "  n=" + affected);
            all.Add(new R(op, driver, rowCount, med, mn, mx, affected, null));
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

    private static Int32[] MakeUpdateData(Int32 count)
    {
        var rnd = new Random(99);
        var ages = new Int32[count];
        for (var i = 0; i < count; i++)
            ages[i] = rnd.Next(18, 80);
        return ages;
    }

    private static void Truncate()
    {
        using var c = new NL.MySqlConnection(_connStr);
        c.Open();
        c.ExecuteNonQuery("TRUNCATE TABLE bench_driver");
    }

    private static void SeedData(Int32 count)
    {
        using var conn = new NL.MySqlConnection(_connStr);
        conn.Open();
        conn.ExecuteNonQuery("TRUNCATE TABLE bench_driver");
        var batchSize = 1000;
        for (var offset = 0; offset < count; offset += batchSize)
        {
            var n = Math.Min(batchSize, count - offset);
            var sb = new StringBuilder("INSERT INTO bench_driver(name,age,email,score,created) VALUES");
            for (var i = 0; i < n; i++)
            {
                var id = offset + i;
                if (i > 0) sb.Append(',');
                sb.Append("('User_").Append(id.ToString("D6")).Append("',25,'user").Append(id).Append("@test.com',50.0,'").Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")).Append("')");
            }
            conn.ExecuteNonQuery(sb.ToString());
        }
    }

    private static void PrintDriverTable(List<R> all, Int32[] rowCounts, String op)
    {
        var items = all.Where(r => r.Op == op).ToList();
        if (items.Count == 0) return;

        Console.WriteLine("### " + op);
        Console.WriteLine();
        var sb = new StringBuilder("| Driver |");
        foreach (var rc in rowCounts) sb.Append(" " + rc + " rows |");
        Console.WriteLine(sb.ToString());
        sb.Clear();
        sb.Append("|------|");
        foreach (var _ in rowCounts) sb.Append("------:|");
        Console.WriteLine(sb.ToString());

        var drivers = items.Select(r => r.Driver).Distinct().ToList();
        foreach (var drv in drivers)
        {
            sb.Clear();
            sb.Append("| " + drv + " |");
            foreach (var rc in rowCounts)
            {
                var r = items.FirstOrDefault(x => x.Driver == drv && x.Rows == rc);
                if (r == null || r.Err != null)
                    sb.Append(" - |");
                else
                    sb.Append(" " + r.Med.ToString("F2") + " |");
            }
            Console.WriteLine(sb.ToString());
        }
        Console.WriteLine();
    }
    #endregion

    #region NewLife.MySql
    private static Int32 NL_SingleInsert(Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new NL.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = new NL.MySqlCommand(conn, "INSERT INTO bench_driver(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)");
            cmd.Parameters.AddWithValue("name", names[i]);
            cmd.Parameters.AddWithValue("age", ages[i]);
            cmd.Parameters.AddWithValue("email", emails[i]);
            cmd.Parameters.AddWithValue("score", scores[i]);
            cmd.Parameters.AddWithValue("created", dates[i]);
            total += cmd.ExecuteNonQuery();
        }
        return total;
    }

    private static Int32 NL_PipelineInsert(Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        var pipeCs = _connStr.TrimEnd(';') + ";Pipeline=true";
        using var conn = new NL.MySqlConnection(pipeCs);
        conn.Open();
        using var tr = conn.BeginTransaction();
        using var cmd = new NL.MySqlCommand(conn, "INSERT INTO bench_driver(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)");
        cmd.Parameters.AddWithValue("name", names);
        cmd.Parameters.AddWithValue("age", ages);
        cmd.Parameters.AddWithValue("email", emails);
        cmd.Parameters.AddWithValue("score", scores);
        cmd.Parameters.AddWithValue("created", dates);
        var result = cmd.ExecuteArrayBatch(count);
        tr.Commit();
        return result;
    }

    private static Int32 NL_SingleSelect(Int32 count)
    {
        using var conn = new NL.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = new NL.MySqlCommand(conn, "SELECT id,name,age,email,score,created FROM bench_driver WHERE id=@id");
            cmd.Parameters.AddWithValue("id", (Int64)(i + 1));
            using var reader = cmd.ExecuteReader();
            if (reader.Read()) total++;
        }
        return total;
    }

    private static Int32 NL_SingleUpdate(Int32 count, Int32[] newAges)
    {
        using var conn = new NL.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = new NL.MySqlCommand(conn, "UPDATE bench_driver SET age=@age WHERE id=@id");
            cmd.Parameters.AddWithValue("age", newAges[i]);
            cmd.Parameters.AddWithValue("id", (Int64)(i + 1));
            total += cmd.ExecuteNonQuery();
        }
        return total;
    }

    private static Int32 NL_PipelineUpdate(Int32 count, Int32[] newAges)
    {
        var pipeCs = _connStr.TrimEnd(';') + ";Pipeline=true";
        using var conn = new NL.MySqlConnection(pipeCs);
        conn.Open();
        using var tr = conn.BeginTransaction();
        var ids = new Int64[count];
        for (var i = 0; i < count; i++) ids[i] = i + 1;
        using var cmd = new NL.MySqlCommand(conn, "UPDATE bench_driver SET age=@age WHERE id=@id");
        cmd.Parameters.AddWithValue("age", newAges);
        cmd.Parameters.AddWithValue("id", ids);
        var result = cmd.ExecuteArrayBatch(count);
        tr.Commit();
        return result;
    }

    private static Int32 NL_SingleDelete(Int32 count)
    {
        using var conn = new NL.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = new NL.MySqlCommand(conn, "DELETE FROM bench_driver WHERE id=@id");
            cmd.Parameters.AddWithValue("id", (Int64)(i + 1));
            total += cmd.ExecuteNonQuery();
        }
        return total;
    }

    private static Int32 NL_PipelineDelete(Int32 count)
    {
        var pipeCs = _connStr.TrimEnd(';') + ";Pipeline=true";
        using var conn = new NL.MySqlConnection(pipeCs);
        conn.Open();
        using var tr = conn.BeginTransaction();
        var ids = new Int64[count];
        for (var i = 0; i < count; i++) ids[i] = i + 1;
        using var cmd = new NL.MySqlCommand(conn, "DELETE FROM bench_driver WHERE id=@id");
        cmd.Parameters.AddWithValue("id", ids);
        var result = cmd.ExecuteArrayBatch(count);
        tr.Commit();
        return result;
    }
    #endregion

    #region MySql.Data (Official)
    private static Int32 Off_SingleInsert(Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new Official.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO bench_driver(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)";
            cmd.Parameters.AddWithValue("@name", names[i]);
            cmd.Parameters.AddWithValue("@age", ages[i]);
            cmd.Parameters.AddWithValue("@email", emails[i]);
            cmd.Parameters.AddWithValue("@score", scores[i]);
            cmd.Parameters.AddWithValue("@created", dates[i]);
            total += cmd.ExecuteNonQuery();
        }
        return total;
    }

    private static Int32 Off_BatchInsert(Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new Official.MySqlConnection(_connStr);
        conn.Open();
        using var tr = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tr;
        cmd.CommandText = "INSERT INTO bench_driver(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)";
        cmd.Parameters.Add(new Official.MySqlParameter("@name", Official.MySqlDbType.VarChar));
        cmd.Parameters.Add(new Official.MySqlParameter("@age", Official.MySqlDbType.Int32));
        cmd.Parameters.Add(new Official.MySqlParameter("@email", Official.MySqlDbType.VarChar));
        cmd.Parameters.Add(new Official.MySqlParameter("@score", Official.MySqlDbType.Double));
        cmd.Parameters.Add(new Official.MySqlParameter("@created", Official.MySqlDbType.DateTime));
        cmd.Prepare();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            cmd.Parameters["@name"].Value = names[i];
            cmd.Parameters["@age"].Value = ages[i];
            cmd.Parameters["@email"].Value = emails[i];
            cmd.Parameters["@score"].Value = scores[i];
            cmd.Parameters["@created"].Value = dates[i];
            total += cmd.ExecuteNonQuery();
        }
        tr.Commit();
        return total;
    }

    private static Int32 Off_SingleSelect(Int32 count)
    {
        using var conn = new Official.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT id,name,age,email,score,created FROM bench_driver WHERE id=@id";
            cmd.Parameters.AddWithValue("@id", (Int64)(i + 1));
            using var reader = cmd.ExecuteReader();
            if (reader.Read()) total++;
        }
        return total;
    }

    private static Int32 Off_SingleUpdate(Int32 count, Int32[] newAges)
    {
        using var conn = new Official.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE bench_driver SET age=@age WHERE id=@id";
            cmd.Parameters.AddWithValue("@age", newAges[i]);
            cmd.Parameters.AddWithValue("@id", (Int64)(i + 1));
            total += cmd.ExecuteNonQuery();
        }
        return total;
    }

    private static Int32 Off_BatchUpdate(Int32 count, Int32[] newAges)
    {
        using var conn = new Official.MySqlConnection(_connStr);
        conn.Open();
        using var tr = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tr;
        cmd.CommandText = "UPDATE bench_driver SET age=@age WHERE id=@id";
        cmd.Parameters.Add(new Official.MySqlParameter("@age", Official.MySqlDbType.Int32));
        cmd.Parameters.Add(new Official.MySqlParameter("@id", Official.MySqlDbType.Int64));
        cmd.Prepare();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            cmd.Parameters["@age"].Value = newAges[i];
            cmd.Parameters["@id"].Value = (Int64)(i + 1);
            total += cmd.ExecuteNonQuery();
        }
        tr.Commit();
        return total;
    }

    private static Int32 Off_SingleDelete(Int32 count)
    {
        using var conn = new Official.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM bench_driver WHERE id=@id";
            cmd.Parameters.AddWithValue("@id", (Int64)(i + 1));
            total += cmd.ExecuteNonQuery();
        }
        return total;
    }

    private static Int32 Off_BatchDelete(Int32 count)
    {
        using var conn = new Official.MySqlConnection(_connStr);
        conn.Open();
        using var tr = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tr;
        cmd.CommandText = "DELETE FROM bench_driver WHERE id=@id";
        cmd.Parameters.Add(new Official.MySqlParameter("@id", Official.MySqlDbType.Int64));
        cmd.Prepare();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            cmd.Parameters["@id"].Value = (Int64)(i + 1);
            total += cmd.ExecuteNonQuery();
        }
        tr.Commit();
        return total;
    }
    #endregion

    #region MySqlConnector
    private static Int32 Conn_SingleInsert(Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new Connector.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO bench_driver(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)";
            cmd.Parameters.AddWithValue("@name", names[i]);
            cmd.Parameters.AddWithValue("@age", ages[i]);
            cmd.Parameters.AddWithValue("@email", emails[i]);
            cmd.Parameters.AddWithValue("@score", scores[i]);
            cmd.Parameters.AddWithValue("@created", dates[i]);
            total += cmd.ExecuteNonQuery();
        }
        return total;
    }

    private static Int32 Conn_BatchInsert(Int32 count, String[] names, Int32[] ages, String[] emails, Double[] scores, DateTime[] dates)
    {
        using var conn = new Connector.MySqlConnection(_connStr);
        conn.Open();
        using var tr = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tr;
        cmd.CommandText = "INSERT INTO bench_driver(name,age,email,score,created) VALUES(@name,@age,@email,@score,@created)";
        cmd.Parameters.Add(new Connector.MySqlParameter("@name", Connector.MySqlDbType.VarChar));
        cmd.Parameters.Add(new Connector.MySqlParameter("@age", Connector.MySqlDbType.Int32));
        cmd.Parameters.Add(new Connector.MySqlParameter("@email", Connector.MySqlDbType.VarChar));
        cmd.Parameters.Add(new Connector.MySqlParameter("@score", Connector.MySqlDbType.Double));
        cmd.Parameters.Add(new Connector.MySqlParameter("@created", Connector.MySqlDbType.DateTime));
        cmd.Prepare();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            cmd.Parameters["@name"].Value = names[i];
            cmd.Parameters["@age"].Value = ages[i];
            cmd.Parameters["@email"].Value = emails[i];
            cmd.Parameters["@score"].Value = scores[i];
            cmd.Parameters["@created"].Value = dates[i];
            total += cmd.ExecuteNonQuery();
        }
        tr.Commit();
        return total;
    }

    private static Int32 Conn_SingleSelect(Int32 count)
    {
        using var conn = new Connector.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT id,name,age,email,score,created FROM bench_driver WHERE id=@id";
            cmd.Parameters.AddWithValue("@id", (Int64)(i + 1));
            using var reader = cmd.ExecuteReader();
            if (reader.Read()) total++;
        }
        return total;
    }

    private static Int32 Conn_SingleUpdate(Int32 count, Int32[] newAges)
    {
        using var conn = new Connector.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE bench_driver SET age=@age WHERE id=@id";
            cmd.Parameters.AddWithValue("@age", newAges[i]);
            cmd.Parameters.AddWithValue("@id", (Int64)(i + 1));
            total += cmd.ExecuteNonQuery();
        }
        return total;
    }

    private static Int32 Conn_BatchUpdate(Int32 count, Int32[] newAges)
    {
        using var conn = new Connector.MySqlConnection(_connStr);
        conn.Open();
        using var tr = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tr;
        cmd.CommandText = "UPDATE bench_driver SET age=@age WHERE id=@id";
        cmd.Parameters.Add(new Connector.MySqlParameter("@age", Connector.MySqlDbType.Int32));
        cmd.Parameters.Add(new Connector.MySqlParameter("@id", Connector.MySqlDbType.Int64));
        cmd.Prepare();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            cmd.Parameters["@age"].Value = newAges[i];
            cmd.Parameters["@id"].Value = (Int64)(i + 1);
            total += cmd.ExecuteNonQuery();
        }
        tr.Commit();
        return total;
    }

    private static Int32 Conn_SingleDelete(Int32 count)
    {
        using var conn = new Connector.MySqlConnection(_connStr);
        conn.Open();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM bench_driver WHERE id=@id";
            cmd.Parameters.AddWithValue("@id", (Int64)(i + 1));
            total += cmd.ExecuteNonQuery();
        }
        return total;
    }

    private static Int32 Conn_BatchDelete(Int32 count)
    {
        using var conn = new Connector.MySqlConnection(_connStr);
        conn.Open();
        using var tr = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tr;
        cmd.CommandText = "DELETE FROM bench_driver WHERE id=@id";
        cmd.Parameters.Add(new Connector.MySqlParameter("@id", Connector.MySqlDbType.Int64));
        cmd.Prepare();
        var total = 0;
        for (var i = 0; i < count; i++)
        {
            cmd.Parameters["@id"].Value = (Int64)(i + 1);
            total += cmd.ExecuteNonQuery();
        }
        tr.Commit();
        return total;
    }
    #endregion

    private record R(String Op, String Driver, Int32 Rows, Double Med, Double Min, Double Max, Int32 Affected, String? Err);
}
