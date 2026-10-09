using System.Data;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using NewLife.MySql;

namespace AotSmoke;

/// <summary>冒烟实体。验证 ReadModelsAsync&lt;T&gt; 的实体映射路径</summary>
public class Row
{
    /// <summary>编号</summary>
    public Int32 Id { get; set; }

    /// <summary>名称</summary>
    public String? Name { get; set; }

    /// <summary>创建时间</summary>
    public DateTime CreateTime { get; set; }

    /// <summary>价格</summary>
    public Decimal Price { get; set; }
}

/// <summary>NewLife.MySql NativeAOT 冒烟探针。逐场景 PASS/FAIL，非零退出码即失败</summary>
/// <remarks>
/// 连接串来源：命令行首个非选项参数（连接串或配置文件路径）&gt; 环境变量 MYSQL_CONN &gt; 本机默认（127.0.0.1/root/root）。
/// 附加 --drop 可删除冒烟库后退出（用于清理）。
/// 使用方式：JIT 基线 `dotnet run`；真实 AOT `dotnet publish -r {rid} -o out` 后运行原生程序。
/// </remarks>
static class Program
{
    private static Int32 _fail;
    private static String _conn = "";
    private static String _connBase = "";

    private const String Ddl = "CREATE TABLE `t1` (`Id` INT NOT NULL AUTO_INCREMENT, `Name` VARCHAR(64) NULL, `CreateTime` DATETIME NULL, `Price` DECIMAL(10,2) NULL, PRIMARY KEY (`Id`)) ENGINE=InnoDB";

    static async Task<Int32> Main(String[] args)
    {
        try { Console.OutputEncoding = Encoding.UTF8; } catch { /* 部分终端不支持设置编码 */ }

        Console.WriteLine($"[Env] AOT={!RuntimeFeature.IsDynamicCodeSupported} DynamicCodeSupported={RuntimeFeature.IsDynamicCodeSupported} DynamicCodeCompiled={RuntimeFeature.IsDynamicCodeCompiled}");
        Console.WriteLine($"[Env] Driver={typeof(SqlClient).Assembly.GetName().Version}");

        // 首个非选项参数支持两种形态：连接串本身，或指向含连接串的配置文件（与 UnitTest 的 Config/mysql.config 习惯一致）
        var cfg = args.FirstOrDefault(e => !e.StartsWith("--", StringComparison.Ordinal));
        if (cfg != null && File.Exists(cfg)) cfg = File.ReadAllText(cfg).Trim();

        _connBase = cfg ?? Environment.GetEnvironmentVariable("MYSQL_CONN")
            ?? "Server=127.0.0.1;Port=3306;Database=mysql;User Id=root;Password=root;";
        _conn = new MySqlConnectionStringBuilder(_connBase) { Database = "aotsmoke" }.ConnectionString;

        // 清理模式：删除冒烟库
        if (args.Contains("--drop"))
        {
            var admin = new MySqlConnectionStringBuilder(_connBase) { Database = "information_schema" };
            using var conn = new MySqlConnection(admin.ConnectionString);
            conn.Open();
            using var cmd = new MySqlCommand(conn, "DROP DATABASE IF EXISTS `aotsmoke`");
            cmd.ExecuteNonQuery();
            Console.WriteLine("dropped database aotsmoke");
            return 0;
        }

        // S0 建库
        Run("S0 CreateDatabase", () =>
        {
            using var conn = new MySqlConnection(_connBase);
            conn.Open();
            using (var cmd = new MySqlCommand(conn, "DROP DATABASE IF EXISTS `aotsmoke`"))
                cmd.ExecuteNonQuery();
            using (var cmd = new MySqlCommand(conn, "CREATE DATABASE `aotsmoke`"))
                cmd.ExecuteNonQuery();
        });

        // S1 连接 + ExecuteScalar
        Run("S1 Connect+Scalar", () =>
        {
            using var conn = new MySqlConnection(_conn);
            conn.Open();
            using var cmd = new MySqlCommand(conn, "SELECT 1");
            var v = cmd.ExecuteScalar();
            if (Convert.ToInt32(v) != 1) throw new Exception($"value={v}");
        });

        // S2 DDL + 参数化插入 + 类型化读取 + GetSchemaTable
        Run("S2 DDL+Insert+TypedReader", () =>
        {
            using var conn = new MySqlConnection(_conn);
            conn.Open();
            using (var cmd = new MySqlCommand(conn, Ddl)) cmd.ExecuteNonQuery();
            using (var cmd = new MySqlCommand(conn, "INSERT INTO t1 (Name, CreateTime, Price) VALUES (@Name, @CreateTime, @Price)"))
            {
                cmd.Parameters.AddWithValue("@Name", "中文名称");
                cmd.Parameters.AddWithValue("@CreateTime", new DateTime(2026, 10, 9, 12, 34, 56));
                cmd.Parameters.AddWithValue("@Price", 123.45m);
                if (cmd.ExecuteNonQuery() != 1) throw new Exception("insert != 1");
            }
            using var rc = new MySqlCommand(conn, "SELECT Id, Name, CreateTime, Price FROM t1 ORDER BY Id");
            using var reader = (MySqlDataReader)rc.ExecuteReader();
            if (!reader.Read()) throw new Exception("no row");
            var id = reader.GetInt32(0);
            var name = reader.GetString(1);
            var ct = reader.GetDateTime(2);
            var price = reader.GetDecimal(3);
            var ft = reader.GetFieldType(1);
            var schema = reader.GetSchemaTable();
            if (id <= 0 || name != "中文名称" || ct.Year != 2026 || price != 123.45m)
                throw new Exception($"row mismatch id={id} name={name} ct={ct} price={price}");
            if (ft != typeof(String)) throw new Exception($"fieldType={ft}");
            if (schema == null || schema.Rows.Count == 0 || schema.Columns.Count == 0)
                throw new Exception($"schema rows={schema?.Rows.Count} cols={schema?.Columns.Count}");
        });

        // S3 服务器预编译 + 参数
        Run("S3 ServerPrepare", () =>
        {
            var b = new MySqlConnectionStringBuilder(_conn) { UseServerPrepare = true };
            using var conn = new MySqlConnection(b.ConnectionString);
            conn.Open();
            using var cmd = new MySqlCommand(conn, "SELECT Name FROM t1 WHERE Id = @Id");
            cmd.Parameters.AddWithValue("@Id", 1);
            cmd.Prepare();
            var v = cmd.ExecuteScalar();
            if (Convert.ToString(v) != "中文名称") throw new Exception($"value={v}");
        });

        // S4 ExecuteBatch 字典参数集
        Run("S4 ExecuteBatch", () =>
        {
            using var conn = new MySqlConnection(_conn);
            conn.Open();
            using var cmd = new MySqlCommand(conn, "INSERT INTO t1 (Name, CreateTime, Price) VALUES (@Name, @CreateTime, @Price)");
            cmd.Parameters.AddWithValue("@Name", "");
            cmd.Parameters.AddWithValue("@CreateTime", DateTime.Now);
            cmd.Parameters.AddWithValue("@Price", 0m);
            var sets = new List<Dictionary<String, Object?>>();
            for (var i = 0; i < 50; i++)
                sets.Add(new() { ["Name"] = "batch" + i, ["CreateTime"] = DateTime.Now, ["Price"] = i * 1.5m });
            var n = cmd.ExecuteBatch(sets.ToArray<IDictionary<String, Object?>>());
            if (n != 50) throw new Exception($"affected={n}");
        });

        // S5 ExecuteBatchValues 值数组 + 管道化
        Run("S5 ExecuteBatchValues+Pipeline", () =>
        {
            var b = new MySqlConnectionStringBuilder(_conn) { Pipeline = true, UseServerPrepare = true };
            using var conn = new MySqlConnection(b.ConnectionString);
            conn.Open();
            using var cmd = new MySqlCommand(conn, "INSERT INTO t1 (Name, CreateTime, Price) VALUES (@Name, @CreateTime, @Price)");
            cmd.Parameters.AddWithValue("@Name", "");
            cmd.Parameters.AddWithValue("@CreateTime", DateTime.Now);
            cmd.Parameters.AddWithValue("@Price", 0m);
            var sets = new List<Object?[]>();
            for (var i = 0; i < 50; i++)
                sets.Add(["pipe" + i, DateTime.Now, i * 2.0m]);
            var n = cmd.ExecuteBatchValues(sets);
            if (n != 50) throw new Exception($"affected={n}");
        });

        // S6 ReadModelsAsync<T>（实体元数据保留路径）
        await RunAsync("S6 ReadModelsAsync", async () =>
        {
            using var conn = new MySqlConnection(_conn);
            await conn.OpenAsync();
            using var cmd = new MySqlCommand(conn, "SELECT Id, Name, CreateTime, Price FROM t1 ORDER BY Id");
            using var reader = (MySqlDataReader)await cmd.ExecuteReaderAsync();
            var list = await reader.ReadModelsAsync<Row>();
            if (list.Count < 100) throw new Exception($"count={list.Count}");
            var first = list[0];
            if (first.Id != 1 || first.Name != "中文名称" || first.Price != 123.45m)
                throw new Exception($"first mismatch Id={first.Id} Name={first.Name} Price={first.Price}");
        });

        // S7 DataAdapter.Fill（DataSet 路径）
        Run("S7 DataAdapter.Fill", () =>
        {
            using var conn = new MySqlConnection(_conn);
            conn.Open();
            using var adapter = new MySqlDataAdapter("SELECT Id, Name FROM t1 ORDER BY Id", conn);
            var dt = new DataTable();
            var n = adapter.Fill(dt);
            if (n < 100 || dt.Rows.Count != n) throw new Exception($"rows={n}/{dt.Rows.Count}");
        });

        // S8 MySqlBulkCopy（DataTable → 表）
        await RunAsync("S8 MySqlBulkCopy", async () =>
        {
            using var conn = new MySqlConnection(_conn);
            conn.Open();
            using (var cmd = new MySqlCommand(conn, "DROP TABLE IF EXISTS `t2`")) cmd.ExecuteNonQuery();
            using (var cmd = new MySqlCommand(conn, "CREATE TABLE `t2` (`Id` INT NULL, `Name` VARCHAR(64) NULL)")) cmd.ExecuteNonQuery();

            var dt = new DataTable("t2");
            dt.Columns.Add("Id", typeof(Int32));
            dt.Columns.Add("Name", typeof(String));
            for (var i = 0; i < 500; i++)
                dt.Rows.Add(i, "bulk" + i);

            using var bulk = new MySqlBulkCopy(conn) { DestinationTableName = "t2" };
            await bulk.WriteToServerAsync(dt);

            using var verify = new MySqlCommand(conn, "SELECT COUNT(*) FROM t2");
            var n = Convert.ToInt32(verify.ExecuteScalar());
            if (n != 500) throw new Exception($"copied={n}");
        });

        // S9 事务提交与回滚
        Run("S9 Transaction", () =>
        {
            using var conn = new MySqlConnection(_conn);
            conn.Open();
            using (var tx = conn.BeginTransaction())
            {
                using var cmd = new MySqlCommand(conn, "INSERT INTO t1 (Name, CreateTime, Price) VALUES ('tx-commit', NOW(), 9.99)");
                cmd.Transaction = tx;
                cmd.ExecuteNonQuery();
                tx.Commit();
            }
            using (var tx = conn.BeginTransaction())
            {
                using var cmd = new MySqlCommand(conn, "INSERT INTO t1 (Name, CreateTime, Price) VALUES ('tx-rollback', NOW(), 9.99)");
                cmd.Transaction = tx;
                cmd.ExecuteNonQuery();
                tx.Rollback();
            }
            using var verify = new MySqlCommand(conn, "SELECT COUNT(*) FROM t1 WHERE Name IN ('tx-commit','tx-rollback')");
            var n = Convert.ToInt32(verify.ExecuteScalar());
            if (n != 1) throw new Exception($"count={n}");
        });

        // S10 压缩协议（zlib）
        Run("S10 Compression", () =>
        {
            var b = new MySqlConnectionStringBuilder(_conn) { UseCompression = true };
            using var conn = new MySqlConnection(b.ConnectionString);
            conn.Open();
            using var cmd = new MySqlCommand(conn, "SELECT Name FROM t1 WHERE Id = 1");
            var v = cmd.ExecuteScalar();
            if (Convert.ToString(v) != "中文名称") throw new Exception($"value={v}");
        });

        // S11 异步读取
        await RunAsync("S11 AsyncReader", async () =>
        {
            using var conn = new MySqlConnection(_conn);
            await conn.OpenAsync();
            using var cmd = new MySqlCommand(conn, "SELECT Id, Name FROM t1 ORDER BY Id LIMIT 5");
            using var reader = await cmd.ExecuteReaderAsync();
            var count = 0;
            while (await reader.ReadAsync()) count++;
            if (count != 5) throw new Exception($"rows={count}");
        });

        // S12 连接属性下发（验证 ConnectAttributes 属性元数据在裁剪后保留）
        Run("S12 ConnectAttrs", () =>
        {
            using var conn = new MySqlConnection(_conn);
            conn.Open();
            using var cmd = new MySqlCommand(conn, "SELECT ATTR_NAME FROM performance_schema.session_connect_attrs WHERE ATTR_NAME LIKE '\\_%'");
            using var reader = cmd.ExecuteReader();
            var names = new HashSet<String>();
            while (reader.Read()) names.Add(reader.GetString(0));
            if (names.Count < 7) throw new Exception($"属性数={names.Count}（应为 7 类），AOT 下元数据可能被裁剪");
        });

        // S13 反射与表达式可用性（验证 ReadModels 依赖的元数据保留与 Expression.Compile 可用）
        Run("S13 ReflectionProbe", () =>
        {
            if (typeof(Row).GetConstructor(Type.EmptyTypes) == null) throw new Exception("Row 无参构造未保留");
            var props = typeof(Row).GetProperties();
            if (props.Length != 4) throw new Exception($"Row 属性数={props.Length}");
            var f = Expression.Lambda<Func<Row>>(Expression.New(typeof(Row))).Compile();
            if (f() is not Row) throw new Exception("Expression.Compile 结果异常");
        });

        Console.WriteLine(_fail == 0 ? "ALL PASS" : $"FAILED={_fail}");
        return _fail == 0 ? 0 : 1;
    }

    private static void Run(String name, Action action)
    {
        try
        {
            action();
            Console.WriteLine($"PASS  {name}");
        }
        catch (Exception ex)
        {
            _fail++;
            Console.WriteLine($"FAIL  {name}  -> {ex.GetType().Name}: {ex.Message}");
        }
    }

    private static async Task RunAsync(String name, Func<Task> action)
    {
        try
        {
            await action();
            Console.WriteLine($"PASS  {name}");
        }
        catch (Exception ex)
        {
            _fail++;
            Console.WriteLine($"FAIL  {name}  -> {ex.GetType().Name}: {ex.Message}");
        }
    }
}
