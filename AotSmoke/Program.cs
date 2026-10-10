using System.Data;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using NewLife.MySql;
using NewLife.MySql.Binlog;
using NewLife.MySql.Common;

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
/// 附加 --no-db 仅运行不依赖数据库的场景（用于无 MySQL 的 CI 环境），依赖数据库的场景标记为 SKIP。
/// 使用方式：JIT 基线 `dotnet run`；真实 AOT `dotnet publish -r {rid} -o out` 后运行原生程序。
/// </remarks>
static class Program
{
    private static Int32 _fail;
    private static Boolean _noDb;
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

        _noDb = args.Contains("--no-db");

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
        }, needsDb: false);

        // S14 链路追踪（验证 ActivitySource 与埋点在裁剪后可用）
        Run("S14 Tracing", () =>
        {
            var names = new List<String>();
            using var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "NewLife.MySql",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => { lock (names) names.Add(activity.OperationName); }
            };
            ActivitySource.AddActivityListener(listener);

            using (var conn = new MySqlConnection(_conn))
            {
                conn.Open();
                using var cmd = new MySqlCommand(conn, "SELECT 1");
                if (Convert.ToInt32(cmd.ExecuteScalar()) != 1) throw new Exception("查询结果异常");
            }

            // 连接打开与命令执行各产生一个跨度
            if (!names.Contains("Open") || !names.Contains("Execute")) throw new Exception($"跨度={String.Join(",", names)}");
        });

        // S15 Schema 元数据查询（SchemaProvider 反射/列类型流转路径）
        Run("S15 GetSchema", () =>
        {
            using var conn = new MySqlConnection(_conn);
            conn.Open();

            var tables = conn.GetSchema("TABLES");
            if (tables.Rows.Count == 0) throw new Exception("no tables");
            var names = tables.Rows.Cast<DataRow>().Select(e => Convert.ToString(e["TABLE_NAME"])).ToList();
            if (!names.Contains("t1")) throw new Exception($"t1 不在表列表中（{String.Join(",", names.Take(5))}）");

            var columns = conn.GetSchema("COLUMNS");
            var colNames = columns.Rows.Cast<DataRow>().Select(e => Convert.ToString(e["COLUMN_NAME"])).ToList();
            if (!colNames.Contains("Price")) throw new Exception("Price 列缺失");
        });

        // S16 DbDataSource（.NET 7+ 数据源抽象）
        Run("S16 DataSource", () =>
        {
            using var ds = new MySqlDataSource(_conn);
            using var conn = ds.CreateConnection();
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM t1";
            var n = Convert.ToInt32(cmd.ExecuteScalar());
            if (n <= 0) throw new Exception($"count={n}");
        });

        // S17 存储过程 IN/OUT 参数
        Run("S17 StoredProcedure", () =>
        {
            using var conn = new MySqlConnection(_conn);
            conn.Open();
            using (var cmd = new MySqlCommand(conn, "DROP PROCEDURE IF EXISTS smoke_proc"))
                cmd.ExecuteNonQuery();
            using (var cmd = new MySqlCommand(conn, "CREATE PROCEDURE smoke_proc(IN p_in INT, OUT p_out INT) BEGIN SET p_out = p_in * 2; END"))
                cmd.ExecuteNonQuery();

            using var call = new MySqlCommand(conn, "smoke_proc") { CommandType = CommandType.StoredProcedure };
            var ps = call.Parameters;
            ps.AddWithValue("p_in", 21);
            var pOut = new MySqlParameter { ParameterName = "p_out", Direction = ParameterDirection.Output };
            ps.Add(pOut);
            call.ExecuteNonQuery();

            if (Convert.ToInt32(pOut.Value) != 42) throw new Exception($"out={pOut.Value}");
        });

        // S18 Binlog 订阅（复制协议解析；服务器未开启 log_bin 时跳过）
        await RunAsync("S18 Binlog", async () =>
        {
            using (var conn = new MySqlConnection(_conn))
            {
                conn.Open();
                using var cmd = new MySqlCommand(conn, "SHOW VARIABLES LIKE 'log_bin'");
                using var reader = cmd.ExecuteReader();
                var on = reader.Read() && String.Equals(Convert.ToString(reader.GetValue(1)), "ON", StringComparison.OrdinalIgnoreCase);
                if (!on)
                {
                    Console.WriteLine("        （服务器未开启 log_bin，跳过 Binlog 订阅）");
                    return;
                }
            }

            var events = new List<BinlogEvent>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            using var client = new BinlogClient(_conn);

            var task = Task.Run(async () =>
            {
                try
                {
                    // SubscribeAsync 直接产出事件（不触发 OnEvent）
                    await foreach (var e in client.SubscribeAsync(cts.Token))
                    {
                        lock (events) events.Add(e);
                    }
                }
                catch (OperationCanceledException) { }
                catch (Exception) { /* 订阅结束阶段异常不影响事件断言 */ }
            });

            // 等订阅就绪后写入一行，触发 WRITE_ROWS_EVENT
            await Task.Delay(1500);
            using (var conn = new MySqlConnection(_conn))
            {
                conn.Open();
                using var cmd = new MySqlCommand(conn, "INSERT INTO t1 (Name, CreateTime, Price) VALUES ('binlog', NOW(), 1)");
                cmd.ExecuteNonQuery();
            }

            for (var i = 0; i < 100 && !HasWriteRows(); i++)
                await Task.Delay(100);

            cts.Cancel();
            await task;

            if (!HasWriteRows()) throw new Exception($"未收到 WRITE_ROWS_EVENT（事件数={events.Count}）");

            Boolean HasWriteRows()
            {
                lock (events) return events.Any(e => e.EventType == BinlogEventType.WRITE_ROWS_EVENT);
            }
        });

        // S19 DataAdapter 填充/架构/回写（System.Data 反射路径，调用方声明保留责任）
        Run("S19 DataAdapter", () => RunDataAdapter(_conn));

        // P1 工厂创建（纯内存）
        Run("P1 ClientFactory", () =>
        {
            var factory = MySqlClientFactory.Instance;
            using var conn = factory.CreateConnection() ?? throw new Exception("CreateConnection 返回空");
            using var cmd = factory.CreateCommand() ?? throw new Exception("CreateCommand 返回空");
            using var adapter = factory.CreateDataAdapter() ?? throw new Exception("CreateDataAdapter 返回空");
            var parameter = factory.CreateParameter() ?? throw new Exception("CreateParameter 返回空");
            if (parameter.ParameterName != null) parameter.ParameterName = "p";

            cmd.Connection = conn;
            cmd.CommandText = "SELECT 1";
            if (cmd.CommandText != "SELECT 1") throw new Exception("CommandText 未生效");
            if (conn is not MySqlConnection) throw new Exception($"连接类型异常：{conn.GetType().Name}");
        }, needsDb: false);

        // P2 参数替换与转义（纯内存，覆盖注入防护路径）
        Run("P2 ParameterSubstitution", () =>
        {
            using var holder = new MySqlConnection();
            using var cmd = new MySqlCommand("SELECT * FROM t1 WHERE Name = @Name AND Price > @Price", holder);
            cmd.Parameters.AddWithValue("@Name", "O'Reilly");
            cmd.Parameters.AddWithValue("@Price", 12.5m);

            var sql = cmd.ToString();
            if (!sql.Contains(@"O\'Reilly")) throw new Exception($"单引号未转义：{sql}");
            if (!sql.Contains("12.5")) throw new Exception($"数值未替换：{sql}");
        }, needsDb: false);

        // P3 Geometry WKB 封装（纯内存）
        Run("P3 GeometryWkb", () =>
        {
            // POINT(1 2) 的 WKB：字节序 + 类型 + X(8) + Y(8)
            var wkb = new Byte[] { 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40 };
            var geometry = new MySqlGeometry(wkb);
            Byte[] back = geometry;
            if (back.Length != wkb.Length) throw new Exception($"长度={back.Length}");
            if (!geometry.ToString()!.StartsWith("0101000000", StringComparison.OrdinalIgnoreCase)) throw new Exception($"WKB={geometry}");
        }, needsDb: false);

        // P4 DbDataSource 创建（纯内存）
        Run("P4 DataSourceCreate", () =>
        {
            using var ds = new MySqlDataSource(_conn);
            using var conn = ds.CreateConnection();
            if (conn is not MySqlConnection) throw new Exception($"连接类型异常：{conn.GetType().Name}");
            using var cmd = ds.CreateCommand("SELECT 1");
            if (cmd.CommandText != "SELECT 1") throw new Exception("CreateCommand 未生效");
        }, needsDb: false);

        Console.WriteLine(_fail == 0 ? "ALL PASS" : $"FAILED={_fail}");
        return _fail == 0 ? 0 : 1;
    }

    /// <summary>DataAdapter 场景。Fill/FillSchema/Update 在基类标注 RequiresUnreferencedCode，调用方按 System.Data 语义承担元数据保留责任</summary>
    /// <param name="connStr">连接字符串</param>
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "冒烟工程主动验证 DataAdapter 标注路径在 AOT 下的运行时行为，调用方负责元数据保留")]
    private static void RunDataAdapter(String connStr)
    {
        using var conn = new MySqlConnection(connStr);
        conn.Open();

        var table = new DataTable();
        using (var adapter = new MySqlDataAdapter("SELECT Id, Name, CreateTime, Price FROM t1 ORDER BY Id", conn))
        {
            adapter.Fill(table);
            if (table.Rows.Count == 0) throw new Exception("no rows");

            adapter.FillSchema(table, SchemaType.Source);
            if (table.Columns.Count < 4) throw new Exception($"列数={table.Columns.Count}");
        }

        var row = table.NewRow();
        row["Name"] = "adapter";
        row["CreateTime"] = DateTime.Now;
        row["Price"] = 9.9m;
        table.Rows.Add(row);

        var insert = new MySqlCommand("INSERT INTO t1 (Name, CreateTime, Price) VALUES (@Name, @CreateTime, @Price)", conn);
        insert.Parameters.Add(new MySqlParameter("@Name", DbType.String) { SourceColumn = "Name", Size = 64 });
        insert.Parameters.Add(new MySqlParameter("@CreateTime", DbType.DateTime) { SourceColumn = "CreateTime" });
        insert.Parameters.Add(new MySqlParameter("@Price", DbType.Decimal) { SourceColumn = "Price" });

        using var da = new MySqlDataAdapter { InsertCommand = insert };
        var affected = da.Update(table);
        if (affected != 1) throw new Exception($"update={affected}");
    }

    private static void Run(String name, Action action, Boolean needsDb = true)
    {
        if (Skip(name, needsDb)) return;

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

    private static async Task RunAsync(String name, Func<Task> action, Boolean needsDb = true)
    {
        if (Skip(name, needsDb)) return;

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

    /// <summary>--no-db 模式下跳过依赖数据库的场景</summary>
    /// <param name="name">场景名称</param>
    /// <param name="needsDb">是否依赖数据库</param>
    /// <returns>是否跳过</returns>
    private static Boolean Skip(String name, Boolean needsDb)
    {
        if (!_noDb || !needsDb) return false;

        Console.WriteLine($"SKIP  {name}  -> --no-db");
        return true;
    }
}
