using System.Data;
using NewLife;
using NewLife.Log;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>批量操作测试。自建测试表，批量 INSERT/UPDATE/DELETE，验证结果后清理</summary>
public class BatchTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    /// <summary>获取一个连接到 sys 数据库的连接，用于建表和清理</summary>
    private static MySqlConnection CreateConnection()
    {
        // 使用 test 数据库，如果不存在则用 sys
        var conn = new MySqlConnection(_ConnStr);
        conn.Open();
        return conn;
    }

    /// <summary>创建测试表</summary>
    private static void CreateTestTable(MySqlConnection conn, String tableName)
    {
        var sql = $@"CREATE TABLE IF NOT EXISTS `{tableName}` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `name` VARCHAR(100) NOT NULL,
            `age` INT NOT NULL DEFAULT 0,
            `score` DECIMAL(10,2) DEFAULT NULL,
            `created` DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        conn.ExecuteNonQuery(sql);
    }

    /// <summary>删除测试表</summary>
    private static void DropTestTable(MySqlConnection conn, String tableName)
    {
        conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{tableName}`");
    }

    #region Prepare 基础测试
    [Fact]
    public void Prepare_LowLevel_Diagnostic()
    {
        // 底层诊断：直接使用 SqlClient API 验证 Prepare 和 Execute
        using var conn = CreateConnection();
        var client = conn.Client!;
        var table = "batch_test_diag_" + Rand.Next(10000);
        CreateTestTable(conn, table);

        try
        {
            var sql = $"INSERT INTO `{table}` (name, age) VALUES (?, ?)";
            var result = client.PrepareStatementAsync(sql).ConfigureAwait(false).GetAwaiter().GetResult();
            var statementId = result.StatementId;
            var paramColumns = result.Columns;

            // statementId 应大于 0
            Assert.True(statementId > 0, $"statementId={statementId} should be > 0");
            // 参数列应有 2 个
            Assert.NotNull(paramColumns);
            Assert.Equal(2, paramColumns.Length);

            // 执行
            var ps = new MySqlParameterCollection();
            ps.AddWithValue("name", "DiagTest");
            ps.AddWithValue("age", 99);
            client.ExecuteStatementAsync(statementId, ps, paramColumns).ConfigureAwait(false).GetAwaiter().GetResult();

            // 读取响应
            var rs = client.ReadPacketAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            Assert.True(rs.IsOK, "Response should be OK for INSERT");

            // 关闭预编译语句
            client.CloseStatementAsync(statementId).ConfigureAwait(false).GetAwaiter().GetResult();

            // 验证插入成功
            using var verifyCmd = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}`");
            var count = verifyCmd.ExecuteScalar().ToInt();
            Assert.Equal(1, count);
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }

    [Fact]
    public void Prepare_And_Execute_Insert()
    {
        var table = "batch_test_prepare_" + Rand.Next(10000);
        using var conn = CreateConnection();
        CreateTestTable(conn, table);

        try
        {
            // 预编译插入语句
            using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age) VALUES (@name, @age)");
            var ps = cmd.Parameters as MySqlParameterCollection;
            ps.AddWithValue("name", "Tom");
            ps.AddWithValue("age", 25);
            cmd.Prepare();

            Assert.True(cmd.IsPrepared);

            // 第一次执行
            var affected = cmd.ExecuteNonQuery();
            Assert.Equal(1, affected);

            // 修改参数再执行
            cmd.Parameters[0].Value = "Jerry";
            cmd.Parameters[1].Value = 30;
            affected = cmd.ExecuteNonQuery();
            Assert.Equal(1, affected);

            // 验证
            using var verifyCmd = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}`");
            var count = verifyCmd.ExecuteScalar().ToInt();
            Assert.Equal(2, count);
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }

    [Fact]
    public void UseServerPrepare_Auto_Prepare()
    {
        var table = "batch_test_auto_" + Rand.Next(10000);
        // 在连接字符串中启用 UseServerPrepare
        var connStr = _ConnStr.TrimEnd(';') + ";UseServerPrepare=true;";
        using var conn = new MySqlConnection(connStr);
        conn.Open();
        CreateTestTable(conn, table);

        try
        {
            // 有参数时应自动走预编译路径
            using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age) VALUES (@name, @age)");
            var ps = cmd.Parameters as MySqlParameterCollection;
            ps.AddWithValue("name", "Alice");
            ps.AddWithValue("age", 28);

            var affected = cmd.ExecuteNonQuery();
            Assert.Equal(1, affected);

            // 验证
            using var verifyCmd = new MySqlCommand(conn, $"SELECT name FROM `{table}` WHERE age=28");
            var name = verifyCmd.ExecuteScalar()?.ToString();
            Assert.Equal("Alice", name);
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }
    #endregion

    #region 批量执行测试 - 字典参数集
    [Fact]
    public void ExecuteBatch_DictParams_Insert()
    {
        var table = "batch_test_dict_" + Rand.Next(10000);
        using var conn = CreateConnection();
        CreateTestTable(conn, table);

        try
        {
            using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age, score) VALUES (@name, @age, @score)");
            var ps = cmd.Parameters as MySqlParameterCollection;
            ps.AddWithValue("name", "");
            ps.AddWithValue("age", 0);
            ps.AddWithValue("score", 0m);

            var paramSets = new List<Dictionary<String, Object?>>
            {
                new() { ["name"] = "Alice", ["age"] = 25, ["score"] = 88.5m },
                new() { ["name"] = "Bob", ["age"] = 30, ["score"] = 92.0m },
                new() { ["name"] = "Charlie", ["age"] = 22, ["score"] = 75.5m },
                new() { ["name"] = "Diana", ["age"] = 28, ["score"] = 95.0m },
            };

            var affected = cmd.ExecuteBatch(paramSets.ToArray<IDictionary<String, Object?>>());
            Assert.Equal(4, affected);

            // 验证
            using var verifyCmd = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}`");
            Assert.Equal(4, verifyCmd.ExecuteScalar().ToInt());

            // 验证具体值
            using var cmd2 = new MySqlCommand(conn, $"SELECT score FROM `{table}` WHERE name='Bob'");
            var score = Convert.ToDecimal(cmd2.ExecuteScalar());
            Assert.Equal(92.0m, score);
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }

    [Fact]
    public void ExecuteBatch_DictParams_Update()
    {
        var table = "batch_test_upd_" + Rand.Next(10000);
        using var conn = CreateConnection();
        CreateTestTable(conn, table);

        try
        {
            // 先插入数据
            conn.ExecuteNonQuery($"INSERT INTO `{table}` (name, age) VALUES ('Alice', 25), ('Bob', 30), ('Charlie', 22)");

            // 批量更新
            using var cmd = new MySqlCommand(conn, $"UPDATE `{table}` SET age=@age WHERE name=@name");
            var ps = cmd.Parameters as MySqlParameterCollection;
            ps.AddWithValue("age", 0);
            ps.AddWithValue("name", "");

            var paramSets = new List<IDictionary<String, Object?>>
            {
                new Dictionary<String, Object?> { ["age"] = 26, ["name"] = "Alice" },
                new Dictionary<String, Object?> { ["age"] = 31, ["name"] = "Bob" },
                new Dictionary<String, Object?> { ["age"] = 23, ["name"] = "Charlie" },
            };

            var affected = cmd.ExecuteBatch(paramSets);
            Assert.Equal(3, affected);

            // 验证
            using var verifyCmd = new MySqlCommand(conn, $"SELECT age FROM `{table}` WHERE name='Alice'");
            Assert.Equal(26, verifyCmd.ExecuteScalar().ToInt());
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }

    [Fact]
    public void ExecuteBatch_DictParams_Delete()
    {
        var table = "batch_test_del_" + Rand.Next(10000);
        using var conn = CreateConnection();
        CreateTestTable(conn, table);

        try
        {
            // 先插入数据
            conn.ExecuteNonQuery($"INSERT INTO `{table}` (name, age) VALUES ('Alice', 25), ('Bob', 30), ('Charlie', 22), ('Diana', 28)");

            // 批量删除
            using var cmd = new MySqlCommand(conn, $"DELETE FROM `{table}` WHERE name=@name");
            var ps = cmd.Parameters as MySqlParameterCollection;
            ps.AddWithValue("name", "");

            var paramSets = new List<IDictionary<String, Object?>>
            {
                new Dictionary<String, Object?> { ["name"] = "Alice" },
                new Dictionary<String, Object?> { ["name"] = "Charlie" },
            };

            var affected = cmd.ExecuteBatch(paramSets);
            Assert.Equal(2, affected);

            // 验证剩余数据
            using var verifyCmd = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}`");
            Assert.Equal(2, verifyCmd.ExecuteScalar().ToInt());
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }
    #endregion

    #region 批量执行测试 - Oracle 风格数组绑定
    [Fact]
    public void ExecuteArrayBatch_Insert()
    {
        var table = "batch_test_arr_" + Rand.Next(10000);
        using var conn = CreateConnection();
        CreateTestTable(conn, table);

        try
        {
            using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age) VALUES (@name, @age)");
            // Oracle 风格：参数值设为数组
            var ps = cmd.Parameters as MySqlParameterCollection;
            ps.AddWithValue("name", new String[] { "A1", "A2", "A3", "A4", "A5" });
            ps.AddWithValue("age", new Int32[] { 10, 20, 30, 40, 50 });

            var affected = cmd.ExecuteArrayBatch(5);
            Assert.Equal(5, affected);

            // 验证
            using var verifyCmd = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}`");
            Assert.Equal(5, verifyCmd.ExecuteScalar().ToInt());

            using var cmd2 = new MySqlCommand(conn, $"SELECT age FROM `{table}` WHERE name='A3'");
            Assert.Equal(30, cmd2.ExecuteScalar().ToInt());
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }

    [Fact]
    public void ExecuteArrayBatch_Update()
    {
        var table = "batch_test_arr_upd_" + Rand.Next(10000);
        using var conn = CreateConnection();
        CreateTestTable(conn, table);

        try
        {
            // 先插入
            conn.ExecuteNonQuery($"INSERT INTO `{table}` (name, age) VALUES ('X1', 10), ('X2', 20), ('X3', 30)");

            // 批量更新
            using var cmd = new MySqlCommand(conn, $"UPDATE `{table}` SET age=@age WHERE name=@name");
            var ps = cmd.Parameters as MySqlParameterCollection;
            ps.AddWithValue("age", new Int32[] { 11, 22, 33 });
            ps.AddWithValue("name", new String[] { "X1", "X2", "X3" });

            var affected = cmd.ExecuteArrayBatch(3);
            Assert.Equal(3, affected);

            // 验证
            using var cmd2 = new MySqlCommand(conn, $"SELECT age FROM `{table}` WHERE name='X2'");
            Assert.Equal(22, cmd2.ExecuteScalar().ToInt());
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }
    #endregion

    #region 多行 INSERT VALUES 语法测试（XCode 场景验证）
    [Fact]
    public void MultiRow_Insert_Values()
    {
        var table = "batch_test_multi_" + Rand.Next(10000);
        using var conn = CreateConnection();
        CreateTestTable(conn, table);

        try
        {
            // 多行 VALUES 语法
            var sql = $"INSERT INTO `{table}` (name, age, score) VALUES ('M1', 10, 80.0), ('M2', 20, 85.0), ('M3', 30, 90.0)";
            var affected = conn.ExecuteNonQuery(sql);
            Assert.Equal(3, affected);

            // 验证
            using var verifyCmd = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}`");
            Assert.Equal(3, verifyCmd.ExecuteScalar().ToInt());
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }
    #endregion

    #region 参数转换测试
    [Fact]
    public void ConvertToPositionalParameters_Basic()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("name", "test");
        ps.AddWithValue("age", 25);

        var sql = "INSERT INTO t (name, age) VALUES (@name, @age)";
        var result = MySqlCommand.ConvertToPositionalParameters(sql, ps);
        Assert.Equal("INSERT INTO t (name, age) VALUES (?, ?)", result);
    }

    [Fact]
    public void ConvertToPositionalParameters_WithStringLiteral()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("name", "test");

        var sql = "SELECT * FROM t WHERE name=@name AND label='@notparam'";
        var result = MySqlCommand.ConvertToPositionalParameters(sql, ps);
        Assert.Equal("SELECT * FROM t WHERE name=? AND label='@notparam'", result);
    }
    #endregion
}
