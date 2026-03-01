using System.ComponentModel;
using System.Data;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>存储过程集成测试。验证 CommandType.StoredProcedure 调用、IN/OUT 参数</summary>
[Collection(TestCollections.DataModification)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class StoredProcedureTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly String _spInsert;
    private readonly String _spQuery;
    private readonly String _spInOut;
    private readonly MySqlConnection _conn;

    public StoredProcedureTests()
    {
        var suffix = Rand.Next(10000).ToString();
        _table = "sp_test_" + suffix;
        _spInsert = "sp_insert_" + suffix;
        _spQuery = "sp_query_" + suffix;
        _spInOut = "sp_inout_" + suffix;

        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();

        // 切换到可操作的数据库（sys 库无法创建表和存储过程）
        _conn.ExecuteNonQuery("CREATE DATABASE IF NOT EXISTS `newlife_test`");
        _conn.ChangeDatabase("newlife_test");

        // 创建测试表
        _conn.ExecuteNonQuery($"CREATE TABLE IF NOT EXISTS `{_table}` (`id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(128) NOT NULL, `age` INT DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        // 创建带 IN 参数的插入存储过程
        _conn.ExecuteNonQuery($"DROP PROCEDURE IF EXISTS `{_spInsert}`");
        _conn.ExecuteNonQuery($@"
CREATE PROCEDURE `{_spInsert}`(IN p_name VARCHAR(128), IN p_age INT)
BEGIN
    INSERT INTO `{_table}` (name, age) VALUES (p_name, p_age);
END");

        // 创建带查询结果的存储过程
        _conn.ExecuteNonQuery($"DROP PROCEDURE IF EXISTS `{_spQuery}`");
        _conn.ExecuteNonQuery($@"
CREATE PROCEDURE `{_spQuery}`(IN p_min_age INT)
BEGIN
    SELECT name, age FROM `{_table}` WHERE age >= p_min_age ORDER BY age;
END");

        // 创建带 OUT 参数的存储过程
        _conn.ExecuteNonQuery($"DROP PROCEDURE IF EXISTS `{_spInOut}`");
        _conn.ExecuteNonQuery($@"
CREATE PROCEDURE `{_spInOut}`(IN p_name VARCHAR(128), OUT p_count INT)
BEGIN
    SELECT COUNT(*) INTO p_count FROM `{_table}` WHERE name = p_name;
END");
    }

    public void Dispose()
    {
        try
        {
            _conn.ExecuteNonQuery($"DROP PROCEDURE IF EXISTS `{_spInsert}`");
            _conn.ExecuteNonQuery($"DROP PROCEDURE IF EXISTS `{_spQuery}`");
            _conn.ExecuteNonQuery($"DROP PROCEDURE IF EXISTS `{_spInOut}`");
            _conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{_table}`");
        }
        catch { }

        _conn.Dispose();
    }

    [Fact]
    [DisplayName("存储过程_IN参数插入")]
    public void WhenCallWithInParamsThenInserts()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = _spInsert;
        cmd.CommandType = CommandType.StoredProcedure;

        var ps = cmd.Parameters as MySqlParameterCollection;
        ps!.AddWithValue("p_name", "SpAlice");
        ps.AddWithValue("p_age", 25);

        cmd.ExecuteNonQuery();

        // 验证插入成功
        using var verifyCmd = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}` WHERE name='SpAlice'");
        Assert.Equal(1, verifyCmd.ExecuteScalar().ToInt());
    }

    [Fact]
    [DisplayName("存储过程_IN参数查询结果集")]
    public void WhenCallWithSelectThenReturnsResultSet()
    {
        // 先插入测试数据
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name, age) VALUES ('A', 20), ('B', 30), ('C', 40)");

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = _spQuery;
        cmd.CommandType = CommandType.StoredProcedure;

        var ps = cmd.Parameters as MySqlParameterCollection;
        ps!.AddWithValue("p_min_age", 25);

        using var dr = cmd.ExecuteReader();
        var rows = 0;
        while (dr.Read())
        {
            rows++;
        }

        Assert.Equal(2, rows); // B(30) 和 C(40) 符合条件
    }

    [Fact]
    [DisplayName("存储过程_OUT参数")]
    public void WhenCallWithOutParamsThenReturnsOutput()
    {
        // 先插入测试数据
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name, age) VALUES ('OutTest', 10), ('OutTest', 20), ('OutTest', 30)");

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = _spInOut;
        cmd.CommandType = CommandType.StoredProcedure;

        var ps = cmd.Parameters as MySqlParameterCollection;
        ps!.AddWithValue("p_name", "OutTest");

        var pOut = new MySqlParameter { ParameterName = "p_count", Direction = ParameterDirection.Output };
        ps.Add(pOut);

        cmd.ExecuteNonQuery();

        // OUT 参数应被设置
        Assert.Equal(3, pOut.Value.ToInt());
    }

    [Fact]
    [DisplayName("存储过程_多次调用不同参数")]
    public void WhenCallMultipleTimesThenAllSucceed()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = _spInsert;
        cmd.CommandType = CommandType.StoredProcedure;

        var ps = cmd.Parameters as MySqlParameterCollection;
        ps!.AddWithValue("p_name", "Multi1");
        ps.AddWithValue("p_age", 10);
        cmd.ExecuteNonQuery();

        // 修改参数重新调用
        cmd.Parameters[0].Value = "Multi2";
        cmd.Parameters[1].Value = 20;
        cmd.ExecuteNonQuery();

        cmd.Parameters[0].Value = "Multi3";
        cmd.Parameters[1].Value = 30;
        cmd.ExecuteNonQuery();

        // 验证
        using var verifyCmd = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}`");
        Assert.Equal(3, verifyCmd.ExecuteScalar().ToInt());
    }
}
