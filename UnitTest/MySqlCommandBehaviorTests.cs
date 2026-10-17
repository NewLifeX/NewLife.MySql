using System.ComponentModel;
using System.Data;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>命令行为（SchemaOnly/SingleRow）回归测试。验证会话变量 SQL_SELECT_LIMIT 在读取器关闭后恢复，不污染连接池</summary>
[Collection(TestCollections.DataModification)]
public class MySqlCommandBehaviorTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public MySqlCommandBehaviorTests()
    {
        _table = "behavior_test_" + Rand.Next(10000);
        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();
        _conn.ExecuteNonQuery($"CREATE TEMPORARY TABLE IF NOT EXISTS `{_table}` (`id` INT NOT NULL PRIMARY KEY, `name` VARCHAR(32)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (id, name) VALUES (1, 'a'), (2, 'b')");
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TEMPORARY TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    [Fact]
    [DisplayName("SchemaOnly读取后SELECT上限恢复")]
    public void WhenSchemaOnlyThenSelectLimitRestored()
    {
        using (var cmd = new MySqlCommand(_conn, $"SELECT * FROM `{_table}`"))
        using (var dr = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
        {
            Assert.Equal(2, dr.FieldCount);
            Assert.False(dr.Read());
        }

        // 会话变量若未恢复，后续查询会被 SQL_SELECT_LIMIT=0 静默截断为空结果
        using var verify = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}`");
        Assert.Equal(2, verify.ExecuteScalar().ToInt());
    }

    [Fact]
    [DisplayName("SingleRow读取后SELECT上限恢复")]
    public void WhenSingleRowThenSelectLimitRestored()
    {
        using (var cmd = new MySqlCommand(_conn, $"SELECT * FROM `{_table}` ORDER BY id"))
        using (var dr = cmd.ExecuteReader(CommandBehavior.SingleRow))
        {
            Assert.True(dr.Read());
            Assert.Equal(1, dr.GetInt32(0));
        }

        using var verify = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}`");
        Assert.Equal(2, verify.ExecuteScalar().ToInt());
    }

    [Fact]
    [DisplayName("SchemaOnly读取后的连接归还池不污染后续查询")]
    public void WhenSchemaOnlyThenPooledSessionUnpolluted()
    {
        // 使用普通表，保证换一条连接仍能访问
        var table = "behavior_pool_" + Rand.Next(10000);
        var connStr = _ConnStr;
        using (var conn = new MySqlConnection(connStr))
        {
            conn.Open();
            conn.ExecuteNonQuery($"CREATE TABLE IF NOT EXISTS `{table}` (`id` INT NOT NULL PRIMARY KEY) ENGINE=InnoDB");
            conn.ExecuteNonQuery($"INSERT INTO `{table}` (id) VALUES (1)");

            using var cmd = new MySqlCommand(conn, $"SELECT * FROM `{table}`");
            using var dr = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
            Assert.Equal(1, dr.FieldCount);
        }

        // 复用池化会话
        using var conn2 = new MySqlConnection(connStr);
        conn2.Open();
        try
        {
            using var cmd2 = new MySqlCommand(conn2, $"SELECT COUNT(*) FROM `{table}`");
            Assert.Equal(1, cmd2.ExecuteScalar().ToInt());
        }
        finally
        {
            conn2.ExecuteNonQuery($"DROP TABLE IF EXISTS `{table}`");
        }
    }
}
