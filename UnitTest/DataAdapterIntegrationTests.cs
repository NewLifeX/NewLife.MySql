using System.ComponentModel;
using System.Data;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>DataAdapter 数据库集成测试。验证 MySqlDataAdapter.Fill 从真实数据库填充 DataTable</summary>
[Collection(TestCollections.DataModification)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class DataAdapterIntegrationTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public DataAdapterIntegrationTests()
    {
        _table = "adapter_test_" + Rand.Next(10000);
        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();
        var sql = $"CREATE TABLE IF NOT EXISTS `{_table}` (`id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(128) NOT NULL, `age` INT DEFAULT NULL, `score` DECIMAL(10,2) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        _conn.ExecuteNonQuery(sql);

        // 插入测试数据
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name, age, score) VALUES ('Alice', 25, 88.50), ('Bob', 30, 92.00), ('Charlie', 22, 75.50)");
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    [Fact]
    [DisplayName("Fill填充DataTable")]
    public void WhenFillThenDataTablePopulated()
    {
        using var cmd = new MySqlCommand(_conn, $"SELECT * FROM `{_table}` ORDER BY id");
        var adapter = new MySqlDataAdapter(cmd);

        var dt = new DataTable();
        var rowCount = adapter.Fill(dt);

        Assert.Equal(3, rowCount);
        Assert.Equal(3, dt.Rows.Count);
        Assert.True(dt.Columns.Count >= 4);
    }

    [Fact]
    [DisplayName("Fill验证列名")]
    public void WhenFillThenColumnNamesCorrect()
    {
        using var cmd = new MySqlCommand(_conn, $"SELECT name, age, score FROM `{_table}` ORDER BY id");
        var adapter = new MySqlDataAdapter(cmd);

        var dt = new DataTable();
        adapter.Fill(dt);

        Assert.Contains("name", dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.Contains("age", dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.Contains("score", dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
    }

    [Fact]
    [DisplayName("Fill验证数据值")]
    public void WhenFillThenDataValuesCorrect()
    {
        using var cmd = new MySqlCommand(_conn, $"SELECT name, age FROM `{_table}` ORDER BY age");
        var adapter = new MySqlDataAdapter(cmd);

        var dt = new DataTable();
        adapter.Fill(dt);

        Assert.Equal("Charlie", dt.Rows[0]["name"]?.ToString());
        Assert.Equal(22, Convert.ToInt32(dt.Rows[0]["age"]));
        Assert.Equal("Alice", dt.Rows[1]["name"]?.ToString());
        Assert.Equal(25, Convert.ToInt32(dt.Rows[1]["age"]));
        Assert.Equal("Bob", dt.Rows[2]["name"]?.ToString());
        Assert.Equal(30, Convert.ToInt32(dt.Rows[2]["age"]));
    }

    [Fact]
    [DisplayName("Fill空结果集")]
    public void WhenFillEmptyResultThenZeroRows()
    {
        using var cmd = new MySqlCommand(_conn, $"SELECT * FROM `{_table}` WHERE name='NotExist'");
        var adapter = new MySqlDataAdapter(cmd);

        var dt = new DataTable();
        var rowCount = adapter.Fill(dt);

        Assert.Equal(0, rowCount);
        Assert.Empty(dt.Rows);
        // 列定义仍然应该存在
        Assert.True(dt.Columns.Count >= 1);
    }

    [Fact]
    [DisplayName("Fill带WHERE条件")]
    public void WhenFillWithWhereClauseThenFilteredRows()
    {
        using var cmd = new MySqlCommand(_conn, $"SELECT name, age FROM `{_table}` WHERE age >= 25");
        var adapter = new MySqlDataAdapter(cmd);

        var dt = new DataTable();
        adapter.Fill(dt);

        Assert.Equal(2, dt.Rows.Count);
    }

    [Fact]
    [DisplayName("Fill使用DataSet")]
    public void WhenFillDataSetThenTableAdded()
    {
        using var cmd = new MySqlCommand(_conn, $"SELECT name, age FROM `{_table}` ORDER BY id");
        var adapter = new MySqlDataAdapter(cmd);

        var ds = new DataSet();
        var rowCount = adapter.Fill(ds, "TestTable");

        Assert.Equal(3, rowCount);
        Assert.Single(ds.Tables);
        Assert.Equal("TestTable", ds.Tables[0].TableName);
        Assert.Equal(3, ds.Tables[0].Rows.Count);
    }

    [Fact]
    [DisplayName("Fill使用SQL和连接字符串构造")]
    public void WhenFillWithSqlAndConnStrThenPopulated()
    {
        var adapter = new MySqlDataAdapter($"SELECT name FROM `{_table}` ORDER BY id", _conn);

        var dt = new DataTable();
        adapter.Fill(dt);

        Assert.Equal(3, dt.Rows.Count);
        Assert.Equal("Alice", dt.Rows[0]["name"]?.ToString());
    }

    [Fact]
    [DisplayName("Fill包含NULL值")]
    public void WhenFillWithNullValuesThenDBNull()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name, age, score) VALUES ('NullTest', NULL, NULL)");

        using var cmd = new MySqlCommand(_conn, $"SELECT name, age, score FROM `{_table}` WHERE name='NullTest'");
        var adapter = new MySqlDataAdapter(cmd);

        var dt = new DataTable();
        adapter.Fill(dt);

        Assert.Single(dt.Rows);
        Assert.Equal("NullTest", dt.Rows[0]["name"]?.ToString());
        Assert.True(dt.Rows[0]["age"] is DBNull);
        Assert.True(dt.Rows[0]["score"] is DBNull);
    }
}
