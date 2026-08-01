using System.Data;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>MySqlBulkCopy 批量复制测试。自建测试表，验证批量写入功能</summary>
[Collection(TestCollections.TableOperations)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class MySqlBulkCopyTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public MySqlBulkCopyTests()
    {
        _table = "bulk_test_" + Rand.Next(10000);
        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();
        var sql = $@"CREATE TABLE IF NOT EXISTS `{_table}` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `name` VARCHAR(100) NOT NULL,
            `age` INT NOT NULL DEFAULT 0,
            `score` DECIMAL(10,2) DEFAULT NULL,
            `created` DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        _conn.ExecuteNonQuery(sql);
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    #region 属性测试
    [Fact(DisplayName = "MySqlBulkCopy默认属性值")]
    public void DefaultProperties()
    {
        var bulkCopy = new MySqlBulkCopy();

        Assert.Equal(1000, bulkCopy.BatchSize);
        Assert.Equal(30, bulkCopy.BulkCopyTimeout);
        Assert.Equal(0, bulkCopy.NotifyAfter);
        Assert.Empty(bulkCopy.ColumnMappings);
        Assert.Equal(0, bulkCopy.TotalRowsCopied);
    }

    [Fact(DisplayName = "MySqlBulkCopy设置DestinationTableName")]
    public void SetDestinationTableName()
    {
        var bulkCopy = new MySqlBulkCopy
        {
            DestinationTableName = "test_table"
        };

        Assert.Equal("test_table", bulkCopy.DestinationTableName);
    }

    [Fact(DisplayName = "MySqlBulkCopy设置BatchSize")]
    public void SetBatchSize()
    {
        var bulkCopy = new MySqlBulkCopy
        {
            BatchSize = 500
        };

        Assert.Equal(500, bulkCopy.BatchSize);
    }

    [Fact(DisplayName = "MySqlBulkCopy_ColumnMapping构造")]
    public void ColumnMapping_Constructor()
    {
        var mapping1 = new MySqlBulkCopyColumnMapping(0, "col_dest");
        Assert.Equal(0, mapping1.SourceOrdinal);
        Assert.Equal("col_dest", mapping1.DestinationColumn);

        var mapping2 = new MySqlBulkCopyColumnMapping("src_col", "dest_col");
        Assert.Equal("src_col", mapping2.SourceColumn);
        Assert.Equal("dest_col", mapping2.DestinationColumn);
    }
    #endregion

    #region DataTable 批量写入
    [Fact(DisplayName = "MySqlBulkCopy_WriteToServer_DataTable写入成功")]
    public void WriteToServer_DataTable_Success()
    {
        var table = new DataTable();
        table.Columns.Add("name", typeof(String));
        table.Columns.Add("age", typeof(Int32));
        table.Columns.Add("score", typeof(Decimal));

        for (var i = 0; i < 10; i++)
        {
            table.Rows.Add($"user_{i}", 20 + i, (Decimal)(80.5 + i));
        }

        var bulkCopy = new MySqlBulkCopy(_conn)
        {
            DestinationTableName = _table
        };

        bulkCopy.WriteToServer(table);

        // 验证数据已写入
        using var cmd = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}`");
        var count = cmd.ExecuteScalar().ToInt();
        Assert.Equal(10, count);
    }

    [Fact(DisplayName = "MySqlBulkCopy_WriteToServer_DataTable空表不写入")]
    public void WriteToServer_DataTable_Empty()
    {
        var table = new DataTable();
        table.Columns.Add("name", typeof(String));

        var bulkCopy = new MySqlBulkCopy(_conn)
        {
            DestinationTableName = _table
        };

        // 空表不应抛异常
        bulkCopy.WriteToServer(table);

        using var cmd = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}`");
        var count = cmd.ExecuteScalar().ToInt();
        Assert.Equal(0, count);
    }

    [Fact(DisplayName = "MySqlBulkCopy_WriteToServer_大数据量分批写入")]
    public void WriteToServer_DataTable_LargeBatch()
    {
        var table = new DataTable();
        table.Columns.Add("name", typeof(String));
        table.Columns.Add("age", typeof(Int32));

        var rowCount = 500;
        for (var i = 0; i < rowCount; i++)
        {
            table.Rows.Add($"user_{i}", 20 + i % 50);
        }

        var bulkCopy = new MySqlBulkCopy(_conn)
        {
            DestinationTableName = _table,
            BatchSize = 200
        };

        bulkCopy.WriteToServer(table);

        // 验证所有行都写入
        using var cmd = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}`");
        var count = cmd.ExecuteScalar().ToInt();
        Assert.Equal(rowCount, count);

        // 验证数据正确
        using var cmd2 = new MySqlCommand(_conn, $"SELECT name FROM `{_table}` WHERE name='user_250'");
        var name = cmd2.ExecuteScalar()?.ToString();
        Assert.Equal("user_250", name);
    }
    #endregion

    #region 列映射测试
    [Fact(DisplayName = "MySqlBulkCopy_WriteToServer_列映射写入")]
    public void WriteToServer_ColumnMapping()
    {
        var table = new DataTable();
        table.Columns.Add("src_name", typeof(String));
        table.Columns.Add("src_age", typeof(Int32));

        for (var i = 0; i < 5; i++)
        {
            table.Rows.Add($"mapped_{i}", 30 + i);
        }

        var bulkCopy = new MySqlBulkCopy(_conn)
        {
            DestinationTableName = _table
        };
        bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping("src_name", "name"));
        bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping("src_age", "age"));

        bulkCopy.WriteToServer(table);

        using var cmd = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}` WHERE name LIKE 'mapped_%'");
        var count = cmd.ExecuteScalar().ToInt();
        Assert.Equal(5, count);
    }
    #endregion

    #region RowsCopied 事件
    [Fact(DisplayName = "MySqlBulkCopy_RowsCopied事件触发")]
    public void RowsCopied_Event()
    {
        var table = new DataTable();
        table.Columns.Add("name", typeof(String));
        table.Columns.Add("age", typeof(Int32));

        for (var i = 0; i < 100; i++)
        {
            table.Rows.Add($"event_{i}", 25);
        }

        var eventCount = 0;
        var lastRowsCopied = 0L;
        var bulkCopy = new MySqlBulkCopy(_conn)
        {
            DestinationTableName = _table,
            BatchSize = 30,
            NotifyAfter = 50
        };
        bulkCopy.RowsCopied += (sender, e) =>
        {
            eventCount++;
            lastRowsCopied = e.RowsCopied;
        };

        bulkCopy.WriteToServer(table);

        Assert.True(eventCount > 0);
        Assert.True(lastRowsCopied > 0);
    }
    #endregion

    #region 值数组批量执行
    [Fact(DisplayName = "MySqlCommand_ExecuteBatchValuesAsync_值数组批量写入")]
    public async Task ExecuteBatchValuesAsync_ValueSets()
    {
        var sets = new List<Object?[]>();
        for (var i = 0; i < 10; i++)
            sets.Add(new Object?[] { $"val_{i}", 20 + i, (Decimal)(80.5 + i) });

        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (name, age, score) VALUES (@name, @age, @score)");
        var affected = await cmd.ExecuteBatchValuesAsync(sets);

        Assert.Equal(10, affected);

        // 验证数据正确落库且值无错位
        using var query = new MySqlCommand(_conn, $"SELECT COUNT(*), SUM(age), MIN(score) FROM `{_table}`");
        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(10, reader.GetInt32(0));
        var sum = 0;
        for (var i = 0; i < 10; i++) sum += 20 + i;
        Assert.Equal(sum, reader.GetInt32(1));
        Assert.Equal((Decimal)80.5, reader.GetDecimal(2));
    }

    [Fact(DisplayName = "MySqlCommand_ExecuteBatchValues_同步值数组批量")]
    public void ExecuteBatchValues_Sync()
    {
        var sets = new List<Object?[]>
        {
            new Object?[] { "sync_0", 1, (Decimal)1.5 },
            new Object?[] { "sync_1", 2, null }
        };

        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (name, age, score) VALUES (@name, @age, @score)");
        var affected = cmd.ExecuteBatchValues(sets);

        Assert.Equal(2, affected);

        using var query = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}` WHERE score IS NULL");
        Assert.Equal(1, query.ExecuteScalar().ToInt());
    }

    [Fact(DisplayName = "MySqlCommand_ExecuteBatchValues_空集合返回0")]
    public void ExecuteBatchValues_Empty()
    {
        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (name, age, score) VALUES (@name, @age, @score)");
        var affected = cmd.ExecuteBatchValues(new List<Object?[]>());

        Assert.Equal(0, affected);
    }
    #endregion

    #region 异常测试
    [Fact(DisplayName = "MySqlBulkCopy_DestinationTableName未设置时抛异常")]
    public void WriteToServer_NoDestinationTable_Throws()
    {
        var table = new DataTable();
        table.Columns.Add("name", typeof(String));
        table.Rows.Add("test");

        var bulkCopy = new MySqlBulkCopy(_conn);
        // DestinationTableName 未设置

        var ex = Assert.Throws<InvalidOperationException>(() => bulkCopy.WriteToServer(table));
        Assert.Contains("DestinationTableName", ex.Message);
    }

    [Fact(DisplayName = "MySqlBulkCopy_Connection未设置时抛异常")]
    public void WriteToServer_NoConnection_Throws()
    {
        var table = new DataTable();
        table.Columns.Add("name", typeof(String));
        table.Rows.Add("test");

        var bulkCopy = new MySqlBulkCopy
        {
            DestinationTableName = "some_table"
        };
        // Connection 未设置

        var ex = Assert.Throws<InvalidOperationException>(() => bulkCopy.WriteToServer(table));
        Assert.Contains("Connection", ex.Message);
    }

    [Fact(DisplayName = "MySqlBulkCopy_DataTable为null时抛ArgumentNullException")]
    public void WriteToServer_NullDataTable_Throws()
    {
        var bulkCopy = new MySqlBulkCopy(_conn)
        {
            DestinationTableName = "some_table"
        };

        Assert.Throws<ArgumentNullException>(() => bulkCopy.WriteToServer((DataTable)null!));
    }
    #endregion
}
