using System.ComponentModel;
using System.Data;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>MySqlDataReader 类型化读取与元数据测试。验证 GetXxx、流式读取、列名索引与多结果集</summary>
[Collection(TestCollections.DataModification)]
public class MySqlDataReaderTypedTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public MySqlDataReaderTypedTests()
    {
        _table = "reader_typed_" + Rand.Next(10000);
        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();
        _conn.ExecuteNonQuery($@"CREATE TEMPORARY TABLE IF NOT EXISTS `{_table}` (
            `b` TINYINT, `s` SMALLINT, `i` INT, `l` BIGINT,
            `f` FLOAT, `d` DOUBLE, `dec` DECIMAL(10,2),
            `str` VARCHAR(64), `bin` BLOB, `dt` DATETIME, `guid` CHAR(36),
            `flag` BIT(1), `nul` INT, `tm` TIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        // dec 是 MySQL 保留字，列名统一加反引号
        _conn.ExecuteNonQuery($@"INSERT INTO `{_table}` (`b`,`s`,`i`,`l`,`f`,`d`,`dec`,`str`,`bin`,`dt`,`guid`,`flag`,`nul`,`tm`) VALUES
            (1, 2, 3, 4, 1.5, 2.5, 3.25, 'abc', X'0102FF', '2025-07-01 12:30:00', '00000000-0000-0000-0000-000000000001', b'1', NULL, '12:34:56')");
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TEMPORARY TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    private MySqlDataReader OpenReader(String sql)
    {
        // 读取器持有命令引用，命令随读取器一起释放
        var cmd = new MySqlCommand(_conn, sql);
        return (MySqlDataReader)cmd.ExecuteReader();
    }

    [Fact]
    [DisplayName("类型化Get方法读取各类列")]
    public void WhenReadThenTypedGettersReturnValues()
    {
        using var dr = OpenReader($"SELECT * FROM `{_table}`");
        Assert.True(dr.Read());

        Assert.Equal((Byte)1, dr.GetByte(0));
        Assert.Equal((Int16)2, dr.GetInt16(1));
        Assert.Equal(3, dr.GetInt32(2));
        Assert.Equal(4L, dr.GetInt64(3));
        Assert.Equal(1.5f, dr.GetFloat(4));
        Assert.Equal(2.5, dr.GetDouble(5));
        Assert.Equal(3.25m, dr.GetDecimal(6));
        Assert.Equal("abc", dr.GetString(7));
        Assert.Equal(new DateTime(2025, 7, 1, 12, 30, 0), dr.GetDateTime(9));
        Assert.Equal(new Guid("00000000-0000-0000-0000-000000000001"), dr.GetGuid(10));
        Assert.True(dr.GetBoolean(11));
        Assert.True(dr.IsDBNull(12));
        Assert.Equal(new TimeSpan(12, 34, 56), (TimeSpan)dr.GetValue(13));
    }

    [Fact]
    [DisplayName("GetChar读取单字符列")]
    public void WhenGetCharThenSingleCharacter()
    {
        using var dr = OpenReader("SELECT 'Z' AS ch");
        Assert.True(dr.Read());

        Assert.Equal('Z', dr.GetChar(0));
    }

    [Fact]
    [DisplayName("按列名与序号读取值")]
    public void WhenIndexerThenValueByOrdinalOrName()
    {
        using var dr = OpenReader($"SELECT * FROM `{_table}`");
        Assert.True(dr.Read());

        Assert.Equal(3, (Int32)dr["i"]!);
        Assert.Equal(3, (Int32)dr["I"]!);
        Assert.Equal(3, (Int32)dr[2]!);
        Assert.Equal("abc", dr["str"]?.ToString());
    }

    [Fact]
    [DisplayName("GetValues按缓冲区长度截断复制")]
    public void WhenGetValuesThenRespectBufferLength()
    {
        using var dr = OpenReader($"SELECT * FROM `{_table}`");
        Assert.True(dr.Read());

        var all = new Object[dr.FieldCount];
        Assert.Equal(dr.FieldCount, dr.GetValues(all));
        Assert.Equal((SByte)1, (SByte)all[0]!);

        var partial = new Object[3];
        Assert.Equal(3, dr.GetValues(partial));
        Assert.Equal((SByte)1, (SByte)partial[0]!);
    }

    [Fact]
    [DisplayName("GetBytes与GetChars支持偏移和长度")]
    public void WhenGetBytesOrCharsThenPartialRead()
    {
        using var dr = OpenReader($"SELECT * FROM `{_table}`");
        Assert.True(dr.Read());

        var buf = new Byte[8];
        Assert.Equal(3L, dr.GetBytes(8, 0, buf, 0, 0));
        Byte[] expected = [0x01, 0x02, 0xFF];
        Assert.Equal(expected, buf[..3]);

        Assert.Equal(1L, dr.GetBytes(8, 1, buf, 0, 1));
        Assert.Equal((Byte)0x02, buf[0]);

        var chars = new Char[3];
        Assert.Equal(3L, dr.GetChars(7, 0, chars, 0, 3));
        Assert.Equal("abc", new String(chars));

        Assert.Equal(2L, dr.GetChars(7, 1, chars, 0, 2));
        Assert.Equal("bc", new String(chars[..2]));
    }

    [Fact]
    [DisplayName("列元数据名称序号与类型")]
    public void WhenMetadataThenNamesOrdinalsAndTypes()
    {
        using var dr = OpenReader($"SELECT * FROM `{_table}`");
        Assert.True(dr.Read());

        Assert.Equal(14, dr.FieldCount);
        Assert.Equal(0, dr.Depth);
        Assert.True(dr.HasRows);
        Assert.False(dr.IsClosed);

        Assert.Equal("b", dr.GetName(0));
        Assert.Equal("i", dr.GetName(2));
        Assert.Equal(2, dr.GetOrdinal("i"));
        Assert.Equal(2, dr.GetOrdinal("I"));
        Assert.Equal(-1, dr.GetOrdinal("not_exists"));

        Assert.Equal(typeof(SByte), dr.GetFieldType(0));
        Assert.Equal(typeof(Int32), dr.GetFieldType(2));
        Assert.Equal(typeof(String), dr.GetFieldType(7));
        Assert.Equal(typeof(Byte[]), dr.GetFieldType(8));
        Assert.Equal(typeof(DateTime), dr.GetFieldType(9));
        Assert.Equal(typeof(TimeSpan), dr.GetFieldType(13));
        Assert.False(dr.GetDataTypeName(0).IsNullOrEmpty());

        dr.Close();
        Assert.True(dr.IsClosed);
    }

    [Fact]
    [DisplayName("GetSchemaTable返回全部列元数据")]
    public void WhenGetSchemaTableThenAllColumnsListed()
    {
        using var dr = OpenReader($"SELECT * FROM `{_table}`");
        Assert.True(dr.Read());

        var schema = dr.GetSchemaTable();

        Assert.Equal(dr.FieldCount, schema.Rows.Count);
        Assert.Equal("b", schema.Rows[0]["ColumnName"]?.ToString());
        Assert.Equal(0, (Int32)schema.Rows[0]["ColumnOrdinal"]!);
        Assert.Equal("i", schema.Rows[2]["ColumnName"]?.ToString());
    }

    [Fact]
    [DisplayName("多结果集逐条前进")]
    public void WhenMultipleResultsThenNextResultAdvances()
    {
        using var dr = OpenReader("SELECT 1 AS a; SELECT 2 AS b, 3 AS c");

        Assert.True(dr.Read());
        Assert.Equal(1, dr.GetInt32(0));
        Assert.Equal(1, dr.FieldCount);

        Assert.True(dr.NextResult());
        Assert.Equal(2, dr.FieldCount);
        Assert.True(dr.Read());
        Assert.Equal(2, dr.GetInt32(0));
        Assert.Equal(3, dr.GetInt32(1));

        Assert.False(dr.NextResult());
    }

    [Fact]
    [DisplayName("无结果集时HasRows为false")]
    public void WhenNoResultRowsThenHasRowsFalse()
    {
        using var dr = OpenReader($"SELECT * FROM `{_table}` WHERE 1=0");

        Assert.False(dr.HasRows);
        Assert.False(dr.Read());
        Assert.Equal(14, dr.FieldCount);
    }

    [Fact]
    [DisplayName("枚举器逐个返回当前行字段值")]
    public void WhenEnumerateThenCurrentRowValues()
    {
        using var dr = OpenReader($"SELECT i, str FROM `{_table}`");
        Assert.True(dr.Read());

        var values = new List<Object?>();
        foreach (var item in dr)
            values.Add(item);

        Assert.Equal(2, values.Count);
        Assert.Equal(3, (Int32)values[0]!);
        Assert.Equal("abc", values[1]?.ToString());
    }
}
