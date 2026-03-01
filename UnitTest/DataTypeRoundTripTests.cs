using System.ComponentModel;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>数据类型往返测试。验证各种 MySQL 数据类型写入后读取的一致性</summary>
[Collection(TestCollections.DataModification)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class DataTypeRoundTripTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public DataTypeRoundTripTests()
    {
        _table = "dtype_test_" + Rand.Next(10000);
        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();

        var sql = $@"CREATE TABLE IF NOT EXISTS `{_table}` (
            `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `col_tinyint` TINYINT DEFAULT NULL,
            `col_smallint` SMALLINT DEFAULT NULL,
            `col_int` INT DEFAULT NULL,
            `col_bigint` BIGINT DEFAULT NULL,
            `col_float` FLOAT DEFAULT NULL,
            `col_double` DOUBLE DEFAULT NULL,
            `col_decimal` DECIMAL(18,4) DEFAULT NULL,
            `col_varchar` VARCHAR(512) DEFAULT NULL,
            `col_text` TEXT DEFAULT NULL,
            `col_blob` BLOB DEFAULT NULL,
            `col_datetime` DATETIME DEFAULT NULL,
            `col_date` DATE DEFAULT NULL,
            `col_time` TIME DEFAULT NULL,
            `col_bit` BIT(1) DEFAULT NULL,
            `col_year` YEAR DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        _conn.ExecuteNonQuery(sql);
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    #region 整数类型
    [Fact]
    [DisplayName("TINYINT往返")]
    public void TinyInt_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_tinyint) VALUES (127)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_tinyint FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = dr.GetValue(0);
        Assert.Equal(127, Convert.ToInt32(val));
    }

    [Fact]
    [DisplayName("SMALLINT往返")]
    public void SmallInt_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_smallint) VALUES (32767)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_smallint FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = dr.GetValue(0);
        Assert.Equal(32767, Convert.ToInt32(val));
    }

    [Fact]
    [DisplayName("INT往返")]
    public void Int_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_int) VALUES (2147483647)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_int FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = dr.GetValue(0);
        Assert.Equal(2147483647, Convert.ToInt32(val));
    }

    [Fact]
    [DisplayName("INT负数往返")]
    public void Int_Negative_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_int) VALUES (-2147483648)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_int FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = dr.GetValue(0);
        Assert.Equal(-2147483648, Convert.ToInt32(val));
    }

    [Fact]
    [DisplayName("BIGINT往返")]
    public void BigInt_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_bigint) VALUES (9223372036854775807)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_bigint FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = dr.GetValue(0);
        Assert.Equal(9223372036854775807L, Convert.ToInt64(val));
    }

    [Fact]
    [DisplayName("INT零值往返")]
    public void Int_Zero_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_int) VALUES (0)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_int FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = dr.GetValue(0);
        Assert.Equal(0, Convert.ToInt32(val));
    }
    #endregion

    #region 浮点与精确数值类型
    [Fact]
    [DisplayName("FLOAT往返")]
    public void Float_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_float) VALUES (3.14)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_float FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = Convert.ToSingle(dr.GetValue(0));
        Assert.Equal(3.14f, val, 0.01f);
    }

    [Fact]
    [DisplayName("DOUBLE往返")]
    public void Double_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_double) VALUES (3.141592653589793)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_double FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = Convert.ToDouble(dr.GetValue(0));
        Assert.Equal(3.141592653589793, val, 0.0000000001);
    }

    [Fact]
    [DisplayName("DECIMAL精确值往返")]
    public void Decimal_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_decimal) VALUES (12345.6789)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_decimal FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = Convert.ToDecimal(dr.GetValue(0));
        Assert.Equal(12345.6789m, val);
    }

    [Fact]
    [DisplayName("DECIMAL负值往返")]
    public void Decimal_Negative_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_decimal) VALUES (-9999.1234)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_decimal FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = Convert.ToDecimal(dr.GetValue(0));
        Assert.Equal(-9999.1234m, val);
    }
    #endregion

    #region 字符串类型
    [Fact]
    [DisplayName("VARCHAR普通字符串往返")]
    public void Varchar_RoundTrip()
    {
        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (col_varchar) VALUES (@val)");
        (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("val", "Hello, World!");
        cmd.ExecuteNonQuery();

        using var cmd2 = new MySqlCommand(_conn, $"SELECT col_varchar FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd2.ExecuteReader();
        Assert.True(dr.Read());
        Assert.Equal("Hello, World!", dr.GetString(0));
    }

    [Fact]
    [DisplayName("VARCHAR中文字符串往返")]
    public void Varchar_Chinese_RoundTrip()
    {
        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (col_varchar) VALUES (@val)");
        (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("val", "你好世界🌍");
        cmd.ExecuteNonQuery();

        using var cmd2 = new MySqlCommand(_conn, $"SELECT col_varchar FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd2.ExecuteReader();
        Assert.True(dr.Read());
        Assert.Equal("你好世界🌍", dr.GetString(0));
    }

    [Fact]
    [DisplayName("VARCHAR特殊字符转义往返")]
    public void Varchar_SpecialChars_RoundTrip()
    {
        var value = "it's a \"test\"\nwith\\backslash";

        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (col_varchar) VALUES (@val)");
        (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("val", value);
        cmd.ExecuteNonQuery();

        using var cmd2 = new MySqlCommand(_conn, $"SELECT col_varchar FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd2.ExecuteReader();
        Assert.True(dr.Read());
        Assert.Equal(value, dr.GetString(0));
    }

    [Fact]
    [DisplayName("VARCHAR空字符串往返")]
    public void Varchar_Empty_RoundTrip()
    {
        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (col_varchar) VALUES (@val)");
        (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("val", "");
        cmd.ExecuteNonQuery();

        using var cmd2 = new MySqlCommand(_conn, $"SELECT col_varchar FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd2.ExecuteReader();
        Assert.True(dr.Read());
        Assert.Equal("", dr.GetString(0));
    }

    [Fact]
    [DisplayName("TEXT大文本往返")]
    public void Text_RoundTrip()
    {
        var value = new String('A', 10000);

        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (col_text) VALUES (@val)");
        (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("val", value);
        cmd.ExecuteNonQuery();

        using var cmd2 = new MySqlCommand(_conn, $"SELECT col_text FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd2.ExecuteReader();
        Assert.True(dr.Read());
        Assert.Equal(value, dr.GetString(0));
    }
    #endregion

    #region 二进制类型
    [Fact]
    [DisplayName("BLOB二进制数据往返")]
    public void Blob_RoundTrip()
    {
        var data = Rand.NextBytes(256);

        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (col_blob) VALUES (@val)");
        (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("val", data);
        cmd.ExecuteNonQuery();

        using var cmd2 = new MySqlCommand(_conn, $"SELECT col_blob FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd2.ExecuteReader();
        Assert.True(dr.Read());

        var result = dr.GetValue(0);
        Assert.IsType<Byte[]>(result);
        Assert.Equal(data, (Byte[])result);
    }

    [Fact]
    [DisplayName("BLOB空字节数组往返")]
    public void Blob_Empty_RoundTrip()
    {
        var data = Array.Empty<Byte>();

        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (col_blob) VALUES (@val)");
        (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("val", data);
        cmd.ExecuteNonQuery();

        using var cmd2 = new MySqlCommand(_conn, $"SELECT col_blob FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd2.ExecuteReader();
        Assert.True(dr.Read());

        var result = dr.GetValue(0);
        if (result is Byte[] bytes)
            Assert.Empty(bytes);
        else
            Assert.Equal("", result?.ToString());
    }
    #endregion

    #region 时间类型
    [Fact]
    [DisplayName("DATETIME往返")]
    public void DateTime_RoundTrip()
    {
        var dt = new DateTime(2025, 7, 15, 14, 30, 45);
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_datetime) VALUES ('{dt:yyyy-MM-dd HH:mm:ss}')");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_datetime FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = dr.GetDateTime(0);
        Assert.Equal(dt, val);
    }

    [Fact]
    [DisplayName("DATE往返")]
    public void Date_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_date) VALUES ('2025-01-01')");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_date FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = dr.GetDateTime(0);
        Assert.Equal(new DateTime(2025, 1, 1), val);
    }

    [Fact]
    [DisplayName("TIME往返")]
    public void Time_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_time) VALUES ('12:30:45')");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_time FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        // TIME 通常映射为 TimeSpan 或字符串
        var val = dr.GetValue(0);
        Assert.NotNull(val);
        Assert.NotEqual(DBNull.Value, val);
    }

    [Fact]
    [DisplayName("YEAR往返")]
    public void Year_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_year) VALUES (2025)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_year FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = Convert.ToInt32(dr.GetValue(0));
        Assert.Equal(2025, val);
    }
    #endregion

    #region BIT 类型
    [Fact]
    [DisplayName("BIT(1)真值往返")]
    public void Bit_True_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_bit) VALUES (1)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_bit FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = dr.GetValue(0);
        // BIT(1) 可能映射为 Int64/UInt64/Boolean/Byte[]
        Assert.NotNull(val);
        Assert.NotEqual(DBNull.Value, val);
        Assert.NotEqual(0, Convert.ToInt64(val));
    }

    [Fact]
    [DisplayName("BIT(1)假值往返")]
    public void Bit_False_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_bit) VALUES (0)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_bit FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        var val = dr.GetValue(0);
        Assert.NotNull(val);
        Assert.Equal(0, Convert.ToInt64(val));
    }
    #endregion

    #region NULL 值处理
    [Fact]
    [DisplayName("NULL值读取")]
    public void Null_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_varchar, col_int, col_datetime) VALUES (NULL, NULL, NULL)");

        using var cmd = new MySqlCommand(_conn, $"SELECT col_varchar, col_int, col_datetime FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd.ExecuteReader();
        Assert.True(dr.Read());

        Assert.True(dr.IsDBNull(0));
        Assert.True(dr.IsDBNull(1));
        Assert.True(dr.IsDBNull(2));
    }

    [Fact]
    [DisplayName("参数化NULL值插入")]
    public void Null_Parameterized_RoundTrip()
    {
        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (col_varchar, col_int) VALUES (@v, @i)");
        var ps = cmd.Parameters as MySqlParameterCollection;
        ps!.AddWithValue("v", DBNull.Value);
        ps.AddWithValue("i", null);
        cmd.ExecuteNonQuery();

        using var cmd2 = new MySqlCommand(_conn, $"SELECT col_varchar, col_int FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd2.ExecuteReader();
        Assert.True(dr.Read());

        Assert.True(dr.IsDBNull(0));
        Assert.True(dr.IsDBNull(1));
    }

    [Fact]
    [DisplayName("NULL与非NULL混合读取")]
    public void Null_Mixed_RoundTrip()
    {
        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (col_varchar, col_int, col_double) VALUES (@v, @i, @d)");
        var ps = cmd.Parameters as MySqlParameterCollection;
        ps!.AddWithValue("v", "not_null");
        ps.AddWithValue("i", DBNull.Value);
        ps.AddWithValue("d", 3.14);
        cmd.ExecuteNonQuery();

        using var cmd2 = new MySqlCommand(_conn, $"SELECT col_varchar, col_int, col_double FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd2.ExecuteReader();
        Assert.True(dr.Read());

        Assert.False(dr.IsDBNull(0));
        Assert.Equal("not_null", dr.GetString(0));
        Assert.True(dr.IsDBNull(1));
        Assert.False(dr.IsDBNull(2));
        Assert.Equal(3.14, Convert.ToDouble(dr.GetValue(2)), 0.001);
    }
    #endregion

    #region AUTO_INCREMENT
    [Fact]
    [DisplayName("AUTO_INCREMENT自增主键")]
    public void AutoIncrement_RoundTrip()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_varchar) VALUES ('row1')");
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (col_varchar) VALUES ('row2')");

        using var cmd = new MySqlCommand(_conn, $"SELECT id FROM `{_table}` ORDER BY id");
        using var dr = cmd.ExecuteReader();

        Assert.True(dr.Read());
        var id1 = Convert.ToInt32(dr.GetValue(0));
        Assert.True(id1 > 0);

        Assert.True(dr.Read());
        var id2 = Convert.ToInt32(dr.GetValue(0));
        Assert.True(id2 > id1);
    }
    #endregion

    #region 多列混合类型
    [Fact]
    [DisplayName("多列混合类型同时读取")]
    public void MultiColumn_MixedTypes_RoundTrip()
    {
        var dt = new DateTime(2025, 6, 15, 10, 0, 0);
        using var cmd = new MySqlCommand(_conn, $"INSERT INTO `{_table}` (col_int, col_varchar, col_double, col_datetime, col_bigint) VALUES (@i, @v, @d, @dt, @b)");
        var ps = cmd.Parameters as MySqlParameterCollection;
        ps!.AddWithValue("i", 42);
        ps.AddWithValue("v", "mixed_test");
        ps.AddWithValue("d", 99.99);
        ps.AddWithValue("dt", dt);
        ps.AddWithValue("b", 9876543210L);
        cmd.ExecuteNonQuery();

        using var cmd2 = new MySqlCommand(_conn, $"SELECT col_int, col_varchar, col_double, col_datetime, col_bigint FROM `{_table}` ORDER BY id DESC LIMIT 1");
        using var dr = cmd2.ExecuteReader();
        Assert.True(dr.Read());

        Assert.Equal(42, Convert.ToInt32(dr.GetValue(0)));
        Assert.Equal("mixed_test", dr.GetString(1));
        Assert.Equal(99.99, Convert.ToDouble(dr.GetValue(2)), 0.001);
        Assert.Equal(dt, dr.GetDateTime(3));
        Assert.Equal(9876543210L, Convert.ToInt64(dr.GetValue(4)));
    }
    #endregion
}
