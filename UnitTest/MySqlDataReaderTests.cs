using System.Reflection;
using NewLife.MySql;

namespace UnitTest;

/// <summary>MySqlDataReader 类型转换测试。验证 GetXxx 方法能安全处理跨类型转换</summary>
[Collection(TestCollections.InMemory)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class MySqlDataReaderTests
{
    /// <summary>创建带有指定值数组的 MySqlDataReader 实例</summary>
    /// <param name="values">值数组</param>
    /// <returns>已初始化的 MySqlDataReader</returns>
    private static MySqlDataReader CreateReader(Object[] values)
    {
        var reader = new MySqlDataReader();
        var type = typeof(MySqlDataReader);

        // 设置私有字段 _Values
        var valuesField = type.GetField("_Values", BindingFlags.NonPublic | BindingFlags.Instance)!;
        valuesField.SetValue(reader, values);

        // 设置私有字段 _FieldCount
        var fieldCountField = type.GetField("_FieldCount", BindingFlags.NonPublic | BindingFlags.Instance)!;
        fieldCountField.SetValue(reader, values.Length);

        return reader;
    }

    [Fact(DisplayName = "GetInt32_从Int64安全转换")]
    public void GetInt32_FromInt64()
    {
        var reader = CreateReader([(Object)(Int64)42]);

        var result = reader.GetInt32(0);

        Assert.Equal(42, result);
    }

    [Fact(DisplayName = "GetInt32_从Int16安全转换")]
    public void GetInt32_FromInt16()
    {
        var reader = CreateReader([(Object)(Int16)123]);

        var result = reader.GetInt32(0);

        Assert.Equal(123, result);
    }

    [Fact(DisplayName = "GetInt32_从Byte安全转换")]
    public void GetInt32_FromByte()
    {
        var reader = CreateReader([(Object)(Byte)200]);

        var result = reader.GetInt32(0);

        Assert.Equal(200, result);
    }

    [Fact(DisplayName = "GetInt32_从String安全转换")]
    public void GetInt32_FromString()
    {
        var reader = CreateReader([(Object)"12345"]);

        var result = reader.GetInt32(0);

        Assert.Equal(12345, result);
    }

    [Fact(DisplayName = "GetInt64_从Int32安全转换")]
    public void GetInt64_FromInt32()
    {
        var reader = CreateReader([(Object)(Int32)42]);

        var result = reader.GetInt64(0);

        Assert.Equal(42L, result);
    }

    [Fact(DisplayName = "GetInt16_从Int32安全转换")]
    public void GetInt16_FromInt32()
    {
        var reader = CreateReader([(Object)(Int32)100]);

        var result = reader.GetInt16(0);

        Assert.Equal((Int16)100, result);
    }

    [Fact(DisplayName = "GetInt16_从Int64安全转换")]
    public void GetInt16_FromInt64()
    {
        var reader = CreateReader([(Object)(Int64)200]);

        var result = reader.GetInt16(0);

        Assert.Equal((Int16)200, result);
    }

    [Fact(DisplayName = "GetBoolean_从Int32安全转换")]
    public void GetBoolean_FromInt32()
    {
        var reader = CreateReader([(Object)(Int32)1]);

        var result = reader.GetBoolean(0);

        Assert.True(result);
    }

    [Fact(DisplayName = "GetBoolean_从Int64安全转换")]
    public void GetBoolean_FromInt64()
    {
        var reader = CreateReader([(Object)(Int64)0]);

        var result = reader.GetBoolean(0);

        Assert.False(result);
    }

    [Fact(DisplayName = "GetByte_从Int32安全转换")]
    public void GetByte_FromInt32()
    {
        var reader = CreateReader([(Object)(Int32)255]);

        var result = reader.GetByte(0);

        Assert.Equal((Byte)255, result);
    }

    [Fact(DisplayName = "GetDouble_从Single安全转换")]
    public void GetDouble_FromSingle()
    {
        var reader = CreateReader([(Object)(Single)3.14f]);

        var result = reader.GetDouble(0);

        Assert.Equal(3.14f, result, 0.001);
    }

    [Fact(DisplayName = "GetDouble_从Int32安全转换")]
    public void GetDouble_FromInt32()
    {
        var reader = CreateReader([(Object)(Int32)42]);

        var result = reader.GetDouble(0);

        Assert.Equal(42.0, result);
    }

    [Fact(DisplayName = "GetFloat_从Double安全转换")]
    public void GetFloat_FromDouble()
    {
        var reader = CreateReader([(Object)(Double)3.14]);

        var result = reader.GetFloat(0);

        Assert.Equal(3.14f, result, 0.01f);
    }

    [Fact(DisplayName = "GetDecimal_从Int64安全转换")]
    public void GetDecimal_FromInt64()
    {
        var reader = CreateReader([(Object)(Int64)99999]);

        var result = reader.GetDecimal(0);

        Assert.Equal(99999m, result);
    }

    [Fact(DisplayName = "GetDecimal_从Double安全转换")]
    public void GetDecimal_FromDouble()
    {
        var reader = CreateReader([(Object)(Double)123.45]);

        var result = reader.GetDecimal(0);

        Assert.Equal(123.45m, result);
    }

    [Fact(DisplayName = "GetDateTime_从String安全转换")]
    public void GetDateTime_FromString()
    {
        var reader = CreateReader([(Object)"2025-01-15 10:30:00"]);

        var result = reader.GetDateTime(0);

        Assert.Equal(new DateTime(2025, 1, 15, 10, 30, 0), result);
    }

    [Fact(DisplayName = "GetGuid_从String安全转换")]
    public void GetGuid_FromString()
    {
        var guid = Guid.NewGuid();
        var reader = CreateReader([(Object)guid.ToString()]);

        var result = reader.GetGuid(0);

        Assert.Equal(guid, result);
    }

    [Fact(DisplayName = "GetGuid_从ByteArray安全转换")]
    public void GetGuid_FromByteArray()
    {
        var guid = Guid.NewGuid();
        var reader = CreateReader([(Object)guid.ToByteArray()]);

        var result = reader.GetGuid(0);

        Assert.Equal(guid, result);
    }

    [Fact(DisplayName = "GetString_从ByteArray转换为UTF8字符串")]
    public void GetString_FromByteArray()
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes("hello");
        var reader = CreateReader([(Object)bytes]);

        var result = reader.GetString(0);

        Assert.Equal("hello", result);
    }

    [Fact(DisplayName = "GetString_从Int32转换为字符串")]
    public void GetString_FromInt32()
    {
        var reader = CreateReader([(Object)42]);

        var result = reader.GetString(0);

        Assert.Equal("42", result);
    }

    [Fact(DisplayName = "GetString_原生String直接返回")]
    public void GetString_NativeString()
    {
        var reader = CreateReader([(Object)"hello"]);

        var result = reader.GetString(0);

        Assert.Equal("hello", result);
    }

    [Fact(DisplayName = "GetValue_返回原始值")]
    public void GetValue_ReturnsRawValue()
    {
        var reader = CreateReader([(Object)42, (Object)"hello"]);

        Assert.Equal(42, reader.GetValue(0));
        Assert.Equal("hello", reader.GetValue(1));
    }

    [Fact(DisplayName = "GetValues_复制当前行到数组")]
    public void GetValues_CopiesCurrentRow()
    {
        var reader = CreateReader([(Object)1, (Object)"name", (Object)3.14]);
        var values = new Object[3];

        var count = reader.GetValues(values);

        Assert.Equal(3, count);
        Assert.Equal(1, values[0]);
        Assert.Equal("name", values[1]);
        Assert.Equal(3.14, values[2]);
    }

    [Fact(DisplayName = "GetValues_目标数组较小时只复制部分")]
    public void GetValues_SmallerArrayCopiesPartial()
    {
        var reader = CreateReader([(Object)1, (Object)"name", (Object)3.14]);
        var values = new Object[2];

        var count = reader.GetValues(values);

        Assert.Equal(2, count);
        Assert.Equal(1, values[0]);
        Assert.Equal("name", values[1]);
    }

    [Fact(DisplayName = "IsDBNull_DBNull返回true")]
    public void IsDBNull_WhenDBNullThenTrue()
    {
        var reader = CreateReader([(Object)DBNull.Value]);

        Assert.True(reader.IsDBNull(0));
    }

    [Fact(DisplayName = "IsDBNull_正常值返回false")]
    public void IsDBNull_WhenNormalValueThenFalse()
    {
        var reader = CreateReader([(Object)42]);

        Assert.False(reader.IsDBNull(0));
    }

    [Fact(DisplayName = "HasRows_有值时返回true")]
    public void HasRows_WhenValuesExistThenTrue()
    {
        var reader = CreateReader([(Object)42]);

        Assert.True(reader.HasRows);
    }

    [Fact(DisplayName = "Depth_始终返回0")]
    public void Depth_AlwaysReturnsZero()
    {
        var reader = CreateReader([(Object)42]);

        Assert.Equal(0, reader.Depth);
    }

    [Fact(DisplayName = "FieldCount_返回正确字段数")]
    public void FieldCount_ReturnsCorrectCount()
    {
        var reader = CreateReader([(Object)1, (Object)2, (Object)3]);

        Assert.Equal(3, reader.FieldCount);
    }

    [Fact(DisplayName = "Close_关闭后IsClosed为true")]
    public void Close_SetsIsClosedToTrue()
    {
        var reader = CreateReader([(Object)42]);

        reader.Close();

        Assert.True(reader.IsClosed);
    }

    [Fact(DisplayName = "Close_重复关闭不抛异常")]
    public void Close_IdempotentCallDoesNotThrow()
    {
        var reader = CreateReader([(Object)42]);

        reader.Close();
        reader.Close();

        Assert.True(reader.IsClosed);
    }

    [Fact(DisplayName = "GetBytes_从ByteArray读取到缓冲区")]
    public void GetBytes_ReadsIntoBuffer()
    {
        var data = new Byte[] { 0xCA, 0xFE, 0xBA, 0xBE };
        var reader = CreateReader([(Object)data]);

        var buffer = new Byte[4];
        var count = reader.GetBytes(0, 0, buffer, 0, 4);

        Assert.Equal(4, count);
        Assert.Equal(data, buffer);
    }

    [Fact(DisplayName = "GetBytes_偏移读取")]
    public void GetBytes_WithOffset()
    {
        var data = new Byte[] { 0x01, 0x02, 0x03, 0x04 };
        var reader = CreateReader([(Object)data]);

        var buffer = new Byte[2];
        var count = reader.GetBytes(0, 2, buffer, 0, 2);

        Assert.Equal(2, count);
        Assert.Equal(new Byte[] { 0x03, 0x04 }, buffer);
    }

    [Fact(DisplayName = "GetChars_从String读取到字符缓冲区")]
    public void GetChars_ReadsIntoBuffer()
    {
        var reader = CreateReader([(Object)"hello"]);

        var buffer = new Char[5];
        var count = reader.GetChars(0, 0, buffer, 0, 5);

        Assert.Equal(5, count);
        Assert.Equal("hello".ToCharArray(), buffer);
    }

    [Fact(DisplayName = "GetChar_返回字符")]
    public void GetChar_ReturnsChar()
    {
        var reader = CreateReader([(Object)'A']);

        var result = reader.GetChar(0);

        Assert.Equal('A', result);
    }
}
