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
}
