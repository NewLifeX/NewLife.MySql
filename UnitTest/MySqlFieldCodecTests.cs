using System.Text;
using NewLife;
using NewLife.Buffers;
using NewLife.MySql;
using NewLife.MySql.Common;

namespace UnitTest;

/// <summary>MySQL 字段编解码器测试。覆盖文本协议和二进制协议的所有 MySqlDbType 类型</summary>
[Collection(TestCollections.InMemory)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class MySqlFieldCodecTests
{
    #region 文本协议读取测试
    /// <summary>测试文本协议解析 Decimal/NewDecimal 类型</summary>
    [Fact(DisplayName = "文本协议_Decimal")]
    public void ReadTextValue_Decimal()
    {
        // MySQL 文本协议中 Decimal 以 ASCII 十进制字符串传输
        var str = "123.456";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Decimal };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Decimal>(result);
        Assert.Equal(123.456m, result);
    }

    [Fact(DisplayName = "文本协议_NewDecimal")]
    public void ReadTextValue_NewDecimal()
    {
        var str = "-9999.9999";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.NewDecimal };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(-9999.9999m, result);
    }

    /// <summary>测试文本协议解析有符号整数类型</summary>
    [Fact(DisplayName = "文本协议_Byte")]
    public void ReadTextValue_Byte()
    {
        var str = "-127";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Byte };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<SByte>(result);
        Assert.Equal((SByte)(-127), result);
    }

    [Fact(DisplayName = "文本协议_UByte")]
    public void ReadTextValue_UByte()
    {
        var str = "255";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.UByte };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Byte>(result);
        Assert.Equal((Byte)255, result);
    }

    [Fact(DisplayName = "文本协议_Int16")]
    public void ReadTextValue_Int16()
    {
        var str = "-32768";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Int16 };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Int16>(result);
        Assert.Equal((Int16)(-32768), result);
    }

    [Fact(DisplayName = "文本协议_UInt16")]
    public void ReadTextValue_UInt16()
    {
        var str = "65535";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.UInt16 };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<UInt16>(result);
        Assert.Equal((UInt16)65535, result);
    }

    [Fact(DisplayName = "文本协议_Int24")]
    public void ReadTextValue_Int24()
    {
        var str = "-8388608";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Int24 };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Int32>(result);
        Assert.Equal(-8388608, result);
    }

    [Fact(DisplayName = "文本协议_UInt24")]
    public void ReadTextValue_UInt24()
    {
        var str = "16777215";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.UInt24 };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<UInt32>(result);
        Assert.Equal(16777215u, result);
    }

    [Fact(DisplayName = "文本协议_Int32")]
    public void ReadTextValue_Int32()
    {
        var str = "-2147483648";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Int32 };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Int32>(result);
        Assert.Equal(-2147483648, result);
    }

    [Fact(DisplayName = "文本协议_UInt32")]
    public void ReadTextValue_UInt32()
    {
        var str = "4294967295";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.UInt32 };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<UInt32>(result);
        Assert.Equal(4294967295u, result);
    }

    [Fact(DisplayName = "文本协议_Int64")]
    public void ReadTextValue_Int64()
    {
        var str = "-9223372036854775808";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Int64 };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Int64>(result);
        Assert.Equal(-9223372036854775808, result);
    }

    [Fact(DisplayName = "文本协议_UInt64")]
    public void ReadTextValue_UInt64()
    {
        var str = "18446744073709551615";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.UInt64 };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<UInt64>(result);
        Assert.Equal(18446744073709551615ul, result);
    }

    /// <summary>测试文本协议解析浮点数类型</summary>
    [Fact(DisplayName = "文本协议_Float")]
    public void ReadTextValue_Float()
    {
        var str = "3.14159";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Float };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Single>(result);
        Assert.Equal(3.14159f, (Single)result, 5);
    }

    [Fact(DisplayName = "文本协议_Double")]
    public void ReadTextValue_Double()
    {
        var str = "3.141592653589793";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Double };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Double>(result);
        Assert.Equal(3.141592653589793, (Double)result, 15);
    }

    /// <summary>测试文本协议解析日期时间类型</summary>
    [Fact(DisplayName = "文本协议_DateTime")]
    public void ReadTextValue_DateTime()
    {
        var str = "2024-12-25 10:30:45";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.DateTime };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<DateTime>(result);
        Assert.Equal(new DateTime(2024, 12, 25, 10, 30, 45), result);
    }

    [Fact(DisplayName = "文本协议_Timestamp")]
    public void ReadTextValue_Timestamp()
    {
        var str = "2024-01-15 08:15:30";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Timestamp };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<DateTime>(result);
        Assert.Equal(new DateTime(2024, 1, 15, 8, 15, 30), result);
    }

    [Fact(DisplayName = "文本协议_Date")]
    public void ReadTextValue_Date()
    {
        var str = "2024-12-31";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Date };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<DateTime>(result);
        var dt = (DateTime)result;
        Assert.Equal(2024, dt.Year);
        Assert.Equal(12, dt.Month);
        Assert.Equal(31, dt.Day);
    }

    [Fact(DisplayName = "文本协议_Time_正值")]
    public void ReadTextValue_Time_Positive()
    {
        var str = "12:34:56";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Time };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<TimeSpan>(result);
        Assert.Equal(new TimeSpan(12, 34, 56), result);
    }

    [Fact(DisplayName = "文本协议_Time_负值")]
    public void ReadTextValue_Time_Negative()
    {
        var str = "-12:34:56";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Time };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<TimeSpan>(result);
        Assert.Equal(new TimeSpan(-12, -34, -56), result);
    }

    [Fact(DisplayName = "文本协议_Time_超24小时")]
    public void ReadTextValue_Time_OverDay()
    {
        var str = "123:45:30";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Time };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<TimeSpan>(result);
        Assert.Equal(new TimeSpan(0, 123, 45, 30, 0), result);
    }

    [Fact(DisplayName = "文本协议_Time_带微秒")]
    public void ReadTextValue_Time_WithMicroseconds()
    {
        var str = "12:34:56.123456";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Time };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<TimeSpan>(result);
        var ts = (TimeSpan)result;
        Assert.Equal(12, ts.Hours);
        Assert.Equal(34, ts.Minutes);
        Assert.Equal(56, ts.Seconds);
        Assert.Equal(123, ts.Milliseconds);
    }

    [Fact(DisplayName = "文本协议_Year")]
    public void ReadTextValue_Year()
    {
        var str = "2024";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Year };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Int32>(result);
        Assert.Equal(2024, result);
    }

    /// <summary>测试文本协议解析字符串类型</summary>
    [Fact(DisplayName = "文本协议_String")]
    public void ReadTextValue_String()
    {
        var str = "Hello MySQL";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.String };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<String>(result);
        Assert.Equal("Hello MySQL", result);
    }

    [Fact(DisplayName = "文本协议_VarString")]
    public void ReadTextValue_VarString()
    {
        var str = "可变字符串";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.VarString };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<String>(result);
        Assert.Equal("可变字符串", result);
    }

    [Fact(DisplayName = "文本协议_VarChar")]
    public void ReadTextValue_VarChar()
    {
        var str = "VARCHAR 字段";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.VarChar };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal("VARCHAR 字段", result);
    }

    [Fact(DisplayName = "文本协议_Text")]
    public void ReadTextValue_Text()
    {
        var str = "This is a long text content";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Text };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(str, result);
    }

    [Fact(DisplayName = "文本协议_TinyText")]
    public void ReadTextValue_TinyText()
    {
        var str = "Tiny";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.TinyText };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(str, result);
    }

    [Fact(DisplayName = "文本协议_MediumText")]
    public void ReadTextValue_MediumText()
    {
        var str = "Medium text content";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.MediumText };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(str, result);
    }

    [Fact(DisplayName = "文本协议_LongText")]
    public void ReadTextValue_LongText()
    {
        var str = "Very long text content";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.LongText };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(str, result);
    }

    [Fact(DisplayName = "文本协议_Json")]
    public void ReadTextValue_Json()
    {
        var str = "{\"name\":\"test\",\"value\":123}";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Json };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(str, result);
    }

    [Fact(DisplayName = "文本协议_Enum")]
    public void ReadTextValue_Enum()
    {
        var str = "active";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Enum };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(str, result);
    }

    [Fact(DisplayName = "文本协议_Set")]
    public void ReadTextValue_Set()
    {
        var str = "read,write,delete";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Set };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(str, result);
    }

    /// <summary>测试文本协议解析二进制类型</summary>
    [Fact(DisplayName = "文本协议_Blob")]
    public void ReadTextValue_Blob()
    {
        var bytes = new Byte[] { 0x01, 0x02, 0x03, 0xFF, 0xFE };
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Blob };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Byte[]>(result);
        Assert.Equal(bytes, result);
    }

    [Fact(DisplayName = "文本协议_TinyBlob")]
    public void ReadTextValue_TinyBlob()
    {
        var bytes = new Byte[] { 0xAA, 0xBB };
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.TinyBlob };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Byte[]>(result);
        Assert.Equal(bytes, result);
    }

    [Fact(DisplayName = "文本协议_MediumBlob")]
    public void ReadTextValue_MediumBlob()
    {
        var bytes = new Byte[] { 0x11, 0x22, 0x33, 0x44 };
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.MediumBlob };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(bytes, result);
    }

    [Fact(DisplayName = "文本协议_LongBlob")]
    public void ReadTextValue_LongBlob()
    {
        var bytes = new Byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 };
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.LongBlob };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(bytes, result);
    }

    [Fact(DisplayName = "文本协议_Binary")]
    public void ReadTextValue_Binary()
    {
        var bytes = new Byte[] { 0xFF, 0x00, 0xFF };
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Binary };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(bytes, result);
    }

    [Fact(DisplayName = "文本协议_VarBinary")]
    public void ReadTextValue_VarBinary()
    {
        var bytes = new Byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.VarBinary };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(bytes, result);
    }

    [Fact(DisplayName = "文本协议_Geometry")]
    public void ReadTextValue_Geometry()
    {
        var bytes = new Byte[] { 0x01, 0x01, 0x00, 0x00, 0x00 };
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Geometry };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(bytes, result);
    }

    [Fact(DisplayName = "文本协议_Vector")]
    public void ReadTextValue_Vector()
    {
        var bytes = new Byte[] { 0x00, 0x11, 0x22, 0x33 };
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Vector };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(bytes, result);
    }

    /// <summary>测试文本协议解析 Bit 类型</summary>
    [Fact(DisplayName = "文本协议_Bit_1字节")]
    public void ReadTextValue_Bit_1Byte()
    {
        // MySQL 文本协议中 Bit 以二进制字节传输（小端序）
        var bytes = new Byte[] { 0x05 }; // BIT(8) = 5
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Bit };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<UInt64>(result);
        Assert.Equal(5UL, result);
    }

    [Fact(DisplayName = "文本协议_Bit_8字节")]
    public void ReadTextValue_Bit_8Bytes()
    {
        // BIT(64) 值 0x0807060504030201（小端序）
        var bytes = new Byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Bit };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.Equal(0x0807060504030201UL, result);
    }

    [Fact(DisplayName = "文本协议_Guid")]
    public void ReadTextValue_Guid()
    {
        var str = "550e8400-e29b-41d4-a716-446655440000";
        var bytes = Encoding.UTF8.GetBytes(str);
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = MySqlDbType.Guid };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<String>(result);
        Assert.Equal(str, result);
    }

    /// <summary>测试文本协议解析未知类型（返回字节数组）</summary>
    [Fact(DisplayName = "文本协议_未知类型")]
    public void ReadTextValue_UnknownType()
    {
        var bytes = new Byte[] { 0x01, 0x02, 0x03 };
        var reader = new SpanReader(bytes);
        var column = new MySqlColumn { Type = (MySqlDbType)255 };

        var result = MySqlFieldCodec.ReadTextValue(ref reader, column, bytes.Length);

        Assert.IsType<Byte[]>(result);
        Assert.Equal(bytes, result);
    }
    #endregion

    #region 二进制协议读取测试
    /// <summary>测试二进制协议解析有符号整数类型</summary>
    [Fact(DisplayName = "二进制协议_Byte")]
    public void ReadBinaryValue_Byte()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((SByte)(-100));
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Byte };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<SByte>(result);
        Assert.Equal((SByte)(-100), result);
    }

    [Fact(DisplayName = "二进制协议_Int16")]
    public void ReadBinaryValue_Int16()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((Int16)(-30000));
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Int16 };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<Int16>(result);
        Assert.Equal((Int16)(-30000), result);
    }

    [Fact(DisplayName = "二进制协议_Int24")]
    public void ReadBinaryValue_Int24()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((Int32)(-5000000));
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Int24 };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<Int32>(result);
        Assert.Equal(-5000000, result);
    }

    [Fact(DisplayName = "二进制协议_Int32")]
    public void ReadBinaryValue_Int32()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((Int32)(-2000000000));
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Int32 };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(-2000000000, result);
    }

    [Fact(DisplayName = "二进制协议_Int64")]
    public void ReadBinaryValue_Int64()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((Int64)(-9000000000000000000));
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Int64 };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(-9000000000000000000L, result);
    }

    /// <summary>测试二进制协议解析无符号整数类型</summary>
    [Fact(DisplayName = "二进制协议_UByte")]
    public void ReadBinaryValue_UByte()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((Byte)200);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.UByte };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<Byte>(result);
        Assert.Equal((Byte)200, result);
    }

    [Fact(DisplayName = "二进制协议_UInt16")]
    public void ReadBinaryValue_UInt16()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((UInt16)60000);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.UInt16 };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<UInt16>(result);
        Assert.Equal((UInt16)60000, result);
    }

    [Fact(DisplayName = "二进制协议_UInt24")]
    public void ReadBinaryValue_UInt24()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((UInt32)15000000);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.UInt24 };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<UInt32>(result);
        Assert.Equal(15000000u, result);
    }

    [Fact(DisplayName = "二进制协议_UInt32")]
    public void ReadBinaryValue_UInt32()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((UInt32)4000000000);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.UInt32 };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(4000000000u, result);
    }

    [Fact(DisplayName = "二进制协议_UInt64")]
    public void ReadBinaryValue_UInt64()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((UInt64)18000000000000000000);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.UInt64 };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(18000000000000000000UL, result);
    }

    /// <summary>测试二进制协议解析浮点数类型</summary>
    [Fact(DisplayName = "二进制协议_Float")]
    public void ReadBinaryValue_Float()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((Single)3.14159f);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Float };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<Single>(result);
        Assert.Equal(3.14159f, (Single)result, 5);
    }

    [Fact(DisplayName = "二进制协议_Double")]
    public void ReadBinaryValue_Double()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((Double)3.141592653589793);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Double };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<Double>(result);
        Assert.Equal(3.141592653589793, (Double)result, 15);
    }

    /// <summary>测试二进制协议解析 Decimal 类型（length-encoded string）</summary>
    [Fact(DisplayName = "二进制协议_Decimal")]
    public void ReadBinaryValue_Decimal()
    {
        var str = "123.456";
        var bytes = Encoding.UTF8.GetBytes(str);
        var ms = new MemoryStream();
        ms.WriteByte((Byte)bytes.Length); // length-encoded
        ms.Write(bytes, 0, bytes.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Decimal };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<Decimal>(result);
        Assert.Equal(123.456m, result);
    }

    [Fact(DisplayName = "二进制协议_NewDecimal")]
    public void ReadBinaryValue_NewDecimal()
    {
        var str = "-999.999";
        var bytes = Encoding.UTF8.GetBytes(str);
        var ms = new MemoryStream();
        ms.WriteByte((Byte)bytes.Length);
        ms.Write(bytes, 0, bytes.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.NewDecimal };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(-999.999m, result);
    }

    /// <summary>测试二进制协议解析 DateTime 类型</summary>
    [Fact(DisplayName = "二进制协议_DateTime_仅日期")]
    public void ReadBinaryValue_DateTime_DateOnly()
    {
        var ms = new MemoryStream();
        ms.WriteByte(4); // length
        ms.Write(BitConverter.GetBytes((UInt16)2024), 0, 2); // year
        ms.WriteByte(12); // month
        ms.WriteByte(25); // day
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.DateTime };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<DateTime>(result);
        var dt = (DateTime)result;
        Assert.Equal(2024, dt.Year);
        Assert.Equal(12, dt.Month);
        Assert.Equal(25, dt.Day);
        Assert.Equal(0, dt.Hour);
    }

    [Fact(DisplayName = "二进制协议_DateTime_日期和时间")]
    public void ReadBinaryValue_DateTime_Full()
    {
        var ms = new MemoryStream();
        ms.WriteByte(7); // length
        ms.Write(BitConverter.GetBytes((UInt16)2024), 0, 2);
        ms.WriteByte(12);
        ms.WriteByte(25);
        ms.WriteByte(10); // hour
        ms.WriteByte(30); // minute
        ms.WriteByte(45); // second
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.DateTime };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        var dt = (DateTime)result;
        Assert.Equal(new DateTime(2024, 12, 25, 10, 30, 45), dt);
    }

    [Fact(DisplayName = "二进制协议_DateTime_含微秒")]
    public void ReadBinaryValue_DateTime_WithMicroseconds()
    {
        var ms = new MemoryStream();
        ms.WriteByte(11); // length
        ms.Write(BitConverter.GetBytes((UInt16)2024), 0, 2);
        ms.WriteByte(12);
        ms.WriteByte(25);
        ms.WriteByte(10);
        ms.WriteByte(30);
        ms.WriteByte(45);
        ms.Write(BitConverter.GetBytes((Int32)123456), 0, 4); // microseconds
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.DateTime };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        var dt = (DateTime)result;
        Assert.Equal(2024, dt.Year);
        Assert.Equal(12, dt.Month);
        Assert.Equal(25, dt.Day);
        Assert.Equal(10, dt.Hour);
        Assert.Equal(30, dt.Minute);
        Assert.Equal(45, dt.Second);
        Assert.Equal(123, dt.Millisecond); // 123456 微秒 = 123 毫秒
    }

    [Fact(DisplayName = "二进制协议_DateTime_零值")]
    public void ReadBinaryValue_DateTime_Zero()
    {
        var ms = new MemoryStream();
        ms.WriteByte(0); // length = 0 表示零值
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.DateTime };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(DateTime.MinValue, result);
    }

    [Fact(DisplayName = "二进制协议_Timestamp")]
    public void ReadBinaryValue_Timestamp()
    {
        var ms = new MemoryStream();
        ms.WriteByte(7);
        ms.Write(BitConverter.GetBytes((UInt16)2024), 0, 2);
        ms.WriteByte(1);
        ms.WriteByte(15);
        ms.WriteByte(8);
        ms.WriteByte(15);
        ms.WriteByte(30);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Timestamp };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(new DateTime(2024, 1, 15, 8, 15, 30), result);
    }

    [Fact(DisplayName = "二进制协议_Date")]
    public void ReadBinaryValue_Date()
    {
        var ms = new MemoryStream();
        ms.WriteByte(4);
        ms.Write(BitConverter.GetBytes((UInt16)2024), 0, 2);
        ms.WriteByte(6);
        ms.WriteByte(30);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Date };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        var dt = (DateTime)result;
        Assert.Equal(2024, dt.Year);
        Assert.Equal(6, dt.Month);
        Assert.Equal(30, dt.Day);
    }

    /// <summary>测试二进制协议解析 Time 类型</summary>
    [Fact(DisplayName = "二进制协议_Time_标准")]
    public void ReadBinaryValue_Time_Standard()
    {
        var ms = new MemoryStream();
        ms.WriteByte(8); // length
        ms.WriteByte(0); // is_negative = false
        ms.Write(BitConverter.GetBytes((Int32)0), 0, 4); // days
        ms.WriteByte(12); // hours
        ms.WriteByte(34); // minutes
        ms.WriteByte(56); // seconds
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Time };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<TimeSpan>(result);
        Assert.Equal(new TimeSpan(0, 12, 34, 56, 0), result);
    }

    [Fact(DisplayName = "二进制协议_Time_负值")]
    public void ReadBinaryValue_Time_Negative()
    {
        var ms = new MemoryStream();
        ms.WriteByte(8);
        ms.WriteByte(1); // is_negative = true
        ms.Write(BitConverter.GetBytes((Int32)0), 0, 4);
        ms.WriteByte(5);
        ms.WriteByte(30);
        ms.WriteByte(15);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Time };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        var ts = (TimeSpan)result;
        Assert.True(ts < TimeSpan.Zero);
        Assert.Equal(-new TimeSpan(0, 5, 30, 15, 0), ts);
    }

    [Fact(DisplayName = "二进制协议_Time_含天数")]
    public void ReadBinaryValue_Time_WithDays()
    {
        var ms = new MemoryStream();
        ms.WriteByte(8);
        ms.WriteByte(0);
        ms.Write(BitConverter.GetBytes((Int32)2), 0, 4); // 2 days
        ms.WriteByte(10);
        ms.WriteByte(20);
        ms.WriteByte(30);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Time };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(new TimeSpan(2, 10, 20, 30, 0), result);
    }

    [Fact(DisplayName = "二进制协议_Time_含微秒")]
    public void ReadBinaryValue_Time_WithMicroseconds()
    {
        var ms = new MemoryStream();
        ms.WriteByte(12); // length
        ms.WriteByte(0);
        ms.Write(BitConverter.GetBytes((Int32)0), 0, 4);
        ms.WriteByte(12);
        ms.WriteByte(34);
        ms.WriteByte(56);
        ms.Write(BitConverter.GetBytes((Int32)123456), 0, 4); // microseconds
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Time };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        var ts = (TimeSpan)result;
        Assert.Equal(12, ts.Hours);
        Assert.Equal(34, ts.Minutes);
        Assert.Equal(56, ts.Seconds);
        Assert.Equal(123, ts.Milliseconds);
    }

    [Fact(DisplayName = "二进制协议_Time_零值")]
    public void ReadBinaryValue_Time_Zero()
    {
        var ms = new MemoryStream();
        ms.WriteByte(0); // length = 0
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Time };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(TimeSpan.Zero, result);
    }

    /// <summary>测试二进制协议解析 Year 类型</summary>
    [Fact(DisplayName = "二进制协议_Year")]
    public void ReadBinaryValue_Year()
    {
        var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes((UInt16)2024), 0, 2);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Year };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<Int32>(result);
        Assert.Equal(2024, result);
    }

    /// <summary>测试二进制协议解析 Bit 类型</summary>
    [Fact(DisplayName = "二进制协议_Bit")]
    public void ReadBinaryValue_Bit()
    {
        var bytes = new Byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
        var ms = new MemoryStream();
        ms.WriteByte((Byte)bytes.Length); // length-encoded
        ms.Write(bytes, 0, bytes.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Bit };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<UInt64>(result);
    }

    /// <summary>测试二进制协议解析二进制类型（length-encoded bytes）</summary>
    [Fact(DisplayName = "二进制协议_Blob")]
    public void ReadBinaryValue_Blob()
    {
        var data = new Byte[] { 0x01, 0x02, 0x03, 0xFF, 0xFE };
        var ms = new MemoryStream();
        ms.WriteByte((Byte)data.Length);
        ms.Write(data, 0, data.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Blob };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<Byte[]>(result);
        Assert.Equal(data, result);
    }

    [Fact(DisplayName = "二进制协议_TinyBlob")]
    public void ReadBinaryValue_TinyBlob()
    {
        var data = new Byte[] { 0xAA, 0xBB };
        var ms = new MemoryStream();
        ms.WriteByte((Byte)data.Length);
        ms.Write(data, 0, data.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.TinyBlob };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(data, result);
    }

    [Fact(DisplayName = "二进制协议_MediumBlob")]
    public void ReadBinaryValue_MediumBlob()
    {
        var data = new Byte[] { 0x11, 0x22, 0x33, 0x44 };
        var ms = new MemoryStream();
        ms.WriteByte((Byte)data.Length);
        ms.Write(data, 0, data.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.MediumBlob };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(data, result);
    }

    [Fact(DisplayName = "二进制协议_LongBlob")]
    public void ReadBinaryValue_LongBlob()
    {
        var data = new Byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 };
        var ms = new MemoryStream();
        ms.WriteByte((Byte)data.Length);
        ms.Write(data, 0, data.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.LongBlob };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(data, result);
    }

    [Fact(DisplayName = "二进制协议_Binary")]
    public void ReadBinaryValue_Binary()
    {
        var data = new Byte[] { 0xFF, 0x00, 0xFF };
        var ms = new MemoryStream();
        ms.WriteByte((Byte)data.Length);
        ms.Write(data, 0, data.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Binary };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(data, result);
    }

    [Fact(DisplayName = "二进制协议_VarBinary")]
    public void ReadBinaryValue_VarBinary()
    {
        var data = new Byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var ms = new MemoryStream();
        ms.WriteByte((Byte)data.Length);
        ms.Write(data, 0, data.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.VarBinary };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(data, result);
    }

    [Fact(DisplayName = "二进制协议_Geometry")]
    public void ReadBinaryValue_Geometry()
    {
        var data = new Byte[] { 0x01, 0x01, 0x00, 0x00, 0x00 };
        var ms = new MemoryStream();
        ms.WriteByte((Byte)data.Length);
        ms.Write(data, 0, data.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Geometry };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(data, result);
    }

    [Fact(DisplayName = "二进制协议_Vector")]
    public void ReadBinaryValue_Vector()
    {
        var data = new Byte[] { 0x00, 0x11, 0x22, 0x33 };
        var ms = new MemoryStream();
        ms.WriteByte((Byte)data.Length);
        ms.Write(data, 0, data.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.Vector };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(data, result);
    }

    /// <summary>测试二进制协议解析字符串类型（length-encoded string）</summary>
    [Fact(DisplayName = "二进制协议_String")]
    public void ReadBinaryValue_String()
    {
        var str = "Hello MySQL";
        var bytes = Encoding.UTF8.GetBytes(str);
        var ms = new MemoryStream();
        ms.WriteByte((Byte)bytes.Length);
        ms.Write(bytes, 0, bytes.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.String };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.IsType<String>(result);
        Assert.Equal(str, result);
    }

    [Fact(DisplayName = "二进制协议_VarString")]
    public void ReadBinaryValue_VarString()
    {
        var str = "可变字符串";
        var bytes = Encoding.UTF8.GetBytes(str);
        var ms = new MemoryStream();
        ms.WriteByte((Byte)bytes.Length);
        ms.Write(bytes, 0, bytes.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.VarString };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(str, result);
    }

    [Fact(DisplayName = "二进制协议_VarChar")]
    public void ReadBinaryValue_VarChar()
    {
        var str = "VARCHAR 字段";
        var bytes = Encoding.UTF8.GetBytes(str);
        var ms = new MemoryStream();
        ms.WriteByte((Byte)bytes.Length);
        ms.Write(bytes, 0, bytes.Length);
        ms.Position = 0;

        var reader = new SpanReader(ms.ToArray());
        var column = new MySqlColumn { Type = MySqlDbType.VarChar };

        var result = MySqlFieldCodec.ReadBinaryValue(ref reader, column);

        Assert.Equal(str, result);
    }
    #endregion

    #region 二进制协议写入测试
    /// <summary>测试 GetMySqlTypeForValue 方法</summary>
    [Fact(DisplayName = "获取类型_Null")]
    public void GetMySqlTypeForValue_Null()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue(null);
        Assert.Equal((Byte)0x06, typeId); // MYSQL_TYPE_NULL
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_DBNull")]
    public void GetMySqlTypeForValue_DBNull()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue(DBNull.Value);
        Assert.Equal((Byte)0x06, typeId);
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_SByte")]
    public void GetMySqlTypeForValue_SByte()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue((SByte)(-100));
        Assert.Equal((Byte)0x01, typeId); // MYSQL_TYPE_TINY
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_Byte")]
    public void GetMySqlTypeForValue_Byte()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue((Byte)200);
        Assert.Equal((Byte)0x01, typeId);
        Assert.True(unsigned);
    }

    [Fact(DisplayName = "获取类型_Int16")]
    public void GetMySqlTypeForValue_Int16()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue((Int16)(-30000));
        Assert.Equal((Byte)0x02, typeId); // MYSQL_TYPE_SHORT
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_UInt16")]
    public void GetMySqlTypeForValue_UInt16()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue((UInt16)60000);
        Assert.Equal((Byte)0x02, typeId);
        Assert.True(unsigned);
    }

    [Fact(DisplayName = "获取类型_Int32")]
    public void GetMySqlTypeForValue_Int32()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue((Int32)(-2000000000));
        Assert.Equal((Byte)0x03, typeId); // MYSQL_TYPE_LONG
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_UInt32")]
    public void GetMySqlTypeForValue_UInt32()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue((UInt32)4000000000);
        Assert.Equal((Byte)0x03, typeId);
        Assert.True(unsigned);
    }

    [Fact(DisplayName = "获取类型_Int64")]
    public void GetMySqlTypeForValue_Int64()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue((Int64)(-9000000000000000000));
        Assert.Equal((Byte)0x08, typeId); // MYSQL_TYPE_LONGLONG
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_UInt64")]
    public void GetMySqlTypeForValue_UInt64()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue((UInt64)18000000000000000000);
        Assert.Equal((Byte)0x08, typeId);
        Assert.True(unsigned);
    }

    [Fact(DisplayName = "获取类型_Single")]
    public void GetMySqlTypeForValue_Single()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue((Single)3.14f);
        Assert.Equal((Byte)0x04, typeId); // MYSQL_TYPE_FLOAT
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_Double")]
    public void GetMySqlTypeForValue_Double()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue((Double)3.14159);
        Assert.Equal((Byte)0x05, typeId); // MYSQL_TYPE_DOUBLE
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_Decimal")]
    public void GetMySqlTypeForValue_Decimal()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue((Decimal)123.456m);
        Assert.Equal((Byte)0xF6, typeId); // MYSQL_TYPE_NEWDECIMAL
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_Boolean")]
    public void GetMySqlTypeForValue_Boolean()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue(true);
        Assert.Equal((Byte)0x01, typeId); // MYSQL_TYPE_TINY
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_DateTime")]
    public void GetMySqlTypeForValue_DateTime()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue(DateTime.Now);
        Assert.Equal((Byte)0x0C, typeId); // MYSQL_TYPE_DATETIME
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_DateTimeOffset")]
    public void GetMySqlTypeForValue_DateTimeOffset()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue(DateTimeOffset.Now);
        Assert.Equal((Byte)0x0C, typeId);
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_TimeSpan")]
    public void GetMySqlTypeForValue_TimeSpan()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue(TimeSpan.FromHours(12));
        Assert.Equal((Byte)0x0B, typeId); // MYSQL_TYPE_TIME
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_ByteArray")]
    public void GetMySqlTypeForValue_ByteArray()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue(new Byte[] { 1, 2, 3 });
        Assert.Equal((Byte)0xFC, typeId); // MYSQL_TYPE_BLOB
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_Guid")]
    public void GetMySqlTypeForValue_Guid()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue(Guid.NewGuid());
        Assert.Equal((Byte)0xFE, typeId); // MYSQL_TYPE_STRING
        Assert.False(unsigned);
    }

    [Fact(DisplayName = "获取类型_String")]
    public void GetMySqlTypeForValue_String()
    {
        var (typeId, unsigned) = MySqlFieldCodec.GetMySqlTypeForValue("test");
        Assert.Equal((Byte)0xFE, typeId);
        Assert.False(unsigned);
    }

    /// <summary>测试 WriteBinaryValue 方法</summary>
    [Fact(DisplayName = "写入二进制_SByte")]
    public void WriteBinaryValue_SByte()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, (SByte)(-100), Encoding.UTF8);

        Assert.Equal(1, writer.Position);
        Assert.Equal(unchecked((Byte)(-100)), buffer[0]);
    }

    [Fact(DisplayName = "写入二进制_Byte")]
    public void WriteBinaryValue_Byte()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, (Byte)200, Encoding.UTF8);

        Assert.Equal(1, writer.Position);
        Assert.Equal(200, buffer[0]);
    }

    [Fact(DisplayName = "写入二进制_Boolean_True")]
    public void WriteBinaryValue_Boolean_True()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, true, Encoding.UTF8);

        Assert.Equal(1, writer.Position);
        Assert.Equal(1, buffer[0]);
    }

    [Fact(DisplayName = "写入二进制_Boolean_False")]
    public void WriteBinaryValue_Boolean_False()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, false, Encoding.UTF8);

        Assert.Equal(1, writer.Position);
        Assert.Equal(0, buffer[0]);
    }

    [Fact(DisplayName = "写入二进制_Int16")]
    public void WriteBinaryValue_Int16()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, (Int16)(-30000), Encoding.UTF8);

        Assert.Equal(2, writer.Position);
        Assert.Equal(-30000, BitConverter.ToInt16(buffer, 0));
    }

    [Fact(DisplayName = "写入二进制_UInt16")]
    public void WriteBinaryValue_UInt16()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, (UInt16)60000, Encoding.UTF8);

        Assert.Equal(2, writer.Position);
        Assert.Equal(60000, BitConverter.ToUInt16(buffer, 0));
    }

    [Fact(DisplayName = "写入二进制_Int32")]
    public void WriteBinaryValue_Int32()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, (Int32)(-2000000000), Encoding.UTF8);

        Assert.Equal(4, writer.Position);
        Assert.Equal(-2000000000, BitConverter.ToInt32(buffer, 0));
    }

    [Fact(DisplayName = "写入二进制_UInt32")]
    public void WriteBinaryValue_UInt32()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, (UInt32)4000000000, Encoding.UTF8);

        Assert.Equal(4, writer.Position);
        Assert.Equal(4000000000u, BitConverter.ToUInt32(buffer, 0));
    }

    [Fact(DisplayName = "写入二进制_Int64")]
    public void WriteBinaryValue_Int64()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, (Int64)(-9000000000000000000), Encoding.UTF8);

        Assert.Equal(8, writer.Position);
        Assert.Equal(-9000000000000000000L, BitConverter.ToInt64(buffer, 0));
    }

    [Fact(DisplayName = "写入二进制_UInt64")]
    public void WriteBinaryValue_UInt64()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, (UInt64)18000000000000000000, Encoding.UTF8);

        Assert.Equal(8, writer.Position);
        Assert.Equal(18000000000000000000UL, BitConverter.ToUInt64(buffer, 0));
    }

    [Fact(DisplayName = "写入二进制_Single")]
    public void WriteBinaryValue_Single()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, (Single)3.14f, Encoding.UTF8);

        Assert.Equal(4, writer.Position);
        Assert.Equal(3.14f, BitConverter.ToSingle(buffer, 0), 5);
    }

    [Fact(DisplayName = "写入二进制_Double")]
    public void WriteBinaryValue_Double()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, (Double)3.141592653589793, Encoding.UTF8);

        Assert.Equal(8, writer.Position);
        Assert.Equal(3.141592653589793, BitConverter.ToDouble(buffer, 0), 15);
    }

    [Fact(DisplayName = "写入二进制_Decimal")]
    public void WriteBinaryValue_Decimal()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, (Decimal)123.456m, Encoding.UTF8);

        // Decimal 写为 length-encoded string
        var reader = new SpanReader(buffer);
        var result = reader.ReadString();
        Assert.Equal("123.456", result);
    }

    [Fact(DisplayName = "写入二进制_DateTime_仅日期")]
    public void WriteBinaryValue_DateTime_DateOnly()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        var dt = new DateTime(2024, 12, 25, 0, 0, 0, 0);
        MySqlFieldCodec.WriteBinaryValue(ref writer, dt, Encoding.UTF8);

        Assert.Equal(5, writer.Position); // 1 length + 4 data
        Assert.Equal(4, buffer[0]); // length
        Assert.Equal(2024, BitConverter.ToUInt16(buffer, 1));
        Assert.Equal(12, buffer[3]);
        Assert.Equal(25, buffer[4]);
    }

    [Fact(DisplayName = "写入二进制_DateTime_含时间")]
    public void WriteBinaryValue_DateTime_Full()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        var dt = new DateTime(2024, 12, 25, 10, 30, 45, 0);
        MySqlFieldCodec.WriteBinaryValue(ref writer, dt, Encoding.UTF8);

        Assert.Equal(8, writer.Position); // 1 length + 7 data
        Assert.Equal(7, buffer[0]);
        Assert.Equal(2024, BitConverter.ToUInt16(buffer, 1));
        Assert.Equal(12, buffer[3]);
        Assert.Equal(25, buffer[4]);
        Assert.Equal(10, buffer[5]);
        Assert.Equal(30, buffer[6]);
        Assert.Equal(45, buffer[7]);
    }

    [Fact(DisplayName = "写入二进制_DateTime_含微秒")]
    public void WriteBinaryValue_DateTime_WithMicroseconds()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        var dt = new DateTime(2024, 12, 25, 10, 30, 45, 123);
        MySqlFieldCodec.WriteBinaryValue(ref writer, dt, Encoding.UTF8);

        Assert.Equal(12, writer.Position); // 1 length + 11 data
        Assert.Equal(11, buffer[0]);
        Assert.Equal(2024, BitConverter.ToUInt16(buffer, 1));
        Assert.Equal(12, buffer[3]);
        Assert.Equal(25, buffer[4]);
        Assert.Equal(10, buffer[5]);
        Assert.Equal(30, buffer[6]);
        Assert.Equal(45, buffer[7]);
        Assert.Equal(123000, BitConverter.ToInt32(buffer, 8)); // 123 ms = 123000 us
    }

    [Fact(DisplayName = "写入二进制_DateTimeOffset")]
    public void WriteBinaryValue_DateTimeOffset()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        var dto = new DateTimeOffset(new DateTime(2024, 6, 15, 8, 30, 0));
        MySqlFieldCodec.WriteBinaryValue(ref writer, dto, Encoding.UTF8);

        Assert.True(writer.Position > 0);
        Assert.Equal(7, buffer[0]); // length
    }

    [Fact(DisplayName = "写入二进制_TimeSpan_标准")]
    public void WriteBinaryValue_TimeSpan_Standard()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        var ts = new TimeSpan(0, 12, 34, 56, 0);
        MySqlFieldCodec.WriteBinaryValue(ref writer, ts, Encoding.UTF8);

        Assert.Equal(9, writer.Position); // 1 length + 8 data
        Assert.Equal(8, buffer[0]);
        Assert.Equal(0, buffer[1]); // is_negative
        Assert.Equal(0, BitConverter.ToInt32(buffer, 2)); // days
        Assert.Equal(12, buffer[6]); // hours
        Assert.Equal(34, buffer[7]); // minutes
        Assert.Equal(56, buffer[8]); // seconds
    }

    [Fact(DisplayName = "写入二进制_TimeSpan_负值")]
    public void WriteBinaryValue_TimeSpan_Negative()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        var ts = new TimeSpan(-1, -5, -30, -15, 0);
        MySqlFieldCodec.WriteBinaryValue(ref writer, ts, Encoding.UTF8);

        Assert.Equal(9, writer.Position);
        Assert.Equal(8, buffer[0]);
        Assert.Equal(1, buffer[1]); // is_negative = true
    }

    [Fact(DisplayName = "写入二进制_TimeSpan_含微秒")]
    public void WriteBinaryValue_TimeSpan_WithMicroseconds()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        var ts = new TimeSpan(0, 12, 34, 56, 123);
        MySqlFieldCodec.WriteBinaryValue(ref writer, ts, Encoding.UTF8);

        Assert.Equal(13, writer.Position); // 1 length + 12 data
        Assert.Equal(12, buffer[0]);
        Assert.Equal(123000, BitConverter.ToInt32(buffer, 9)); // 123 ms = 123000 us
    }

    [Fact(DisplayName = "写入二进制_ByteArray")]
    public void WriteBinaryValue_ByteArray()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        var data = new Byte[] { 0x01, 0x02, 0x03, 0xFF };
        MySqlFieldCodec.WriteBinaryValue(ref writer, data, Encoding.UTF8);

        // length-encoded bytes
        var reader = new SpanReader(buffer);
        var len = reader.ReadLength();
        Assert.Equal(4, len);
        var result = reader.ReadBytes((Int32)len).ToArray();
        Assert.Equal(data, result);
    }

    [Fact(DisplayName = "写入二进制_Guid")]
    public void WriteBinaryValue_Guid()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        var guid = Guid.Parse("550e8400-e29b-41d4-a716-446655440000");
        MySqlFieldCodec.WriteBinaryValue(ref writer, guid, Encoding.UTF8);

        var reader = new SpanReader(buffer);
        var result = reader.ReadString();
        Assert.Equal("550e8400-e29b-41d4-a716-446655440000", result);
    }

    [Fact(DisplayName = "写入二进制_String")]
    public void WriteBinaryValue_String()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, "Hello MySQL", Encoding.UTF8);

        var reader = new SpanReader(buffer);
        var result = reader.ReadString();
        Assert.Equal("Hello MySQL", result);
    }

    [Fact(DisplayName = "写入二进制_Enum")]
    public void WriteBinaryValue_Enum()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        MySqlFieldCodec.WriteBinaryValue(ref writer, DayOfWeek.Monday, Encoding.UTF8);

        Assert.Equal(8, writer.Position);
        Assert.Equal((Int64)DayOfWeek.Monday, BitConverter.ToInt64(buffer, 0));
    }

    [Fact(DisplayName = "写入二进制_Object_ToString")]
    public void WriteBinaryValue_Object_ToString()
    {
        var buffer = new Byte[100];
        var writer = new SpanWriter(buffer);
        var obj = new { Name = "test", Value = 123 };
        MySqlFieldCodec.WriteBinaryValue(ref writer, obj, Encoding.UTF8);

        var reader = new SpanReader(buffer);
        var result = reader.ReadString();
        Assert.Contains("Name", result);
        Assert.Contains("test", result);
    }
    #endregion
}
