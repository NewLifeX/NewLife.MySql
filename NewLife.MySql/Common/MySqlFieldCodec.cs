using System.Text;
using NewLife.Buffers;

namespace NewLife.MySql.Common;

/// <summary>MySQL 字段编解码器。负责文本协议和二进制协议的字段读取与写入</summary>
/// <remarks>
/// MySQL 协议中字段数据有两种传输格式：
/// <list type="bullet">
/// <item>文本协议（COM_QUERY）：所有值以 UTF-8 字符串形式传输，前缀为 length-encoded 长度</item>
/// <item>二进制协议（COM_STMT_EXECUTE）：每种类型有独立的二进制编码格式，整数为小端序，字符串为 length-encoded</item>
/// </list>
/// </remarks>
public static class MySqlFieldCodec
{
    #region 文本协议读取
    /// <summary>从文本协议数据中解析字段值。文本协议中所有值以 UTF-8 字符串传输</summary>
    /// <remarks>
    /// MySQL 文本协议传输格式：
    /// <list type="bullet">
    /// <item>所有字段值均以 length-encoded string 传输，即 length-encoded integer + UTF-8 字节</item>
    /// <item>NULL 值由 length = 0xFB（-1）表示，调用方已处理</item>
    /// <item>数值类型（INT/FLOAT/DECIMAL 等）：ASCII 十进制字符串，如 "12345"、"3.14"</item>
    /// <item>日期时间：ISO 格式字符串，如 "2024-01-15 10:30:00"</item>
    /// <item>TIME：格式 "HH:MM:SS" 或 "-HHH:MM:SS.ffffff"，范围 -838:59:59 到 838:59:59</item>
    /// <item>二进制/BLOB：原始字节</item>
    /// </list>
    /// </remarks>
    /// <param name="reader">数据读取器，需 ref 传递以保留位置前进</param>
    /// <param name="column">列定义，包含类型信息</param>
    /// <param name="len">字段数据长度（字节数），由调用方从 length-encoded 整数中读取</param>
    /// <returns>解析后的 .NET 对象</returns>
    public static Object ReadTextValue(ref SpanReader reader, MySqlColumn column, Int32 len)
    {
        var span = reader.ReadBytes(len);
        var reader2 = new SpanReader(span);

        return column.Type switch
        {
            // 数值类型：ASCII 十进制字符串 → 对应 .NET 类型
            MySqlDbType.Decimal or MySqlDbType.NewDecimal => Decimal.Parse(span.ToStr()),
            MySqlDbType.Byte => (SByte)Int64.Parse(span.ToStr()),
            MySqlDbType.UByte => (Byte)Int64.Parse(span.ToStr()),
            MySqlDbType.Int16 => (Int16)Int64.Parse(span.ToStr()),
            MySqlDbType.UInt16 => (UInt16)Int64.Parse(span.ToStr()),
            MySqlDbType.Int24 or MySqlDbType.Int32 => (Int32)Int64.Parse(span.ToStr()),
            MySqlDbType.UInt24 or MySqlDbType.UInt32 => (UInt32)Int64.Parse(span.ToStr()),
            MySqlDbType.Int64 => Int64.Parse(span.ToStr()),
            MySqlDbType.UInt64 => UInt64.Parse(span.ToStr()),
            MySqlDbType.Float => Single.Parse(span.ToStr()),
            MySqlDbType.Double => Double.Parse(span.ToStr()),

            // 日期时间类型：ISO 格式字符串
            MySqlDbType.DateTime or MySqlDbType.Timestamp or MySqlDbType.Date => span.ToStr().ToDateTime(),
            MySqlDbType.Time => ParseTextTime(span.ToStr()),
            MySqlDbType.Year => span.ToStr().ToInt(),

            // 字符串类型：直接转为 .NET String
            MySqlDbType.String or MySqlDbType.VarString or MySqlDbType.VarChar
                or MySqlDbType.TinyText or MySqlDbType.MediumText or MySqlDbType.LongText or MySqlDbType.Text
                or MySqlDbType.Enum or MySqlDbType.Set or MySqlDbType.Json => span.ToStr(),

            // 二进制类型：保留为字节数组
            MySqlDbType.Blob or MySqlDbType.TinyBlob or MySqlDbType.MediumBlob or MySqlDbType.LongBlob
                or MySqlDbType.Binary or MySqlDbType.VarBinary or MySqlDbType.Geometry or MySqlDbType.Vector => span.ToArray(),

            // BIT：MySQL 文本协议中 Bit 类型以二进制字节传输，需从字节数组转为 UInt64（小端序）
            MySqlDbType.Bit => ConvertBitBytesToUInt64(span),

            // GUID：字符串形式
            MySqlDbType.Guid => span.ToStr(),

            // 未知类型：保留为字节数组
            _ => span.ToArray(),
        };
    }

    /// <summary>将 MySQL 文本协议中 Bit 类型的字节数组转换为 UInt64</summary>
    /// <remarks>
    /// MySQL 文本协议中 Bit 类型以二进制字节传输（小端序）。
    /// 例如：BIT(64) 值 0x0807060504030201 传输为字节 [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
    /// </remarks>
    /// <param name="span">字节数据</param>
    /// <returns>UInt64 值</returns>
    private static UInt64 ConvertBitBytesToUInt64(ReadOnlySpan<Byte> span)
    {
        if (span.Length == 0) return 0;

        // MySQL Bit 字节以小端序传输，逐字节读取并构建 UInt64
        var value = 0UL;
        for (var i = 0; i < span.Length && i < 8; i++)
        {
            value |= (UInt64)span[i] << (i * 8);
        }
        return value;
    }

    /// <summary>解析 MySQL 文本协议的 TIME 值</summary>
    /// <remarks>
    /// MySQL TIME 格式为 "[-]H:MM:SS[.ffffff]"，范围 -838:59:59.000000 到 838:59:59.000000。
    /// 标准 TimeSpan.Parse 无法处理超过 24 小时的值，因此需要自定义解析。
    /// </remarks>
    /// <param name="str">TIME 字符串</param>
    /// <returns>解析后的 TimeSpan</returns>
    private static TimeSpan ParseTextTime(String str)
    {
        if (str.IsNullOrEmpty()) return TimeSpan.Zero;

        var isNeg = str[0] == '-';
        if (isNeg) str = str.Substring(1);

        // 格式：H:MM:SS 或 HHH:MM:SS 或 HH:MM:SS.ffffff
        var colonIdx1 = str.IndexOf(':');
        if (colonIdx1 < 0) return TimeSpan.Zero;

        var colonIdx2 = str.IndexOf(':', colonIdx1 + 1);
        if (colonIdx2 < 0) return TimeSpan.Zero;

        var hours = Int32.Parse(str.Substring(0, colonIdx1));
        var minutes = Int32.Parse(str.Substring(colonIdx1 + 1, colonIdx2 - colonIdx1 - 1));

        var secStr = str.Substring(colonIdx2 + 1);
        var dotIdx = secStr.IndexOf('.');
        Int32 seconds;
        var milliseconds = 0;

        if (dotIdx >= 0)
        {
            seconds = Int32.Parse(secStr.Substring(0, dotIdx));
            // 微秒部分补齐到6位后取前3位作为毫秒
            var fracStr = secStr.Substring(dotIdx + 1).PadRight(6, '0');
            milliseconds = Int32.Parse(fracStr.Substring(0, 3));
        }
        else
        {
            seconds = Int32.Parse(secStr);
        }

        // TimeSpan(days, hours, minutes, seconds, milliseconds) 构造函数支持 hours > 23
        var ts = new TimeSpan(0, hours, minutes, seconds, milliseconds);
        return isNeg ? ts.Negate() : ts;
    }
    #endregion

    #region 二进制协议读取
    /// <summary>从二进制协议数据中解析字段值。二进制协议中每种类型有独立的编码格式</summary>
    /// <remarks>
    /// MySQL 二进制协议传输格式：
    /// <list type="bullet">
    /// <item>整数类型：小端序固定长度（TINY=1字节, SHORT=2, LONG=4, LONGLONG=8）</item>
    /// <item>浮点数：IEEE 754 小端序（FLOAT=4字节, DOUBLE=8字节）</item>
    /// <item>DECIMAL/NEWDECIMAL：length-encoded string 形式</item>
    /// <item>DATETIME/TIMESTAMP/DATE：length(1) + year(2) + month(1) + day(1) + [hour(1) + min(1) + sec(1)] + [microsec(4)]</item>
    /// <item>TIME：length(1) + is_negative(1) + days(4) + hours(1) + minutes(1) + seconds(1) + [microseconds(4)]</item>
    /// <item>YEAR：2字节无符号整数</item>
    /// <item>BIT/BLOB/BINARY：length-encoded integer + 原始字节</item>
    /// <item>字符串类型：length-encoded string</item>
    /// </list>
    /// </remarks>
    /// <param name="reader">数据读取器，需 ref 传递以保留位置前进</param>
    /// <param name="column">列定义，包含类型信息</param>
    /// <returns>解析后的 .NET 对象</returns>
    public static Object ReadBinaryValue(ref SpanReader reader, MySqlColumn column)
    {
        return column.Type switch
        {
            // 有符号整数：小端序固定长度
            MySqlDbType.Byte => (SByte)reader.ReadByte(),
            MySqlDbType.Int16 => reader.ReadInt16(),
            MySqlDbType.Int24 or MySqlDbType.Int32 => reader.ReadInt32(),
            MySqlDbType.Int64 => reader.ReadInt64(),

            // 无符号整数：小端序固定长度
            MySqlDbType.UByte => reader.ReadByte(),
            MySqlDbType.UInt16 => (UInt16)reader.ReadInt16(),
            MySqlDbType.UInt24 or MySqlDbType.UInt32 => (UInt32)reader.ReadInt32(),
            MySqlDbType.UInt64 => (UInt64)reader.ReadInt64(),

            // 浮点数：IEEE 754 小端序
            MySqlDbType.Float => BitConverter.ToSingle(reader.ReadBytes(4).ToArray(), 0),
            MySqlDbType.Double => BitConverter.ToDouble(reader.ReadBytes(8).ToArray(), 0),

            // 高精度小数：length-encoded string
            MySqlDbType.Decimal or MySqlDbType.NewDecimal => Decimal.Parse(reader.ReadString()),

            // 日期时间：自定义二进制格式
            MySqlDbType.DateTime or MySqlDbType.Timestamp or MySqlDbType.Date => ReadBinaryDateTime(ref reader),
            MySqlDbType.Time => ReadBinaryTime(ref reader),
            MySqlDbType.Year => (Int32)reader.ReadUInt16(),

            // 位字段：length-encoded bytes → Int64
            MySqlDbType.Bit => (UInt64)reader.ReadBytes((Int32)reader.ReadLength()).ToArray().ToLong(),

            // 二进制类型：length-encoded bytes
            MySqlDbType.Blob or MySqlDbType.TinyBlob or MySqlDbType.MediumBlob or MySqlDbType.LongBlob
                or MySqlDbType.Binary or MySqlDbType.VarBinary or MySqlDbType.Geometry or MySqlDbType.Vector
                => reader.ReadBytes((Int32)reader.ReadLength()).ToArray(),

            // 字符串类型（VarString, String, VarChar, Text, JSON, Guid, Enum, Set 等）：length-encoded string
            _ => reader.ReadString(),
        };
    }

    /// <summary>读取二进制 DATETIME/TIMESTAMP/DATE 值</summary>
    /// <remarks>
    /// 二进制格式：length(1) + year(2) + month(1) + day(1) + [hour(1) + min(1) + sec(1)] + [microsec(4)]。
    /// length 为 0 表示零值，4 为仅日期，7 为日期+时间，11 为日期+时间+微秒。
    /// </remarks>
    private static Object ReadBinaryDateTime(ref SpanReader reader)
    {
        var len = reader.ReadByte();
        if (len == 0) return DateTime.MinValue;

        var year = (Int32)reader.ReadUInt16();
        var month = reader.ReadByte();
        var day = reader.ReadByte();

        var hour = 0;
        var minute = 0;
        var second = 0;
        var microsecond = 0;

        if (len >= 7)
        {
            hour = reader.ReadByte();
            minute = reader.ReadByte();
            second = reader.ReadByte();
        }
        if (len >= 11)
        {
            microsecond = reader.ReadInt32();
        }

        return new DateTime(year, month, day, hour, minute, second, microsecond / 1000);
    }

    /// <summary>读取二进制 TIME 值</summary>
    /// <remarks>
    /// 二进制格式：length(1) + is_negative(1) + days(4) + hours(1) + minutes(1) + seconds(1) + [microseconds(4)]。
    /// length 为 0 表示零值，8 为标准时间，12 为含微秒。
    /// </remarks>
    private static Object ReadBinaryTime(ref SpanReader reader)
    {
        var len = reader.ReadByte();
        if (len == 0) return TimeSpan.Zero;

        var isNeg = reader.ReadByte() != 0;
        var days = reader.ReadInt32();
        var hours = reader.ReadByte();
        var minutes = reader.ReadByte();
        var seconds = reader.ReadByte();

        var microseconds = 0;
        if (len >= 12)
            microseconds = reader.ReadInt32();

        var ts = new TimeSpan(days, hours, minutes, seconds, microseconds / 1000);
        return isNeg ? ts.Negate() : ts;
    }
    #endregion

    #region 二进制协议写入
    /// <summary>根据 C# 值推断 MySQL 二进制协议类型 ID</summary>
    /// <param name="value">参数值</param>
    /// <returns>MySQL type ID 和是否无符号</returns>
    public static (Byte typeId, Boolean unsigned) GetMySqlTypeForValue(Object? value) => value switch
    {
        null or DBNull => (0x06, false),        // MYSQL_TYPE_NULL
        SByte => (0x01, false),                 // MYSQL_TYPE_TINY
        Byte => (0x01, true),                   // MYSQL_TYPE_TINY unsigned
        Int16 => (0x02, false),                 // MYSQL_TYPE_SHORT
        UInt16 => (0x02, true),                 // MYSQL_TYPE_SHORT unsigned
        Int32 => (0x03, false),                 // MYSQL_TYPE_LONG
        UInt32 => (0x03, true),                 // MYSQL_TYPE_LONG unsigned
        Int64 => (0x08, false),                 // MYSQL_TYPE_LONGLONG
        UInt64 => (0x08, true),                 // MYSQL_TYPE_LONGLONG unsigned
        Single => (0x04, false),                // MYSQL_TYPE_FLOAT
        Double => (0x05, false),                // MYSQL_TYPE_DOUBLE
        Decimal => (0xF6, false),               // MYSQL_TYPE_NEWDECIMAL → length-encoded string
        Boolean => (0x01, false),               // MYSQL_TYPE_TINY
        DateTime => (0x0C, false),              // MYSQL_TYPE_DATETIME
        DateTimeOffset => (0x0C, false),        // MYSQL_TYPE_DATETIME
        TimeSpan => (0x0B, false),              // MYSQL_TYPE_TIME
        Byte[] => (0xFC, false),                // MYSQL_TYPE_BLOB
        Guid => (0xFE, false),                  // MYSQL_TYPE_STRING
        String => (0xFE, false),                // MYSQL_TYPE_STRING
        Enum => (0x08, false),                  // MYSQL_TYPE_LONGLONG
        _ => (0xFE, false),                     // MYSQL_TYPE_STRING (fallback: ToString)
    };

    /// <summary>将 C# 值以 MySQL 二进制协议格式写入 SpanWriter</summary>
    /// <param name="writer">数据写入器，需 ref 传递以保留位置前进</param>
    /// <param name="value">参数值</param>
    /// <param name="encoding">字符编码，用于字符串类型转换</param>
    public static void WriteBinaryValue(ref SpanWriter writer, Object value, Encoding encoding)
    {
        switch (value)
        {
            case SByte v:
                writer.Write(unchecked((Byte)v));
                break;
            case Byte v:
                writer.Write(v);
                break;
            case Boolean v:
                writer.Write(v ? (Byte)1 : (Byte)0);
                break;
            case Int16 v:
                writer.Write(v);
                break;
            case UInt16 v:
                writer.Write(v);
                break;
            case Int32 v:
                writer.Write(v);
                break;
            case UInt32 v:
                writer.Write(v);
                break;
            case Int64 v:
                writer.Write(v);
                break;
            case UInt64 v:
                writer.Write(v);
                break;
            case Single v:
                writer.Write(BitConverter.GetBytes(v));
                break;
            case Double v:
                writer.Write(BitConverter.GetBytes(v));
                break;
            case Decimal v:
                WriteLengthEncodedString(ref writer, v.ToString(), encoding);
                break;
            case DateTime v:
                WriteBinaryDateTime(ref writer, v);
                break;
            case DateTimeOffset v:
                WriteBinaryDateTime(ref writer, v.DateTime);
                break;
            case TimeSpan v:
                WriteBinaryTime(ref writer, v);
                break;
            case Byte[] v:
                WriteLengthEncodedBytes(ref writer, v);
                break;
            case Guid v:
                WriteLengthEncodedString(ref writer, v.ToString(), encoding);
                break;
            case String v:
                WriteLengthEncodedString(ref writer, v, encoding);
                break;
            case Enum v:
                writer.Write(Convert.ToInt64(v));
                break;
            default:
                WriteLengthEncodedString(ref writer, value.ToString()!, encoding);
                break;
        }
    }

    /// <summary>写入 length-encoded 字符串</summary>
    /// <param name="writer">数据写入器</param>
    /// <param name="value">字符串值</param>
    /// <param name="encoding">字符编码</param>
    public static void WriteLengthEncodedString(ref SpanWriter writer, String value, Encoding encoding)
    {
        var bytes = encoding.GetBytes(value);
        WriteLengthEncodedBytes(ref writer, bytes);
    }

    /// <summary>写入 length-encoded 字节数组</summary>
    /// <param name="writer">数据写入器</param>
    /// <param name="bytes">字节数组</param>
    public static void WriteLengthEncodedBytes(ref SpanWriter writer, Byte[] bytes)
    {
        writer.WriteLength(bytes.Length);
        writer.Write(bytes);
    }

    /// <summary>写入 MySQL 二进制 DATETIME 格式</summary>
    /// <remarks>
    /// 二进制格式：length(1) + year(2) + month(1) + day(1) + [hour(1) + min(1) + sec(1)] + [microsec(4)]。
    /// length 为 0 表示零值，4 为仅日期，7 为日期+时间，11 为日期+时间+微秒。
    /// </remarks>
    /// <param name="writer">数据写入器</param>
    /// <param name="dt">日期时间值</param>
    private static void WriteBinaryDateTime(ref SpanWriter writer, DateTime dt)
    {
        var hasTime = dt.Hour != 0 || dt.Minute != 0 || dt.Second != 0 || dt.Millisecond != 0;
        var hasMicro = dt.Millisecond != 0;

        if (!hasTime)
        {
            // 仅日期: 4 bytes
            writer.Write((Byte)4);
            writer.Write((UInt16)dt.Year);
            writer.Write((Byte)dt.Month);
            writer.Write((Byte)dt.Day);
        }
        else if (!hasMicro)
        {
            // 日期 + 时间: 7 bytes
            writer.Write((Byte)7);
            writer.Write((UInt16)dt.Year);
            writer.Write((Byte)dt.Month);
            writer.Write((Byte)dt.Day);
            writer.Write((Byte)dt.Hour);
            writer.Write((Byte)dt.Minute);
            writer.Write((Byte)dt.Second);
        }
        else
        {
            // 日期 + 时间 + 微秒: 11 bytes
            writer.Write((Byte)11);
            writer.Write((UInt16)dt.Year);
            writer.Write((Byte)dt.Month);
            writer.Write((Byte)dt.Day);
            writer.Write((Byte)dt.Hour);
            writer.Write((Byte)dt.Minute);
            writer.Write((Byte)dt.Second);
            writer.Write((Int32)(dt.Millisecond * 1000));
        }
    }

    /// <summary>写入 MySQL 二进制 TIME 格式</summary>
    /// <remarks>
    /// 二进制格式：length(1) + is_negative(1) + days(4) + hours(1) + minutes(1) + seconds(1) + [microseconds(4)]。
    /// length 为 0 表示零值，8 为标准时间，12 为含微秒。
    /// </remarks>
    /// <param name="writer">数据写入器</param>
    /// <param name="ts">时间值</param>
    private static void WriteBinaryTime(ref SpanWriter writer, TimeSpan ts)
    {
        var isNegative = ts < TimeSpan.Zero;
        if (isNegative) ts = ts.Negate();

        var hasMicro = ts.Milliseconds != 0;

        if (!hasMicro)
        {
            // 8 bytes: is_negative(1) + days(4) + hours(1) + minutes(1) + seconds(1)
            writer.Write((Byte)8);
            writer.Write(isNegative ? (Byte)1 : (Byte)0);
            writer.Write((Int32)ts.Days);
            writer.Write((Byte)ts.Hours);
            writer.Write((Byte)ts.Minutes);
            writer.Write((Byte)ts.Seconds);
        }
        else
        {
            // 12 bytes: 包含微秒
            writer.Write((Byte)12);
            writer.Write(isNegative ? (Byte)1 : (Byte)0);
            writer.Write((Int32)ts.Days);
            writer.Write((Byte)ts.Hours);
            writer.Write((Byte)ts.Minutes);
            writer.Write((Byte)ts.Seconds);
            writer.Write((Int32)(ts.Milliseconds * 1000));
        }
    }
    #endregion
}
