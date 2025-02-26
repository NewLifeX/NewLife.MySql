using NewLife.Buffers;
using NewLife.Data;

namespace NewLife.MySql.Common;

public static class BinaryHelper
{
    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="pk"></param>
    /// <returns></returns>
    public static IPacket ReadZero(this IPacket pk)
    {
        for (var k = 0; k < pk.Length; k++)
        {
            if (pk[k] == 0) return pk.Slice(0, k);
        }

        return pk;
    }

    /// <summary>读取零结尾的C格式字符串</summary>
    public static ReadOnlySpan<Byte> ReadZero(this ref SpanReader reader)
    {
        var span = reader.GetSpan();
        for (var k = 0; k < span.Length; k++)
        {
            if (span[k] == 0)
            {
                reader.Advance(k + 1);
                return span[..(k + 1)];
            }
        }

        reader.Advance(span.Length);
        return span;
    }

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="pk"></param>
    /// <returns></returns>
    public static String ReadZeroString(this IPacket pk) => pk.ReadZero().ToStr();

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    public static String ReadZeroString(this ref SpanReader reader) => reader.ReadZero()[..^1].ToStr();

    /// <summary>写入C格式字符串</summary>
    /// <param name="writer"></param>
    /// <param name="value"></param>
    public static void WriteZeroString(this ref SpanWriter writer, String value)
    {
        writer.Write(value, value.Length);
        writer.Write((Byte)0);
    }

    /// <summary>读取集合元素个数</summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    public static Int32 ReadLength(this BinaryReader reader)
    {
        var c = reader.ReadByte();

        return c switch
        {
            251 => -1,
            252 => reader.ReadUInt16(),
            253 => reader.ReadBytes(3).ToInt(),
            254 => reader.ReadInt32(),
            _ => c,
        };
    }

    /// <summary>写入集合元素个数</summary>
    /// <param name="writer"></param>
    /// <param name="length"></param>
    public static void WriteLength(this ref SpanWriter writer, Int64 length)
    {
        if (length < 251)
            writer.Write((Byte)length);
        else if (length < 0xFFFF)
        {
            writer.Write((Byte)252);
            writer.Write((UInt16)length);
        }
        else if (length <= 0xFF_FFFF)
        {
            writer.Write((Byte)253);
            writer.Write((Byte)(length & 0xFF));
            writer.Write((Byte)((length >> 8) & 0xFF));
            writer.Write((Byte)((length >> 16) & 0xFF));
        }
        else
        {
            writer.Write((Byte)254);
            writer.Write((UInt32)length);
        }
    }
}