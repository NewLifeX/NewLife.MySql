using System.Text;
using NewLife.Buffers;
using NewLife.Data;

namespace NewLife.MySql.Common;

/// <summary>二进制辅助</summary>
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
    /// <param name="stream"></param>
    /// <param name="span"></param>
    /// <returns></returns>
    public static Span<Byte> ReadZero(this Stream stream, Span<Byte> span)
    {
        var i = 0;
        for (i = 0; i < span.Length; i++)
        {
            var b = stream.ReadByte();
            if (b <= 0) break;

            span[i] = (Byte)b;
        }

        return span[..i];
    }

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    public static Span<Byte> ReadZero(this BinaryReader reader)
    {
        var maxLength = 1024;
        //if (reader.BaseStream is NetworkStream ns) maxLength = ns.Socket.Available;

        return reader.BaseStream.ReadZero(new Byte[maxLength]);
    }

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="pk"></param>
    /// <returns></returns>
    public static String ReadZeroString(this IPacket pk) => pk.ReadZero().ToStr();

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    public static String ReadZeroString(this ref SpanReader reader) => reader.ReadZero()[..^1].ToStr();

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="data"></param>
    /// <returns></returns>
    public static String ReadZeroString(this Byte[] data)
    {
        var p = 0;
        while (p < data.Length && data[p] != 0) p++;

        return Encoding.UTF8.GetString(data, 0, p);
    }

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="stream"></param>
    /// <param name="maxLength"></param>
    /// <returns></returns>
    public static String ReadZeroString(this Stream stream, Int32 maxLength)
    {
        Span<Byte> span = stackalloc Byte[maxLength];
        span = stream.ReadZero(span);

        return Encoding.UTF8.GetString(span);
    }

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    public static String ReadZeroString(this BinaryReader reader) => reader.BaseStream.ReadZeroString(1024);

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

    /// <summary>读取集合元素个数</summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    public static Int32 ReadLength(this ref SpanReader reader)
    {
        var c = reader.ReadByte();

        return c switch
        {
            251 => -1,
            252 => reader.ReadUInt16(),
            253 => reader.ReadByte() << 16 | reader.ReadByte() << 8 | reader.ReadByte(),
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

    /// <summary>是否结尾数据包</summary>
    public static Boolean IsEOF(this IPacket pk) => pk.Length != 0 && pk[0] == 0xFE;

    /// <summary>是否OK数据包</summary>
    public static Boolean IsOK(this IPacket pk) => pk.Length != 0 && pk[0] == 0x00;

    /// <summary>是否错误数据包</summary>
    public static Boolean IsError(this IPacket pk) => pk.Length != 0 && pk[0] == 0xFF;
}