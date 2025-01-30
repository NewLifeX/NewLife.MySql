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
    /// <param name="reader"></param>
    /// <returns></returns>
    public static IPacket ReadZero(this BinaryReader reader)
    {
        var ms = reader.BaseStream as MemoryStream;
        var p = (Int32)ms.Position;
        var buf = ms.GetBuffer();
        var k = 0;
        for (k = p; k < ms.Length; k++)
        {
            if (buf[k] == 0) break;
        }

        var len = k - p;
        ms.Seek(len + 1, SeekOrigin.Current);

        return new ArrayPacket(buf, p, len);
    }

    /// <summary>读取零结尾的C格式字符串</summary>
    public static ReadOnlySpan<Byte> ReadZero(this ReadOnlySpan<Byte> span)
    {
        for (var k = 0; k < span.Length; k++)
        {
            if (span[k] == 0) return span[..(k + 1)];
        }

        return span;
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
    public static String ReadZeroString(this BinaryReader reader) => reader.ReadZero().ToStr();

    public static String ReadZeroString(this ref SpanReader reader) => reader.ReadZero()[..^1].ToStr();

    public static void WriteZero(this BinaryWriter writer, IPacket pk)
    {
        //writer.Write(pk.Data, pk.Offset, pk.Count);
        pk.CopyTo(writer.BaseStream);
        writer.Write((Byte)0);
    }

    public static void WriteZeroString(this BinaryWriter writer, String value)
    {
        WriteZero(writer, (ArrayPacket)value.GetBytes());
    }

    /// <summary>写入C格式字符串</summary>
    /// <param name="writer"></param>
    /// <param name="value"></param>
    public static void WriteZeroString(this ref SpanWriter writer, String value)
    {
        writer.Write(value, value.Length);
        writer.Write((Byte)0);
    }

    public static Int64 ReadFieldLength(this BinaryReader reader)
    {
        var c = reader.ReadByte();

        return c switch
        {
            251 => -1,
            252 => reader.ReadUInt16(),
            253 => reader.ReadBytes(3).ToInt(),
            254 => reader.ReadInt64(),
            _ => c,
        };
    }

    public static void WriteLength(this BinaryWriter writer, Int64 length)
    {
        if (length < 251)
            writer.Write((Byte)length);
        else if (length < 65536L)
        {
            writer.Write((Byte)252);
            writer.Write((UInt16)length);
        }
        else if (length < 16777216L)
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

    public static void WriteLength(this SpanWriter writer, Int64 length)
    {
        if (length < 251)
            writer.Write((Byte)length);
        else if (length < 65536L)
        {
            writer.Write((Byte)252);
            writer.Write((UInt16)length);
        }
        else if (length < 16777216L)
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