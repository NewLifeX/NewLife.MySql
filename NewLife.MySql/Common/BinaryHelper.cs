using NewLife.Data;

namespace NewLife.MySql.Common;

static class BinaryHelper
{
    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="pk"></param>
    /// <returns></returns>
    public static Packet ReadZero(this Packet pk)
    {
        for (var k = 0; k < pk.Count; k++)
        {
            if (pk[k] == 0) return pk.Slice(0, k);
        }

        return pk;
    }

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    public static Packet ReadZero(this BinaryReader reader)
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

        return new Packet(buf, p, len);
    }

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="pk"></param>
    /// <returns></returns>
    public static String ReadZeroString(this Packet pk) => pk.ReadZero().ToStr();

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    public static String ReadZeroString(this BinaryReader reader) => reader.ReadZero().ToStr();

    public static void WriteZero(this BinaryWriter writer, Packet pk)
    {
        //writer.Write(pk.Data, pk.Offset, pk.Count);
        pk.CopyTo(writer.BaseStream);
        writer.Write((Byte)0);
    }

    public static void WriteZeroString(this BinaryWriter writer, String value)
    {
        WriteZero(writer, value.GetBytes());
    }

    public static Int64 ReadFieldLength(this BinaryReader reader)
    {
        var c = reader.ReadByte();

        switch (c)
        {
            case 251: return -1;
            case 252: return reader.ReadUInt16();
            case 253: return reader.ReadBytes(3).ToInt();
            case 254: return reader.ReadInt64();
            default: return c;
        }
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
}