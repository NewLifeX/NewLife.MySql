using System.ComponentModel;
using NewLife.MySql;
using NewLife.MySql.Common;

namespace UnitTest;

/// <summary>MySqlColumn 类型映射单元测试</summary>
[Collection(TestCollections.InMemory)]
public class MySqlColumnTests
{
    [Theory]
    [InlineData(MySqlDbType.TinyBlob, 33, MySqlDbType.TinyText)]
    [InlineData(MySqlDbType.Blob, 33, MySqlDbType.Text)]
    [InlineData(MySqlDbType.MediumBlob, 33, MySqlDbType.MediumText)]
    [InlineData(MySqlDbType.LongBlob, 33, MySqlDbType.LongText)]
    [InlineData(MySqlDbType.TinyBlob, 45, MySqlDbType.TinyText)]
    [InlineData(MySqlDbType.Blob, 45, MySqlDbType.Text)]
    [InlineData(MySqlDbType.MediumBlob, 45, MySqlDbType.MediumText)]
    [InlineData(MySqlDbType.LongBlob, 45, MySqlDbType.LongText)]
    [DisplayName("非binary字符集时BLOB系列重映射为TEXT系列")]
    public async Task RemapBlobToText(MySqlDbType wireType, Int16 charset, MySqlDbType expectedType)
    {
        var seq = (Byte)1;
        var ms = new MemoryStream();

        WriteColumnPacket(ms, ref seq, "def", "information_schema", "COLUMNS", "COLUMNS",
            "COLUMN_TYPE", "COLUMN_TYPE", 0x0C, charset, 65535, wireType, 0, 0);
        WriteEofPacket(ms, ref seq);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var columns = await client.GetColumnsAsync(1);

        Assert.Single(columns);
        Assert.Equal(expectedType, columns[0].Type);
    }

    [Theory]
    [InlineData(MySqlDbType.TinyBlob, MySqlDbType.TinyBlob)]
    [InlineData(MySqlDbType.Blob, MySqlDbType.Blob)]
    [InlineData(MySqlDbType.MediumBlob, MySqlDbType.MediumBlob)]
    [InlineData(MySqlDbType.LongBlob, MySqlDbType.LongBlob)]
    [DisplayName("binary字符集时BLOB系列保持不变")]
    public async Task BlobKeepsBinaryCharset(MySqlDbType wireType, MySqlDbType expectedType)
    {
        var seq = (Byte)1;
        var ms = new MemoryStream();

        WriteColumnPacket(ms, ref seq, "def", "testdb", "files", "files",
            "content", "content", 0x0C, 63, 65535, wireType, 0, 0);
        WriteEofPacket(ms, ref seq);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var columns = await client.GetColumnsAsync(1);

        Assert.Single(columns);
        Assert.Equal(expectedType, columns[0].Type);
    }

    [Theory]
    [InlineData(MySqlDbType.String, MySqlDbType.Binary)]
    [InlineData(MySqlDbType.VarChar, MySqlDbType.VarBinary)]
    [DisplayName("binary字符集时String/VarChar重映射为Binary/VarBinary")]
    public async Task RemapStringToBinary(MySqlDbType wireType, MySqlDbType expectedType)
    {
        var seq = (Byte)1;
        var ms = new MemoryStream();

        WriteColumnPacket(ms, ref seq, "def", "testdb", "files", "files",
            "hash", "hash", 0x0C, 63, 32, wireType, 0, 0);
        WriteEofPacket(ms, ref seq);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var columns = await client.GetColumnsAsync(1);

        Assert.Single(columns);
        Assert.Equal(expectedType, columns[0].Type);
    }

    [Theory]
    [InlineData(MySqlDbType.String, 33, MySqlDbType.String)]
    [InlineData(MySqlDbType.VarChar, 45, MySqlDbType.VarChar)]
    [InlineData(MySqlDbType.Int32, 63, MySqlDbType.Int32)]
    [InlineData(MySqlDbType.Int32, 33, MySqlDbType.Int32)]
    [InlineData(MySqlDbType.Double, 63, MySqlDbType.Double)]
    [DisplayName("不受重映射影响的类型保持原值")]
    public async Task UnaffectedTypesUnchanged(MySqlDbType wireType, Int16 charset, MySqlDbType expectedType)
    {
        var seq = (Byte)1;
        var ms = new MemoryStream();

        WriteColumnPacket(ms, ref seq, "def", "testdb", "t", "t",
            "col", "col", 0x0C, charset, 11, wireType, 0, 0);
        WriteEofPacket(ms, ref seq);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var columns = await client.GetColumnsAsync(1);

        Assert.Single(columns);
        Assert.Equal(expectedType, columns[0].Type);
    }

    #region 辅助
    /// <summary>写入 MySQL 协议数据包</summary>
    private static void WritePacket(MemoryStream ms, ref Byte seq, Byte[] data)
    {
        var len = data.Length;
        var n = len | (seq++ << 24);
        ms.Write(BitConverter.GetBytes(n), 0, 4);
        ms.Write(data, 0, data.Length);
    }

    /// <summary>写入 EOF 数据包</summary>
    private static void WriteEofPacket(MemoryStream ms, ref Byte seq)
    {
        // EOF: 0xFE + warnings(2) + status(2)
        var data = new Byte[] { 0xFE, 0, 0, 0, 0 };
        WritePacket(ms, ref seq, data);
    }

    /// <summary>写入列定义数据包</summary>
    private static void WriteColumnPacket(MemoryStream ms, ref Byte seq,
        String catalog, String database, String table, String realTable,
        String name, String originalName, Byte flag, Int16 charset,
        Int32 length, MySqlDbType type, Int16 columnFlags, Byte scale)
    {
        using var buf = new MemoryStream();

        // 6 个 length-encoded string
        WriteLengthString(buf, catalog);
        WriteLengthString(buf, database);
        WriteLengthString(buf, table);
        WriteLengthString(buf, realTable);
        WriteLengthString(buf, name);
        WriteLengthString(buf, originalName);

        // 固定长度字段
        buf.WriteByte(flag);
        buf.Write(BitConverter.GetBytes(charset), 0, 2);
        buf.Write(BitConverter.GetBytes(length), 0, 4);
        buf.WriteByte((Byte)type);
        buf.Write(BitConverter.GetBytes(columnFlags), 0, 2);
        buf.WriteByte(scale);
        buf.Write(new Byte[2], 0, 2); // filler

        WritePacket(ms, ref seq, buf.ToArray());
    }

    /// <summary>写入 length-encoded string</summary>
    private static void WriteLengthString(MemoryStream ms, String value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        ms.WriteByte((Byte)bytes.Length);
        ms.Write(bytes, 0, bytes.Length);
    }
    #endregion
}
