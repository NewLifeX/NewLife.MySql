using System.ComponentModel;
using System.Text;
using NewLife.MySql.Common;

namespace UnitTest;

/// <summary>WebAuthn 最小 CBOR 编解码单元测试。验证 Map/字符串/字节串的编码与解码、整数键与跳过嵌套值</summary>
[Collection(TestCollections.InMemory)]
public class CborTests
{
    [Fact]
    [DisplayName("CBOR写入Map与字符串字节串")]
    public void WhenWriteThenEncodedBytesCorrect()
    {
        var writer = new CborWriter();
        writer.WriteMap(2);
        writer.WriteString("ab");
        writer.WriteBytes([0x01, 0x02]);

        Byte[] expected = [0xA2, 0x62, (Byte)'a', (Byte)'b', 0x42, 0x01, 0x02];
        Assert.Equal(expected, writer.ToArray());
    }

    [Fact]
    [DisplayName("CBOR长字符串使用扩展长度编码")]
    public void WhenWriteLongStringThenLengthPrefixExtended()
    {
        var value = new String('x', 30);

        var writer = new CborWriter();
        writer.WriteString(value);

        var bytes = writer.ToArray();
        Assert.Equal(0x78, bytes[0]);
        Assert.Equal(30, bytes[1]);
        Assert.Equal(32, bytes.Length);

        var reader = new CborReader(bytes);
        Assert.True(reader.ReadString(out var read));
        Assert.Equal(value, read);
    }

    [Fact]
    [DisplayName("CBOR超长字符串使用32位长度编码")]
    public void WhenWriteVeryLongStringThen32BitLengthPrefix()
    {
        var value = new String('y', 70000);

        var writer = new CborWriter();
        writer.WriteString(value);

        var bytes = writer.ToArray();
        Assert.Equal(0x7A, bytes[0]);
        Assert.Equal(70000, (bytes[1] << 24) | (bytes[2] << 16) | (bytes[3] << 8) | bytes[4]);

        var reader = new CborReader(bytes);
        Assert.True(reader.ReadString(out var read));
        Assert.Equal(value, read);
    }

    [Fact]
    [DisplayName("CBOR超长字节串使用32位长度编码")]
    public void WhenWriteVeryLongBytesThen32BitLengthPrefix()
    {
        var value = new Byte[70000];

        var writer = new CborWriter();
        writer.WriteBytes(value);

        var bytes = writer.ToArray();
        Assert.Equal(0x5A, bytes[0]);

        var reader = new CborReader(bytes);
        Assert.True(reader.ReadBytes(out var read));
        Assert.Equal(value, read);
    }

    [Fact]
    [DisplayName("CBOR读写Map字符串字节串往返一致")]
    public void WhenWriteThenReadRoundTrip()
    {
        var writer = new CborWriter();
        writer.WriteMap(3);
        writer.WriteString("first");
        writer.WriteString("second");
        writer.WriteBytes([0xFF, 0x00]);

        var reader = new CborReader(writer.ToArray());
        Assert.True(reader.ReadMap(out var count));
        Assert.Equal(3, count);

        Assert.True(reader.ReadString(out var s1));
        Assert.Equal("first", s1);

        Assert.True(reader.ReadString(out var s2));
        Assert.Equal("second", s2);

        Assert.True(reader.ReadBytes(out var buf));
        Byte[] expected = [0xFF, 0x00];
        Assert.Equal(expected, buf);
    }

    [Fact]
    [DisplayName("CBOR读取整数键包含负数与扩展长度")]
    public void WhenReadInt32ThenSupportNegativeAndExtended()
    {
        AssertReadInt([0x01], 1);
        AssertReadInt([0x18, 0x64], 100);
        AssertReadInt([0x19, 0x01, 0x00], 256);
        AssertReadInt([0x1A, 0x00, 0x01, 0x00, 0x00], 65536);
        AssertReadInt([0x1A, 0x01, 0x00, 0x00, 0x00], 0x100_0000);
        AssertReadInt([0x20], -1);
        AssertReadInt([0x38, 0x63], -100);
    }

    [Fact]
    [DisplayName("CBOR读取类型不匹配返回false")]
    public void WhenReadWrongMajorTypeThenFalse()
    {
        var reader = new CborReader([0x62, (Byte)'a', (Byte)'b']);

        Assert.False(reader.ReadInt32(out _));
        Assert.False(reader.ReadMap(out _));
        Assert.False(reader.ReadBytes(out _));
    }

    [Fact]
    [DisplayName("CBOR数据截断或为空时返回false")]
    public void WhenDataTruncatedThenFalse()
    {
        var empty = new CborReader([]);
        Assert.False(empty.ReadMap(out _));
        Assert.False(empty.ReadInt32(out _));
        Assert.False(empty.ReadString(out _));
        Assert.False(empty.ReadBytes(out _));

        var shortString = new CborReader([0x65, (Byte)'a']);
        Assert.False(shortString.ReadString(out _));

        var shortBytes = new CborReader([0x45, 0x01]);
        Assert.False(shortBytes.ReadBytes(out _));
    }

    [Fact]
    [DisplayName("CBOR跳过嵌套Map与数组后读取下一值")]
    public void WhenSkipNestedValueThenNextValueReadable()
    {
        // { "a": 1, "b": [2, 3] } 后跟字符串 "tail"
        Byte[] data =
        [
            0xA2,
            0x61, (Byte)'a', 0x01,
            0x61, (Byte)'b', 0x82, 0x02, 0x03,
            0x64, (Byte)'t', (Byte)'a', (Byte)'i', (Byte)'l',
        ];
        var reader = new CborReader(data);

        reader.SkipValue();

        Assert.True(reader.ReadString(out var tail));
        Assert.Equal("tail", tail);
    }

    [Fact]
    [DisplayName("CBOR跳过字节串与字符串")]
    public void WhenSkipStringAndBytesThenNextValueReadable()
    {
        var writer = new CborWriter();
        writer.WriteString("skip");
        writer.WriteBytes([1, 2, 3, 4]);
        writer.WriteString("tail");

        var reader = new CborReader(writer.ToArray());
        reader.SkipValue();
        reader.SkipValue();

        Assert.True(reader.ReadString(out var tail));
        Assert.Equal("tail", tail);
    }

    [Fact]
    [DisplayName("CBOR UTF8字符串往返")]
    public void WhenWriteUtf8StringThenRoundTrip()
    {
        var value = "中文WebAuthn";

        var writer = new CborWriter();
        writer.WriteString(value);

        Assert.Equal(Encoding.UTF8.GetByteCount(value) + 1, writer.ToArray().Length);

        var reader = new CborReader(writer.ToArray());
        Assert.True(reader.ReadString(out var read));
        Assert.Equal(value, read);
    }

    private static void AssertReadInt(Byte[] data, Int32 expected)
    {
        var reader = new CborReader(data);
        Assert.True(reader.ReadInt32(out var value));
        Assert.Equal(expected, value);
    }
}
