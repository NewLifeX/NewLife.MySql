using System.ComponentModel;
using System.Text;
using NewLife.Buffers;
using NewLife.Data;
using NewLife.MySql.Common;

namespace UnitTest;

/// <summary>BinaryHelper 二进制协议辅助单元测试。验证零结尾字符串、长度编码与数据包判定</summary>
[Collection(TestCollections.InMemory)]
public class BinaryHelperTests
{
    [Fact]
    [DisplayName("IPacket读取零结尾字符串")]
    public void WhenPacketHasZeroThenReadZeroStops()
    {
        var pk = new ArrayPacket(Encoding.UTF8.GetBytes("abc\0def"), 0, 7);

        Assert.Equal("abc", pk.ReadZeroString());
        Assert.Equal(3, pk.ReadZero().Length);
    }

    [Fact]
    [DisplayName("IPacket无零结尾时返回全部内容")]
    public void WhenPacketHasNoZeroThenReadZeroReturnsAll()
    {
        var pk = new ArrayPacket(Encoding.UTF8.GetBytes("abc"), 0, 3);

        Assert.Equal("abc", pk.ReadZeroString());
    }

    [Fact]
    [DisplayName("SpanReader读取零结尾字符串并前进位置")]
    public void WhenSpanReaderReadZeroThenAdvancePastZero()
    {
        var reader = new SpanReader(Encoding.UTF8.GetBytes("abc\0def"));
        Assert.Equal("abc", reader.ReadZeroString());

        var rest = reader.ReadBytes(3);
        Assert.Equal("def", Encoding.UTF8.GetString(rest));
    }

    [Fact]
    [DisplayName("SpanReader无零结尾时消费全部剩余")]
    public void WhenSpanReaderNoZeroThenConsumeAll()
    {
        var reader = new SpanReader(Encoding.UTF8.GetBytes("tail"));

        Assert.Equal("tail", reader.ReadZeroString());
        Assert.Equal(0, reader.GetSpan().Length);
    }

    [Fact]
    [DisplayName("Stream读取零结尾字符串")]
    public void WhenStreamReadZeroThenStopAtZero()
    {
        var ms = new MemoryStream(Encoding.UTF8.GetBytes("xy\0z"));
        var buffer = new Byte[8];

        var span = ms.ReadZero(buffer);
        Assert.Equal("xy", Encoding.UTF8.GetString(span));

        Assert.Equal("z", ms.ReadZeroString(8));
    }

    [Fact]
    [DisplayName("BinaryReader读取零结尾字符串")]
    public void WhenBinaryReaderReadZeroThenStopAtZero()
    {
        var ms = new MemoryStream(Encoding.UTF8.GetBytes("hi\0there"));
        var reader = new BinaryReader(ms);

        Assert.Equal("hi", reader.ReadZeroString());
    }

    [Fact]
    [DisplayName("Byte数组读取零结尾字符串")]
    public void WhenByteArrayReadZeroThenStopAtZero()
    {
        Assert.Equal("hello", Encoding.UTF8.GetBytes("hello\0world").ReadZeroString());
        Assert.Equal("abc", Encoding.UTF8.GetBytes("abc").ReadZeroString());
    }

    [Fact]
    [DisplayName("SpanWriter写入零结尾字符串后可读回")]
    public void WhenWriteZeroStringThenRoundTrip()
    {
        Span<Byte> buffer = stackalloc Byte[16];
        var writer = new SpanWriter(buffer);
        writer.WriteZeroString("abc");

        var reader = new SpanReader(buffer[..writer.Position]);
        Assert.Equal("abc", reader.ReadZeroString());
    }

    [Theory]
    [DisplayName("长度编码往返一致")]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(250)]
    [InlineData(251)]
    [InlineData(65534)]
    [InlineData(65535)]
    [InlineData(65536)]
    [InlineData(0xFF_FFFF)]
    [InlineData(0x100_0000)]
    public void WhenWriteLengthThenRoundTrip(Int64 length)
    {
        Span<Byte> buffer = stackalloc Byte[16];
        var writer = new SpanWriter(buffer);
        writer.WriteLength(length);

        var reader = new SpanReader(buffer[..writer.Position]);
        Assert.Equal(length, reader.ReadLength());
    }

    [Fact]
    [DisplayName("长度0xFB按协议解析为NULL")]
    public void WhenReadNullLengthThenMinusOne()
    {
        var reader = new SpanReader([0xFB]);

        Assert.Equal(-1, reader.ReadLength());
    }

    [Fact]
    [DisplayName("数据包类型判定")]
    public void WhenCheckPacketTypeThenCorrectFlag()
    {
        var ok = new ArrayPacket([0x00], 0, 1);
        var eof = new ArrayPacket([0xFE], 0, 1);
        var error = new ArrayPacket([0xFF], 0, 1);
        var empty = new ArrayPacket([], 0, 0);

        Assert.True(ok.IsOK());
        Assert.False(ok.IsEOF());
        Assert.False(ok.IsError());

        Assert.True(eof.IsEOF());
        Assert.False(eof.IsOK());

        Assert.True(error.IsError());
        Assert.False(error.IsOK());

        Assert.False(empty.IsOK());
        Assert.False(empty.IsEOF());
        Assert.False(empty.IsError());
    }
}
