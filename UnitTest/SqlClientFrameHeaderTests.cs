using System.ComponentModel;
using NewLife;
using NewLife.Data;
using NewLife.MySql;
using NewLife.Reflection;

namespace UnitTest;

/// <summary>SqlClient 发送帧头准备路径测试。验证 PrepareFrameHeader 的零拷贝借位分支与压缩协议发送路径</summary>
/// <remarks>对应 Core v12 适配：ExpandHeader 拆分为严格借位与 PrepareHeader 自动降级，驱动组合两者保持 v11 行为</remarks>
[Collection(TestCollections.InMemory)]
public class SqlClientFrameHeaderTests
{
    [Fact]
    [DisplayName("ArrayPacket预留帧头零拷贝借位发送")]
    public async Task SendPacketAsync_ArrayPacketReserved_BorrowsHeader()
    {
        using var sql = new SqlClient { BaseStream = new MemoryStream(), Timeout = 5 };

        // 驱动发送路径的标准构造：缓冲偏移 4 字节预留帧头
        var buf = new Byte[16];
        var payload = new ArrayPacket(buf, 4, 3);
        buf[4] = 0xAA;
        buf[5] = 0xBB;
        buf[6] = 0xCC;

        await sql.SendPacketAsync(payload);

        var written = ((MemoryStream)sql.BaseStream!).ToArray();
        Assert.Equal(7, written.Length);
        Assert.Equal(3, written[0]);
        Assert.Equal(0, written[1]);
        Assert.Equal(0, written[2]);
        Assert.Equal(1, written[3]);
        Assert.Equal(0xAA, written[4]);
        Assert.Equal(0xCC, written[6]);

        // 零拷贝借位：帧头直接写回原缓冲预留区，不产生新分配
        Assert.Equal(3, buf[0]);
        Assert.Equal(1, buf[3]);
    }

    [Fact]
    [DisplayName("OwnerPacket预留帧头共享借位发送且发送即消费")]
    public async Task SendPacketAsync_OwnerPacketReserved_BorrowsSharedHeader()
    {
        using var sql = new SqlClient { BaseStream = new MemoryStream(), Timeout = 5 };

        // 预留 4 字节帧头（BuildExecutePacket 切片后的形态：Offset=4、FreeHeader=4）
        var pk = new OwnerPacket(4, 4);
        pk[0] = 0x01;
        pk[1] = 0x02;
        pk[2] = 0x03;
        pk[3] = 0x04;

        await sql.SendPacketAsync(pk);

        var written = ((MemoryStream)sql.BaseStream!).ToArray();
        Assert.Equal(8, written.Length);
        Assert.Equal(4, written[0]);
        Assert.Equal(1, written[3]);
        Assert.Equal(0x01, written[4]);
        Assert.Equal(0x04, written[7]);

        // 发送即消费：传入句柄已被释放，池化缓冲已归还
        Assert.Throws<ObjectDisposedException>(() => _ = pk.Buffer);
    }

    [Fact]
    [DisplayName("压缩协议发送小包回退未压缩并释放句柄")]
    public async Task SendPacketAsync_Compressed_SmallPacketFallback()
    {
        using var sql = new SqlClient { BaseStream = new MemoryStream(), Timeout = 5, UseCompression = true };

        var pk = new OwnerPacket(4);
        pk[0] = 0x01;
        pk[1] = 0x02;
        pk[2] = 0x03;
        pk[3] = 0x04;

        await sql.SendPacketAsync(pk);

        var written = ((MemoryStream)sql.BaseStream!).ToArray();

        // 压缩包结构：3字节长度 + 1字节seq + 3字节未压缩长度 + payload
        // 小包压缩后更大，回退发送未压缩：长度=8、未压缩标记=0
        Assert.Equal(15, written.Length);
        Assert.Equal(8, written[0] | (written[1] << 8) | (written[2] << 16));
        Assert.Equal(1, written[3]);
        Assert.Equal(0, written[4] | written[5] | written[6]);

        // payload 为带标准帧头的包：4字节帧头(seq=0) + 4字节数据
        Assert.Equal(4, written[7] | (written[8] << 8) | (written[9] << 16));
        Assert.Equal(0, written[10]);
        Assert.Equal(0x01, written[11]);
        Assert.Equal(0x04, written[14]);

        // 发送即消费：原包句柄已释放，链头与原包缓冲均归还
        Assert.Throws<ObjectDisposedException>(() => _ = pk.Buffer);
    }

    private const Int32 MaxPayload = 0xFF_FFFF;

    [Fact]
    [DisplayName("超过16MB的报文按0xFFFFFF切帧发送")]
    public async Task SendPacketAsync_LargePayload_Chunked()
    {
        using var sql = new SqlClient { BaseStream = new MemoryStream(), Timeout = 0 };
        sql.SetValue("_seq", 7);

        // 非整倍数：满长帧 + 尾帧
        var data1 = CreatePattern(MaxPayload + 1000, 7);
        await sql.SendPacketAsync((ArrayPacket)data1);

        // 恰好整倍数：满长帧 + 0 长度结束帧
        var data2 = CreatePattern(MaxPayload, 11);
        await sql.SendPacketAsync((ArrayPacket)data2);

        var written = ((MemoryStream)sql.BaseStream!).ToArray();
        var expected = (4 + MaxPayload) + (4 + 1000) + (4 + MaxPayload) + 4;
        Assert.Equal(expected, written.Length);

        // 第一帧：长度 0xFFFFFF、序列号 7，载荷为 data1 前 16MB
        Assert.Equal(MaxPayload, written[0] | (written[1] << 8) | (written[2] << 16));
        Assert.Equal(7, written[3]);
        Assert.Equal(data1.AsSpan(0, MaxPayload).ToArray(), written.AsSpan(4, MaxPayload).ToArray());

        // 第二帧：尾帧长度 1000、序列号 8
        var pos = 4 + MaxPayload;
        Assert.Equal(1000, written[pos] | (written[pos + 1] << 8) | (written[pos + 2] << 16));
        Assert.Equal(8, written[pos + 3]);
        Assert.Equal(data1.AsSpan(MaxPayload, 1000).ToArray(), written.AsSpan(pos + 4, 1000).ToArray());

        // 第三条报文：满长帧 + 0 长度结束帧
        pos += 4 + 1000;
        Assert.Equal(MaxPayload, written[pos] | (written[pos + 1] << 8) | (written[pos + 2] << 16));
        Assert.Equal(9, written[pos + 3]);
        pos += 4 + MaxPayload;
        Assert.Equal(0, written[pos] | (written[pos + 1] << 8) | (written[pos + 2] << 16));
        Assert.Equal(10, written[pos + 3]);
    }

    [Fact]
    [DisplayName("压缩协议载荷上限为16MB-4，超限明确拒绝")]
    public async Task SendPacketAsync_Compressed_PayloadLimit()
    {
        using var sql = new SqlClient { BaseStream = new MemoryStream(), Timeout = 0, UseCompression = true };

        // 最大可压缩载荷：普通帧头 4 字节 + 载荷 = 0xFFFFFF，压缩包的未压缩长度字段刚好写满
        var data = new Byte[MaxPayload - 4];
        await sql.SendPacketAsync((ArrayPacket)data);

        var written = ((MemoryStream)sql.BaseStream!).ToArray();
        Assert.True(written.Length >= 7);
        var count = written[4] | (written[5] << 8) | (written[6] << 16);
        Assert.Equal(MaxPayload, count);
        var clen = written[0] | (written[1] << 8) | (written[2] << 16);
        Assert.InRange(clen, 0, MaxPayload);

        // 再多 1 字节即超出 3 字节长度字段，必须拒绝而不是发出回绕长度
        await Assert.ThrowsAsync<NotSupportedException>(() => sql.SendPacketAsync((ArrayPacket)new Byte[MaxPayload - 3]));
    }

    [Fact]
    [DisplayName("超过16MB的连续帧接收自动拼接")]
    public async Task ReadPacketAsync_LargePayload_JoinsFrames()
    {
        var head = CreatePattern(MaxPayload, 3);
        var tail = CreatePattern(1000, 5);

        var ms = new MemoryStream(4 + MaxPayload + 4 + 1000);
        WriteFrame(ms, head, 3);
        WriteFrame(ms, tail, 4);
        ms.Position = 0;

        using var sql = new SqlClient { BaseStream = ms, Timeout = 0 };
        var rs = await sql.ReadPacketAsync();

        Assert.Equal(MaxPayload + 1000, rs.Length);
        var data = rs.Data!.ReadBytes();
        Assert.Equal(MaxPayload + 1000, data.Length);
        Assert.Equal(head, data.AsSpan(0, MaxPayload).ToArray());
        Assert.Equal(tail, data.AsSpan(MaxPayload, 1000).ToArray());
        Assert.Equal(5, (Byte)sql.GetValue("_seq")!);
    }

    private static Byte[] CreatePattern(Int32 length, Int32 seed)
    {
        var data = new Byte[length];
        for (var i = 0; i < length; i++) data[i] = (Byte)(i * 31 + seed);

        return data;
    }

    private static void WriteFrame(Stream stream, Byte[] data, Byte sequence)
    {
        stream.WriteByte((Byte)(data.Length & 0xFF));
        stream.WriteByte((Byte)((data.Length >> 8) & 0xFF));
        stream.WriteByte((Byte)((data.Length >> 16) & 0xFF));
        stream.WriteByte(sequence);
        stream.Write(data, 0, data.Length);
    }
}
