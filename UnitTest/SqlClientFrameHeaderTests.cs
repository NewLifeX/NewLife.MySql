using System.ComponentModel;
using NewLife.Data;
using NewLife.MySql;

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
}
