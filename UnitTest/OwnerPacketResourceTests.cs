using System.ComponentModel;
using NewLife.Data;
using NewLife.MySql;

namespace UnitTest;

/// <summary>OwnerPacket 资源管理与发送路径释放测试。验证 Slice 所有权转移语义与 SendPacketAsync 归还池化缓冲</summary>
/// <remarks>对应审计遗留项 L1：OwnerPacket 基于 ArrayPool.Shared，发送路径必须 Dispose，否则池化失效产生 GC 压力</remarks>
[Collection(TestCollections.InMemory)]
public class OwnerPacketResourceTests
{
    [Fact]
    [DisplayName("OwnerPacket.Slice默认转移所有权且Dispose幂等")]
    public void Slice_TransfersOwnership_DisposeIdempotent()
    {
        var owner = new OwnerPacket(8192);
        // 模拟 BuildExecutePacket：预留帧头后切片
        var slice = owner.Slice(4, 100);

        // Slice 默认转移所有权，返回 IOwnerPacket 接管缓冲
        Assert.IsAssignableFrom<IOwnerPacket>(slice);
        var ownerSlice = (IOwnerPacket)slice;

        // 所有权已转移：原实例 Dispose 是幂等 no-op，不抛异常
        owner.Dispose();
        ownerSlice.Dispose();

        // 再次 Dispose 仍安全（幂等）
        ownerSlice.Dispose();
    }

    [Fact]
    [DisplayName("SendPacketAsync输出正确的MySQL帧头")]
    public async Task SendPacketAsync_WritesCorrectFrame()
    {
        using var sql = new SqlClient { BaseStream = new MemoryStream(), Timeout = 5 };

        // 构造负载（3字节数据）
        var pk = new OwnerPacket(3);
        pk[0] = 0xAA;
        pk[1] = 0xBB;
        pk[2] = 0xCC;

        await sql.SendPacketAsync(pk);

        var ms = (MemoryStream)sql.BaseStream!;
        var buf = ms.ToArray();

        // 帧头：3字节长度（小端）+ 1字节序列号（初始 1）+ 数据
        Assert.Equal(7, buf.Length);
        Assert.Equal(3, buf[0]);
        Assert.Equal(0, buf[1]);
        Assert.Equal(0, buf[2]);
        Assert.Equal(1, buf[3]);
        Assert.Equal(0xAA, buf[4]);
        Assert.Equal(0xBB, buf[5]);
        Assert.Equal(0xCC, buf[6]);
    }

    [Fact]
    [DisplayName("SendPacketAsync发送后归还池化缓冲")]
    public async Task SendPacketAsync_DisposesOwnerPacket()
    {
        using var sql = new SqlClient { BaseStream = new MemoryStream(), Timeout = 5 };

        // OwnerPacket 无前置空间时，ExpandHeader 创建新链头 { Next = pk }
        // 发送完成后 SendPacketAsync 应释放链头并级联释放原包
        var pk = new OwnerPacket(4);
        pk[0] = 0x01;
        pk[1] = 0x02;
        pk[2] = 0x03;
        pk[3] = 0x04;

        await sql.SendPacketAsync(pk);

        // 发送完成后原包所有权已由链头级联释放，访问 Buffer 应抛 ObjectDisposedException（缓冲已归还）
        Assert.Throws<ObjectDisposedException>(() => _ = pk.Buffer);
    }
}
