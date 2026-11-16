using System.ComponentModel;
using NewLife.Data;
using NewLife.MySql;
using NewLife.MySql.Messages;

namespace UnitTest;

/// <summary>OwnerPacket 资源管理与发送路径释放测试。验证 Slice 引用计数共享语义与 SendPacketAsync 归还池化缓冲</summary>
/// <remarks>对应审计遗留项 L1：OwnerPacket 基于 ArrayPool.Shared，发送路径必须 Dispose，否则池化失效产生 GC 压力</remarks>
[Collection(TestCollections.InMemory)]
public class OwnerPacketResourceTests
{
    [Fact]
    [DisplayName("OwnerPacket.Slice为引用计数共享且Dispose幂等")]
    public void Slice_SharedReference_DisposeIdempotent()
    {
        var owner = new OwnerPacket(8192);
        owner[0] = 0x11;
        owner[4] = 0x22;

        // 模拟 BuildExecutePacket：预留帧头后切片，两者共享底层缓冲（引用计数）
        var slice = owner.Slice(4, 100);
        Assert.IsAssignableFrom<IOwnerPacket>(slice);

        // v12 语义：原句柄保持有效，双方各自可读，双方各持一份引用
        Assert.Equal(0x11, owner[0]);
        Assert.Equal(0x22, slice[0]);
        Assert.Equal(2, slice.RefCount);

        // 释放原句柄：缓冲不归还（切片仍持有引用），切片继续可读
        owner.Dispose();
        Assert.Equal(0x22, slice[0]);
        Assert.Equal(1, slice.RefCount);

        // 释放切片：最后一份引用归还池化缓冲；再次 Dispose 仍安全（幂等）
        var ownerSlice = (IOwnerPacket)slice;
        ownerSlice.Dispose();
        ownerSlice.Dispose();
    }

    [Fact]
    [DisplayName("BuildExecutePacket不泄漏原句柄")]
    public void BuildExecutePacket_ReleasesOriginalHandle()
    {
        using var sql = new SqlClient();
        using var pk = (OwnerPacket)sql.BuildExecutePacket(1, [1, "x"]);

        // 原句柄已在方法内释放；若漏释放，返回切片会与原句柄同时持有引用，RefCount 为 2
        Assert.Equal(1, pk.RefCount);
    }

    [Fact]
    [DisplayName("CreateReader不增加数据包引用计数")]
    public void CreateReader_DoesNotLeakSlice()
    {
        var data = new OwnerPacket(8);
        data[0] = 0x00;

        var rs = new ServerPacket(System.IO.Stream.Null) { Length = 8 };
        rs.Set(data);
        try
        {
            Assert.Equal(1, data.RefCount);

            // 读取器直接复用数据包并按 offset 前进；若内部再 Slice，会出现第二份引用且无人释放
            var reader = rs.CreateReader(1);
            Assert.Equal(1, data.RefCount);
            reader.Dispose();
        }
        finally
        {
            rs.Dispose();
        }
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

        // OwnerPacket 无前置空间时，PrepareFrameHeader 新建链头 { Next = 原包切片 }
        // 发送完成后 SendPacketAsync 释放链头并释放原包句柄（发送即消费）
        var pk = new OwnerPacket(4);
        pk[0] = 0x01;
        pk[1] = 0x02;
        pk[2] = 0x03;
        pk[3] = 0x04;

        await sql.SendPacketAsync(pk);

        // 发送完成后原包句柄已由 SendPacketAsync 释放（发送即消费），访问 Buffer 应抛 ObjectDisposedException（缓冲已归还）
        Assert.Throws<ObjectDisposedException>(() => _ = pk.Buffer);
    }
}
