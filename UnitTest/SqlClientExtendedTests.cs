using NewLife;
using NewLife.Buffers;
using NewLife.Data;
using NewLife.MySql;
using NewLife.MySql.Common;
using NewLife.MySql.Messages;
using NewLife.Reflection;
using NewLife.Security;
using System.Net.Sockets;

namespace UnitTest;

/// <summary>SqlClient 新增单元测试</summary>
[Collection(TestCollections.WriteOperations)]
public class SqlClientExtendedTests
{
    [Fact(DisplayName = "测试连接超时处理")]
    public async Task TestConnectionTimeout()
    {
        // 使用一个不可路由的IP地址（RFC 5737 TEST-NET-1）和一个不太可能开放的端口
        // 注意：这个测试依赖于网络环境，在某些系统上可能立即失败而不是超时
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "10.255.255.1", // 私有地址空间中不太可能存在的地址
            Port = 9999,
            UserID = "root",
            Password = "root",
            ConnectionTimeout = 1 // 使用较短的超时时间加快测试
        };

        var client = new SqlClient(setting);

        // 可能抛出 TimeoutException 或 SocketException，取决于网络环境
        var exception = await Assert.ThrowsAnyAsync<Exception>(async () => await client.OpenAsync());
        
        // 验证是超时异常或连接失败异常
        Assert.True(
            exception is TimeoutException || exception is SocketException,
            $"期望 TimeoutException 或 SocketException，实际: {exception.GetType().Name}"
        );
    }

    [Fact(DisplayName = "测试连接超时配置生效")]
    public void TestConnectionTimeout_Configuration()
    {
        // 验证超时配置正确传递到客户端
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "localhost",
            Port = 3306,
            ConnectionTimeout = 30
        };

        var client = new SqlClient(setting);
        
        Assert.Equal(30, client.Setting.ConnectionTimeout);
    }

    [Fact(DisplayName = "测试Reset方法清除残留数据")]
    public void TestReset_WithAvailableData()
    {
        // 创建包含残留数据的NetworkStream模拟场景
        var buf = Rand.NextBytes(100);
        var ms = new MemoryStream(buf);
        
        var client = new SqlClient { BaseStream = ms };
        client.SetValue("Active", true);

        // NetworkStream模拟需要实际网络连接，这里测试基本流的情况
        var result = client.Reset();
        
        // 基本Stream不是NetworkStream，应该能正常执行
        Assert.True(result);
    }

    [Fact(DisplayName = "测试Reset方法在连接关闭时返回false")]
    public void TestReset_WhenInactive()
    {
        var client = new SqlClient();
        
        var result = client.Reset();
        
        Assert.False(result);
    }

    [Fact(DisplayName = "测试Dispose释放资源")]
    public async Task TestDispose()
    {
        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        client.Dispose();
        
        // 确认资源已释放
        Assert.Null(client.GetValue("_client"));
        Assert.Null(client.GetValue("_stream"));
    }

    [Fact(DisplayName = "测试ReadPacketAsync数据不足异常")]
    public async Task TestReadPacket_InsufficientData()
    {
        // 只写入2字节，不足4字节帧头
        var ms = new MemoryStream([0x01, 0x02]);
        var client = new SqlClient { BaseStream = ms };

        // ReadExactlyAsync 在数据不足时抛出 EndOfStreamException
        await Assert.ThrowsAsync<EndOfStreamException>(async () => await client.ReadPacketAsync());
    }

    [Fact(DisplayName = "测试SendPacketAsync自动填充帧头")]
    public async Task TestSendPacket_AutoFillHeader()
    {
        var data = Rand.NextBytes(50);
        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };
        
        client.SetValue("_seq", (Byte)5);

        await client.SendPacketAsync((ArrayPacket)data);

        var result = ms.ToArray();
        
        // 验证帧头：3字节长度 + 1字节序列号
        Assert.Equal(54, result.Length); // 4字节帧头 + 50字节数据
        Assert.Equal(50, (Int32)(result.ToUInt32(0) & 0xFF_FFFF));
        Assert.Equal(5, result[3]);
        Assert.Equal(6, (Byte)client.GetValue("_seq")!); // 序列号递增
    }

    [Fact(DisplayName = "测试SendQueryAsync重置序列号")]
    public async Task TestSendQuery_ResetSequence()
    {
        var sql = "SELECT 1";
        using var pk = new OwnerPacket(4 + 1 + sql.Length);
        var writer = new SpanWriter(pk);
        writer.Advance(4);
        writer.WriteByte(0);
        writer.Write(sql, -1);

        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };
        
        client.SetValue("_seq", (Byte)100); // 设置一个非0序列号

        await client.SendQueryAsync(pk.Slice(4, -1));

        var result = ms.ToArray();
        
        // 验证序列号被重置为0
        Assert.Equal(0, result[3]);
        Assert.Equal((Byte)DbCmd.QUERY, result[4]);
    }

    [Fact(DisplayName = "测试Close多次调用不抛异常")]
    public void TestClose_MultipleCall()
    {
        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        // 多次调用Close不应抛异常
        client.Close();
        client.Close();
        
        Assert.False(client.Active);
    }

    [Fact(DisplayName = "测试GetResult解析OK包")]
    public void TestGetResult_OKPacket()
    {
        // 构造OK包：0x00 + affected_rows(1) + insert_id(5) + status(2) + warnings(2)
        var data = new Byte[] { 0x00, 0x0A, 0x05, 0x00, 0x02, 0x00, 0x00 };
        using var pk = new OwnerPacket(data.Length);
        data.CopyTo(pk.Buffer, pk.Offset);
        var response = new ServerPacket(new MemoryStream());
        response.Set(pk);

        var client = new SqlClient();
        client.SetValue("Capability", ClientFlags.PROTOCOL_41);
        
        var result = client.GetResult(response);
        
        Assert.Equal(0, result.FieldCount);
        Assert.Equal(10, result.AffectedRows);
        Assert.Equal(5, result.InsertedId);
    }

    [Fact(DisplayName = "测试GetResult解析结果集包")]
    public void TestGetResult_ResultSet()
    {
        // 构造结果集包：列数为3
        var data = new Byte[] { 0x03 };
        using var pk = new OwnerPacket(data.Length);
        data.CopyTo(pk.Buffer, pk.Offset);
        var response = new ServerPacket(new MemoryStream());
        response.Set(pk);

        var client = new SqlClient();
        
        var result = client.GetResult(response);
        
        Assert.Equal(3, result.FieldCount);
        Assert.Equal(0, result.AffectedRows);
    }

    [Fact(DisplayName = "测试PingAsync在连接关闭时返回false")]
    public async Task TestPing_WhenInactive()
    {
        var client = new SqlClient();
        
        var result = await client.PingAsync();
        
        Assert.False(result);
    }

    [Fact(DisplayName = "测试配置属性初始化")]
    public void TestProperties_Initialization()
    {
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "localhost",
            Port = 3307,
            UserID = "testuser",
            Database = "testdb"
        };

        var client = new SqlClient(setting);
        
        Assert.Equal("localhost", client.Setting.Server);
        Assert.Equal(3307, client.Setting.Port);
        Assert.Equal("testuser", client.Setting.UserID);
        Assert.Equal("testdb", client.Setting.Database);
        Assert.False(client.Active);
        Assert.NotNull(client.Encoding);
    }
}
