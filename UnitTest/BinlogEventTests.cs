using NewLife.Buffers;
using NewLife.Data;
using NewLife.MySql;
using NewLife.MySql.Binlog;
using NewLife.MySql.Common;
using NewLife.MySql.Messages;
using NewLife.Reflection;

namespace UnitTest;

/// <summary>Binlog 事件模型和协议解析单元测试</summary>
[Collection(TestCollections.InMemory)]
public class BinlogEventTests
{
    [Fact(DisplayName = "测试BinlogEvent基本属性")]
    public void TestBinlogEventProperties()
    {
        var ev = new BinlogEvent
        {
            Timestamp = 1700000000,
            EventType = BinlogEventType.QUERY_EVENT,
            ServerId = 1,
            EventLength = 100,
            NextPosition = 200,
            Flags = 0,
        };

        Assert.Equal(BinlogEventType.QUERY_EVENT, ev.EventType);
        Assert.Equal(1u, ev.ServerId);
        Assert.Equal(100u, ev.EventLength);
        Assert.Equal(200u, ev.NextPosition);
        Assert.True(ev.EventTime.Year >= 2023);
    }

    [Fact(DisplayName = "测试BinlogEvent的ToString包含表名")]
    public void TestBinlogEventToStringWithTable()
    {
        var ev = new BinlogEvent
        {
            EventType = BinlogEventType.WRITE_ROWS_EVENT,
            ServerId = 1,
            EventLength = 50,
            NextPosition = 100,
            DatabaseName = "testdb",
            TableName = "user",
        };

        var str = ev.ToString();

        Assert.Contains("WRITE_ROWS_EVENT", str);
        Assert.Contains("testdb.user", str);
    }

    [Fact(DisplayName = "测试BinlogEvent的ToString包含SQL")]
    public void TestBinlogEventToStringWithQuery()
    {
        var ev = new BinlogEvent
        {
            EventType = BinlogEventType.QUERY_EVENT,
            ServerId = 1,
            EventLength = 50,
            NextPosition = 100,
            Query = "CREATE TABLE test(id int)",
        };

        var str = ev.ToString();

        Assert.Contains("QUERY_EVENT", str);
        Assert.Contains("CREATE TABLE test(id int)", str);
    }

    [Fact(DisplayName = "测试BinlogEvent的ToString包含RotateInfo")]
    public void TestBinlogEventToStringWithRotate()
    {
        var ev = new BinlogEvent
        {
            EventType = BinlogEventType.ROTATE_EVENT,
            ServerId = 1,
            EventLength = 30,
            NextPosition = 0,
            NextBinlogFile = "binlog.000002",
            NextBinlogPosition = 4,
        };

        var str = ev.ToString();

        Assert.Contains("ROTATE_EVENT", str);
        Assert.Contains("binlog.000002:4", str);
    }

    [Fact(DisplayName = "测试BinlogPosition的ToString")]
    public void TestBinlogPositionToString()
    {
        var pos = new BinlogPosition("binlog.000001", 154);

        Assert.Equal("binlog.000001", pos.FileName);
        Assert.Equal(154u, pos.Position);
        Assert.Equal("binlog.000001:154", pos.ToString());
    }

    [Fact(DisplayName = "测试BinlogPosition默认构造")]
    public void TestBinlogPositionDefault()
    {
        var pos = new BinlogPosition();

        Assert.Equal("", pos.FileName);
        Assert.Equal(0u, pos.Position);
    }

    [Fact(DisplayName = "测试ReadBinlogEventAsync解析事件头")]
    public async Task TestReadBinlogEventAsyncParsesHeader()
    {
        // 构造 binlog 事件包：MySQL 协议帧头(4) + OK标志(1) + 事件头(19) + 数据体
        // 事件头：timestamp(4) + type_code(1) + server_id(4) + event_length(4) + next_position(4) + flags(2)

        // 构造事件头数据
        var eventData = new Byte[] { 0x01, 0x02, 0x03 }; // 数据体
        var payloadLen = 1 + 19 + eventData.Length; // OK标志 + 事件头 + 数据体

        using var pk = new OwnerPacket(payloadLen);
        var writer = new SpanWriter(pk);

        // OK 标志
        writer.Write((Byte)0x00);
        // timestamp = 1700000000 (0x6554_B400)
        writer.Write((UInt32)1700000000);
        // type_code = QUERY_EVENT (2)
        writer.Write((Byte)BinlogEventType.QUERY_EVENT);
        // server_id = 100
        writer.Write((UInt32)100);
        // event_length = 22 (19 + 3)
        writer.Write((UInt32)22);
        // next_position = 500
        writer.Write((UInt32)500);
        // flags = 0
        writer.Write((UInt16)0);
        // 数据体
        writer.Write(eventData);

        // 构造 MySQL 协议帧：3字节长度 + 1字节序列号 + 上面的 payload
        var frameMs = new MemoryStream();
        frameMs.WriteByte((Byte)(payloadLen & 0xFF));
        frameMs.WriteByte((Byte)((payloadLen >> 8) & 0xFF));
        frameMs.WriteByte((Byte)((payloadLen >> 16) & 0xFF));
        frameMs.WriteByte(0); // sequence
        frameMs.Write(pk.Buffer, pk.Offset, payloadLen);
        frameMs.Position = 0;

        var client = new SqlClient { BaseStream = frameMs };
        client.SetValue("Active", true);

        var ev = await client.ReadBinlogEventAsync();

        Assert.NotNull(ev);
        Assert.Equal(1700000000u, ev.Timestamp);
        Assert.Equal(BinlogEventType.QUERY_EVENT, ev.EventType);
        Assert.Equal(100u, ev.ServerId);
        Assert.Equal(22u, ev.EventLength);
        Assert.Equal(500u, ev.NextPosition);
        Assert.Equal(0, ev.Flags);
        Assert.NotNull(ev.Data);
        Assert.Equal(3, ev.Data!.Length);
        Assert.Equal(0x01, ev.Data[0]);
    }

    [Fact(DisplayName = "测试ReadBinlogEventAsync遇到EOF返回null")]
    public async Task TestReadBinlogEventAsyncReturnsNullOnEof()
    {
        // 构造 EOF 包：帧头(4) + 0xFE + warnings(2) + status(2)
        var payload = new Byte[] { 0xFE, 0x00, 0x00, 0x00, 0x00 };
        var frameMs = new MemoryStream();
        frameMs.WriteByte((Byte)payload.Length);
        frameMs.WriteByte(0);
        frameMs.WriteByte(0);
        frameMs.WriteByte(0); // sequence
        frameMs.Write(payload, 0, payload.Length);
        frameMs.Position = 0;

        var client = new SqlClient { BaseStream = frameMs };
        client.SetValue("Active", true);

        var ev = await client.ReadBinlogEventAsync();

        Assert.Null(ev);
    }

    [Fact(DisplayName = "测试BinlogDumpAsync构建正确的协议包")]
    public async Task TestBinlogDumpAsyncBuildsCorrectPacket()
    {
        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        var position = new BinlogPosition("binlog.000001", 154);
        await client.BinlogDumpAsync(position, 65535, 0);

        var result = ms.ToArray();

        // 跳过 4 字节帧头
        Assert.True(result.Length > 4);
        // 命令字节 = COM_BINLOG_DUMP (18)
        Assert.Equal((Byte)DbCmd.BINLOG_DUMP, result[4]);

        // binlog_pos = 154 (LE)
        var pos = BitConverter.ToUInt32(result, 5);
        Assert.Equal(154u, pos);

        // flags = 0
        var flags = BitConverter.ToUInt16(result, 9);
        Assert.Equal(0, flags);

        // server_id = 65535
        var serverId = BitConverter.ToInt32(result, 11);
        Assert.Equal(65535, serverId);

        // binlog_filename = "binlog.000001"
        var fileName = System.Text.Encoding.UTF8.GetString(result, 15, result.Length - 15);
        Assert.Equal("binlog.000001", fileName);
    }

    [Fact(DisplayName = "测试StopBinlogDumpAsync不抛异常")]
    public async Task TestStopBinlogDumpAsyncWhenInactive()
    {
        var client = new SqlClient();

        // 未激活状态调用不应抛异常
        await client.StopBinlogDumpAsync();

        Assert.False(client.Active);
    }

    [Fact(DisplayName = "测试BinlogEventType枚举值")]
    public void TestBinlogEventTypeValues()
    {
        Assert.Equal(0, (Int32)BinlogEventType.UNKNOWN_EVENT);
        Assert.Equal(2, (Int32)BinlogEventType.QUERY_EVENT);
        Assert.Equal(4, (Int32)BinlogEventType.ROTATE_EVENT);
        Assert.Equal(15, (Int32)BinlogEventType.FORMAT_DESCRIPTION_EVENT);
        Assert.Equal(19, (Int32)BinlogEventType.TABLE_MAP_EVENT);
        Assert.Equal(30, (Int32)BinlogEventType.WRITE_ROWS_EVENT);
        Assert.Equal(31, (Int32)BinlogEventType.UPDATE_ROWS_EVENT);
        Assert.Equal(32, (Int32)BinlogEventType.DELETE_ROWS_EVENT);
    }
}
