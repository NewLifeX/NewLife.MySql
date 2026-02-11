using NewLife;
using NewLife.Buffers;
using NewLife.Data;
using NewLife.MySql;
using NewLife.MySql.Common;
using NewLife.MySql.Messages;
using NewLife.Reflection;
using NewLife.Security;

namespace UnitTest;

[Collection(TestCollections.InMemory)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class SqlClientTests
{
    [Fact]
    public async Task ReadPacket()
    {
        // 通过Mod基础数据流BaseStream来测试数据包读取
        var len = Rand.Next(10, 1024);
        var buf = Rand.NextBytes(len);
        var seq = (Byte)Rand.Next(1, 256);

        var ms = new MemoryStream();
        var writer = new BinaryWriter(ms);

        // 3字节长度 + 1字节序列号。小端字节序
        var n = len | seq << 24;
        writer.Write(n);
        writer.Write(buf);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };
        var rs = await client.ReadPacketAsync();
        var pk = rs.Data;

        Assert.Equal(len, rs.Length);
        //Assert.Equal(len, pk.Length);
        Assert.Equal(buf, pk.ReadBytes());
        Assert.Equal(seq + 1, (Byte)client.GetValue("_seq")!);
        Assert.Equal(ms.Position, ms.Length);
    }

    [Fact]
    public async Task ReadPacket_256()
    {
        // 通过Mod基础数据流BaseStream来测试数据包读取
        var len = Rand.Next(1 << 8, 1 << 16);
        var buf = Rand.NextBytes(len);
        var seq = (Byte)Rand.Next(1, 256);

        var ms = new MemoryStream();
        var writer = new BinaryWriter(ms);

        // 3字节长度 + 1字节序列号。小端字节序
        var n = len | seq << 24;
        writer.Write(n);
        writer.Write(buf);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };
        var rs = await client.ReadPacketAsync();
        var reader = rs.CreateReader(0);

        Assert.Equal(len, rs.Length);
        Assert.Equal(buf, reader.ReadBytes(len));
        Assert.Equal(seq + 1, (Byte)client.GetValue("_seq")!);
        Assert.Equal(ms.Position, ms.Length);
    }

    [Fact]
    public async Task ReadPacket_65536()
    {
        // 通过Mod基础数据流BaseStream来测试数据包读取
        var len = Rand.Next(1 << 16, 1 << 24);
        var buf = Rand.NextBytes(len);
        var seq = (Byte)Rand.Next(1, 256);

        var ms = new MemoryStream();
        var writer = new BinaryWriter(ms);

        // 3字节长度 + 1字节序列号。小端字节序
        var n = len | seq << 24;
        writer.Write(n);
        writer.Write(buf);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };
        var rs = await client.ReadPacketAsync();
        var reader = rs.CreateReader(0);

        Assert.Equal(len, rs.Length);
        Assert.Equal(buf, reader.ReadBytes(len));
        Assert.Equal(seq + 1, (Byte)client.GetValue("_seq")!);
        Assert.Equal(ms.Position, ms.Length);
    }

    [Fact]
    public async Task ReadPacket_Error()
    {
        // 通过Mod基础数据流BaseStream来测试数据包读取
        var code = (UInt16)Rand.Next(1, 1 << 16);
        var msg = Rand.NextString(64);
        var seq = (Byte)Rand.Next(1, 256);

        var len = 1 + 2 + msg.Length + 1;

        var ms = new MemoryStream();
        var writer = new BinaryWriter(ms);

        // 3字节长度 + 1字节序列号。小端字节序
        var n = len | seq << 24;
        writer.Write(n);
        //writer.Write(buf);

        writer.Write((Byte)0xFF);
        writer.Write(code);
        //writer.WriteZeroString(msg);
        var buf = msg.GetBytes();
        writer.Write(buf);
        writer.Write((Byte)0);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var ex = await Assert.ThrowsAsync<MySqlException>(() => client.ReadPacketAsync());

        Assert.Equal(code, ex.ErrorCode);
        Assert.Equal(msg, ex.Message);
        Assert.Equal(ms.Position, ms.Length);
    }

    [Fact]
    public async Task ReadPacket_FE()
    {
        // 通过Mod基础数据流BaseStream来测试数据包读取
        //var len = Rand.Next(10, 1 << 24);
        //var buf = Rand.NextBytes(len);
        var seq = (Byte)Rand.Next(1, 256);
        var warnings = (UInt16)Rand.Next(1, 1 << 16);
        var status = (UInt16)Rand.Next(1, 1 << 16);

        var len = 1 + 2 + 2;

        var ms = new MemoryStream();
        var writer = new BinaryWriter(ms);

        // 3字节长度 + 1字节序列号。小端字节序
        var n = len | seq << 24;
        writer.Write(n);
        //writer.Write(buf);

        writer.Write((Byte)0xFE);
        writer.Write(warnings);
        writer.Write(status);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };
        var rs = await client.ReadPacketAsync();

        Assert.True(rs.IsEOF);
        //Assert.Null(pk);
        Assert.Equal(seq + 1, (Byte)client.GetValue("_seq")!);
        Assert.Equal(ms.Position, ms.Length);
    }

    [Fact]
    public async Task SendPacket()
    {
        // 通过Mod基础数据流BaseStream来测试数据包写入
        var len = Rand.Next(10, 1 << 24);
        var buf = Rand.NextBytes(len);
        var seq = (Byte)Rand.Next(1, 256);

        using var pk = new OwnerPacket(4 + len);
        var writer = new SpanWriter(pk);
        writer.Advance(4);

        writer.Write(buf);

        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        client.SetValue("_seq", seq);
        await client.SendPacketAsync(pk.Slice(4, -1));

        var rs = ms.ToArray();
        Assert.Equal(pk.Length, rs.Length);
        Assert.Equal(len, (Int32)(rs.ToUInt32(0) & 0xFF_FFFF));
        Assert.Equal(seq, rs[3]);
        Assert.Equal(seq + 1, (Byte)client.GetValue("_seq")!);
        Assert.Equal(buf, rs.ReadBytes(4, -1));
    }

    [Fact]
    public async Task SendPacket_NoExpand()
    {
        // 通过Mod基础数据流BaseStream来测试数据包写入
        var len = Rand.Next(10, 1 << 24);
        var buf = Rand.NextBytes(len);
        var seq = (Byte)Rand.Next(1, 256);

        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        client.SetValue("_seq", seq);
        await client.SendPacketAsync((ArrayPacket)buf);

        var rs = ms.ToArray();
        Assert.Equal(4 + buf.Length, rs.Length);
        Assert.Equal(len, (Int32)(rs.ToUInt32(0) & 0xFF_FFFF));
        Assert.Equal(seq, rs[3]);
        Assert.Equal(seq + 1, (Byte)client.GetValue("_seq")!);
        Assert.Equal(buf, rs.ReadBytes(4, -1));
    }

    [Fact]
    public void SendPacket_Close()
    {
        // 通过Mod基础数据流BaseStream来测试数据包写入
        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        var seq = (Byte)Rand.Next(1, 256);
        client.SetValue("_seq", seq);
        client.Close();

        var rs = ms.ToArray();
        Assert.Equal(5, rs.Length);
        Assert.Equal(DbCmd.QUIT, (DbCmd)rs[4]);
        Assert.Equal(1, (Byte)client.GetValue("_seq")!);
    }

    [Fact]
    public async Task SendQuery()
    {
        // 通过Mod基础数据流BaseStream来测试数据包写入
        var sql = "select * from role";

        using var pk = new OwnerPacket(4 + 1 + sql.Length);
        var writer = new SpanWriter(pk);
        writer.Advance(4);

        writer.WriteByte(0);
        writer.Write(sql, -1);

        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };
        await client.SendQueryAsync(pk.Slice(4, -1));

        var buf = ms.ToArray();
        Assert.Equal(pk.Length, buf.Length);
        Assert.Equal(0, buf[3]);
        Assert.Equal(1, (Byte)client.GetValue("_seq")!);
        Assert.Equal(DbCmd.QUERY, (DbCmd)buf[4]);
        Assert.Equal(sql, buf.ReadBytes(4 + 1, -1).ToStr());
    }

    [Fact]
    public async Task TestOpen()
    {
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "127.0.0.2",
            Port = 3306,
            UserID = "root",
            Password = "root",
            Database = "sys",
            ConnectionTimeout = 15
        };
        var set = new MySqlConnectionStringBuilder(DALTests.GetConnStr());
        setting.Password = set.Password;

        var client = new SqlClient(setting);
        await client.OpenAsync();

        Assert.NotNull(client);
        Assert.NotNull(client.Setting);
        Assert.Equal(3306, client.Setting.Port);
        Assert.NotNull(client.GetValue("_client"));
        Assert.NotEqual(0, (Int32)client.Capability);

        var welcome = client.Welcome;
        Assert.NotNull(welcome);
        Assert.NotEmpty(welcome.ServerVersion);
        Assert.Equal(ServerStatus.AutoCommitMode, welcome.Status);

        client.Close();
        Assert.Null(client.GetValue("_client"));
    }

    [Fact]
    public async Task TestConfigure()
    {
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "127.0.0.2",
            Port = 3306,
            UserID = "root",
            Password = "root",
            Database = "sys",
            ConnectionTimeout = 15
        };
        var set = new MySqlConnectionStringBuilder(DALTests.GetConnStr());
        setting.Password = set.Password;

        using var client = new SqlClient(setting);
        await client.OpenAsync();

        Assert.NotNull(client);
        Assert.NotNull(client.Setting);
        Assert.Equal(3306, client.Setting.Port);

        //var conn = new MySqlConnection(setting.ConnectionString);
        //conn.Client = client;
        await client.ConfigureAsync();
        Assert.True(client.MaxPacketSize >= 1024);

        Assert.NotNull(client.Variables);
        Assert.True(client.Variables.Count > 0);
    }

    [Fact]
    public async Task SendCommand_Ping()
    {
        // 验证SendCommand发送正确的命令字节，序列号重置为0
        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        var seq = (Byte)Rand.Next(10, 256);
        client.SetValue("_seq", seq);
        await client.SendCommandAsync(DbCmd.PING);

        var rs = ms.ToArray();

        // 4字节帧头 + 1字节命令
        Assert.Equal(5, rs.Length);
        Assert.Equal(1, (Int32)(rs.ToUInt32(0) & 0xFF_FFFF)); // 数据长度=1
        Assert.Equal(0, rs[3]); // 序列号被重置为0
        Assert.Equal(DbCmd.PING, (DbCmd)rs[4]);
        Assert.Equal(1, (Byte)client.GetValue("_seq")!);
    }

    [Fact]
    public async Task SendCommand_QUIT()
    {
        // 验证QUIT命令
        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        await client.SendCommandAsync(DbCmd.QUIT);

        var rs = ms.ToArray();
        Assert.Equal(5, rs.Length);
        Assert.Equal(1, (Int32)(rs.ToUInt32(0) & 0xFF_FFFF));
        Assert.Equal(DbCmd.QUIT, (DbCmd)rs[4]);
    }

    [Fact]
    public void Reset_WithMemoryStream()
    {
        // MemoryStream 不是 NetworkStream，Reset 应直接返回 true
        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };
        client.SetValue("Active", true);

        var result = client.Reset();

        Assert.True(result);
        Assert.True(client.Active);
    }

    [Fact]
    public void Reset_WhenInactive()
    {
        // Active=false 时 Reset 返回 false
        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        var result = client.Reset();

        Assert.False(result);
    }

    [Fact]
    public void Reset_WhenStreamNull()
    {
        // BaseStream 为 null 时 Reset 返回 false
        var client = new SqlClient();

        var result = client.Reset();

        Assert.False(result);
    }

    [Fact]
    public async Task Ping_WhenInactive()
    {
        // 未激活时 Ping 返回 false
        var client = new SqlClient();

        var result = await client.PingAsync();

        Assert.False(result);
    }

    [Fact]
    public async Task Ping_WhenStreamNull()
    {
        // BaseStream 为 null 时 Ping 直接返回 false，不改变 Active 状态
        var client = new SqlClient();
        client.SetValue("Active", true);

        var result = await client.PingAsync();

        Assert.False(result);
        // Active 不变，因为只是流不可用的早期返回
        Assert.True(client.Active);
    }

    [Fact]
    public async Task ReadPacket_Error_StateCode()
    {
        // 验证带状态码的错误包解析：msg[0]=='#' 时分离6字符状态码
        var code = (UInt16)Rand.Next(1, 1 << 16);
        var stateCode = "#42S02";
        var errorMsg = Rand.NextString(32);
        var fullMsg = stateCode + errorMsg;
        var seq = (Byte)Rand.Next(1, 256);

        var msgBytes = fullMsg.GetBytes();
        var len = 1 + 2 + msgBytes.Length + 1;

        var ms = new MemoryStream();
        var writer = new BinaryWriter(ms);

        // 3字节长度 + 1字节序列号。小端字节序
        var n = len | seq << 24;
        writer.Write(n);
        writer.Write((Byte)0xFF);
        writer.Write(code);
        writer.Write(msgBytes);
        writer.Write((Byte)0);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var ex = await Assert.ThrowsAsync<MySqlException>(() => client.ReadPacketAsync());

        Assert.Equal(code, ex.ErrorCode);
        Assert.Equal(stateCode, ex.State);
        Assert.Equal(errorMsg, ex.Message);
    }

    [Fact]
    public async Task GetResult_OK()
    {
        // 验证 OK 包解析：获取影响行数和最后插入ID
        var affectedRows = Rand.Next(1, 200);
        var insertedId = Rand.Next(1, 200);
        var seq = (Byte)Rand.Next(1, 256);

        var ms = new MemoryStream();
        var writer = new BinaryWriter(ms);

        // OK 包: 0x00 + affected_rows(length-encoded) + last_insert_id(length-encoded) + status(2) + warnings(2)
        var dataLen = 1 + 1 + 1 + 2 + 2;
        var n = dataLen | (seq << 24);
        writer.Write(n);
        writer.Write((Byte)0x00); // OK 标记
        writer.Write((Byte)affectedRows); // length-encoded (< 251 单字节)
        writer.Write((Byte)insertedId); // length-encoded
        writer.Write((UInt16)0); // status
        writer.Write((UInt16)0); // warnings

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var response = await client.ReadPacketAsync();
        var result = client.GetResult(response);

        Assert.Equal(0, result.FieldCount);
        Assert.Equal(affectedRows, result.AffectedRows);
        Assert.Equal(insertedId, result.InsertedId);
    }

    [Fact]
    public async Task GetResult_OK_Accumulated()
    {
        // 验证 affectedRow 累加行为
        var affectedRows = Rand.Next(1, 100);
        var seq = (Byte)Rand.Next(1, 256);

        var ms = new MemoryStream();
        var writer = new BinaryWriter(ms);

        var dataLen = 1 + 1 + 1 + 2 + 2;
        var n = dataLen | (seq << 24);
        writer.Write(n);
        writer.Write((Byte)0x00);
        writer.Write((Byte)affectedRows);
        writer.Write((Byte)0); // insertedId=0
        writer.Write((UInt16)0);
        writer.Write((UInt16)0);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var response = await client.ReadPacketAsync();
        var result = client.GetResult(response);

        Assert.Equal(0, result.FieldCount);
        Assert.Equal(affectedRows, result.AffectedRows);
    }

    [Fact]
    public async Task GetResult_ColumnCount()
    {
        // 验证结果集列数返回：非 OK 包时读取 length-encoded 列数
        var columnCount = Rand.Next(1, 50);
        var seq = (Byte)Rand.Next(1, 256);

        var ms = new MemoryStream();
        var writer = new BinaryWriter(ms);

        // 列数包: 单个 length-encoded integer (值 1~50 为单字节)
        var dataLen = 1;
        var n = dataLen | (seq << 24);
        writer.Write(n);
        writer.Write((Byte)columnCount);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var response = await client.ReadPacketAsync();
        var result = client.GetResult(response);

        Assert.Equal(columnCount, result.FieldCount);
        Assert.Equal(0, result.AffectedRows);
        Assert.Equal(0L, result.InsertedId);
    }

    [Fact]
    public async Task GetColumns_SingleColumn()
    {
        // 验证单列信息解析
        var seq = (Byte)1;

        var ms = new MemoryStream();

        // 写入列定义包
        WriteColumnPacket(ms, ref seq, "def", "testdb", "users", "users", "id", "id",
            0x0C, 33, 11, MySqlDbType.Int32, 0, 0);

        // 写入 EOF 包
        WriteEofPacket(ms, ref seq);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var columns = await client.GetColumnsAsync(1);

        Assert.Single(columns);
        var col = columns[0];
        Assert.Equal("def", col.Catalog);
        Assert.Equal("testdb", col.Database);
        Assert.Equal("users", col.Table);
        Assert.Equal("users", col.RealTable);
        Assert.Equal("id", col.Name);
        Assert.Equal("id", col.OriginalName);
        Assert.Equal(0x0C, col.Flag);
        Assert.Equal(33, col.Charset);
        Assert.Equal(11, col.Length);
        Assert.Equal(MySqlDbType.Int32, col.Type);
    }

    [Fact]
    public async Task GetColumns_MultipleColumns()
    {
        // 验证多列信息解析
        var seq = (Byte)1;

        var ms = new MemoryStream();

        WriteColumnPacket(ms, ref seq, "def", "testdb", "users", "users", "id", "id",
            0x0C, 33, 11, MySqlDbType.Int32, 0, 0);
        WriteColumnPacket(ms, ref seq, "def", "testdb", "users", "users", "name", "name",
            0x0C, 33, 255, MySqlDbType.VarString, 0, 0);
        WriteColumnPacket(ms, ref seq, "def", "testdb", "users", "users", "age", "age",
            0x0C, 33, 3, MySqlDbType.Int16, 0, 0);

        WriteEofPacket(ms, ref seq);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var columns = await client.GetColumnsAsync(3);

        Assert.Equal(3, columns.Length);
        Assert.Equal("id", columns[0].Name);
        Assert.Equal(MySqlDbType.Int32, columns[0].Type);
        Assert.Equal("name", columns[1].Name);
        Assert.Equal(MySqlDbType.VarString, columns[1].Type);
        Assert.Equal("age", columns[2].Name);
        Assert.Equal(MySqlDbType.Int16, columns[2].Type);
    }

    [Fact]
    public async Task NextRow_IntAndVarString()
    {
        // 验证行数据读取：Int32 和 VarString 类型
        var seq = (Byte)1;
        var ms = new MemoryStream();

        // 构造行数据包：id=42, name="hello"
        using var rowBuf = new MemoryStream();
        WriteLengthBytes(rowBuf, "42".GetBytes());
        WriteLengthBytes(rowBuf, "hello".GetBytes());
        WritePacket(ms, ref seq, rowBuf.ToArray());

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var columns = new MySqlColumn[]
        {
            new() { Name = "id", Type = MySqlDbType.Int32 },
            new() { Name = "name", Type = MySqlDbType.VarString },
        };
        var values = new Object[2];

        var result = await client.NextRowAsync(values, columns);

        Assert.True(result.HasRow);
        Assert.Equal(42, values[0]);
        Assert.Equal("hello", values[1]);
    }

    [Fact]
    public async Task NextRow_NullValue()
    {
        // 验证 NULL 值处理：length-encoded 0xFB 表示 NULL
        var seq = (Byte)1;
        var ms = new MemoryStream();

        using var rowBuf = new MemoryStream();
        WriteLengthBytes(rowBuf, "100".GetBytes());
        rowBuf.WriteByte(0xFB); // NULL 标记
        WritePacket(ms, ref seq, rowBuf.ToArray());

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var columns = new MySqlColumn[]
        {
            new() { Name = "id", Type = MySqlDbType.Int32 },
            new() { Name = "name", Type = MySqlDbType.VarString },
        };
        var values = new Object[2];

        var result = await client.NextRowAsync(values, columns);

        Assert.True(result.HasRow);
        Assert.Equal(100, values[0]);
        Assert.Equal(DBNull.Value, values[1]);
    }

    [Fact]
    public async Task NextRow_EOF()
    {
        // 验证收到 EOF 包时返回 false
        var seq = (Byte)1;
        var ms = new MemoryStream();

        WriteEofPacket(ms, ref seq);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var columns = new MySqlColumn[] { new() { Name = "id", Type = MySqlDbType.Int32 } };
        var values = new Object[1];

        var result = await client.NextRowAsync(values, columns);

        Assert.False(result.HasRow);
    }

    [Fact]
    public async Task NextRow_MultipleTypes()
    {
        // 验证多种数据类型解析：Decimal, Double, Blob, Bit, Json
        var seq = (Byte)1;
        var ms = new MemoryStream();

        using var rowBuf = new MemoryStream();
        WriteLengthBytes(rowBuf, "123.45".GetBytes()); // Decimal
        WriteLengthBytes(rowBuf, "3.14".GetBytes()); // Double
        WriteLengthBytes(rowBuf, new Byte[] { 0x01, 0x02, 0x03 }); // Blob
        // Bit 类型: 8字节小端序。0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08 -> 0x0807060504030201UL = 578437695752307201UL
        WriteLengthBytes(rowBuf, new Byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 });
        WriteLengthBytes(rowBuf, "{\"key\":1}".GetBytes()); // Json
        WritePacket(ms, ref seq, rowBuf.ToArray());

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var columns = new MySqlColumn[]
        {
            new() { Name = "price", Type = MySqlDbType.Decimal },
            new() { Name = "rate", Type = MySqlDbType.Double },
            new() { Name = "data", Type = MySqlDbType.Blob },
            new() { Name = "flag", Type = MySqlDbType.Bit },
            new() { Name = "info", Type = MySqlDbType.Json },
        };
        var values = new Object[5];

        var result = await client.NextRowAsync(values, columns);

        Assert.True(result.HasRow);
        Assert.Equal(123.45m, values[0]);
        Assert.Equal(3.14d, values[1]);
        Assert.Equal(new Byte[] { 0x01, 0x02, 0x03 }, values[2]);
        Assert.Equal(578437695752307201UL, values[3]);
        Assert.Equal("{\"key\":1}", values[4]);
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

    /// <summary>写入行数据包中的一个字段值</summary>
    private static void WriteLengthBytes(MemoryStream ms, Byte[] data)
    {
        ms.WriteByte((Byte)data.Length);
        ms.Write(data, 0, data.Length);
    }
    #endregion
}
