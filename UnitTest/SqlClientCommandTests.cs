using NewLife;
using NewLife.Buffers;
using NewLife.Data;
using NewLife.MySql;
using NewLife.MySql.Common;
using NewLife.MySql.Messages;
using NewLife.Reflection;
using NewLife.Security;

namespace UnitTest;

/// <summary>SqlClient 扩展命令单元测试</summary>
[Collection(TestCollections.InMemory)]
public class SqlClientCommandTests
{
    #region 辅助方法
    /// <summary>构造一个标准 OK 响应帧并写入流</summary>
    private static void WriteOkPacket(BinaryWriter writer, Byte seq = 1)
    {
        // OK_Packet: header(0x00) + affected_rows(0) + insert_id(0) + status(2) + warnings(2)
        var data = new Byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        var len = data.Length;
        writer.Write(len | (seq << 24));
        writer.Write(data);
    }

    /// <summary>构造一个 EOF 响应帧并写入流</summary>
    private static void WriteEofPacket(BinaryWriter writer, Byte seq = 1)
    {
        // EOF_Packet: header(0xFE) + warnings(2) + status(2)
        var data = new Byte[] { 0xFE, 0x00, 0x00, 0x00, 0x00 };
        var len = data.Length;
        writer.Write(len | (seq << 24));
        writer.Write(data);
    }

    /// <summary>构造一个纯文本响应帧并写入流</summary>
    private static void WriteTextPacket(BinaryWriter writer, String text, Byte seq = 1)
    {
        var data = text.GetBytes();
        var len = data.Length;
        writer.Write(len | (seq << 24));
        writer.Write(data);
    }

    /// <summary>构造一个结果集头部帧（列数）并写入流</summary>
    private static void WriteResultSetHeaderPacket(BinaryWriter writer, Byte fieldCount, Byte seq = 1)
    {
        var data = new Byte[] { fieldCount };
        writer.Write(data.Length | (seq << 24));
        writer.Write(data);
    }

    /// <summary>创建预配置响应流的 SqlClient</summary>
    private static (SqlClient client, MemoryStream requestStream) CreateClientWithResponse(Action<BinaryWriter> writeResponse)
    {
        // 响应流
        var responseMs = new MemoryStream();
        var writer = new BinaryWriter(responseMs);
        writeResponse(writer);
        responseMs.Position = 0;

        // 使用双流：发送写到 requestMs，接收从 responseMs 读取
        // 但 SqlClient 只有一个 BaseStream，所以我们需要用组合流
        // 简化方案：使用 MemoryStream 并在发送后重置位置
        // 实际上对于测试发送包的场景需要独立流
        var client = new SqlClient { BaseStream = responseMs };
        return (client, responseMs);
    }
    #endregion

    #region FieldListAsync
    [Fact(DisplayName = "FieldListAsync发送正确的命令字节")]
    public async Task FieldListAsync_SendsCorrectCommand()
    {
        // 模拟响应：构造包含发送占位空间 + EOF 响应的流
        var table = "test_table";
        var tableBytes = System.Text.Encoding.UTF8.GetBytes(table);
        var payloadLen = 1 + tableBytes.Length + 1; // cmd + table + NUL
        var sendFrameSize = 4 + payloadLen;

        var ms = new MemoryStream();
        // 预留发送空间
        ms.Write(new Byte[sendFrameSize], 0, sendFrameSize);
        // 追加 EOF 响应
        var writer = new BinaryWriter(ms);
        WriteEofPacket(writer);
        ms.Position = 0;

        var client = new SqlClient { BaseStream = ms };

        var columns = await client.FieldListAsync(table);

        // 验证发送了正确的命令并收到空结果
        Assert.Empty(columns);

        // 验证命令字节
        var sentData = ms.ToArray();
        Assert.Equal((Byte)DbCmd.FIELD_LIST, sentData[4]);
    }

    [Fact(DisplayName = "FieldListAsync表名为空时抛出异常")]
    public async Task FieldListAsync_NullTable_ThrowsException()
    {
        var client = new SqlClient { BaseStream = new MemoryStream() };

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => client.FieldListAsync(null!));
    }

    [Fact(DisplayName = "FieldListAsync空字符串表名抛出异常")]
    public async Task FieldListAsync_EmptyTable_ThrowsException()
    {
        var client = new SqlClient { BaseStream = new MemoryStream() };

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => client.FieldListAsync(""));
    }

    [Fact(DisplayName = "FieldListAsync发送的数据包包含表名和通配符")]
    public async Task FieldListAsync_WithWildcard_SendsCorrectPacket()
    {
        // 先把发送包写到流中，然后在流末尾追加 EOF 响应供读取
        // 由于 MemoryStream 可同时读写，SendPacketAsync 写入后 Position 前移，
        // 我们在其后写入响应帧让 ReadPacketAsync 读到
        var ms = new MemoryStream();

        // 预写 EOF 响应到足够远的位置
        // 我们需要预估发送包的大小。FIELD_LIST: 4(header) + 1(cmd) + 10("test_table") + 1(NUL) + 1("*") = 17
        // 先在流中放一些占位字节，然后在发送后追加响应
        var client = new SqlClient { BaseStream = ms };

        // 写入 EOF 响应帧到一个临时缓冲区
        var eofData = new Byte[] { 0xFE, 0x00, 0x00, 0x00, 0x00 };
        var eofLen = eofData.Length;
        var eofFrame = new Byte[4 + eofLen];
        eofFrame[0] = (Byte)(eofLen & 0xFF);
        eofFrame[1] = (Byte)((eofLen >> 8) & 0xFF);
        eofFrame[2] = (Byte)((eofLen >> 16) & 0xFF);
        eofFrame[3] = 1; // seq

        Array.Copy(eofData, 0, eofFrame, 4, eofLen);

        // 先发送命令，然后在流末尾追加响应
        // 使用特殊的测试策略：在发送后手动注入响应
        var table = "test_table";
        var wildcard = "*";

        // 预先计算发送包大小并在那个位置之后放响应
        var tableBytes = System.Text.Encoding.UTF8.GetBytes(table);
        var wildcardBytes = System.Text.Encoding.UTF8.GetBytes(wildcard);
        var payloadLen = 1 + tableBytes.Length + 1 + wildcardBytes.Length;
        var frameSize = 4 + payloadLen; // 帧头 + 载荷

        // 在流中写入足够的空字节让发送写入，然后是 EOF 响应
        ms.Write(new Byte[frameSize], 0, frameSize);
        ms.Write(eofFrame, 0, eofFrame.Length);
        ms.Position = 0;

        var columns = await client.FieldListAsync(table, wildcard);
        Assert.Empty(columns);

        // 验证发送的包
        var sentData = ms.ToArray();
        Assert.Equal((Byte)DbCmd.FIELD_LIST, sentData[4]);

        // 验证表名
        var sentTable = System.Text.Encoding.UTF8.GetString(sentData, 5, tableBytes.Length);
        Assert.Equal(table, sentTable);

        // 验证 NUL 终止符
        Assert.Equal(0, sentData[5 + tableBytes.Length]);

        // 验证通配符
        var sentWild = System.Text.Encoding.UTF8.GetString(sentData, 6 + tableBytes.Length, wildcardBytes.Length);
        Assert.Equal(wildcard, sentWild);
    }
    #endregion

    #region CreateDatabaseAsync
    [Fact(DisplayName = "CreateDatabaseAsync发送正确命令并读取OK")]
    public async Task CreateDatabaseAsync_SendsCorrectCommand()
    {
        var dbName = "test_db_create";
        var dbBytes = System.Text.Encoding.UTF8.GetBytes(dbName);
        var payloadLen = 1 + dbBytes.Length;
        var frameSize = 4 + payloadLen;

        var ms = new MemoryStream();
        // 预留发送空间 + OK 响应
        ms.Write(new Byte[frameSize], 0, frameSize);

        var okWriter = new BinaryWriter(ms);
        WriteOkPacket(okWriter);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        await client.CreateDatabaseAsync(dbName);

        // 验证发送的命令字节
        var sentData = ms.ToArray();
        Assert.Equal((Byte)DbCmd.CREATE_DB, sentData[4]);

        // 验证数据库名
        var sentName = System.Text.Encoding.UTF8.GetString(sentData, 5, dbBytes.Length);
        Assert.Equal(dbName, sentName);
    }

    [Fact(DisplayName = "CreateDatabaseAsync空名称抛出异常")]
    public async Task CreateDatabaseAsync_EmptyName_ThrowsException()
    {
        var client = new SqlClient { BaseStream = new MemoryStream() };

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => client.CreateDatabaseAsync(""));
    }
    #endregion

    #region DropDatabaseAsync
    [Fact(DisplayName = "DropDatabaseAsync发送正确命令并读取OK")]
    public async Task DropDatabaseAsync_SendsCorrectCommand()
    {
        var dbName = "test_db_drop";
        var dbBytes = System.Text.Encoding.UTF8.GetBytes(dbName);
        var payloadLen = 1 + dbBytes.Length;
        var frameSize = 4 + payloadLen;

        var ms = new MemoryStream();
        ms.Write(new Byte[frameSize], 0, frameSize);

        var okWriter = new BinaryWriter(ms);
        WriteOkPacket(okWriter);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        await client.DropDatabaseAsync(dbName);

        var sentData = ms.ToArray();
        Assert.Equal((Byte)DbCmd.DROP_DB, sentData[4]);

        var sentName = System.Text.Encoding.UTF8.GetString(sentData, 5, dbBytes.Length);
        Assert.Equal(dbName, sentName);
    }

    [Fact(DisplayName = "DropDatabaseAsync空名称抛出异常")]
    public async Task DropDatabaseAsync_EmptyName_ThrowsException()
    {
        var client = new SqlClient { BaseStream = new MemoryStream() };

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => client.DropDatabaseAsync(""));
    }
    #endregion

    #region StatisticsAsync
    [Fact(DisplayName = "StatisticsAsync发送命令并返回统计字符串")]
    public async Task StatisticsAsync_ReturnsStatisticsString()
    {
        var statsText = "Uptime: 12345  Threads: 1  Questions: 678";

        // SendCommandAsync 发送 5 字节帧（4 header + 1 cmd），然后读取响应
        var sendFrameSize = 4 + 1;

        var ms = new MemoryStream();
        ms.Write(new Byte[sendFrameSize], 0, sendFrameSize);

        var writer = new BinaryWriter(ms);
        WriteTextPacket(writer, statsText);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var result = await client.StatisticsAsync();

        Assert.Equal(statsText, result);
    }
    #endregion

    #region ProcessInfoAsync
    [Fact(DisplayName = "ProcessInfoAsync发送命令并返回结果集头")]
    public async Task ProcessInfoAsync_ReturnsQueryResult()
    {
        var sendFrameSize = 4 + 1;

        var ms = new MemoryStream();
        ms.Write(new Byte[sendFrameSize], 0, sendFrameSize);

        var writer = new BinaryWriter(ms);
        // 返回结果集头（8列：Id, User, Host, db, Command, Time, State, Info）
        WriteResultSetHeaderPacket(writer, 8);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        var result = await client.ProcessInfoAsync();

        Assert.Equal(8, result.FieldCount);
        Assert.Equal(0, result.AffectedRows);
    }
    #endregion

    #region ProcessKillAsync
    [Fact(DisplayName = "ProcessKillAsync发送正确的进程ID")]
    public async Task ProcessKillAsync_SendsCorrectProcessId()
    {
        var processId = 12345;
        var sendFrameSize = 4 + 1 + 4; // header + cmd + process_id

        var ms = new MemoryStream();
        ms.Write(new Byte[sendFrameSize], 0, sendFrameSize);

        var writer = new BinaryWriter(ms);
        WriteOkPacket(writer);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        await client.ProcessKillAsync(processId);

        var sentData = ms.ToArray();
        Assert.Equal((Byte)DbCmd.PROCESS_KILL, sentData[4]);

        // 验证进程 ID（小端序）
        var sentId = sentData[5] | (sentData[6] << 8) | (sentData[7] << 16) | (sentData[8] << 24);
        Assert.Equal(processId, sentId);
    }
    #endregion

    #region SetOptionAsync
    [Fact(DisplayName = "SetOptionAsync发送多语句开启选项")]
    public async Task SetOptionAsync_MultiStatementsOn_SendsCorrectOption()
    {
        Int16 option = 0; // MYSQL_OPTION_MULTI_STATEMENTS_ON
        var sendFrameSize = 4 + 1 + 2; // header + cmd + option

        var ms = new MemoryStream();
        ms.Write(new Byte[sendFrameSize], 0, sendFrameSize);

        var writer = new BinaryWriter(ms);
        WriteEofPacket(writer);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        await client.SetOptionAsync(option);

        var sentData = ms.ToArray();
        Assert.Equal((Byte)DbCmd.SET_OPTION, sentData[4]);
        Assert.Equal(0, sentData[5]); // option low byte
        Assert.Equal(0, sentData[6]); // option high byte
    }

    [Fact(DisplayName = "SetOptionAsync发送多语句关闭选项")]
    public async Task SetOptionAsync_MultiStatementsOff_SendsCorrectOption()
    {
        Int16 option = 1; // MYSQL_OPTION_MULTI_STATEMENTS_OFF
        var sendFrameSize = 4 + 1 + 2;

        var ms = new MemoryStream();
        ms.Write(new Byte[sendFrameSize], 0, sendFrameSize);

        var writer = new BinaryWriter(ms);
        WriteEofPacket(writer);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        await client.SetOptionAsync(option);

        var sentData = ms.ToArray();
        Assert.Equal((Byte)DbCmd.SET_OPTION, sentData[4]);
        Assert.Equal(1, sentData[5]); // option = 1
        Assert.Equal(0, sentData[6]);
    }
    #endregion

    #region SendLongDataAsync
    [Fact(DisplayName = "SendLongDataAsync发送正确的数据包格式")]
    public async Task SendLongDataAsync_SendsCorrectPacket()
    {
        var statementId = 42;
        Int16 paramIndex = 1;
        var data = Rand.NextBytes(100);

        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        await client.SendLongDataAsync(statementId, paramIndex, data);

        var sentData = ms.ToArray();
        // 帧头 4 字节 + command(1) + statement_id(4) + param_id(2) + data(100) = 111
        Assert.Equal(4 + 1 + 4 + 2 + data.Length, sentData.Length);

        // 验证命令字节
        Assert.Equal((Byte)DbCmd.LONG_DATA, sentData[4]);

        // 验证 statement_id（小端序）
        var sentStmtId = sentData[5] | (sentData[6] << 8) | (sentData[7] << 16) | (sentData[8] << 24);
        Assert.Equal(statementId, sentStmtId);

        // 验证 param_index（小端序）
        var sentParamIdx = (Int16)(sentData[9] | (sentData[10] << 8));
        Assert.Equal(paramIndex, sentParamIdx);

        // 验证数据内容
        var sentPayload = new Byte[data.Length];
        Array.Copy(sentData, 11, sentPayload, 0, data.Length);
        Assert.Equal(data, sentPayload);
    }

    [Fact(DisplayName = "SendLongDataAsync空数据抛出异常")]
    public async Task SendLongDataAsync_NullData_ThrowsException()
    {
        var client = new SqlClient { BaseStream = new MemoryStream() };

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => client.SendLongDataAsync(1, 0, null!));
    }

    [Fact(DisplayName = "SendLongDataAsync无响应包")]
    public async Task SendLongDataAsync_NoResponse()
    {
        // COM_STMT_SEND_LONG_DATA 不返回响应包，验证发送后不会尝试读取
        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        await client.SendLongDataAsync(1, 0, new Byte[] { 0x01, 0x02, 0x03 });

        // 流中只有发送的数据，没有多余读取
        Assert.True(ms.Position > 0);
    }
    #endregion

    #region FetchAsync
    [Fact(DisplayName = "FetchAsync发送正确的语句ID和行数")]
    public async Task FetchAsync_SendsCorrectPacket()
    {
        var statementId = 99;
        var numRows = 1000;

        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };

        await client.FetchAsync(statementId, numRows);

        var sentData = ms.ToArray();
        // 帧头 4 + command(1) + statement_id(4) + num_rows(4) = 13
        Assert.Equal(4 + 1 + 4 + 4, sentData.Length);

        Assert.Equal((Byte)DbCmd.FETCH, sentData[4]);

        // 验证 statement_id
        var sentStmtId = sentData[5] | (sentData[6] << 8) | (sentData[7] << 16) | (sentData[8] << 24);
        Assert.Equal(statementId, sentStmtId);

        // 验证 num_rows
        var sentRows = sentData[9] | (sentData[10] << 8) | (sentData[11] << 16) | (sentData[12] << 24);
        Assert.Equal(numRows, sentRows);
    }

    [Fact(DisplayName = "FetchAsync序列号重置为0")]
    public async Task FetchAsync_ResetsSequence()
    {
        var ms = new MemoryStream();
        var client = new SqlClient { BaseStream = ms };
        client.SetValue("_seq", (Byte)50);

        await client.FetchAsync(1, 10);

        var sentData = ms.ToArray();
        // 序列号应该是 0（_seq 重置后第一个包）
        Assert.Equal(0, sentData[3]);
    }
    #endregion

    #region TimeAsync
    [Fact(DisplayName = "TimeAsync发送正确命令")]
    public async Task TimeAsync_SendsCorrectCommand()
    {
        var sendFrameSize = 4 + 1;

        var ms = new MemoryStream();
        ms.Write(new Byte[sendFrameSize], 0, sendFrameSize);

        var writer = new BinaryWriter(ms);
        WriteOkPacket(writer);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        await client.TimeAsync();

        var sentData = ms.ToArray();
        Assert.Equal((Byte)DbCmd.TIME, sentData[4]);
    }
    #endregion

    #region RegisterSlaveAsync
    [Fact(DisplayName = "RegisterSlaveAsync发送正确的注册数据包")]
    public async Task RegisterSlaveAsync_SendsCorrectPacket()
    {
        var serverId = 100;
        var host = "slave1";
        var user = "repl_user";
        var password = "repl_pwd";
        Int16 port = 3306;

        var hostBytes = System.Text.Encoding.UTF8.GetBytes(host);
        var userBytes = System.Text.Encoding.UTF8.GetBytes(user);
        var pwdBytes = System.Text.Encoding.UTF8.GetBytes(password);
        // command(1) + server_id(4) + hostname_len(1) + hostname + user_len(1) + user
        // + password_len(1) + password + port(2) + replication_rank(4) + master_id(4)
        var payloadLen = 1 + 4 + 1 + hostBytes.Length + 1 + userBytes.Length + 1 + pwdBytes.Length + 2 + 4 + 4;
        var sendFrameSize = 4 + payloadLen;

        var ms = new MemoryStream();
        ms.Write(new Byte[sendFrameSize], 0, sendFrameSize);

        var writer = new BinaryWriter(ms);
        WriteOkPacket(writer);

        ms.Position = 0;
        var client = new SqlClient { BaseStream = ms };

        await client.RegisterSlaveAsync(serverId, host, user, password, port);

        var sentData = ms.ToArray();
        Assert.Equal((Byte)DbCmd.REGISTER_SLAVE, sentData[4]);

        // 验证 server_id
        var sentServerId = sentData[5] | (sentData[6] << 8) | (sentData[7] << 16) | (sentData[8] << 24);
        Assert.Equal(serverId, sentServerId);

        // 验证 hostname
        var offset = 9;
        Assert.Equal(hostBytes.Length, sentData[offset]);
        offset++;
        var sentHost = System.Text.Encoding.UTF8.GetString(sentData, offset, hostBytes.Length);
        Assert.Equal(host, sentHost);
        offset += hostBytes.Length;

        // 验证 user
        Assert.Equal(userBytes.Length, sentData[offset]);
        offset++;
        var sentUser = System.Text.Encoding.UTF8.GetString(sentData, offset, userBytes.Length);
        Assert.Equal(user, sentUser);
        offset += userBytes.Length;

        // 验证 password
        Assert.Equal(pwdBytes.Length, sentData[offset]);
        offset++;
        var sentPwd = System.Text.Encoding.UTF8.GetString(sentData, offset, pwdBytes.Length);
        Assert.Equal(password, sentPwd);
        offset += pwdBytes.Length;

        // 验证 port
        var sentPort = (Int16)(sentData[offset] | (sentData[offset + 1] << 8));
        Assert.Equal(port, sentPort);
    }
    #endregion

    #region SetDatabaseAsync 补充
    [Fact(DisplayName = "SetDatabaseAsync空名称抛出异常")]
    public async Task SetDatabaseAsync_EmptyName_ThrowsException()
    {
        var client = new SqlClient { BaseStream = new MemoryStream() };

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => client.SetDatabaseAsync(""));
    }
    #endregion
}
