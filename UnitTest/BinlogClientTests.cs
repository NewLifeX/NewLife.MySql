using System.Text;
using NewLife.MySql.Binlog;
using NewLife.Reflection;

namespace UnitTest;

/// <summary>BinlogClient 高级封装单元测试</summary>
[Collection(TestCollections.InMemory)]
public class BinlogClientTests
{
    [Fact(DisplayName = "测试BinlogClient默认属性")]
    public void TestDefaultProperties()
    {
        var client = new BinlogClient();

        Assert.True(client.ServerId > 0);
        Assert.True(client.AutoReconnect);
        Assert.Equal(3000, client.ReconnectInterval);
        Assert.False(client.Running);
        Assert.Empty(client.ConnectionString);
        Assert.Null(client.Position);
        Assert.Empty(client.DatabaseNames);
        Assert.Empty(client.TableNames);
    }

    [Fact(DisplayName = "测试BinlogClient连接字符串构造")]
    public void TestConnectionStringConstructor()
    {
        var connStr = "Server=127.0.0.1;Port=3306;UserID=root;Password=root";
        var client = new BinlogClient(connStr);

        Assert.Equal(connStr, client.ConnectionString);
        Assert.True(client.ServerId >= 100000 && client.ServerId <= 999999);
    }

    [Fact(DisplayName = "测试BinlogClient的Stop不抛异常")]
    public void TestStopWhenNotRunning()
    {
        var client = new BinlogClient();

        // 未启动时调用不应抛异常
        client.Stop();

        Assert.False(client.Running);
    }

    [Fact(DisplayName = "测试BinlogClient的StopAsync不抛异常")]
    public async Task TestStopAsyncWhenNotRunning()
    {
        var client = new BinlogClient();

        await client.StopAsync();

        Assert.False(client.Running);
    }

    [Fact(DisplayName = "测试BinlogClient的Dispose")]
    public void TestDispose()
    {
        var client = new BinlogClient();
        client.ServerId = 12345;

        client.Dispose();

        Assert.False(client.Running);
    }

    [Fact(DisplayName = "测试FilterEvent放行非行事件")]
    public void TestFilterEventPassesNonRowEvents()
    {
        var client = new BinlogClient();
        client.TableNames.Add("user");

        // 非行事件（如 QUERY_EVENT）不应被过滤
        var ev = new BinlogEvent { EventType = BinlogEventType.QUERY_EVENT };

        var method = client.GetType().GetMethod("FilterEvent", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var result = (Boolean)method!.Invoke(client, [ev])!;

        Assert.True(result);
    }

    [Fact(DisplayName = "测试FilterEvent按表名过滤")]
    public void TestFilterEventByTableName()
    {
        var client = new BinlogClient();
        client.TableNames.Add("user");

        var method = client.GetType().GetMethod("FilterEvent", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // 匹配的表名应通过
        var ev1 = new BinlogEvent
        {
            EventType = BinlogEventType.WRITE_ROWS_EVENT,
            TableName = "user",
        };
        Assert.True((Boolean)method!.Invoke(client, [ev1])!);

        // 不匹配的表名应被过滤
        var ev2 = new BinlogEvent
        {
            EventType = BinlogEventType.WRITE_ROWS_EVENT,
            TableName = "order",
        };
        Assert.False((Boolean)method!.Invoke(client, [ev2])!);
    }

    [Fact(DisplayName = "测试FilterEvent按数据库过滤")]
    public void TestFilterEventByDatabaseName()
    {
        var client = new BinlogClient();
        client.DatabaseNames.Add("mydb");

        var method = client.GetType().GetMethod("FilterEvent", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // 匹配的数据库应通过
        var ev1 = new BinlogEvent
        {
            EventType = BinlogEventType.TABLE_MAP_EVENT,
            DatabaseName = "mydb",
            TableName = "user",
        };
        Assert.True((Boolean)method!.Invoke(client, [ev1])!);

        // 不匹配的数据库应被过滤
        var ev2 = new BinlogEvent
        {
            EventType = BinlogEventType.TABLE_MAP_EVENT,
            DatabaseName = "otherdb",
            TableName = "user",
        };
        Assert.False((Boolean)method!.Invoke(client, [ev2])!);
    }

    [Fact(DisplayName = "测试FilterEvent无过滤条件全部放行")]
    public void TestFilterEventPassesAllWhenNoFilter()
    {
        var client = new BinlogClient();
        // 不设置过滤条件

        var method = client.GetType().GetMethod("FilterEvent", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        var ev = new BinlogEvent
        {
            EventType = BinlogEventType.WRITE_ROWS_EVENT,
            DatabaseName = "anydb",
            TableName = "anytable",
        };
        Assert.True((Boolean)method!.Invoke(client, [ev])!);
    }

    [Fact(DisplayName = "测试ParseRotateEvent解析")]
    public void TestParseRotateEvent()
    {
        var client = new BinlogClient();
        var method = client.GetType().GetMethod("ParseEvent", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // 构造 ROTATE_EVENT 数据：position(8) + filename
        var fileName = "binlog.000002";
        var fileBytes = Encoding.UTF8.GetBytes(fileName);
        var data = new Byte[8 + fileBytes.Length];
        BitConverter.GetBytes((UInt64)4).CopyTo(data, 0);
        Array.Copy(fileBytes, 0, data, 8, fileBytes.Length);

        var ev = new BinlogEvent
        {
            EventType = BinlogEventType.ROTATE_EVENT,
            Data = data,
        };

        method!.Invoke(client, [ev]);

        Assert.Equal("binlog.000002", ev.NextBinlogFile);
        Assert.Equal(4ul, ev.NextBinlogPosition);
        // Position 也应更新
        Assert.NotNull(client.Position);
        Assert.Equal("binlog.000002", client.Position!.FileName);
        Assert.Equal(4u, client.Position.Position);
    }

    [Fact(DisplayName = "测试ParseQueryEvent解析")]
    public void TestParseQueryEvent()
    {
        var client = new BinlogClient();
        var method = client.GetType().GetMethod("ParseEvent", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // 构造 QUERY_EVENT 数据：
        // thread_id(4) + exec_time(4) + db_name_len(1) + error_code(2) + status_vars_len(2) + status_vars + db_name + NUL + sql
        var dbName = "testdb";
        var sql = "INSERT INTO user VALUES(1)";
        var dbBytes = Encoding.UTF8.GetBytes(dbName);
        var sqlBytes = Encoding.UTF8.GetBytes(sql);

        var statusVarsLen = 0;
        var totalLen = 4 + 4 + 1 + 2 + 2 + statusVarsLen + dbBytes.Length + 1 + sqlBytes.Length;
        var data = new Byte[totalLen];

        // thread_id = 1
        BitConverter.GetBytes(1).CopyTo(data, 0);
        // exec_time = 0
        // db_name_len
        data[8] = (Byte)dbBytes.Length;
        // error_code = 0
        // status_vars_len = 0
        // db_name
        Array.Copy(dbBytes, 0, data, 13, dbBytes.Length);
        // NUL
        data[13 + dbBytes.Length] = 0;
        // SQL
        Array.Copy(sqlBytes, 0, data, 14 + dbBytes.Length, sqlBytes.Length);

        var ev = new BinlogEvent
        {
            EventType = BinlogEventType.QUERY_EVENT,
            Data = data,
        };

        method!.Invoke(client, [ev]);

        Assert.Equal("testdb", ev.QueryDatabase);
        Assert.Equal("INSERT INTO user VALUES(1)", ev.Query);
    }

    [Fact(DisplayName = "测试ParseTableMapEvent解析")]
    public void TestParseTableMapEvent()
    {
        var client = new BinlogClient();
        var method = client.GetType().GetMethod("ParseEvent", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // 构造 TABLE_MAP_EVENT 数据：
        // table_id(6) + flags(2) + db_name_len(1) + db_name + NUL + tbl_name_len(1) + tbl_name + NUL + col_count(1) + col_types
        var dbName = "mydb";
        var tblName = "user";
        var dbBytes = Encoding.UTF8.GetBytes(dbName);
        var tblBytes = Encoding.UTF8.GetBytes(tblName);
        var colCount = 3;
        var colTypes = new Byte[] { 3, 15, 12 }; // INT, VARCHAR, DATETIME

        var totalLen = 6 + 2 + 1 + dbBytes.Length + 1 + 1 + tblBytes.Length + 1 + 1 + colTypes.Length;
        var data = new Byte[totalLen];
        var offset = 0;

        // table_id = 42 (6 bytes LE)
        data[offset] = 42;
        offset += 6;

        // flags
        offset += 2;

        // db_name_len + db_name + NUL
        data[offset++] = (Byte)dbBytes.Length;
        Array.Copy(dbBytes, 0, data, offset, dbBytes.Length);
        offset += dbBytes.Length;
        data[offset++] = 0;

        // tbl_name_len + tbl_name + NUL
        data[offset++] = (Byte)tblBytes.Length;
        Array.Copy(tblBytes, 0, data, offset, tblBytes.Length);
        offset += tblBytes.Length;
        data[offset++] = 0;

        // col_count (length-encoded: 直接写 3)
        data[offset++] = (Byte)colCount;

        // col_types
        Array.Copy(colTypes, 0, data, offset, colTypes.Length);

        var ev = new BinlogEvent
        {
            EventType = BinlogEventType.TABLE_MAP_EVENT,
            Data = data,
        };

        method!.Invoke(client, [ev]);

        Assert.Equal(42ul, ev.TableId);
        Assert.Equal("mydb", ev.DatabaseName);
        Assert.Equal("user", ev.TableName);
        Assert.Equal(3, ev.ColumnCount);
        Assert.NotNull(ev.ColumnTypes);
        Assert.Equal(3, ev.ColumnTypes![0]); // INT
        Assert.Equal(15, ev.ColumnTypes[1]); // VARCHAR
        Assert.Equal(12, ev.ColumnTypes[2]); // DATETIME
    }

    [Fact(DisplayName = "测试ParseRowsEvent关联表映射")]
    public void TestParseRowsEventAssociatesTableMap()
    {
        var client = new BinlogClient();
        var method = client.GetType().GetMethod("ParseEvent", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // 先解析 TABLE_MAP_EVENT 建立映射
        var dbName = "mydb";
        var tblName = "order";
        var dbBytes = Encoding.UTF8.GetBytes(dbName);
        var tblBytes = Encoding.UTF8.GetBytes(tblName);
        var colCount = 2;
        var colTypes = new Byte[] { 3, 15 };

        var mapLen = 6 + 2 + 1 + dbBytes.Length + 1 + 1 + tblBytes.Length + 1 + 1 + colTypes.Length;
        var mapData = new Byte[mapLen];
        var off = 0;
        mapData[off] = 99; // table_id = 99
        off += 6;
        off += 2;
        mapData[off++] = (Byte)dbBytes.Length;
        Array.Copy(dbBytes, 0, mapData, off, dbBytes.Length);
        off += dbBytes.Length;
        mapData[off++] = 0;
        mapData[off++] = (Byte)tblBytes.Length;
        Array.Copy(tblBytes, 0, mapData, off, tblBytes.Length);
        off += tblBytes.Length;
        mapData[off++] = 0;
        mapData[off++] = (Byte)colCount;
        Array.Copy(colTypes, 0, mapData, off, colTypes.Length);

        var tableMapEv = new BinlogEvent { EventType = BinlogEventType.TABLE_MAP_EVENT, Data = mapData };
        method!.Invoke(client, [tableMapEv]);

        // 构造 WRITE_ROWS_EVENT_V1 数据
        // table_id(6) + flags(2) + col_count(1) + columns-present-bitmap + row_data
        var bitmapLen = (colCount + 7) / 8;
        var rowsLen = 6 + 2 + 1 + bitmapLen + 4; // 加一些伪行数据
        var rowsData = new Byte[rowsLen];
        rowsData[0] = 99; // table_id = 99（与 TABLE_MAP 一致）

        // flags
        rowsData[8] = (Byte)colCount; // col_count
        // bitmap 全 0xFF 表示所有列都存在
        rowsData[9] = 0xFF;

        var rowsEv = new BinlogEvent { EventType = BinlogEventType.WRITE_ROWS_EVENT_V1, Data = rowsData };
        method!.Invoke(client, [rowsEv]);

        // 验证关联了表映射信息
        Assert.Equal(99ul, rowsEv.TableId);
        Assert.Equal("mydb", rowsEv.DatabaseName);
        Assert.Equal("order", rowsEv.TableName);
        Assert.NotNull(rowsEv.Rows);
    }

    [Fact(DisplayName = "测试多个ServerId唯一")]
    public void TestMultipleServerIdUnique()
    {
        var ids = new HashSet<Int32>();
        for (var i = 0; i < 100; i++)
        {
            var client = new BinlogClient();
            ids.Add(client.ServerId);
        }

        // 100 次随机应该大部分不同（极小概率全部不同）
        Assert.True(ids.Count > 90, $"100 次随机 ServerId 中仅有 {ids.Count} 个不同");
    }

    [Fact(DisplayName = "测试ParseEvent忽略空数据")]
    public void TestParseEventIgnoresNullData()
    {
        var client = new BinlogClient();
        var method = client.GetType().GetMethod("ParseEvent", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // 数据为 null 不应抛异常
        var ev = new BinlogEvent { EventType = BinlogEventType.TABLE_MAP_EVENT, Data = null };
        method!.Invoke(client, [ev]);

        Assert.Null(ev.TableName);
    }

    [Fact(DisplayName = "测试ParseEvent忽略过短数据")]
    public void TestParseEventIgnoresShortData()
    {
        var client = new BinlogClient();
        var method = client.GetType().GetMethod("ParseEvent", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        // 数据不足 8 字节不应抛异常
        var ev = new BinlogEvent { EventType = BinlogEventType.TABLE_MAP_EVENT, Data = [0x01, 0x02] };
        method!.Invoke(client, [ev]);

        Assert.Null(ev.TableName);
    }
}
