using NewLife;
using NewLife.Buffers;
using NewLife.Data;
using NewLife.MySql;
using NewLife.MySql.Common;
using NewLife.MySql.Messages;
using NewLife.Reflection;
using NewLife.Security;

namespace UnitTest;

public class SqlClientTests
{
    [Fact]
    public void ReadPacket()
    {
        // 通过Mod基础数据流BaseStream来测试数据包读取
        var len = Rand.Next(10, 1 << 24);
        var buf = Rand.NextBytes(len);
        var seq = (Byte)Rand.Next(1, 256);

        var ms = new MemoryStream();
        var writer = new BinaryWriter(ms);

        // 3字节长度 + 1字节序列号。小端字节序
        var n = len | seq << 24;
        writer.Write(n);
        writer.Write(buf);

        ms.Position = 0;
        var client = new SqlClient(null!) { BaseStream = ms };
        var pk = (ArrayPacket)client.ReadPacket();

        Assert.Equal(len, pk.Length);
        Assert.Equal(buf, pk.Buffer);
        Assert.Equal(seq + 1, (Byte)client.GetValue("_seq")!);
        Assert.Equal(ms.Position, ms.Length);
    }

    [Fact]
    public void ReadPacket_Error()
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
        var client = new SqlClient(null!) { BaseStream = ms };

        var ex = Assert.Throws<MySqlException>(() => client.ReadPacket());

        Assert.Equal(code, ex.ErrorCode);
        Assert.Equal(msg, ex.Message);
        Assert.Equal(ms.Position, ms.Length);
    }

    [Fact]
    public void ReadPacket_FE()
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
        var client = new SqlClient(null!) { BaseStream = ms };
        var pk = client.ReadPacket();

        //Assert.Null(pk);
        Assert.Equal(seq + 1, (Byte)client.GetValue("_seq")!);
        Assert.Equal(ms.Position, ms.Length);
    }

    [Fact]
    public void SendPacket()
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
        var client = new SqlClient(null!) { BaseStream = ms };

        client.SetValue("_seq", seq);
        client.SendPacket(pk.Slice(4, -1));

        var rs = ms.ToArray();
        Assert.Equal(pk.Length, rs.Length);
        Assert.Equal(len, (Int32)(rs.ToUInt32(0) & 0xFF_FFFF));
        Assert.Equal(seq, rs[3]);
        Assert.Equal(seq + 1, (Byte)client.GetValue("_seq")!);
        Assert.Equal(buf, rs.ReadBytes(4, -1));
    }

    [Fact]
    public void SendPacket_NoExpand()
    {
        // 通过Mod基础数据流BaseStream来测试数据包写入
        var len = Rand.Next(10, 1 << 24);
        var buf = Rand.NextBytes(len);
        var seq = (Byte)Rand.Next(1, 256);

        var ms = new MemoryStream();
        var client = new SqlClient(null!) { BaseStream = ms };

        client.SetValue("_seq", seq);
        client.SendPacket(buf);

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
        var client = new SqlClient(null!) { BaseStream = ms };

        var seq = (Byte)Rand.Next(1, 256);
        client.SetValue("_seq", seq);
        client.Close();

        var rs = ms.ToArray();
        Assert.Equal(5, rs.Length);
        Assert.Equal(DbCmd.QUIT, (DbCmd)rs[4]);
        Assert.Equal(1, (Byte)client.GetValue("_seq")!);
    }

    [Fact]
    public void SendQuery()
    {
        // 通过Mod基础数据流BaseStream来测试数据包写入
        var sql = "select * from role";

        using var pk = new OwnerPacket(4 + 1 + sql.Length);
        var writer = new SpanWriter(pk);
        writer.Advance(4);

        writer.WriteByte(0);
        writer.Write(sql, -1);

        var ms = new MemoryStream();
        var client = new SqlClient(null!) { BaseStream = ms };
        client.SendQuery(pk.Slice(4, -1));

        var buf = ms.ToArray();
        Assert.Equal(pk.Length, buf.Length);
        Assert.Equal(0, buf[3]);
        Assert.Equal(1, (Byte)client.GetValue("_seq")!);
        Assert.Equal(DbCmd.QUERY, (DbCmd)buf[4]);
        Assert.Equal(sql, buf.ReadBytes(4 + 1, -1).ToStr());
    }

    [Fact]
    public void TestOpen()
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

        var client = new SqlClient(setting);
        client.Open();

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
    public void TestConfigure()
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

        using var client = new SqlClient(setting);
        client.Open();

        Assert.NotNull(client);
        Assert.NotNull(client.Setting);
        Assert.Equal(3306, client.Setting.Port);

        //var conn = new MySqlConnection(setting.ConnectionString);
        //conn.Client = client;
        client.Configure();
        Assert.True(client.MaxPacketSize >= 1024 * 1024);

        Assert.NotNull(client.Variables);
        Assert.True(client.Variables.Count > 0);
    }
}
