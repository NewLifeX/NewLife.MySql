using NewLife;
using NewLife.Data;
using NewLife.MySql;
using NewLife.MySql.Common;
using NewLife.Reflection;
using NewLife.Security;

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
        Assert.Equal(seq, (Byte)client.GetValue("_seq")!);
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

        Assert.Null(pk);
        Assert.Equal(seq, (Byte)client.GetValue("_seq")!);
        Assert.Equal(ms.Position, ms.Length);
    }

    [Fact]
    public void TestOpen()
    {
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "localhost",
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
    }

    [Fact]
    public void TestClose()
    {
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "localhost",
            Port = 3306,
            UserID = "root",
            Password = "password",
            Database = "test",
            ConnectionTimeout = 15
        };

        var client = new SqlClient(setting);
        client.Open();
        client.Close();

        //Assert.Null(client._Client);
        //Assert.Null(client._Stream);
    }

    [Fact]
    public void TestConfigure()
    {
        var setting = new MySqlConnectionStringBuilder
        {
            Server = "localhost",
            Port = 3306,
            UserID = "root",
            Password = "password",
            Database = "test",
            ConnectionTimeout = 15
        };

        var client = new SqlClient(setting);
        var conn = new MySqlConnection(setting.ConnectionString);
        client.Configure(conn);

        Assert.NotNull(client.Variables);
        Assert.True(client.Variables.Count > 0);
    }
}
