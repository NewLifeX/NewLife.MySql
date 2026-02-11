using NewLife;
using NewLife.MySql.Messages;

namespace UnitTest;

[Collection(TestCollections.InMemory)]
public class WelcomeMessageTests
{
    /// <summary>构造一个标准 HandshakeV10 测试包</summary>
    /// <remarks>
    /// seed1 = [0x61,0x62,0x63,0x64,0x65,0x66,0x67,0x68]（8字节固定），
    /// seed2 = [0x69,0x6A,0x6B,0x6C,0x6D,0x6E,0x6F,0x70,0x71,0x72,0x73,0x74]（12字节有效 + 1字节NUL = 13字节），
    /// 总 Seed = 20 字节
    /// </remarks>
    private static Byte[] BuildHandshakeV10()
    {
        var ms = new MemoryStream();
        ms.WriteByte(10); // Protocol

        // ServerVersion "5.7.2\0"
        ms.Write([53, 46, 55, 46, 50, 0]);

        // ThreadID = 1
        ms.Write([1, 0, 0, 0]);

        // auth-plugin-data-part-1（固定8字节）
        ms.Write([0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68]);

        // filler 0x00
        ms.WriteByte(0);

        // capability_flags_1 (lower 16 bits): 0xFFFF
        ms.Write([0xFF, 0xFF]);

        // character_set: utf8 = 33
        ms.WriteByte(33);

        // status_flags: AutoCommitMode = 2
        ms.Write([2, 0]);

        // capability_flags_2 (upper 16 bits): 0xFFFF
        ms.Write([0xFF, 0xFF]);

        // auth_plugin_data_len = 21（8 + 13）
        ms.WriteByte(21);

        // reserved（10字节全零）
        ms.Write(new Byte[10]);

        // auth-plugin-data-part-2（13字节：12字节有效 + NUL）
        ms.Write([0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70, 0x71, 0x72, 0x73, 0x74, 0]);

        // auth_plugin_name "mysql_native_password\0"
        var pluginName = System.Text.Encoding.UTF8.GetBytes("mysql_native_password");
        ms.Write(pluginName);
        ms.WriteByte(0);

        return ms.ToArray();
    }

    /// <summary>解析标准 HandshakeV10 握手包</summary>
    [Fact]
    public void ReadSpan_Standard()
    {
        var data = BuildHandshakeV10();
        var message = new WelcomeMessage();

        message.Read(new ReadOnlySpan<Byte>(data));

        Assert.Equal(10, message.Protocol);
        Assert.Equal("5.7.2", message.ServerVersion);
        Assert.Equal(1u, message.ThreadID);
        Assert.Equal(20, message.Seed!.Length);
        Assert.Equal(
            new Byte[] { 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
                         0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70, 0x71, 0x72, 0x73, 0x74 },
            message.Seed);
        Assert.Equal((Byte)33, message.CharSet);
        Assert.Equal(ServerStatus.AutoCommitMode, message.Status);
        Assert.True(message.Capability.HasFlag(ClientFlags.SECURE_CONNECTION));
        Assert.True(message.Capability.HasFlag(ClientFlags.PLUGIN_AUTH));
        Assert.Equal("mysql_native_password", message.AuthMethod);
    }

    /// <summary>解析 MySQL 8.0 真实握手包（caching_sha2_password）</summary>
    [Fact]
    public void ReadSpan_MySQL80()
    {
        var str = "0A382E302E3339001C000000024439523E53074100FFFFFF0200FFDF15000000000000000000007F14467E1B0B131D1C5C5C250063616368696E675F736861325F70617373776F726400";
        var buf = str.ToHex();
        var message = new WelcomeMessage();

        message.Read(buf);

        Assert.Equal(10, message.Protocol);
        Assert.Equal("8.0.39", message.ServerVersion);
        Assert.Equal(28u, message.ThreadID);
        Assert.Equal(20, message.Seed!.Length);
        Assert.Equal(
            new Byte[] { 2, 68, 57, 82, 62, 83, 7, 65, 127, 20, 70, 126, 27, 11, 19, 29, 28, 92, 92, 37 },
            message.Seed);
        Assert.Equal((Byte)0xFF, message.CharSet);
        Assert.Equal(ServerStatus.AutoCommitMode, message.Status);
        Assert.Equal("caching_sha2_password", message.AuthMethod);
    }

    /// <summary>BinaryReader 重载与 Span 重载结果一致</summary>
    [Fact]
    public void ReadBinaryReader_SameAsSpan()
    {
        var data = BuildHandshakeV10();

        var msg1 = new WelcomeMessage();
        msg1.Read(new ReadOnlySpan<Byte>(data));

        var msg2 = new WelcomeMessage();
        using var ms = new MemoryStream(data);
        using var br = new BinaryReader(ms);
        msg2.Read(br);

        Assert.Equal(msg1.Protocol, msg2.Protocol);
        Assert.Equal(msg1.ServerVersion, msg2.ServerVersion);
        Assert.Equal(msg1.ThreadID, msg2.ThreadID);
        Assert.Equal(msg1.Seed, msg2.Seed);
        Assert.Equal(msg1.Capability, msg2.Capability);
        Assert.Equal(msg1.CharSet, msg2.CharSet);
        Assert.Equal(msg1.Status, msg2.Status);
        Assert.Equal(msg1.AuthMethod, msg2.AuthMethod);
    }

    /// <summary>无 PLUGIN_AUTH 时默认 mysql_native_password</summary>
    [Fact]
    public void ReadSpan_NoPluginAuth()
    {
        // 构造一个不带 PLUGIN_AUTH 能力标志的包
        var ms = new MemoryStream();
        ms.WriteByte(10);

        // ServerVersion "5.1.73\0"
        ms.Write(System.Text.Encoding.UTF8.GetBytes("5.1.73"));
        ms.WriteByte(0);

        // ThreadID = 42
        ms.Write([42, 0, 0, 0]);

        // auth-plugin-data-part-1（固定8字节）
        ms.Write([1, 2, 3, 4, 5, 6, 7, 8]);

        // filler
        ms.WriteByte(0);

        // capability_flags_1: 包含 SECURE_CONNECTION(0x8000) 但不包含 PLUGIN_AUTH
        // SECURE_CONNECTION = 32768 = 0x8000
        var capLow = (UInt16)(ClientFlags.LONG_PASSWORD | ClientFlags.FOUND_ROWS | ClientFlags.LONG_FLAG |
                              ClientFlags.PROTOCOL_41 | ClientFlags.TRANSACTIONS | ClientFlags.SECURE_CONNECTION);
        ms.WriteByte((Byte)(capLow & 0xFF));
        ms.WriteByte((Byte)(capLow >> 8));

        // character_set
        ms.WriteByte(8);

        // status_flags
        ms.Write([2, 0]);

        // capability_flags_2: 高16位为 0（不含 PLUGIN_AUTH 等高位标志）
        ms.Write([0, 0]);

        // auth_plugin_data_len = 0（无 PLUGIN_AUTH 时为 0）
        ms.WriteByte(0);

        // reserved 10 bytes
        ms.Write(new Byte[10]);

        // auth-plugin-data-part-2（SECURE_CONNECTION 要求，MAX(13, 0-8)=13 字节）
        ms.Write([9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0]);

        // 无 auth_plugin_name

        var data = ms.ToArray();
        var message = new WelcomeMessage();

        message.Read(new ReadOnlySpan<Byte>(data));

        Assert.Equal(10, message.Protocol);
        Assert.Equal("5.1.73", message.ServerVersion);
        Assert.Equal(42u, message.ThreadID);
        Assert.Equal(20, message.Seed!.Length);
        Assert.Equal(
            new Byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 },
            message.Seed);
        Assert.Equal("mysql_native_password", message.AuthMethod);
        Assert.False(message.Capability.HasFlag(ClientFlags.PLUGIN_AUTH));
    }

    /// <summary>ToString 输出摘要信息</summary>
    [Fact]
    public void ToStringFormat()
    {
        var data = BuildHandshakeV10();
        var message = new WelcomeMessage();
        message.Read(new ReadOnlySpan<Byte>(data));

        var str = message.ToString();

        Assert.Contains("5.7.2", str);
        Assert.Contains("mysql_native_password", str);
        Assert.Contains("ThreadID=1", str);
    }

    /// <summary>BinaryReader 解析 MySQL 8.0 真实握手包</summary>
    [Fact]
    public void ReadBinaryReader_MySQL80()
    {
        var str = "0A382E302E3339001C000000024439523E53074100FFFFFF0200FFDF15000000000000000000007F14467E1B0B131D1C5C5C250063616368696E675F736861325F70617373776F726400";
        var buf = str.ToHex();

        var message = new WelcomeMessage();
        using var ms = new MemoryStream(buf);
        using var br = new BinaryReader(ms);
        message.Read(br);

        Assert.Equal(10, message.Protocol);
        Assert.Equal("8.0.39", message.ServerVersion);
        Assert.Equal(28u, message.ThreadID);
        Assert.Equal(20, message.Seed!.Length);
        Assert.Equal("caching_sha2_password", message.AuthMethod);
    }
}
