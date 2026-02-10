using NewLife.Buffers;
using NewLife.MySql.Common;

namespace NewLife.MySql.Messages;

/// <summary>握手消息。MySQL 协议 HandshakeV10 包，服务器在连接建立后首先发送</summary>
public class WelcomeMessage
{
    #region 属性
    /// <summary>协议版本。当前仅支持版本 10（HandshakeV10）</summary>
    public Byte Protocol { get; set; } = 10;

    /// <summary>服务器版本</summary>
    public String? ServerVersion { get; set; }

    /// <summary>连接ID</summary>
    public UInt32 ThreadID { get; set; }

    /// <summary>握手数据。由 auth-plugin-data-part-1（8字节）和 auth-plugin-data-part-2 拼接而成</summary>
    public Byte[]? Seed { get; set; }

    /// <summary>能力标志</summary>
    public ClientFlags Capability { get; set; }

    /// <summary>字符集</summary>
    public Byte CharSet { get; set; }

    /// <summary>状态标志</summary>
    public ServerStatus Status { get; set; }

    /// <summary>认证插件名</summary>
    public String? AuthMethod { get; set; }
    #endregion

    #region 方法
    /// <summary>从 Span 读取握手消息</summary>
    /// <param name="span">包含握手消息的字节序列</param>
    public void Read(ReadOnlySpan<Byte> span)
    {
        var reader = new SpanReader(span) { IsLittleEndian = true };

        // 1 byte: 协议版本
        Protocol = reader.ReadByte();

        // NUL-terminated: 服务器版本字符串
        ServerVersion = reader.ReadZeroString();

        // 4 bytes: 连接线程ID
        ThreadID = reader.ReadUInt32();

        // 8 bytes: auth-plugin-data-part-1（固定8字节）
        var remaining = reader.GetSpan();
        var seed1 = remaining[..8];
        reader.Advance(8);

        // 1 byte: filler（始终为 0x00）
        reader.Advance(1);

        // 2 bytes: capability_flags_1（低16位）
        var cap = (ClientFlags)reader.ReadUInt16();

        // 1 byte: 字符集
        CharSet = reader.ReadByte();

        // 2 bytes: 状态标志
        Status = (ServerStatus)reader.ReadUInt16();

        // 2 bytes: capability_flags_2（高16位）
        Capability = cap | (ClientFlags)(reader.ReadUInt16() << 16);

        // 1 byte: auth_plugin_data_len（若有 PLUGIN_AUTH 能力）或 0x00
        var authDataLen = reader.ReadByte();

        // 10 bytes: reserved（全零）
        reader.Advance(10);

        // auth-plugin-data-part-2：仅在服务器支持 SECURE_CONNECTION 时存在
        if (Capability.HasFlag(ClientFlags.SECURE_CONNECTION))
        {
            // 长度 = MAX(13, auth_plugin_data_len - 8)，最后一个字节为 NUL 终止符
            var seed2Len = Math.Max(13, authDataLen - 8);
            remaining = reader.GetSpan();
            var seed2 = remaining[..(seed2Len - 1)];
            reader.Advance(seed2Len);

            var seed = new Byte[seed1.Length + seed2.Length];
            seed1.CopyTo(seed);
            seed2.CopyTo(new Span<Byte>(seed, seed1.Length, seed2.Length));
            Seed = seed;
        }
        else
        {
            Seed = seed1.ToArray();
        }

        if (Capability.HasFlag(ClientFlags.PLUGIN_AUTH))
            AuthMethod = reader.ReadZeroString();
        else
            // 一些 MySql 版本如 5.1，不提供插件名称，默认为 native password。
            AuthMethod = "mysql_native_password";
    }

    /// <summary>从 BinaryReader 读取握手消息</summary>
    /// <param name="reader">二进制读取器</param>
    public void Read(BinaryReader reader)
    {
        Protocol = reader.ReadByte();
        ServerVersion = reader.ReadZeroString();
        ThreadID = reader.ReadUInt32();

        // 8 bytes: auth-plugin-data-part-1（固定8字节）
        var seed1Buf = reader.ReadBytes(8);

        // 1 byte: filler（始终为 0x00）
        reader.ReadByte();

        var cap = (ClientFlags)reader.ReadUInt16();
        CharSet = reader.ReadByte();
        Status = (ServerStatus)reader.ReadUInt16();

        Capability = cap | (ClientFlags)(reader.ReadUInt16() << 16);

        var authDataLen = reader.ReadByte();

        // 10 bytes: reserved
        reader.ReadBytes(10);

        if (Capability.HasFlag(ClientFlags.SECURE_CONNECTION))
        {
            var seed2Len = Math.Max(13, authDataLen - 8);
            var seed2Buf = reader.ReadBytes(seed2Len);

            // 最后一个字节为 NUL 终止符，不计入有效数据
            var seed = new Byte[seed1Buf.Length + seed2Len - 1];
            seed1Buf.CopyTo(seed, 0);
            Array.Copy(seed2Buf, 0, seed, seed1Buf.Length, seed2Len - 1);
            Seed = seed;
        }
        else
        {
            Seed = seed1Buf;
        }

        if (Capability.HasFlag(ClientFlags.PLUGIN_AUTH))
            AuthMethod = reader.ReadZeroString();
        else
            // 一些 MySql 版本如 5.1，不提供插件名称，默认为 native password。
            AuthMethod = "mysql_native_password";
    }

    /// <summary>已重载。显示握手消息摘要</summary>
    public override String ToString() => $"Protocol={Protocol}, Server={ServerVersion}, ThreadID={ThreadID}, CharSet={CharSet}, Auth={AuthMethod}";
    #endregion
}
