using NewLife.Buffers;
using NewLife.MySql.Common;

namespace NewLife.MySql.Messages;

/// <summary>握手消息</summary>
public class WelcomeMessage
{
    #region 属性
    /// <summary>协议版本</summary>
    public Byte Protocol { get; set; } = 10;

    /// <summary>服务器版本</summary>
    public String? Version { get; set; }

    /// <summary>连接ID</summary>
    public UInt32 ThreadID { get; set; }

    /// <summary>握手数据</summary>
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
    /// <summary>读取</summary>
    public void Read(ReadOnlySpan<Byte> span)
    {
        var reader = new SpanReader(span) { IsLittleEndian = true };

        Protocol = reader.ReadByte();
        Version = reader.ReadZeroString();
        ThreadID = reader.ReadUInt32();

        var seed1 = reader.ReadZero();
        var cap = (ClientFlags)reader.ReadUInt16();
        CharSet = reader.ReadByte();
        Status = (ServerStatus)reader.ReadUInt16();

        Capability = cap | (ClientFlags)(reader.ReadUInt16() << 16);

        var len = reader.ReadByte();
        reader.Advance(10);
        var seed2 = reader.ReadZero();
        var seed = new Byte[seed1.Length + seed2.Length];
        seed1.CopyTo(seed);
        seed2.CopyTo(new Span<Byte>(seed, seed1.Length, seed2.Length));
        Seed = seed;

        if (Capability.HasFlag(ClientFlags.PLUGIN_AUTH))
            AuthMethod = reader.ReadZeroString();
        else
            // 一些 MySql 版本如 5.1，不提供插件名称，默认为 native password。
            AuthMethod = "mysql_native_password";
    }
    #endregion
}
