using NewLife.Buffers;
using NewLife.Data;
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
    public UInt32 ConnectionId { get; set; }

    /// <summary>握手数据</summary>
    public Byte[]? Seed { get; set; }

    /// <summary>能力标志</summary>
    public UInt32 Capability { get; set; }

    /// <summary>字符集</summary>
    public Byte CharacterSet { get; set; }

    /// <summary>状态标志</summary>
    public UInt16 StatusFlags { get; set; }

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
        ConnectionId = reader.ReadUInt32();

        var seed1 = reader.ReadZero();
        var cap = (UInt32)reader.ReadUInt16();
        CharacterSet = reader.ReadByte();
        StatusFlags = reader.ReadUInt16();

        Capability = cap | (UInt32)(reader.ReadUInt16() << 16);

        var len = reader.ReadByte();
        reader.Advance(10);
        var seed2 = reader.ReadZero();
        var seed = new Byte[seed1.Length + seed2.Length];
        seed1.CopyTo(seed);
        seed2.CopyTo(new Span<Byte>(seed, seed1.Length, seed2.Length));
        Seed = seed;

        AuthMethod = reader.ReadZeroString();
    }
    #endregion
}
