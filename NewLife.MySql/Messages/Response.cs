using NewLife.Data;
using NewLife.MySql.Common;

namespace NewLife.MySql.Messages;

/// <summary>响应</summary>
public class Response
{
    #region 属性
    /// <summary>数据包</summary>
    public IPacket Data { get; set; } = null!;

    /// <summary>数据包长度</summary>
    public Int32 Length { get; set; }

    /// <summary>数据包序号</summary>
    public Byte Sequence { get; set; }

    private Stream _stream;
    private Byte _kind;
    #endregion

    #region 扩展属性
    /// <summary>是否成功。0x00</summary>
    public Boolean IsOK => _kind == 0x00;

    /// <summary>是否错误。0xFF</summary>
    public Boolean IsError => _kind == 0xFF;

    /// <summary>是否结尾。0xFE</summary>
    public Boolean IsEOF => _kind == 0xFE;
    #endregion

    #region 构造
    /// <summary>实例化响应</summary>
    public Response(Stream stream)
    {
        _stream = stream;
    }
    #endregion

    #region 方法
    public void Set(IPacket data)
    {
        _kind = data[0];
        Data = data;
    }

    //private BinaryReader? _reader;
    ///// <summary>获取读取器</summary>
    //public BinaryReader GetReader() => _reader ??= new(Stream);

    /// <summary>获取读取器</summary>
    public BufferedReader CreateReader(Int32 offset) => new(_stream, Data.Slice(offset, -1), 8192);

    /// <summary>获取Span数据，用于解析短小报文</summary>
    public ReadOnlySpan<Byte> GetSpan(Int32 offset)
    {
        //var len = Length;
        //if (Packet.Total - 4 >= len)
        return Data.GetSpan()[(offset)..];
    }

    ///// <summary>读取长度</summary>
    //public Int32 ReadLength()
    //{
    //    var reader = GetReader();
    //    return Kind switch
    //    {
    //        251 => -1,
    //        252 => reader.ReadUInt16(),
    //        253 => reader.ReadBytes(3).ToInt(),
    //        254 => reader.ReadInt32(),
    //        _ => Kind,
    //    };
    //}
    #endregion
}
