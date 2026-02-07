using NewLife.Buffers;
using NewLife.Data;

namespace NewLife.MySql.Messages;

/// <summary>响应</summary>
/// <remarks>实例化响应</remarks>
public class Response(Stream stream)
{
    #region 属性
    /// <summary>数据包</summary>
    public IPacket Data { get; set; } = null!;

    /// <summary>数据包长度</summary>
    public Int32 Length { get; set; }

    /// <summary>数据包序号</summary>
    public Byte Sequence { get; set; }

    private Stream _stream = stream;
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

    #region 方法
    /// <summary>设置数据</summary>
    /// <param name="data"></param>
    public void Set(IPacket data)
    {
        _kind = data[0];
        Data = data;
    }

    /// <summary>获取缓存读取器，数据不足时自动从网络流读取</summary>
    public SpanReader CreateReader(Int32 offset) => new(_stream, Data.Slice(offset, -1), 8192) { MaxCapacity = Length, IsLittleEndian = true };
    #endregion
}
