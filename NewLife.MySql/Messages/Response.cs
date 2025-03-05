namespace NewLife.MySql.Messages;

/// <summary>响应</summary>
public class Response(Stream stream)
{
    #region 属性
    /// <summary>数据流</summary>
    public Stream Stream { get; set; } = stream;

    /// <summary>数据包长度</summary>
    public Int32 Length { get; set; }

    /// <summary>数据包序号</summary>
    public Byte Sequence { get; set; }

    /// <summary>响应代码。OK=0，EOF=FE</summary>
    public Byte Kind { get; set; }
    #endregion

    #region 扩展属性
    /// <summary>是否成功。0x00</summary>
    public Boolean IsOK => Kind == 0x00;

    /// <summary>是否错误。0xFF</summary>
    public Boolean IsError => Kind == 0xFF;

    /// <summary>是否结尾。0xFE</summary>
    public Boolean IsEOF => Kind == 0xFE;
    #endregion

    #region 方法
    private BinaryReader? _reader;
    /// <summary>获取读取器</summary>
    public BinaryReader GetReader() => _reader ??= new(Stream);

    /// <summary>读取长度</summary>
    public Int32 ReadLength()
    {
        var reader = GetReader();
        return Kind switch
        {
            251 => -1,
            252 => reader.ReadUInt16(),
            253 => reader.ReadBytes(3).ToInt(),
            254 => reader.ReadInt32(),
            _ => Kind,
        };
    }
    #endregion
}
