namespace NewLife.MySql.Binlog;

/// <summary>Binlog 位置。标识 binlog 文件中的精确位置，用于断点续传</summary>
public class BinlogPosition
{
    /// <summary>Binlog 文件名</summary>
    public String FileName { get; set; } = "";

    /// <summary>在 binlog 文件中的偏移位置</summary>
    public UInt32 Position { get; set; }

    /// <summary>实例化 Binlog 位置</summary>
    public BinlogPosition() { }

    /// <summary>实例化 Binlog 位置</summary>
    /// <param name="fileName">Binlog 文件名</param>
    /// <param name="position">偏移位置</param>
    public BinlogPosition(String fileName, UInt32 position)
    {
        FileName = fileName;
        Position = position;
    }

    /// <summary>已重写</summary>
    public override String ToString() => $"{FileName}:{Position}";
}
