using ZstdSharp;

namespace NewLife.MySql.Zstd;

/// <summary>zstd 压缩注册入口。应用启动时调用一次 <see cref="Use"/>，核心驱动据此创建 zstd 编解码器</summary>
public static class ZstdCompression
{
    /// <summary>默认压缩级别</summary>
    public const Int32 DefaultLevel = 3;

    /// <summary>注册 zstd 编解码器。重复调用会覆盖之前的注册</summary>
    public static void Use() => MySqlCompression.Register(MySqlCompressionAlgorithm.Zstd, level => new ZstdCodec(level));
}

/// <summary>zstd 编解码器。每个连接持有独立实例，避免多连接共享非线程安全状态</summary>
internal sealed class ZstdCodec : IMySqlCompressionCodec, IDisposable
{
    private readonly Compressor _compressor;
    private readonly Decompressor _decompressor;

    public ZstdCodec(Int32 level)
    {
        _compressor = new Compressor(level);
        _decompressor = new Decompressor();
    }

    public Byte[] Compress(Byte[] data, Int32 offset, Int32 count) => _compressor.Wrap(new ReadOnlySpan<Byte>(data, offset, count)).ToArray();

    public Int32 Decompress(Byte[] compressed, Int32 offset, Int32 count, Byte[] output, Int32 outputOffset, Int32 outputCount)
        => _decompressor.Unwrap(new ReadOnlySpan<Byte>(compressed, offset, count), new Span<Byte>(output, outputOffset, outputCount));

    public void Dispose()
    {
        _compressor.Dispose();
        _decompressor.Dispose();
    }
}
