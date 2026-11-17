using System.Collections.Concurrent;

namespace NewLife.MySql;

/// <summary>MySQL 压缩协议算法</summary>
public enum MySqlCompressionAlgorithm
{
    /// <summary>zlib。驱动内置实现，默认算法</summary>
    Zlib = 0,

    /// <summary>zstd。需要安装 NewLife.MySql.Zstd 可选包并注册编解码器</summary>
    Zstd = 1,
}

/// <summary>MySQL 压缩协议编解码器。由可选压缩包实现，核心驱动不直接依赖第三方压缩库</summary>
public interface IMySqlCompressionCodec
{
    /// <summary>压缩数据</summary>
    /// <param name="data">源数据</param>
    /// <param name="offset">源偏移</param>
    /// <param name="count">源长度</param>
    /// <returns>压缩后的数据</returns>
    Byte[] Compress(Byte[] data, Int32 offset, Int32 count);

    /// <summary>解压数据</summary>
    /// <param name="compressed">压缩数据</param>
    /// <param name="offset">源偏移</param>
    /// <param name="count">源长度</param>
    /// <param name="output">目标缓冲</param>
    /// <param name="outputOffset">目标偏移</param>
    /// <param name="outputCount">目标容量</param>
    /// <returns>实际解压字节数</returns>
    Int32 Decompress(Byte[] compressed, Int32 offset, Int32 count, Byte[] output, Int32 outputOffset, Int32 outputCount);
}

/// <summary>压缩算法注册表。核心只内置 zlib，zstd 等算法由可选包注册</summary>
public static class MySqlCompression
{
    private static readonly ConcurrentDictionary<MySqlCompressionAlgorithm, Func<Int32, IMySqlCompressionCodec>> _factories = new();

    /// <summary>注册压缩算法工厂。注册多次时后面的覆盖前面的，便于应用自定义实现</summary>
    /// <param name="algorithm">算法</param>
    /// <param name="factory">工厂，参数为压缩级别</param>
    public static void Register(MySqlCompressionAlgorithm algorithm, Func<Int32, IMySqlCompressionCodec> factory)
    {
        if (factory == null) throw new ArgumentNullException(nameof(factory));

        _factories[algorithm] = factory;
    }

    /// <summary>判断算法是否可用。zlib 始终可用</summary>
    /// <param name="algorithm">算法</param>
    /// <returns>可用返回 true</returns>
    public static Boolean IsRegistered(MySqlCompressionAlgorithm algorithm)
        => algorithm == MySqlCompressionAlgorithm.Zlib || _factories.ContainsKey(algorithm);

    /// <summary>创建算法编解码器。每次连接独立创建，避免多连接共享非线程安全状态</summary>
    /// <param name="algorithm">算法</param>
    /// <param name="level">压缩级别</param>
    /// <returns>编解码器；zlib 返回 null，由核心内置实现处理</returns>
    internal static IMySqlCompressionCodec? CreateCodec(MySqlCompressionAlgorithm algorithm, Int32 level)
    {
        if (algorithm == MySqlCompressionAlgorithm.Zlib) return null;

        if (!_factories.TryGetValue(algorithm, out var factory))
            throw new NotSupportedException(L.T("zstd compression is not registered. Install NewLife.MySql.Zstd and call ZstdCompression.Use()."));

        return factory(level);
    }
}
