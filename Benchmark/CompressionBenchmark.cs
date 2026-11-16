using System;
using System.IO;
using BenchmarkDotNet.Attributes;
using NewLife.Data;
using NewLife.MySql;
using NewLife.Reflection;

namespace Benchmark;

/// <summary>压缩协议发送路径基准。验证 P4：每包 MemoryStream/DeflateStream/ToArray 及帧缓冲分配</summary>
/// <remarks>写入 Stream.Null，测量纯压缩+协议封装开销，不包含真实网络耗时</remarks>
[MemoryDiagnoser]
[ShortRunJob]
public class CompressionBenchmark
{
    private SqlClient _client = null!;
    private ArrayPacket _packet;

    /// <summary>报文大小</summary>
    [Params(64 * 1024, 1024 * 1024)]
    public Int32 Size { get; set; }

    /// <summary>是否可压缩（文本重复 vs 随机字节）</summary>
    [Params(true, false)]
    public Boolean Compressible { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var data = new Byte[Size];
        if (Compressible)
        {
            for (var i = 0; i < data.Length; i++) data[i] = (Byte)('a' + i % 26);
        }
        else
        {
            new Random(42).NextBytes(data);
        }

        _packet = new ArrayPacket(data, 0, data.Length);
        _client = new SqlClient(new MySqlConnectionStringBuilder("server=127.0.0.1;UseCompression=true"))
        {
            BaseStream = Stream.Null,
            Timeout = 0,
        };

        // 握手前直接打开压缩发送分支（与服务端协商后的运行态一致）
        _client.SetValue("_useCompression", true);
    }

    [GlobalCleanup]
    public void Cleanup() => _client.Dispose();

    [Benchmark]
    public void SendCompressedPacket() => _client.SendPacketAsync(_packet).GetAwaiter().GetResult();
}
