using System.ComponentModel;
using System.Text;
using NewLife.MySql;

namespace UnitTest;

/// <summary>zlib 编解码单元测试。验证压缩协议 zlib 往返（MySQL 压缩协议使用 zlib 格式）</summary>
/// <remarks>对应审计遗留项 L2：验证 ZlibCompress/ZlibDecompress 往返正确</remarks>
[Collection(TestCollections.InMemory)]
public class ZlibCodecTests
{
    [Fact]
    [DisplayName("zlib压缩解压往返一致")]
    public void Compress_Decompress_RoundTrip()
    {
        var data = Encoding.UTF8.GetBytes(new String('x', 10000));

        var compressed = SqlClient.ZlibCompress(data);

        // 可压缩数据应压缩变小
        Assert.True(compressed.Length < data.Length);

        var output = new Byte[data.Length];
        var n = SqlClient.ZlibDecompress(compressed, 0, compressed.Length, output, 0, output.Length);

        Assert.Equal(data.Length, n);
        Assert.Equal(data, output);
    }

    [Fact]
    [DisplayName("zlib压缩数据带标准zlib头")]
    public void Compressed_HasZlibHeader()
    {
        var data = Encoding.UTF8.GetBytes("hello zlib");

        var compressed = SqlClient.ZlibCompress(data);

        // zlib 头：CMF=0x78 (deflate, 32K window)
        Assert.Equal(0x78, compressed[0]);
        Assert.Equal(0x9C, compressed[1]);
    }

    [Fact]
    [DisplayName("zlib解压不满目标容量按实际返回")]
    public void Decompress_ShorterThanCapacity()
    {
        var data = Encoding.UTF8.GetBytes(new String('a', 500));
        var compressed = SqlClient.ZlibCompress(data);

        // 目标容量大于实际解压长度
        var output = new Byte[1000];
        var n = SqlClient.ZlibDecompress(compressed, 0, compressed.Length, output, 0, output.Length);

        Assert.Equal(data.Length, n);
        Assert.Equal(data, output[..n]);
    }
}