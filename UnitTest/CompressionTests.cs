using System.ComponentModel;
using NewLife;
using NewLife.MySql;
using NewLife.MySql.Zstd;
using NewLife.Security;

namespace UnitTest;

/// <summary>压缩协议测试。验证 UseCompression=true 时握手、查询、大数据包往返正确（真实 MySQL）</summary>
/// <remarks>对应审计遗留项 L2：此前 SendCompressedPacketAsync/ReadCompressedPacketAsync 无测试覆盖</remarks>
[Collection(TestCollections.DataModification)]
public class CompressionTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public CompressionTests()
    {
        _table = "comp_test_" + Rand.Next(10000);
        // 在原始连接串上追加 UseCompression=true
        var connStr = _ConnStr.TrimEnd(';') + ";UseCompression=true;";
        _conn = new MySqlConnection(connStr);
        _conn.Open();

        _conn.ExecuteNonQuery($@"CREATE TEMPORARY TABLE IF NOT EXISTS `{_table}` (
            `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `name` VARCHAR(64) DEFAULT NULL,
            `payload` LONGTEXT DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TEMPORARY TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    [Fact]
    [DisplayName("压缩协议下简单查询")]
    public void Compressed_SimpleQuery()
    {
        using var cmd = new MySqlCommand(_conn, "SELECT 1");
        var rs = cmd.ExecuteScalar();
        Assert.Equal(1L, rs);
    }

    [Fact]
    [DisplayName("压缩连接关闭重开后仍可查询")]
    public void Compressed_CloseAndReopen()
    {
        var connStr = _ConnStr.TrimEnd(';') + ";UseCompression=true;MaxPoolSize=1;";
        using var conn = new MySqlConnection(connStr);
        conn.Open();
        using (var cmd = new MySqlCommand(conn, "SELECT 1")) Assert.Equal(1L, cmd.ExecuteScalar());

        conn.Close();
        conn.Open();
        using (var cmd = new MySqlCommand(conn, "SELECT 2")) Assert.Equal(2L, cmd.ExecuteScalar());
    }

    [Fact]
    [DisplayName("压缩协议下预编译语句查询")]
    public void Compressed_PreparedQuery()
    {
        using var cmd = new MySqlCommand("SELECT ?", _conn);
        cmd.Parameters.AddWithValue("", 42);
        cmd.Prepare();

        var rs = cmd.ExecuteScalar();
        Assert.Equal(42L, rs);
    }

    [Fact]
    [DisplayName("压缩协议下大数据包往返")]
    public void Compressed_LargePayload()
    {
        // 构造大于压缩阈值的数据（8KB，触发压缩发送与解压读取）
        var big = new String('x', 1024 * 8);
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name, payload) VALUES ('big', '{big}')");

        using var cmd = new MySqlCommand(_conn, $"SELECT payload FROM `{_table}` WHERE name='big'");
        var rs = cmd.ExecuteScalar();
        Assert.Equal(big, rs);
    }

    [Fact]
    [DisplayName("压缩协议下不可压缩数据回退明文且往返一致")]
    public void Compressed_IncompressiblePayload()
    {
        // Base64 后的随机字节近似不可压缩，覆盖“压缩后更大则回退未压缩”发送分支
        var text = Convert.ToBase64String(Rand.NextBytes(256 * 1024));

        using (var insert = new MySqlCommand(_conn, $"REPLACE INTO `{_table}` (id, name, payload) VALUES (98, 'random', @d)"))
        {
            insert.Parameters.AddWithValue("@d", text);
            insert.ExecuteNonQuery();
        }

        using var select = new MySqlCommand(_conn, $"SELECT payload FROM `{_table}` WHERE id=98");
        Assert.Equal(text, select.ExecuteScalar());
    }

    [Fact]
    [DisplayName("压缩协议下超过16MB的报文给出明确提示")]
    public void Compressed_HugePayload()
    {
        // 传输层压缩包与普通包的长度字段都只有 3 字节，>16MB 需要压缩层分帧；
        // 驱动选择明确拒绝，而不是发出错长度的包把协议流搞乱
        var data = new Byte[17 * 1024 * 1024];

        using var cmd = new MySqlCommand(_conn, $"REPLACE INTO `{_table}` (id, name, payload) VALUES (99, 'huge', @d)");
        cmd.Parameters.AddWithValue("@d", Convert.ToBase64String(data));

        var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
        Assert.Contains("UseCompression", ex.ToString());
    }

    [Fact]
    [DisplayName("zstd压缩协议下查询与大数据往返")]
    public void Zstd_QueryAndLargePayload()
    {
        ZstdCompression.Use();

        var table = "zstd_comp_" + Rand.Next(10000);
        var connStr = _ConnStr.TrimEnd(';') + ";UseCompression=true;CompressionAlgorithm=zstd;MaxPoolSize=1;";
        using var conn = new MySqlConnection(connStr);
        conn.Open();

        using (var cmd = new MySqlCommand(conn, "SELECT 1"))
            Assert.Equal(1L, cmd.ExecuteScalar());

        conn.ExecuteNonQuery($"CREATE TEMPORARY TABLE `{table}` (`id` INT NOT NULL PRIMARY KEY, `payload` LONGTEXT) ENGINE=InnoDB");

        var text = new String('z', 256 * 1024);
        using (var insert = new MySqlCommand(conn, $"INSERT INTO `{table}` VALUES (1, @p)"))
        {
            insert.Parameters.AddWithValue("@p", text);
            insert.ExecuteNonQuery();
        }

        using (var select = new MySqlCommand(conn, $"SELECT payload FROM `{table}` WHERE id=1"))
            Assert.Equal(text, select.ExecuteScalar());

        // 预编译参数路径
        using (var prepared = new MySqlCommand("SELECT @v", conn))
        {
            prepared.Parameters.AddWithValue("@v", 42);
            prepared.Prepare();
            Assert.Equal(42L, prepared.ExecuteScalar());
        }

        // 不可压缩数据走“压缩后更大则回退明文”分支
        var random = Convert.ToBase64String(Rand.NextBytes(256 * 1024));
        using (var insert = new MySqlCommand(conn, $"REPLACE INTO `{table}` VALUES (2, @p)"))
        {
            insert.Parameters.AddWithValue("@p", random);
            insert.ExecuteNonQuery();
        }
        using (var select = new MySqlCommand(conn, $"SELECT payload FROM `{table}` WHERE id=2"))
            Assert.Equal(random, select.ExecuteScalar());
    }

    [Fact]
    [DisplayName("压缩与管道化同时开启时批量执行回退串行且结果正确")]
    public void Compressed_PipelineBatch()
    {
        var table = "comp_pipe_" + Rand.Next(10000);
        var connStr = _ConnStr.TrimEnd(';') + ";UseCompression=true;Pipeline=true;";
        using var conn = new MySqlConnection(connStr);
        conn.Open();
        conn.ExecuteNonQuery($"CREATE TABLE IF NOT EXISTS `{table}` (`id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(32)) ENGINE=InnoDB");
        try
        {
            using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (`name`) VALUES (@n)");
            var sets = new List<Object?[]> { new Object?[] { "a" }, new Object?[] { "b" }, new Object?[] { "c" } };
            Assert.Equal(3, cmd.ExecuteBatchValues(sets));

            using var check = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}`");
            Assert.Equal(3, check.ExecuteScalar().ToInt());
        }
        finally
        {
            conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{table}`");
        }
    }
}
