using System.ComponentModel;
using NewLife.MySql;
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

        _conn.ExecuteNonQuery($@"CREATE TABLE IF NOT EXISTS `{_table}` (
            `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `name` VARCHAR(64) DEFAULT NULL,
            `payload` LONGTEXT DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{_table}`");
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
}
