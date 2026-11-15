using System.ComponentModel;
using NewLife;
using NewLife.MySql;

namespace UnitTest;

/// <summary>超过单帧上限（16MB）的报文的收发测试。协议帧头只有 3 字节长度，超长报文需按 0xFFFFFF 切帧，
/// 旧实现发送侧长度回绕、接收侧不续帧，会导致协议错位与数据截断</summary>
[Collection(TestCollections.ReadOnly)]
public class LargePacketTests
{
    [Fact]
    [DisplayName("17MB参数写入与读取往返一致")]
    public async Task LargePayload_RoundTrip()
    {
        using var conn = new MySqlConnection(DALTests.GetConnStr());
        await conn.OpenAsync();

        // 服务端 max_allowed_packet 过小时无法验证（MySQL 8 默认 64MB）
        using (var check = new MySqlCommand(conn, "SELECT @@max_allowed_packet"))
        {
            var max = (await check.ExecuteScalarAsync()).ToLong();
            if (max < 40L * 1024 * 1024) return;
        }

        var data = new Byte[17 * 1024 * 1024];
        for (var i = 0; i < data.Length; i++) data[i] = (Byte)(i * 31 + 7);

        using (var cmd = new MySqlCommand(conn, "CREATE TABLE IF NOT EXISTS test.big_payload (id INT PRIMARY KEY, data LONGBLOB)"))
            await cmd.ExecuteNonQueryAsync();

        try
        {
            using (var cmd = new MySqlCommand(conn, "REPLACE INTO test.big_payload (id, data) VALUES (1, @d)"))
            {
                cmd.Parameters.AddWithValue("@d", data);
                await cmd.ExecuteNonQueryAsync();
            }

            using (var cmd = new MySqlCommand(conn, "SELECT data FROM test.big_payload WHERE id = 1"))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.True(await reader.ReadAsync());

                var value = (Byte[])reader.GetValue(0);
                Assert.Equal(data.Length, value.Length);
                Assert.Equal(data, value);
            }
        }
        finally
        {
            using var cleanup = new MySqlCommand(conn, "DELETE FROM test.big_payload WHERE id = 1");
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
