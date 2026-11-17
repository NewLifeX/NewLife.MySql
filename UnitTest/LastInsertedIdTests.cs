using System.ComponentModel;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>自增ID回填测试。驱动直接读取 OK 包的 last_insert_id，上层无需再执行 SELECT LAST_INSERT_ID()</summary>
[Collection(TestCollections.DataModification)]
public class LastInsertedIdTests
{
    [Fact]
    [DisplayName("插入后命令回填自增ID且与服务端LAST_INSERT_ID一致")]
    public async Task Insert_FillsLastInsertedId()
    {
        using var conn = new MySqlConnection(DALTests.GetConnStr());
        await conn.OpenAsync();

        using (var create = conn.CreateCommand())
        {
            create.CommandText = "CREATE DATABASE IF NOT EXISTS `newlife_test`";
            await create.ExecuteNonQueryAsync();
        }
        conn.ChangeDatabase("newlife_test");

        var table = "identity_" + Rand.Next(100000);
        try
        {
            using (var create = conn.CreateCommand())
            {
                create.CommandText = $"CREATE TABLE `{table}` (`id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(32))";
                await create.ExecuteNonQueryAsync();
            }

            using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (`name`) VALUES ('a')");
            Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
            Assert.True(cmd.LastInsertedId > 0);

            // 与服务端 LAST_INSERT_ID() 对比，确认取自 OK 包
            using var verify = new MySqlCommand(conn, "SELECT LAST_INSERT_ID()");
            Assert.Equal(cmd.LastInsertedId, (await verify.ExecuteScalarAsync()).ToLong());
        }
        finally
        {
            using var drop = conn.CreateCommand();
            drop.CommandText = $"DROP TABLE IF EXISTS `{table}`";
            await drop.ExecuteNonQueryAsync();
        }
    }
}
