using System.ComponentModel;
using System.Data;
using NewLife.MySql;

namespace UnitTest;

/// <summary>MySqlDataSource 集成测试。验证 DbDataSource 打开连接与命令执行</summary>
[Collection(TestCollections.ReadOnly)]
public class MySqlDataSourceIntegrationTests
{
    [Fact]
    [DisplayName("OpenConnectionAsync返回已打开连接")]
    public async Task OpenConnection()
    {
        var ds = new MySqlDataSource(DALTests.GetConnStr());

        await using var conn = await ds.OpenConnectionAsync();
        Assert.Equal(ConnectionState.Open, conn.State);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        Assert.Equal(1, Convert.ToInt32(cmd.ExecuteScalar()));
    }
}
