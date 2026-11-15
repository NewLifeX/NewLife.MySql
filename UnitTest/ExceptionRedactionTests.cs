using System.ComponentModel;
using NewLife.MySql;

namespace UnitTest;

/// <summary>异常与日志脱敏测试：命令失败时只暴露命令文本与参数名，不把参数实际值写进异常消息</summary>
[Collection(TestCollections.ReadOnly)]
public class ExceptionRedactionTests
{
    [Fact]
    [DisplayName("命令异常不泄漏参数实际值")]
    public async Task Exception_DoesNotLeakParameterValue()
    {
        using var conn = new MySqlConnection(DALTests.GetConnStr());
        await conn.OpenAsync();

        using var cmd = new MySqlCommand(conn, "SELECT * FROM no_such_table_for_redaction WHERE name=@n");
        cmd.Parameters.AddWithValue("@n", "secret-13900001111");

        var ex = await Assert.ThrowsAsync<MySqlException>(() => cmd.ExecuteScalarAsync());

        Assert.DoesNotContain("secret-13900001111", ex.Message);
        Assert.Contains("@n", ex.Message);
    }
}
