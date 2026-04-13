using System.ComponentModel;
using NewLife.MySql;

namespace UnitTest;

[Collection(TestCollections.InMemory)]
public class MySqlCommandTimeoutTests
{
    [Fact]
    [DisplayName("命令默认继承连接字符串中的超时设置")]
    public void CommandTimeout_ShouldInheritFromConnectionSetting()
    {
        var conn = new MySqlConnection("server=localhost;database=test;command timeout=66");

        using var cmd = new MySqlCommand(conn, "select 1");

        Assert.Equal(66, cmd.CommandTimeout);
    }
}