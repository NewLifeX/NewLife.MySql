using System.ComponentModel;
using NewLife.MySql;

namespace UnitTest;

/// <summary>MySqlCommand.Cancel 行为测试。当前驱动未实现取消，应明确抛异常而非静默无操作</summary>
[Collection(TestCollections.InMemory)]
public class MySqlCommandCancelTests
{
    [Fact]
    [DisplayName("Cancel未实现时抛出NotSupportedException")]
    public void Cancel_Throws()
    {
        using var cmd = new MySqlCommand();

        Assert.Throws<NotSupportedException>(() => cmd.Cancel());
    }
}
