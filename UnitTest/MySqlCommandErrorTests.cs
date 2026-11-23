using System.ComponentModel;
using NewLife.MySql;

namespace UnitTest;

/// <summary>命令异常消息测试：超长 SQL 只保留头尾，避免批量 SQL 把异常日志撑大</summary>
[Collection(TestCollections.InMemory)]
public class MySqlCommandErrorTests
{
    [Fact]
    [DisplayName("超长SQL截断后保留头尾且不超过上限")]
    public void TruncateSql_KeepsHeadAndTail()
    {
        var head = "Insert Into biz_day_plan_cars(car_cost) Values";
        var tail = "'2027-05-01 22:00:00')";
        var sql = head + new String('x', 100_000) + tail;

        var result = MySqlCommand.TruncateSql(sql);

        Assert.Equal(MySqlCommand.MaxErrorSqlLength, result.Length);
        Assert.StartsWith(head, result);
        Assert.EndsWith(tail, result);
        Assert.Contains("...", result);
    }

    [Theory]
    [DisplayName("空字符串和短SQL保持原样")]
    [InlineData("")]
    [InlineData("Select 1")]
    public void TruncateSql_ShortSqlUnchanged(String sql) => Assert.Equal(sql, MySqlCommand.TruncateSql(sql));

    [Fact]
    [DisplayName("长度等于上限的SQL保持原样")]
    public void TruncateSql_ExactLimitUnchanged()
    {
        var sql = new String('a', MySqlCommand.MaxErrorSqlLength);

        Assert.Equal(sql, MySqlCommand.TruncateSql(sql));
    }

    [Fact]
    [DisplayName("命令异常消息中的超长SQL被截断")]
    public void CreateCommandException_TruncatesLongSql()
    {
        var head = "Insert Into biz_day_plan_cars(ds,car_cost) Values";
        var tail = "'2027-05-01 22:00:00')";
        var sql = head + new String('x', 1_000_000) + tail;

        using var cmd = new MySqlCommand { CommandText = sql };
        var error = cmd.CreateCommandException(new MySqlException(1054, "Unknown column 'car_cost' in 'field list'"), cmd.ToString());

        Assert.IsType<MySqlException>(error);
        Assert.Equal(1054, ((MySqlException)error).ErrorCode);
        Assert.StartsWith("Unknown column", error.Message);
        Assert.Contains("SQL", error.Message);
        Assert.Contains(head, error.Message);
        Assert.Contains(tail, error.Message);
        Assert.DoesNotContain(new String('x', 4000), error.Message);
        Assert.True(error.Message.Length < MySqlCommand.MaxErrorSqlLength + 100, $"异常消息长度 {error.Message.Length} 超限");
    }
}
