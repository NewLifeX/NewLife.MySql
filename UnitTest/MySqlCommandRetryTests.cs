using System.ComponentModel;
using System.Data;
using NewLife.MySql;

namespace UnitTest;

/// <summary>MySqlCommand 断线重试策略测试。验证仅读语句可重试、DML 不重试，防止断线重连重复执行非幂等写入</summary>
[Collection(TestCollections.InMemory)]
public class MySqlCommandRetryTests
{
    [Theory]
    [DisplayName("只读语句标记为可重试")]
    [InlineData("SELECT * FROM t", CommandType.Text)]
    [InlineData("select id from t", CommandType.Text)]
    [InlineData("  SELECT 1", CommandType.Text)]
    [InlineData("SHOW VARIABLES", CommandType.Text)]
    [InlineData("DESC t", CommandType.Text)]
    [InlineData("DESCRIBE t", CommandType.Text)]
    [InlineData("EXPLAIN SELECT 1", CommandType.Text)]
    [InlineData("t", CommandType.TableDirect)]
    public void ReadOnlySql_IsRetryable(String sql, CommandType commandType)
    {
        Assert.True(MySqlCommand.IsReadOnlySql(sql, commandType));
    }

    [Theory]
    [DisplayName("DML与存储过程不重试")]
    [InlineData("INSERT INTO t VALUES(1)", CommandType.Text)]
    [InlineData("UPDATE t SET a=1", CommandType.Text)]
    [InlineData("DELETE FROM t", CommandType.Text)]
    [InlineData("CALL proc", CommandType.StoredProcedure)]
    [InlineData("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", CommandType.Text)]
    [InlineData("", CommandType.Text)]
    [InlineData(null, CommandType.Text)]
    public void DmlSql_NotRetryable(String? sql, CommandType commandType)
    {
        Assert.False(MySqlCommand.IsReadOnlySql(sql, commandType));
    }
}
