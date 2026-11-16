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
    [InlineData("EXPLAIN ANALYZE UPDATE t SET a=1", CommandType.Text)]
    [InlineData("EXPLAIN ANALYZE SELECT * FROM t", CommandType.Text)]
    [InlineData("SELECT * FROM t INTO OUTFILE '/tmp/out.txt'", CommandType.Text)]
    [InlineData("SELECT * FROM t INTO DUMPFILE '/tmp/out.bin'", CommandType.Text)]
    [InlineData("SELECT *\nINTO OUTFILE '/tmp/out.txt'", CommandType.Text)]
    [InlineData("", CommandType.Text)]
    [InlineData(null, CommandType.Text)]
    public void DmlSql_NotRetryable(String? sql, CommandType commandType)
    {
        Assert.False(MySqlCommand.IsReadOnlySql(sql, commandType));
    }

    [Theory]
    [DisplayName("分号后仍有语句的只读头不重试")]
    [InlineData("SELECT 1; INSERT INTO t VALUES(1)")]
    [InlineData("SELECT 1;\nUPDATE t SET a=1")]
    [InlineData("SHOW VARIABLES; DELETE FROM t")]
    [InlineData("SELECT ';'; INSERT INTO t VALUES(1)")]
    [InlineData("SELECT 1 /* ; */; INSERT INTO t VALUES(1)")]
    [InlineData("SELECT 1 /*! ; INSERT INTO t VALUES(1) */")]
    public void MultiStatementSelect_NotRetryable(String sql)
    {
        Assert.False(MySqlCommand.IsReadOnlySql(sql, CommandType.Text));
    }

    [Theory]
    [DisplayName("字符串与注释内的分号不影响只读判定")]
    [InlineData("SELECT ';'")]
    [InlineData("SELECT 1 /* ; */")]
    [InlineData("SELECT 1 -- ;")]
    [InlineData("SELECT 1;  ")]
    public void SemicolonInsideLiteralOrComment_Retryable(String sql)
    {
        Assert.True(MySqlCommand.IsReadOnlySql(sql, CommandType.Text));
    }

    [Theory]
    [DisplayName("NO_BACKSLASH_ESCAPES下反斜杠不吞引号，多语句不可重试")]
    [InlineData("SELECT 'a\\'; INSERT INTO t VALUES(1)--'", true, false)]
    [InlineData("SELECT 'a\\'; INSERT INTO t VALUES(1)--'", false, true)]
    public void NoBackslashEscapes_MultiStatement(String sql, Boolean noBackslashEscapes, Boolean readOnly)
    {
        Assert.Equal(readOnly, MySqlCommand.IsReadOnlySql(sql, CommandType.Text, noBackslashEscapes));
    }
}
