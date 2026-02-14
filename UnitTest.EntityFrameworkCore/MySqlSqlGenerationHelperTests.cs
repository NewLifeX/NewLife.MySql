using System.ComponentModel;
using NewLife.MySql.EntityFrameworkCore;

namespace UnitTest.EntityFrameworkCore;

/// <summary>SQL 生成辅助器测试</summary>
public class MySqlSqlGenerationHelperTests
{
    [Fact]
    [DisplayName("DelimitIdentifier应使用反引号")]
    public void DelimitIdentifier_ShouldUseBackticks()
    {
        var helper = CreateHelper();

        var result = helper.DelimitIdentifier("table_name");

        Assert.Equal("`table_name`", result);
    }

    [Fact]
    [DisplayName("DelimitIdentifier应转义反引号")]
    public void DelimitIdentifier_ShouldEscapeBackticks()
    {
        var helper = CreateHelper();

        var result = helper.DelimitIdentifier("table`name");

        Assert.Equal("`table``name`", result);
    }

    [Fact]
    [DisplayName("DelimitIdentifier带Schema应使用点号分隔")]
    public void DelimitIdentifier_WithSchema_ShouldUseDotSeparator()
    {
        var helper = CreateHelper();

        var result = helper.DelimitIdentifier("table_name", "schema_name");

        Assert.Equal("`schema_name`.`table_name`", result);
    }

    [Fact]
    [DisplayName("StatementTerminator应为分号")]
    public void StatementTerminator_ShouldBeSemicolon()
    {
        var helper = CreateHelper();

        Assert.Equal(";", helper.StatementTerminator);
    }

    [Fact]
    [DisplayName("EscapeIdentifier应转义反引号")]
    public void EscapeIdentifier_ShouldEscapeBackticks()
    {
        var helper = CreateHelper();

        var result = helper.EscapeIdentifier("my`table");

        Assert.Equal("my``table", result);
    }

    private static MySqlSqlGenerationHelper CreateHelper()
    {
        // 使用最简构造，传入默认依赖
        var deps = new Microsoft.EntityFrameworkCore.Storage.RelationalSqlGenerationHelperDependencies();
        return new MySqlSqlGenerationHelper(deps);
    }
}
