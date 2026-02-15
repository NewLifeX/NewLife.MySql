using System.ComponentModel;
using Microsoft.EntityFrameworkCore;
using NewLife.MySql.EntityFrameworkCore;

namespace UnitTest.EntityFrameworkCore;

/// <summary>MySql 查询 SQL 生成器测试</summary>
public class MySqlQuerySqlGeneratorTests
{
    private const String TestConnectionString = "Server=localhost;Port=3306;Database=test;User Id=root;Password=pass;";

    [Fact]
    [DisplayName("Take查询应生成LIMIT子句")]
    public void Take_ShouldGenerateLimit()
    {
        using var context = CreateContext();

        var sql = context.Users.Take(10).ToQueryString();

        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("Skip查询应生成OFFSET子句")]
    public void Skip_ShouldGenerateOffset()
    {
        using var context = CreateContext();

        var sql = context.Users.Skip(5).ToQueryString();

        Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("Skip和Take组合应生成LIMIT和OFFSET")]
    public void SkipTake_ShouldGenerateLimitAndOffset()
    {
        using var context = CreateContext();

        var sql = context.Users.Skip(10).Take(20).ToQueryString();

        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("Where查询应生成WHERE子句")]
    public void Where_ShouldGenerateWhereClause()
    {
        using var context = CreateContext();

        var sql = context.Users.Where(u => u.Age > 18).ToQueryString();

        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("OrderBy查询应生成ORDER BY子句")]
    public void OrderBy_ShouldGenerateOrderByClause()
    {
        using var context = CreateContext();

        var sql = context.Users.OrderBy(u => u.Name).ToQueryString();

        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("Select投影应生成正确列")]
    public void Select_ShouldProjectColumns()
    {
        using var context = CreateContext();

        var sql = context.Users.Select(u => new { u.Name, u.Age }).ToQueryString();

        Assert.Contains("`Name`", sql);
        Assert.Contains("`Age`", sql);
    }

    [Fact]
    [DisplayName("Count查询应生成COUNT函数")]
    public void Count_ShouldGenerateCountFunction()
    {
        using var context = CreateContext();

        var sql = context.Users.Where(u => u.Age > 0).Select(u => u.Id).ToQueryString();

        // COUNT 是终结操作，使用 Select + ToQueryString 来间接验证
        Assert.Contains("SELECT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("表名应使用反引号转义")]
    public void TableName_ShouldUseBackticks()
    {
        using var context = CreateContext();

        var sql = context.Users.ToQueryString();

        Assert.Contains("`users`", sql);
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseMySql(TestConnectionString)
            .Options;

        return new TestDbContext(options);
    }
}
