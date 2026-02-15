using System.ComponentModel;
using Microsoft.EntityFrameworkCore;
using NewLife.MySql.EntityFrameworkCore;

namespace UnitTest.EntityFrameworkCore;

/// <summary>方法翻译器测试。验证 .NET 方法到 MySQL 函数的翻译</summary>
public class MySqlMethodTranslatorTests
{
    private const String TestConnectionString = "Server=localhost;Port=3306;Database=test;User Id=root;Password=pass;";

    #region String 方法翻译
    [Fact]
    [DisplayName("String.Contains应翻译为LOCATE")]
    public void StringContains_ShouldTranslateToLocate()
    {
        using var context = CreateContext();

        var sql = context.Users.Where(u => u.Name.Contains("test")).ToQueryString();

        Assert.Contains("LOCATE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("String.StartsWith应翻译为LIKE CONCAT")]
    public void StringStartsWith_ShouldTranslateToLike()
    {
        using var context = CreateContext();

        var sql = context.Users.Where(u => u.Name.StartsWith("test")).ToQueryString();

        Assert.Contains("LIKE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CONCAT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("String.EndsWith应翻译为LIKE CONCAT")]
    public void StringEndsWith_ShouldTranslateToLike()
    {
        using var context = CreateContext();

        var sql = context.Users.Where(u => u.Name.EndsWith("test")).ToQueryString();

        Assert.Contains("LIKE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CONCAT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("String.ToUpper应翻译为UPPER")]
    public void StringToUpper_ShouldTranslateToUpper()
    {
        using var context = CreateContext();

        var sql = context.Users.Select(u => u.Name.ToUpper()).ToQueryString();

        Assert.Contains("UPPER", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("String.ToLower应翻译为LOWER")]
    public void StringToLower_ShouldTranslateToLower()
    {
        using var context = CreateContext();

        var sql = context.Users.Select(u => u.Name.ToLower()).ToQueryString();

        Assert.Contains("LOWER", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("String.Trim应翻译为TRIM")]
    public void StringTrim_ShouldTranslateToTrim()
    {
        using var context = CreateContext();

        var sql = context.Users.Select(u => u.Name.Trim()).ToQueryString();

        Assert.Contains("TRIM", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("String.Replace应翻译为REPLACE")]
    public void StringReplace_ShouldTranslateToReplace()
    {
        using var context = CreateContext();

        var sql = context.Users.Select(u => u.Name.Replace("a", "b")).ToQueryString();

        Assert.Contains("REPLACE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("String.Substring应翻译为SUBSTRING")]
    public void StringSubstring_ShouldTranslateToSubstring()
    {
        using var context = CreateContext();

        var sql = context.Users.Select(u => u.Name.Substring(0, 5)).ToQueryString();

        Assert.Contains("SUBSTRING", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("String.Length应翻译为CHAR_LENGTH")]
    public void StringLength_ShouldTranslateToCharLength()
    {
        using var context = CreateContext();

        var sql = context.Users.Select(u => u.Name.Length).ToQueryString();

        Assert.Contains("CHAR_LENGTH", sql, StringComparison.OrdinalIgnoreCase);
    }
    #endregion

    #region DateTime 成员翻译
    [Fact]
    [DisplayName("DateTime.Year应翻译为YEAR")]
    public void DateTimeYear_ShouldTranslateToYear()
    {
        using var context = CreateContext();

        var sql = context.Users.Select(u => u.CreatedAt.Year).ToQueryString();

        Assert.Contains("YEAR", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("DateTime.Month应翻译为MONTH")]
    public void DateTimeMonth_ShouldTranslateToMonth()
    {
        using var context = CreateContext();

        var sql = context.Users.Select(u => u.CreatedAt.Month).ToQueryString();

        Assert.Contains("MONTH", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("DateTime.Day应翻译为DAY")]
    public void DateTimeDay_ShouldTranslateToDay()
    {
        using var context = CreateContext();

        var sql = context.Users.Select(u => u.CreatedAt.Day).ToQueryString();

        Assert.Contains("DAY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("DateTime.Hour应翻译为HOUR")]
    public void DateTimeHour_ShouldTranslateToHour()
    {
        using var context = CreateContext();

        var sql = context.Users.Select(u => u.CreatedAt.Hour).ToQueryString();

        Assert.Contains("HOUR", sql, StringComparison.OrdinalIgnoreCase);
    }
    #endregion

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseMySql(TestConnectionString)
            .Options;

        return new TestDbContext(options);
    }
}
