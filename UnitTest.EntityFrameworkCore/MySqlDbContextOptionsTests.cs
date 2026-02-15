using System.ComponentModel;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using NewLife.MySql.EntityFrameworkCore;

namespace UnitTest.EntityFrameworkCore;

/// <summary>DbContext 选项扩展测试</summary>
public class MySqlDbContextOptionsTests
{
    private const String TestConnectionString = "Server=localhost;Port=3306;Database=test;User Id=root;Password=pass;";

    [Fact]
    [DisplayName("UseMySql应注册MySqlOptionsExtension")]
    public void UseMySql_ShouldRegisterExtension()
    {
        var builder = new DbContextOptionsBuilder<TestDbContext>();

        builder.UseMySql(TestConnectionString);

        var options = builder.Options;
        var extension = options.FindExtension<MySqlOptionsExtension>();
        Assert.NotNull(extension);
    }

    [Fact]
    [DisplayName("UseMySql应设置连接字符串")]
    public void UseMySql_ShouldSetConnectionString()
    {
        var builder = new DbContextOptionsBuilder<TestDbContext>();

        builder.UseMySql(TestConnectionString);

        var extension = builder.Options.FindExtension<MySqlOptionsExtension>();
        Assert.NotNull(extension);
        Assert.Equal(TestConnectionString, extension.ConnectionString);
    }

    [Fact]
    [DisplayName("UseMySql连接字符串为空应抛出异常")]
    public void UseMySql_NullConnectionString_ShouldThrow()
    {
        var builder = new DbContextOptionsBuilder<TestDbContext>();

        Assert.Throws<ArgumentNullException>(() => builder.UseMySql((String)null!));
    }

    [Fact]
    [DisplayName("UseMySql空白连接字符串应抛出异常")]
    public void UseMySql_EmptyConnectionString_ShouldThrow()
    {
        var builder = new DbContextOptionsBuilder<TestDbContext>();

        Assert.Throws<ArgumentNullException>(() => builder.UseMySql("  "));
    }

    [Fact]
    [DisplayName("MySqlOptionsExtension信息应包含LogFragment")]
    public void Extension_Info_ShouldHaveLogFragment()
    {
        var extension = new MySqlOptionsExtension().WithConnectionString(TestConnectionString);

        Assert.Contains("NewLife.MySql", extension.Info.LogFragment);
    }
}
