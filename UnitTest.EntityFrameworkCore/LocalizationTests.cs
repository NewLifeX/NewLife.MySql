using System.ComponentModel;
using System.Globalization;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using NewLife;
using NewLife.MySql.EntityFrameworkCore;

namespace UnitTest.EntityFrameworkCore;

/// <summary>EF Core 提供程序运行时词表测试</summary>
[Collection("GlobalState")]
public class LocalizationTests
{
    [Fact]
    [DisplayName("提供程序消息在模块初始化时注册")]
    public void ProviderMessagesRegistered()
    {
        // 触发 EF Core 提供程序程序集加载与模块初始化
        using var context = new TestDbContext(new DbContextOptionsBuilder<TestDbContext>()
            .UseMySql("Server=localhost;Port=3306;Database=test;User Id=root;Password=******;")
            .Options);

        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-Hans");
            Assert.Equal("连接字符串未指定数据库名，无法创建。", Localization.Translate("The connection string does not specify a database name; unable to create it."));

            Localization.Culture = CultureInfo.GetCultureInfo("en-US");
            Assert.Equal("The connection string does not specify a database name; unable to drop it.", Localization.Translate("The connection string does not specify a database name; unable to drop it."));

            foreach (var name in new[] { "zh-Hans", "zh-Hant", "es", "de", "ja", "fr", "pt", "ru", "ko", "ar" })
            {
                Localization.Culture = CultureInfo.GetCultureInfo(name);
                var text = Localization.Translate("The connection string does not specify a database name; unable to create it.");
                Assert.True(text != "The connection string does not specify a database name; unable to create it.", $"{name} 缺少译文");
            }
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact]
    [DisplayName("程序集标题与描述可翻译")]
    public void AssemblyMetadataTranslates()
    {
        var assembly = typeof(MySqlDbContextOptionsExtensions).Assembly;
        var title = assembly.GetCustomAttribute<AssemblyTitleAttribute>()?.Title;
        var description = assembly.GetCustomAttribute<AssemblyDescriptionAttribute>()?.Description;
        Assert.False(title.IsNullOrEmpty());
        Assert.False(description.IsNullOrEmpty());

        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-Hans");
            Assert.Equal("新生命MySql EFCore提供程序", Localization.Translate(title!));
            Assert.Contains("基于 NewLife.MySql", Localization.Translate(description!));

            Localization.Culture = CultureInfo.GetCultureInfo("en-US");
            Assert.Equal(title, Localization.Translate(title!));
        }
        finally
        {
            Localization.Culture = saved;
        }
    }
}
