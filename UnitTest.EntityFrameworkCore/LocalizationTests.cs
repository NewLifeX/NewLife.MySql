using System.ComponentModel;
using System.Globalization;
using Microsoft.EntityFrameworkCore;
using NewLife;
using NewLife.MySql.EntityFrameworkCore;

namespace UnitTest.EntityFrameworkCore;

/// <summary>EF Core 提供程序运行时词表测试</summary>
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
        }
        finally
        {
            Localization.Culture = saved;
        }
    }
}
