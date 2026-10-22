using System.ComponentModel;
using System.Globalization;
using System.Reflection;
using NewLife;
using NewLife.MySql;
using NewLife.MySql.EntityFramework;

namespace UnitTest.EntityFramework;

/// <summary>EF6 提供程序运行时词表测试</summary>
public class LocalizationTests
{
    [Fact]
    [DisplayName("提供程序消息在模块初始化时注册")]
    public void ProviderMessagesRegistered()
    {
        // 触发 EF6 程序集加载与模块初始化
        _ = MySqlProviderServices.Instance.GetProviderManifest("8.0");

        // 触发主驱动程序集加载，验证两个程序集的词表合并而不是互相替换
        GC.KeepAlive(typeof(MySqlConnection));

        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-Hans");
            Assert.Equal("不支持的 EDM 类型：{0}。", Localization.Translate("Unsupported EDM type: {0}."));
            Assert.Equal("连接未打开。", Localization.Translate("Connection is not open."));

            Localization.Culture = CultureInfo.GetCultureInfo("en-US");
            Assert.Equal("The connection string does not specify a database name.", Localization.Translate("The connection string does not specify a database name."));
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
        var assembly = typeof(MySqlProviderServices).Assembly;
        var title = assembly.GetCustomAttribute<AssemblyTitleAttribute>()?.Title;
        var description = assembly.GetCustomAttribute<AssemblyDescriptionAttribute>()?.Description;
        Assert.False(title.IsNullOrEmpty());
        Assert.False(description.IsNullOrEmpty());

        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-Hans");
            Assert.Equal("新生命MySql EF6提供程序", Localization.Translate(title!));
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
