using System.ComponentModel;
using System.Globalization;
using NewLife;
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

        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-Hans");
            Assert.Equal("不支持的 EDM 类型：{0}。", Localization.Translate("Unsupported EDM type: {0}."));

            Localization.Culture = CultureInfo.GetCultureInfo("en-US");
            Assert.Equal("The connection string does not specify a database name.", Localization.Translate("The connection string does not specify a database name."));
        }
        finally
        {
            Localization.Culture = saved;
        }
    }
}
