using System.Globalization;
using NewLife;
using NewLife.Log;
using NewLife.MySql;
using NewLife.MySql.Binlog;

namespace UnitTest;

/// <summary>运行时多语言与带前缀日志测试</summary>
[Collection(TestCollections.InMemory)]
public class LocalizationTests
{
    [Fact(DisplayName = "运行时消息按目标语言翻译")]
    public void TranslateRuntimeMessages()
    {
        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-Hans");
            Assert.Equal("连接未打开。", L.T("Connection is not open."));
            Assert.Equal("未找到参数“id”。", L.T("Parameter '{0}' was not found.", "id"));

            Localization.Culture = CultureInfo.GetCultureInfo("ja");
            Assert.Equal("接続が開かれていません。", L.T("Connection is not open."));
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "未知语言回退英文原文")]
    public void UnknownCultureFallsBack()
    {
        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("sw-KE");
            Assert.Equal("Connection is not open.", L.T("Connection is not open."));
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "带前缀日志先翻译再拼接前缀")]
    public void PrefixedLogTranslates()
    {
        var messages = new List<String>();
        var log = new ActionLog((format, args) => messages.Add(format));
        var client = new BinlogClient { Log = log };

        var saved = Localization.LogCulture;
        try
        {
            Localization.LogCulture = CultureInfo.GetCultureInfo("zh-Hans");
            client.WriteLog("Binlog reconnected: {0}", "mysql-bin.000001");
        }
        finally
        {
            Localization.LogCulture = saved;
        }

        Assert.Equal("[Binlog] Binlog 重连成功：mysql-bin.000001", Assert.Single(messages));
    }

    [Fact(DisplayName = "批量复制日志使用默认前缀")]
    public void BulkCopyLogUsesDefaultPrefix()
    {
        var messages = new List<String>();
        var log = new ActionLog((format, args) => messages.Add(format));
        var bulkCopy = new MySqlBulkCopy { Log = log };

        var saved = Localization.LogCulture;
        try
        {
            Localization.LogCulture = CultureInfo.GetCultureInfo("zh-Hans");
            bulkCopy.WriteLog("Rows copied: {0}", 100);
        }
        finally
        {
            Localization.LogCulture = saved;
        }

        Assert.Equal("[BulkCopy] Rows copied: 100", Assert.Single(messages));
    }
}
