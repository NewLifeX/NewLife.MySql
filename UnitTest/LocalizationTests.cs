using System.Globalization;
using System.Reflection;
using NewLife;
using NewLife.Log;
using NewLife.MySql;
using NewLife.MySql.Binlog;

namespace UnitTest;

/// <summary>运行时多语言与带前缀日志测试</summary>
[Collection(TestCollections.Localization)]
public class LocalizationTests
{
    static LocalizationTests() => GC.KeepAlive(typeof(MySqlConnection));

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

    [Theory(DisplayName = "区域与中立文化回退到对应语言词表")]
    [InlineData("zh", "连接未打开。")]
    [InlineData("zh-CN", "连接未打开。")]
    [InlineData("zh-SG", "连接未打开。")]
    [InlineData("zh-TW", "連線未開啟。")]
    [InlineData("zh-HK", "連線未開啟。")]
    [InlineData("zh-MO", "連線未開啟。")]
    [InlineData("ja-JP", "接続が開かれていません。")]
    [InlineData("pt-PT", "A conexão não está aberta.")]
    [InlineData("es-419", "La conexión no está abierta.")]
    [InlineData("ru-BY", "Соединение не открыто.")]
    [InlineData("ar-EG", "الاتصال غير مفتوح.")]
    public void RegionalCultureFallsBackToLanguage(String culture, String expected)
    {
        var savedCulture = Localization.Culture;
        var savedUI = CultureInfo.CurrentUICulture;
        try
        {
            Localization.Culture = null;
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo(culture);

            Assert.Equal(expected, L.T("Connection is not open."));
        }
        finally
        {
            CultureInfo.CurrentUICulture = savedUI;
            Localization.Culture = savedCulture;
        }
    }

    [Fact(DisplayName = "全部内置语言均有译文")]
    public void AllLanguagesTranslate()
    {
        var saved = Localization.Culture;
        try
        {
            foreach (var name in new[] { "zh-Hans", "zh-Hant", "es", "de", "ja", "fr", "pt", "ru", "ko", "ar" })
            {
                Localization.Culture = CultureInfo.GetCultureInfo(name);
                Assert.True(L.T("Connection is not open.") != "Connection is not open.", $"{name} 缺少译文");
            }
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "未打开连接时抛出本地化消息")]
    public void UnopenedConnectionUsesLocalizedMessage()
    {
        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-Hans");

            using var conn = new MySqlConnection("Server=localhost;Database=test;Uid=root");
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            var ex = Assert.Throws<InvalidOperationException>(() => cmd.ExecuteReader());
            Assert.Equal("连接未打开。", ex.Message);
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "SSL 回退日志按目标语言翻译")]
    public void SslFallbackLogTranslates()
    {
        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-Hans");

            Assert.Equal(
                "[MySqlSsl] db=test server=localhost:3306 SslMode=Preferred 跳过 TLS，使用缓存的明文回退",
                L.T("[MySqlSsl] db={0} server={1}:{2} SslMode=Preferred skip TLS and use cached plain-text fallback", "test", "localhost", 3306));
            Assert.Equal(
                "[MySqlSsl] db=test server=localhost:3306 SslMode=Preferred 回退到明文：IOException：boom",
                L.T("[MySqlSsl] db={0} server={1}:{2} SslMode=Preferred fallback to plain text: {3}: {4}", "test", "localhost", 3306, "IOException", "boom"));
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "命令异常上下文按目标语言翻译")]
    public void CommandExceptionContextTranslates()
    {
        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-Hans");

            Assert.Equal("SQL：SELECT 1", L.T("SQL: {0}", "SELECT 1"));
            Assert.Equal("批次数：3", L.T("BatchCount: {0}", 3));
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "日志语言未指定时跟随目标语言")]
    public void LogCultureFollowsTargetCulture()
    {
        var savedLog = Localization.LogCulture;
        var saved = Localization.Culture;
        try
        {
            Localization.LogCulture = null;
            Localization.Culture = CultureInfo.GetCultureInfo("zh-CN");

            Assert.Equal("连接未打开。", Localization.TranslateForLog("Connection is not open."));
        }
        finally
        {
            Localization.Culture = saved;
            Localization.LogCulture = savedLog;
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

    [Fact(DisplayName = "程序集标题与描述可翻译")]
    public void AssemblyMetadataTranslates()
    {
        var assembly = typeof(MySqlConnection).Assembly;
        var title = assembly.GetCustomAttribute<AssemblyTitleAttribute>()?.Title;
        var description = assembly.GetCustomAttribute<AssemblyDescriptionAttribute>()?.Description;
        Assert.False(title.IsNullOrEmpty());
        Assert.False(description.IsNullOrEmpty());

        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-Hans");
            Assert.Equal("新生命MySQL驱动", Localization.Translate(title!));
            Assert.Contains("纯 C#", Localization.Translate(description!));

            Localization.Culture = CultureInfo.GetCultureInfo("en-US");
            Assert.Equal(title, Localization.Translate(title!));
            Assert.Equal(description, Localization.Translate(description!));
        }
        finally
        {
            Localization.Culture = saved;
        }
    }
}
