using System.ComponentModel;
using System.Globalization;
using System.Text;
using NewLife.Buffers;
using NewLife.MySql;
using NewLife.MySql.Common;

namespace UnitTest;

/// <summary>MySqlCommand.SerializeValue 区域性安全测试。验证日期时间格式化不随 CurrentCulture 变化</summary>
[Collection(TestCollections.InMemory)]
public class MySqlCommandSerializeTests
{
    private static readonly DateTime Time = new(2026, 8, 1, 10, 30, 45, 123);

    [Fact]
    [DisplayName("DateTime序列化不随CurrentCulture变化")]
    public void DateTime_Invariant()
    {
        var old = CultureInfo.CurrentCulture;
        try
        {
            // id-ID 时间分隔符为 '.'，若未用 InvariantCulture 会生成非法 SQL 字面量
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("id-ID");

            var sql = MySqlCommand.SerializeValue(Time);

            Assert.Equal("'2026-08-01 10:30:45.123'", sql);
        }
        finally
        {
            CultureInfo.CurrentCulture = old;
        }
    }

    [Fact]
    [DisplayName("DateTimeOffset序列化不随CurrentCulture变化")]
    public void DateTimeOffset_Invariant()
    {
        var old = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("id-ID");

            var sql = MySqlCommand.SerializeValue(new DateTimeOffset(Time));

            Assert.Equal("'2026-08-01 10:30:45.123'", sql);
        }
        finally
        {
            CultureInfo.CurrentCulture = old;
        }
    }

    [Fact]
    [DisplayName("整秒DateTime去掉小数部分")]
    public void DateTime_WholeSeconds()
    {
        var sql = MySqlCommand.SerializeValue(new DateTime(2026, 8, 1, 10, 30, 45));

        Assert.Equal("'2026-08-01 10:30:45'", sql);
    }

    [Fact]
    [DisplayName("空值序列化为NULL")]
    public void Null_SerializesAsNull()
    {
        Assert.Equal("NULL", MySqlCommand.SerializeValue(null));
        Assert.Equal("NULL", MySqlCommand.SerializeValue(DBNull.Value));
    }

    [Fact]
    [DisplayName("默认模式用反斜杠转义单引号")]
    public void EscapeString_Default_UsesBackslash()
    {
        Assert.Equal(@"a\'b", MySqlCommand.EscapeString("a'b"));
        Assert.Equal(@"a\\b", MySqlCommand.EscapeString(@"a\b"));
    }

    [Fact]
    [DisplayName("NO_BACKSLASH_ESCAPES模式改为双写单引号")]
    public void EscapeString_NoBackslashEscapes_DoublesQuote()
    {
        // 该模式下 反斜杠是普通字符，\' 不会闭合引号，继续用反斜杠转义等于把引号裸露出去
        Assert.Equal("a''b", MySqlCommand.EscapeString("a'b", true));
        Assert.Equal(@"a\b", MySqlCommand.EscapeString(@"a\b", true));
        // 反斜杠保持原样，只把每个单引号双写
        Assert.Equal(@"a\''b''c", MySqlCommand.EscapeString(@"a\'b'c", true));
    }

    [Fact]
    [DisplayName("字符串参数在NO_BACKSLASH_ESCAPES模式下不产生注入")]
    public void SerializeValue_String_NoBackslashEscapes_IsQuoteSafe()
    {
        Assert.Equal("'a''b'", MySqlCommand.SerializeValue("a'b", true));
        Assert.Equal("'a\\'b'", MySqlCommand.SerializeValue("a'b"));
    }

    [Fact]
    [DisplayName("Char与TimeSpan按字面量加引号而非裸拼接")]
    public void SerializeValue_CharAndTimeSpan_Quoted()
    {
        Assert.Equal("'a'", MySqlCommand.SerializeValue('a'));
        Assert.Equal("'01:00:00'", MySqlCommand.SerializeValue(TimeSpan.FromHours(1)));
    }

    [Fact]
    [DisplayName("TimeSpan超过24小时折算总小时数且保留微秒")]
    public void SerializeValue_TimeSpan_TimeLiteral()
    {
        // .NET "c" 格式会输出 1.01:00:00，MySQL TIME 不接受天分量
        Assert.Equal("'25:00:00'", MySqlCommand.SerializeValue(TimeSpan.FromHours(25)));
        Assert.Equal("'-25:30:00'", MySqlCommand.SerializeValue(TimeSpan.FromHours(-25.5)));
        Assert.Equal("'838:59:59'", MySqlCommand.SerializeValue(new TimeSpan(838, 59, 59)));
        // 微秒截断到 6 位，与二进制协议 WriteBinaryTime 一致
        Assert.Equal("'00:00:00.000001'", MySqlCommand.SerializeValue(TimeSpan.FromTicks(10)));
    }

    [Fact]
    [DisplayName("未知类型按字符串字面量加引号，不裸拼接")]
    public void SerializeValue_UnknownType_Quoted()
    {
        // 曾经直接 ToString 输出裸值，自定义类型的文本会破坏 SQL 结构
        Assert.Equal(@"'a\'b'", MySqlCommand.SerializeValue(new UnknownValue("a'b")));
        Assert.Equal("'a''b'", MySqlCommand.SerializeValue(new UnknownValue("a'b"), true));
    }

    [Fact]
    [DisplayName("小数序列化不随CurrentCulture变化")]
    public void Decimal_Invariant()
    {
        var old = CultureInfo.CurrentCulture;
        try
        {
            // de-DE 小数点为逗号，未用 InvariantCulture 会生成 "1,5" 这种非法字面量
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");

            Assert.Equal("1.5", MySqlCommand.SerializeValue(1.5m));
            Assert.Equal("1.5", MySqlCommand.SerializeValue(1.5d));
            Assert.Equal("1.5", MySqlCommand.SerializeValue(1.5f));
            Assert.Equal("42", MySqlCommand.SerializeValue((UInt16)42));
            Assert.Equal("-42", MySqlCommand.SerializeValue((SByte)(-42)));
        }
        finally
        {
            CultureInfo.CurrentCulture = old;
        }
    }

    [Fact]
    [DisplayName("参数替换跳过注释、反引号标识符与字符串")]
    public void SubstituteParameters_SkipsCommentsAndQuotedIdentifiers()
    {
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("@p", "x");

        var sql = "SELECT `@p`, '@p', @p /* @p */ -- @p";
        var result = MySqlCommand.SubstituteParameters(sql, parameters);

        Assert.Equal("SELECT `@p`, '@p', 'x' /* @p */ -- @p", result);
    }

    [Fact]
    [DisplayName("系统变量@@name不按参数替换")]
    public void SubstituteParameters_SkipsSystemVariables()
    {
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("@version", "hacked");

        var result = MySqlCommand.SubstituteParameters("SELECT @@version, @version", parameters);

        Assert.Equal("SELECT @@version, 'hacked'", result);
    }

    [Fact]
    [DisplayName("NO_BACKSLASH_ESCAPES下反斜杠不转义字符串结束引号")]
    public void SubstituteParameters_NoBackslashEscapes_EndsStringAtQuote()
    {
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("@p", "x");

        // 默认模式下 \' 被当作转义引号，字符串延续到结尾，@p 不替换
        Assert.Equal(@"SELECT 'a\' , @p", MySqlCommand.SubstituteParameters(@"SELECT 'a\' , @p", parameters));
        // NO_BACKSLASH_ESCAPES 下 \ 是普通字符，引号关闭字符串，后面的 @p 必须替换
        Assert.Equal(@"SELECT 'a\' , 'x'", MySqlCommand.SubstituteParameters(@"SELECT 'a\' , @p", parameters, true));
    }

    [Fact]
    [DisplayName("ANSI_QUOTES下双引号标识符不吞参数，反斜杠不转义")]
    public void SubstituteParameters_AnsiQuotes_EndsIdentifierAtQuote()
    {
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("@p", "x");

        const String sql = "SELECT \"a\\\" , @p";
        // 默认模式：\" 当作转义，字符串延续到结尾，@p 不替换
        Assert.Equal(sql, MySqlCommand.SubstituteParameters(sql, parameters));
        // ANSI_QUOTES：反斜杠是普通字符，标识符在引号处结束，后面的 @p 必须替换
        Assert.Equal("SELECT \"a\\\" , 'x'", MySqlCommand.SubstituteParameters(sql, parameters, ansiQuotes: true));
    }

    [Fact]
    [DisplayName("ANSI_QUOTES下预编译占位符不被标识符吞掉")]
    public void ConvertToPositionalParameters_AnsiQuotes()
    {
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("@p", "x");

        const String sql = "SELECT \"a\\\" , @p";
        // 默认模式整段被当作未闭合字符串，不做占位符替换
        Assert.Equal(sql, MySqlCommand.ConvertToPositionalParameters(sql, parameters));
        // ANSI_QUOTES 下标识符提前结束，@p 必须变成 ?
        Assert.Equal("SELECT \"a\\\" , ?", MySqlCommand.ConvertToPositionalParameters(sql, parameters, ansiQuotes: true));
    }

    [Fact]
    [DisplayName("TimeSpan超出MySQL TIME范围时给出明确异常")]
    public void SerializeValue_TimeSpan_OutOfRange()
    {
        // MySQL TIME 上限 838:59:59；TimeSpan.MinValue 取负还会溢出
        Assert.Throws<ArgumentOutOfRangeException>(() => MySqlCommand.SerializeValue(TimeSpan.FromHours(839)));
        Assert.Throws<ArgumentOutOfRangeException>(() => MySqlCommand.SerializeValue(TimeSpan.MinValue));

        var buffer = new Byte[256];
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var writer = new SpanWriter(buffer);
            MySqlFieldCodec.WriteBinaryValue(ref writer, TimeSpan.FromHours(839), Encoding.UTF8);
        });
    }

    [Fact]
    [DisplayName("ToString只展示参数名，不泄露参数值")]
    public void ToString_DoesNotLeakParameterValues()
    {
        using var cmd = new MySqlCommand(new MySqlConnection(), "SELECT * FROM t WHERE pwd=@pwd");
        cmd.Parameters.AddWithValue("@pwd", "s3cr3t-value");

        var text = cmd.ToString();

        Assert.Contains("@pwd", text);
        Assert.DoesNotContain("s3cr3t-value", text);
    }

    /// <summary>自定义类型，用于验证兜底分支按字符串加引号</summary>
    private sealed class UnknownValue
    {
        private readonly String _text;

        public UnknownValue(String text) => _text = text;

        public override String ToString() => _text;
    }
}
