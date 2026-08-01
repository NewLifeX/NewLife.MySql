using System.ComponentModel;
using System.Globalization;
using NewLife.MySql;

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
}
