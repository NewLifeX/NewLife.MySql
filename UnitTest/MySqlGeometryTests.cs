using System.ComponentModel;
using NewLife.MySql.Common;

namespace UnitTest;

/// <summary>MySqlGeometry 几何类型单元测试。验证 WKB 封装、隐式转换、十六进制输出与相等性</summary>
[Collection(TestCollections.InMemory)]
public class MySqlGeometryTests
{
    [Fact]
    [DisplayName("空字节创建几何对象返回空数组")]
    public void WhenCreateWithNullThenEmptyValue()
    {
        var geo = new MySqlGeometry(null!);

        Assert.NotNull(geo.Value);
        Assert.Empty(geo.Value);
    }

    [Fact]
    [DisplayName("WKB字节与几何对象双向隐式转换")]
    public void WhenImplicitConvertThenValuePreserved()
    {
        Byte[] wkb = [0x01, 0x02, 0x03];

        MySqlGeometry geo = wkb;
        Assert.Same(wkb, geo.Value);

        Byte[] back = geo;
        Assert.Same(wkb, back);
    }

    [Fact]
    [DisplayName("几何对象输出大写十六进制字符串")]
    public void WhenToStringThenUppercaseHex()
    {
        var geo = new MySqlGeometry([0x01, 0xAB, 0xFF]);

        Assert.Equal("01ABFF", geo.ToString());
    }

    [Fact]
    [DisplayName("相同WKB的几何对象相等")]
    public void WhenSameWkbThenEqualsAndSameHashCode()
    {
        var geo1 = new MySqlGeometry([0x01, 0x02]);
        var geo2 = new MySqlGeometry([0x01, 0x02]);

        Assert.True(geo1.Equals(geo2));
        Assert.True(geo1.Equals(new Byte[] { 0x01, 0x02 }));
        Assert.Equal(geo1.GetHashCode(), geo2.GetHashCode());
    }

    [Fact]
    [DisplayName("不同WKB的几何对象不相等")]
    public void WhenDifferentWkbThenNotEquals()
    {
        var geo = new MySqlGeometry([0x01, 0x02]);

        Assert.False(geo.Equals(new MySqlGeometry([0x02, 0x01])));
        Assert.False(geo.Equals(new Byte[] { 0x01 }));
        Assert.False(geo.Equals("01"));
        Assert.False(geo.Equals(null));
    }
}
