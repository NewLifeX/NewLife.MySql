using System.ComponentModel;
using NewLife.MySql.EntityFrameworkCore;

namespace UnitTest.EntityFrameworkCore;

/// <summary>值生成策略和注解测试</summary>
public class MySqlValueGenerationTests
{
    [Fact]
    [DisplayName("MySqlValueGenerationStrategy.None应为0")]
    public void ValueGenerationStrategy_None_ShouldBeZero()
    {
        Assert.Equal(0, (Int32)MySqlValueGenerationStrategy.None);
    }

    [Fact]
    [DisplayName("MySqlValueGenerationStrategy.AutoIncrement应为1")]
    public void ValueGenerationStrategy_AutoIncrement_ShouldBeOne()
    {
        Assert.Equal(1, (Int32)MySqlValueGenerationStrategy.AutoIncrement);
    }

    [Fact]
    [DisplayName("注解名称前缀应为MySql:")]
    public void AnnotationNames_Prefix_ShouldBeMySql()
    {
        Assert.Equal("MySql:", MySqlAnnotationNames.Prefix);
    }

    [Fact]
    [DisplayName("ValueGenerationStrategy注解名称应正确")]
    public void AnnotationNames_ValueGenerationStrategy_ShouldBeCorrect()
    {
        Assert.Equal("MySql:ValueGenerationStrategy", MySqlAnnotationNames.ValueGenerationStrategy);
        Assert.StartsWith(MySqlAnnotationNames.Prefix, MySqlAnnotationNames.ValueGenerationStrategy);
    }

    [Fact]
    [DisplayName("CharSet注解名称应正确")]
    public void AnnotationNames_CharSet_ShouldBeCorrect()
    {
        Assert.Equal("MySql:CharSet", MySqlAnnotationNames.CharSet);
    }

    [Fact]
    [DisplayName("Collation注解名称应正确")]
    public void AnnotationNames_Collation_ShouldBeCorrect()
    {
        Assert.Equal("MySql:Collation", MySqlAnnotationNames.Collation);
    }

    [Fact]
    [DisplayName("Engine注解名称应正确")]
    public void AnnotationNames_Engine_ShouldBeCorrect()
    {
        Assert.Equal("MySql:Engine", MySqlAnnotationNames.Engine);
    }
}
