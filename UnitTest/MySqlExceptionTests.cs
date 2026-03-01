using System.ComponentModel;
using NewLife.MySql;

namespace UnitTest;

/// <summary>MySqlException 构造函数和属性测试</summary>
[Collection(TestCollections.InMemory)]
public class MySqlExceptionTests
{
    [Fact]
    [DisplayName("无参构造函数")]
    public void WhenNewDefaultThenPropertiesAreDefault()
    {
        var ex = new MySqlException();

        Assert.Equal(0, ex.ErrorCode);
        Assert.Null(ex.State);
        Assert.False(ex.IsFatal);
    }

    [Fact]
    [DisplayName("带消息构造函数")]
    public void WhenNewWithMessageThenMessageIsSet()
    {
        var ex = new MySqlException("test error");

        Assert.Equal("test error", ex.Message);
        Assert.Equal(0, ex.ErrorCode);
    }

    [Fact]
    [DisplayName("带错误码和消息构造函数")]
    public void WhenNewWithErrorAndMessageThenBothSet()
    {
        var ex = new MySqlException(1045, "Access denied");

        Assert.Equal(1045, ex.ErrorCode);
        Assert.Equal("Access denied", ex.Message);
    }

    [Fact]
    [DisplayName("带错误码、状态和消息构造函数")]
    public void WhenNewWithErrorStateMessageThenAllSet()
    {
        var ex = new MySqlException(1045, "28000", "Access denied");

        Assert.Equal(1045, ex.ErrorCode);
        Assert.Equal("28000", ex.State);
        Assert.Equal("Access denied", ex.Message);
    }

    [Fact]
    [DisplayName("IsFatal在错误码4031时返回true")]
    public void WhenErrorCode4031ThenIsFatalIsTrue()
    {
        var ex = new MySqlException(4031, "Connection killed");

        Assert.True(ex.IsFatal);
    }

    [Fact]
    [DisplayName("IsFatal在其他错误码时返回false")]
    public void WhenErrorCodeNot4031ThenIsFatalIsFalse()
    {
        var ex = new MySqlException(1045, "Access denied");

        Assert.False(ex.IsFatal);
    }

    [Fact]
    [DisplayName("MySqlException继承自Exception")]
    public void WhenNewThenIsException()
    {
        var ex = new MySqlException("test");

        Assert.IsAssignableFrom<Exception>(ex);
    }
}
