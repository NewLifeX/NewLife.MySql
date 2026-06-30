using System.ComponentModel;
using NewLife.MySql.Common;

namespace UnitTest;

/// <summary>连接健康判定纯逻辑测试。覆盖活跃、失效、到龄、空闲全部边界，无任何外部依赖</summary>
[Collection(TestCollections.InMemory)]
public class ConnectionHealthTests
{
    private static readonly DateTime _now = new(2026, 1, 1, 12, 0, 0);

    [Fact]
    [DisplayName("Evaluate_连接已失效_返回Discard")]
    public void Evaluate_Inactive_ReturnsDiscard()
    {
        var d = ConnectionHealth.Evaluate(false, _now, _now, _now, 600, 30);

        Assert.Equal(ConnectionDecision.Discard, d);
    }

    [Fact]
    [DisplayName("Evaluate_活跃且年轻且刚活跃_返回Reusable")]
    public void Evaluate_ActiveYoungFresh_ReturnsReusable()
    {
        var created = _now.AddSeconds(-10);
        var lastActive = _now.AddSeconds(-5);

        var d = ConnectionHealth.Evaluate(true, created, lastActive, _now, 600, 30);

        Assert.Equal(ConnectionDecision.Reusable, d);
    }

    [Fact]
    [DisplayName("Evaluate_超过最大存活期_返回Discard")]
    public void Evaluate_ExceedLifetime_ReturnsDiscard()
    {
        // 创建于 601 秒前，超过 600s 存活期
        var created = _now.AddSeconds(-601);

        var d = ConnectionHealth.Evaluate(true, created, _now, _now, 600, 30);

        Assert.Equal(ConnectionDecision.Discard, d);
    }

    [Fact]
    [DisplayName("Evaluate_存活期边界恰好相等_返回Discard")]
    public void Evaluate_LifetimeBoundaryEqual_ReturnsDiscard()
    {
        // 恰好 600s，按 <= 判定应回收
        var created = _now.AddSeconds(-600);

        var d = ConnectionHealth.Evaluate(true, created, _now, _now, 600, 0);

        Assert.Equal(ConnectionDecision.Discard, d);
    }

    [Fact]
    [DisplayName("Evaluate_存活期禁用_永不因年龄丢弃")]
    public void Evaluate_LifetimeDisabled_NeverDiscardByAge()
    {
        var created = _now.AddSeconds(-100000);

        var d = ConnectionHealth.Evaluate(true, created, _now, _now, 0, 0);

        Assert.Equal(ConnectionDecision.Reusable, d);
    }

    [Fact]
    [DisplayName("Evaluate_创建时间未知_跳过存活期判定")]
    public void Evaluate_CreatedTimeUnknown_SkipsLifetime()
    {
        var d = ConnectionHealth.Evaluate(true, default, _now.AddSeconds(-5), _now, 600, 30);

        Assert.Equal(ConnectionDecision.Reusable, d);
    }

    [Fact]
    [DisplayName("Evaluate_空闲超过阈值_返回NeedPing")]
    public void Evaluate_IdleBeyondThreshold_ReturnsNeedPing()
    {
        var created = _now.AddSeconds(-100);
        var lastActive = _now.AddSeconds(-31);

        var d = ConnectionHealth.Evaluate(true, created, lastActive, _now, 600, 30);

        Assert.Equal(ConnectionDecision.NeedPing, d);
    }

    [Fact]
    [DisplayName("Evaluate_空闲边界恰好相等_返回NeedPing")]
    public void Evaluate_IdleBoundaryEqual_ReturnsNeedPing()
    {
        var lastActive = _now.AddSeconds(-30);

        var d = ConnectionHealth.Evaluate(true, _now.AddSeconds(-100), lastActive, _now, 600, 30);

        Assert.Equal(ConnectionDecision.NeedPing, d);
    }

    [Fact]
    [DisplayName("Evaluate_空闲PING禁用_永不NeedPing")]
    public void Evaluate_IdlePingDisabled_NeverNeedPing()
    {
        var lastActive = _now.AddSeconds(-100000);

        var d = ConnectionHealth.Evaluate(true, _now.AddSeconds(-200), lastActive, _now, 600, 0);

        Assert.Equal(ConnectionDecision.Reusable, d);
    }

    [Fact]
    [DisplayName("Evaluate_从未活跃_返回NeedPing")]
    public void Evaluate_NeverActive_ReturnsNeedPing()
    {
        var d = ConnectionHealth.Evaluate(true, _now.AddSeconds(-100), default, _now, 600, 30);

        Assert.Equal(ConnectionDecision.NeedPing, d);
    }

    [Fact]
    [DisplayName("Evaluate_同时到龄和空闲_存活期优先返回Discard")]
    public void Evaluate_BothExpiredAndIdle_LifetimeWins()
    {
        var created = _now.AddSeconds(-700);
        var lastActive = _now.AddSeconds(-100);

        var d = ConnectionHealth.Evaluate(true, created, lastActive, _now, 600, 30);

        Assert.Equal(ConnectionDecision.Discard, d);
    }

    [Theory]
    [DisplayName("Evaluate_失效标志优先于一切配置")]
    [InlineData(0, 0)]
    [InlineData(600, 30)]
    public void Evaluate_InactiveOverridesAll(Int32 lifetime, Int32 idlePing)
    {
        var d = ConnectionHealth.Evaluate(false, _now.AddSeconds(-1), _now.AddSeconds(-1), _now, lifetime, idlePing);

        Assert.Equal(ConnectionDecision.Discard, d);
    }
}
