namespace NewLife.MySql.Common;

/// <summary>底层 socket 探活结果</summary>
public enum SocketHealth
{
    /// <summary>连接正常。TCP 层无异常，且无意料之外的残留数据</summary>
    Alive = 0,

    /// <summary>连接已被对端关闭或重置</summary>
    Closed = 1,

    /// <summary>连接上存在意料之外的残留数据。多为上一次响应未读尽导致的协议错位脏连接</summary>
    HasResidualData = 2,
}

/// <summary>连接复用决策</summary>
public enum ConnectionDecision
{
    /// <summary>可直接复用</summary>
    Reusable = 0,

    /// <summary>空闲较久，需先 PING 验活再复用</summary>
    NeedPing = 1,

    /// <summary>不可复用，应丢弃并重建</summary>
    Discard = 2,
}

/// <summary>连接健康判定。纯逻辑，不触碰网络，便于穷举单元测试</summary>
/// <remarks>
/// 连接池借出/归还时据此决定连接去留。把"是否可复用/是否该 PING/是否该丢弃"的判定
/// 从 socket IO 中剥离为纯函数，既消除繁忙应用下 PING 被时间窗口门控而几乎不执行的问题，
/// 又便于覆盖活跃、失效、到龄、空闲等全部边界。
/// </remarks>
public static class ConnectionHealth
{
    /// <summary>评估池中连接是否可复用</summary>
    /// <param name="active">连接活动标志</param>
    /// <param name="createdTime">连接创建时间。default 表示未知，跳过存活期判定</param>
    /// <param name="lastActive">最后活跃时间。default 表示从未活跃，按需 PING 处理</param>
    /// <param name="now">当前时间</param>
    /// <param name="lifetimeSeconds">连接最大存活期（秒）。0 表示不限制</param>
    /// <param name="idlePingSeconds">空闲多久后需 PING 验活（秒）。0 表示从不主动 PING</param>
    /// <returns>复用决策</returns>
    public static ConnectionDecision Evaluate(Boolean active, DateTime createdTime, DateTime lastActive, DateTime now, Int32 lifetimeSeconds, Int32 idlePingSeconds)
    {
        // 已失效连接直接丢弃
        if (!active) return ConnectionDecision.Discard;

        // 超过最大存活期则主动回收，解决长寿连接累积半开、或被服务端 wait_timeout 静默杀掉的问题
        if (lifetimeSeconds > 0 && createdTime != default && createdTime.AddSeconds(lifetimeSeconds) <= now)
            return ConnectionDecision.Discard;

        // 空闲超过阈值需要 PING 验活，兜底无 FIN 的黑洞型断连（NAT/防火墙静默丢弃连接状态）
        if (idlePingSeconds > 0 && (lastActive == default || lastActive.AddSeconds(idlePingSeconds) <= now))
            return ConnectionDecision.NeedPing;

        return ConnectionDecision.Reusable;
    }
}
