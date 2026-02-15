using System.Data.Entity.Infrastructure;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySql 执行策略。提供瞬时故障重试支持</summary>
public class MySqlExecutionStrategy : DbExecutionStrategy
{
    /// <summary>最大重试次数</summary>
    private const Int32 DefaultMaxRetryCount = 3;

    /// <summary>默认最大重试延迟</summary>
    private static readonly TimeSpan DefaultMaxDelay = TimeSpan.FromSeconds(30);

    /// <summary>实例化执行策略，使用默认重试配置</summary>
    public MySqlExecutionStrategy() : base(DefaultMaxRetryCount, DefaultMaxDelay) { }

    /// <summary>实例化执行策略</summary>
    /// <param name="maxRetryCount">最大重试次数</param>
    /// <param name="maxDelay">最大重试延迟</param>
    public MySqlExecutionStrategy(Int32 maxRetryCount, TimeSpan maxDelay) : base(maxRetryCount, maxDelay) { }

    /// <summary>判断异常是否为瞬时故障</summary>
    /// <param name="exception">异常</param>
    /// <returns></returns>
    protected override Boolean ShouldRetryOn(Exception exception)
    {
        // MySQL 瞬时错误码
        if (exception is MySqlException mysqlEx)
        {
            return mysqlEx.ErrorCode switch
            {
                // 连接相关
                1040 => true,   // Too many connections
                1042 => true,   // Unable to connect to any of the specified MySQL hosts
                1043 => true,   // Bad handshake
                1152 => true,   // Aborted connection
                1153 => true,   // Got a packet bigger than 'max_allowed_packet' bytes
                1154 => true,   // Got a read error from the connection pipe
                1155 => true,   // Got an error from fcntl()
                1156 => true,   // Got packets out of order
                1157 => true,   // Couldn't uncompress communication packet
                1158 => true,   // Got an error reading communication packets
                1159 => true,   // Got timeout reading communication packets
                1160 => true,   // Got an error writing communication packets
                1161 => true,   // Got timeout writing communication packets
                // 锁相关
                1205 => true,   // Lock wait timeout exceeded
                1213 => true,   // Deadlock found when trying to get lock
                _ => false,
            };
        }

        // 检查内部异常
        return exception.InnerException != null && ShouldRetryOn(exception.InnerException);
    }
}
