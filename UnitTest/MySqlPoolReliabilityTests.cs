using System.Collections.Concurrent;
using System.ComponentModel;
using System.Diagnostics;
using System.Net.Sockets;
using NewLife;
using NewLife.MySql;
using NewLife.MySql.Common;

namespace UnitTest;

/// <summary>连接池可靠性测试。基于回环 socket 验证半开探测、EOF/取消标记失效、busy 记账无泄漏，无需真实 MySQL</summary>
[Collection(TestCollections.InMemory)]
public class MySqlPoolReliabilityTests
{
    [Fact]
    [DisplayName("IsSocketAlive_连接正常_返回Alive")]
    public void IsSocketAlive_Healthy_ReturnsAlive()
    {
        var (client, server) = ConnectionTestKit.CreatePair();
        using var _ = server;
        using var sql = ConnectionTestKit.WrapClient(client);

        Assert.Equal(SocketHealth.Alive, sql.IsSocketAlive());
    }

    [Fact]
    [DisplayName("IsSocketAlive_对端关闭_返回Closed")]
    public void IsSocketAlive_PeerClosed_ReturnsClosed()
    {
        var (client, server) = ConnectionTestKit.CreatePair();
        using var sql = ConnectionTestKit.WrapClient(client);

        // 服务端关闭，模拟服务器 wait_timeout 杀连接或网络 FIN
        server.Close();

        var health = ConnectionTestKit.WaitHealth(sql, SocketHealth.Closed);

        Assert.Equal(SocketHealth.Closed, health);
    }

    [Fact]
    [DisplayName("IsSocketAlive_存在残留数据_返回HasResidualData")]
    public void IsSocketAlive_ResidualData_ReturnsHasResidualData()
    {
        var (client, server) = ConnectionTestKit.CreatePair();
        using var _ = server;
        using var sql = ConnectionTestKit.WrapClient(client);

        // 服务端推送意料之外的数据，模拟上一次响应未读尽的协议错位脏连接
        var ns = server.GetStream();
        ns.Write([1, 2, 3, 4], 0, 4);
        ns.Flush();

        var health = ConnectionTestKit.WaitHealth(sql, SocketHealth.HasResidualData);

        Assert.Equal(SocketHealth.HasResidualData, health);
    }

    [Fact]
    [DisplayName("Reset_连接正常_返回true")]
    public void Reset_Healthy_ReturnsTrue()
    {
        var (client, server) = ConnectionTestKit.CreatePair();
        using var _ = server;
        using var sql = ConnectionTestKit.WrapClient(client);

        Assert.True(sql.Reset());
        Assert.True(sql.Active);
    }

    [Fact]
    [DisplayName("Reset_对端关闭_返回false并标记失效")]
    public void Reset_PeerClosed_ReturnsFalseAndInactive()
    {
        var (client, server) = ConnectionTestKit.CreatePair();
        using var sql = ConnectionTestKit.WrapClient(client);

        server.Close();
        ConnectionTestKit.WaitHealth(sql, SocketHealth.Closed);

        Assert.False(sql.Reset());
        Assert.False(sql.Active);
    }

    [Fact]
    [DisplayName("ReadPacketAsync_读取中对端关闭_标记失效并抛IOException")]
    public async Task ReadPacketAsync_PeerClosedDuringRead_MarksInactive()
    {
        var (client, server) = ConnectionTestKit.CreatePair();
        using var sql = ConnectionTestKit.WrapClient(client);

        // 服务端不发送任何数据直接关闭，客户端读包将遇到 EOF
        server.Close();

        await Assert.ThrowsAnyAsync<IOException>(() => sql.ReadPacketAsync());
        Assert.False(sql.Active);
    }

    [Fact]
    [DisplayName("ReadPacketAsync_调用方主动取消_标记失效")]
    public async Task ReadPacketAsync_Cancelled_MarksInactive()
    {
        var (client, server) = ConnectionTestKit.CreatePair();
        using var _ = server;
        using var sql = ConnectionTestKit.WrapClient(client);

        var cancelled = new CancellationToken(true);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sql.ReadPacketAsync(cancelled));
        Assert.False(sql.Active);
    }

    [Fact]
    [DisplayName("连接池_借出失效空闲连接不泄漏busy槽")]
    public async Task Pool_DiscardingBadClients_DoesNotLeakBusy()
    {
        var servers = new List<TcpClient>();
        var pool = new ReliabilityTestPool(servers);
        try
        {
            // 取出并归还 3 个健康连接，使其进入空闲集合
            var clients = new List<SqlClient>();
            for (var i = 0; i < 3; i++)
                clients.Add(pool.Get());
            foreach (var c in clients)
                pool.Return(c);

            Assert.Equal(0, pool.BusyCount);
            Assert.Equal(3, pool.FreeCount);

            // 模拟服务端杀掉所有空闲连接，并确保客户端已感知关闭
            foreach (var s in servers) s.Close();
            foreach (var c in clients) ConnectionTestKit.WaitHealth(c, SocketHealth.Closed);

            // 再次借出：基类应剔除所有失效空闲连接并新建一个；busy 记账必须正确，无泄漏
            // 旧实现裸 TryDispose 不从 _busy 移除，这里 BusyCount 会累加到 4
            var got = await pool.GetAsync();

            Assert.NotNull(got);
            Assert.Equal(1, pool.BusyCount);
            Assert.Equal(0, pool.FreeCount);
        }
        finally
        {
            pool.Clear();
            foreach (var s in servers) s.TryDispose();
        }
    }

    [Fact]
    [DisplayName("连接池_满时GetAsync信号量阻塞等待槽位释放")]
    public async Task Pool_Full_GetAsyncBlocksUntilSlotReleased()
    {
        var servers = new List<TcpClient>();
        var pool = new ReliabilityTestPool(servers) { Max = 3 };
        try
        {
            // 借出全部 3 个连接使池满
            var clients = new List<SqlClient>();
            for (var i = 0; i < 3; i++)
                clients.Add(pool.Get());
            Assert.Equal(3, pool.BusyCount);

            // 延迟归还一个连接释放信号量槽位
            _ = Task.Run(async () =>
            {
                await Task.Delay(300);
                pool.Return(clients[0]);
            });

            // base.GetAsync 阻塞在 SemaphoreSlim 上，槽位释放后唤醒
            var sw = Stopwatch.StartNew();
            var got = await pool.GetAsync();
            sw.Stop();

            Assert.NotNull(got);
            Assert.True(sw.ElapsedMilliseconds >= 200,
                $"信号量等待时间应 >=200ms，实际 {sw.ElapsedMilliseconds}ms");
            // 归还的连接应被复用（Welcome 非 null 且 socket 健康）
            Assert.NotNull(got.Welcome);
        }
        finally
        {
            pool.Clear();
            foreach (var s in servers) s.TryDispose();
        }
    }

    [Fact]
    [DisplayName("连接池_失效空闲连接全部丢弃后可等额重建不缩水")]
    public async Task Pool_DeadFreeItems_CanBeReplacedWithoutShrinking()
    {
        var servers = new List<TcpClient>();
        // Max=3，池全部空闲连接失效后应能全部重建
        var pool = new ReliabilityTestPool(servers) { Max = 3 };
        try
        {
            // 借出并归还 3 个连接，全部进入空闲池
            var clients = new List<SqlClient>();
            for (var i = 0; i < 3; i++)
                clients.Add(pool.Get());
            foreach (var c in clients)
                pool.Return(c);
            Assert.Equal(0, pool.BusyCount);
            Assert.Equal(3, pool.FreeCount);

            // 服务端杀死所有空闲连接
            foreach (var s in servers) s.Close();
            foreach (var c in clients) ConnectionTestKit.WaitHealth(c, SocketHealth.Closed);

            // 连续借出 3 次：每轮先遇到失效空闲连接→丢弃→借用下一空闲→再失效→再丢弃→最终无空闲→需重建。
            // 修复前：失效项被丢弃但 BusyCount 未减、且 Max 检查阻止重建 → 第二个连接开始抛"申请失败"。
            // 修复后：_freeDiscarded 记录丢弃额度，允许等额超 Max 创建替换连接。
            var gotAll = new List<SqlClient>();
            for (var i = 0; i < 3; i++)
            {
                var got = await pool.GetAsync();
                Assert.NotNull(got);
                gotAll.Add(got);
            }

            Assert.Equal(3, pool.BusyCount);
            Assert.Equal(0, pool.FreeCount);
        }
        finally
        {
            pool.Clear();
            foreach (var s in servers) s.TryDispose();
        }
    }

    [Fact]
    [DisplayName("连接池_满时并发借出归还BusyCount不异常增长")]
    public async Task Pool_ConcurrentAcquireReturn_BusyCountStaysInRange()
    {
        var servers = new List<TcpClient>();
        var pool = new ReliabilityTestPool(servers) { Max = 5 };
        try
        {
            var tasks = new List<Task>();
            var errors = new ConcurrentBag<Exception>();
            var maxObservedBusy = 0;

            // 多线程并发借出+归还，模拟高并发场景
            for (var t = 0; t < 10; t++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    for (var i = 0; i < 20; i++)
                    {
                        try
                        {
                            var client = await pool.GetAsync();
                            InterlockedMax(ref maxObservedBusy, pool.BusyCount);

                            // 短暂持有后归还
                            await Task.Delay(10);
                            pool.Return(client);
                        }
                        catch (Exception ex)
                        {
                            errors.Add(ex);
                        }
                    }
                }));
            }

            await Task.WhenAll(tasks);

            Assert.Empty(errors);
            // 信号量保证总连接数恒 ≤ Max，BusyCount 不应持续超过 Max（短暂竞态允许 ≤Max+1）
            Assert.True(maxObservedBusy <= pool.Max + 1,
                $"最大观察 BusyCount={maxObservedBusy} 不应超过 Max={pool.Max}+1");
            Assert.True(pool.BusyCount <= pool.Max,
                $"最终 BusyCount={pool.BusyCount} 应回归 Max={pool.Max} 以内");
        }
        finally
        {
            pool.Clear();
            foreach (var s in servers) s.TryDispose();
        }
    }

    private static void InterlockedMax(ref Int32 target, Int32 newValue)
    {
        while (true)
        {
            var current = Volatile.Read(ref target);
            if (newValue <= current) return;
            if (Interlocked.CompareExchange(ref target, newValue, current) == current) return;
        }
    }

    /// <summary>测试连接池：OnCreate 产出回环 socket 支撑的连接，便于模拟服务端杀连接</summary>
    private sealed class ReliabilityTestPool : MySqlPool
    {
        private readonly List<TcpClient> _servers;

        public ReliabilityTestPool(List<TcpClient> servers)
        {
            _servers = servers;
            Setting = new MySqlConnectionStringBuilder("server=127.0.0.1;port=3306;uid=root;pwd=root;database=test");
        }

        protected override SqlClient OnCreate()
        {
            var (client, server) = ConnectionTestKit.CreatePair();
            _servers.Add(server);
            return ConnectionTestKit.WrapClient(client);
        }
    }
}
