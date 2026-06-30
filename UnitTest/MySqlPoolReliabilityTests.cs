using System.ComponentModel;
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
