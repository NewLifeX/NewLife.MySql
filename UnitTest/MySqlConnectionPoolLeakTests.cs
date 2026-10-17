using System.ComponentModel;
using System.Data;
using System.Net;
using System.Net.Sockets;
using NewLife.MySql;

namespace UnitTest;

/// <summary>连接池槽位泄漏回归测试。验证 MySqlConnection.OpenAsync 建连失败后，借出的槽位归还连接池，
/// 不会永久占用 BusyCount / 信号量容量。
/// 背景：生产事故"MySqlPool 申请失败，已有 100 达到或超过最大值 100"——数据库短暂不可达期间，
/// 每次建连失败若只 TryDispose 客户端而不 pool.Return，则永久泄漏一个池槽位，直至打满 Max。</summary>
[Collection(TestCollections.InMemory)]
public class MySqlConnectionPoolLeakTests
{
    [Fact]
    [DisplayName("OpenAsync_建连失败_槽位归还池不泄漏BusyCount")]
    public async Task OpenAsync_ConnectFail_SlotReturned_NoBusyLeak()
    {
        // 握手即断开：部分环境（如本机防火墙）对“连接被拒绝”要等 2 秒以上才返回 RST，
        // 用立即关闭连接的本地监听器可让每次建连在毫秒级失败，测试仍覆盖同一条失败归还路径
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        using var stop = new CancellationTokenSource();
        var acceptTask = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                }
                catch
                {
                    break;
                }
                client.Close();
            }
        });

        var factory = new MySqlClientFactory { PoolManager = new MySqlPoolManager() };
        var setting = new MySqlConnectionStringBuilder($"server=127.0.0.1;port={port};uid=root;pwd=root;database=test;MaxPoolSize=3");
        var pool = factory.PoolManager.GetPool(setting);

        // 缩短池满等待超时，若槽位泄漏可快速暴露（默认 15s 太久）
        pool.WaitTimeout = TimeSpan.FromMilliseconds(500);

        using var conn = new MySqlConnection(setting.ConnectionString) { Factory = factory };
        try
        {
            // 连续多次建连失败（服务端接受后立即断开 → 握手失败）。
            // 修复前：每次失败裸 TryDispose 泄漏 1 槽，BusyCount 只增不减；
            // 修复后：失败路径 pool.Return 归还槽位，BusyCount 恒为 0，池容量不缩水。
            for (var i = 0; i < 6; i++)
            {
                await Assert.ThrowsAnyAsync<Exception>(() => conn.OpenAsync());

                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.True(pool.BusyCount == 0,
                    $"第 {i + 1} 次建连失败后 BusyCount={pool.BusyCount}，槽位未归还，存在泄漏");
            }

            // 泄漏不发生时，池仍可按 Max 额度正常借出（信号量容量未被蚕食）
            var clients = new List<SqlClient>();
            for (var i = 0; i < pool.Max; i++)
            {
                clients.Add(pool.Get());
            }
            Assert.Equal(pool.Max, pool.BusyCount);

            foreach (var c in clients) pool.Return(c);
            Assert.Equal(0, pool.BusyCount);
        }
        finally
        {
            pool.Clear();

            stop.Cancel();
            listener.Stop();
            try
            {
                await acceptTask.ConfigureAwait(false);
            }
            catch
            {
                // 监听器停止时 Accept 抛出异常，属预期
            }
        }
    }
}
