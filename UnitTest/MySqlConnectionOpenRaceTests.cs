using System.Collections.Concurrent;
using System.ComponentModel;
using System.Data;
using System.Net;
using System.Net.Sockets;
using NewLife.MySql;

namespace UnitTest;

/// <summary>连接打开/关闭竞态回归测试。慢网络下 OpenAsync 与 OpenAsync/Close 并发时，
/// 旧实现会各自从池借出客户端，后完成者覆盖 Client 字段，先借出的池槽位永久无人归还，
/// 最终 BusyCount 打满 Max，恢复网络后仍持续 PoolFull。</summary>
[Collection(TestCollections.ReadOnly)]
public class MySqlConnectionOpenRaceTests
{
    [Fact]
    [DisplayName("并发OpenAsync_只占一个池槽位")]
    public async Task ConcurrentOpenAsync_OnlyOnePoolSlotHeld()
    {
        var upstream = new MySqlConnectionStringBuilder(DALTests.GetConnStr());
        using var proxy = new DelayedTcpProxy(upstream.Server!, upstream.Port > 0 ? upstream.Port : 3306, 100);

        var setting = new MySqlConnectionStringBuilder(DALTests.GetConnStr())
        {
            Server = "127.0.0.1",
            Port = proxy.Port,
            MaxPoolSize = 5,
            ConnectionTimeout = 10,
        };
        var factory = new MySqlClientFactory { PoolManager = new MySqlPoolManager() };
        var pool = factory.PoolManager.GetPool(setting);
        pool.WaitTimeout = TimeSpan.FromSeconds(5);

        try
        {
            using var conn = new MySqlConnection(setting.ConnectionString) { Factory = factory };

            // 首包被代理延迟，第一次 OpenAsync 必然停留在握手阶段；此时第二次 OpenAsync 进入同一连接对象
            var first = conn.OpenAsync();
            await Task.Delay(50);

            var second = conn.OpenAsync();

            await Task.WhenAll(first, second);

            Assert.Equal(ConnectionState.Open, conn.State);
            Assert.Equal(1, pool.BusyCount);
        }
        finally
        {
            pool.Clear();
        }
    }

    [Fact]
    [DisplayName("打开过程中Close_取消打开并释放池槽位")]
    public async Task CloseDuringOpenAsync_ReleasesPoolSlot()
    {
        var upstream = new MySqlConnectionStringBuilder(DALTests.GetConnStr());
        using var proxy = new DelayedTcpProxy(upstream.Server!, upstream.Port > 0 ? upstream.Port : 3306, 100);

        var setting = new MySqlConnectionStringBuilder(DALTests.GetConnStr())
        {
            Server = "127.0.0.1",
            Port = proxy.Port,
            MaxPoolSize = 5,
            ConnectionTimeout = 10,
        };
        var factory = new MySqlClientFactory { PoolManager = new MySqlPoolManager() };
        var pool = factory.PoolManager.GetPool(setting);
        pool.WaitTimeout = TimeSpan.FromSeconds(5);

        try
        {
            var conn = new MySqlConnection(setting.ConnectionString) { Factory = factory };
            try
            {
                var openTask = conn.OpenAsync();
                await Task.Delay(150);

                // 慢网络下应用可能因超时主动关闭连接；旧实现会在 Close 之后仍完成握手并把池客户端挂在连接上
                conn.Close();

                try
                {
                    await openTask;
                }
                catch (OperationCanceledException)
                {
                    // Close 取消进行中的打开操作，属预期
                }

                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Equal(0, pool.BusyCount);
            }
            finally
            {
                conn.Dispose();
            }
        }
        finally
        {
            pool.Clear();
        }
    }
}

/// <summary>延迟转发的 TCP 代理。用于在真实 MySQL 前注入慢网络，放慢握手以便稳定复现打开/关闭竞态</summary>
sealed class DelayedTcpProxy : IDisposable
{
    private readonly TcpListener _listener;
    private readonly String _upstreamHost;
    private readonly Int32 _upstreamPort;
    private readonly Int32 _delayMs;
    private readonly CancellationTokenSource _cts = new();
    private readonly ConcurrentDictionary<Int32, (TcpClient Client, TcpClient Upstream)> _connections = new();
    private readonly Task _acceptTask;
    private Int32 _nextId;

    /// <summary>监听端口</summary>
    public Int32 Port { get; }

    /// <summary>实例化代理</summary>
    /// <param name="upstreamHost">上游 MySQL 地址</param>
    /// <param name="upstreamPort">上游 MySQL 端口</param>
    /// <param name="delayMs">每个数据块转发前的延迟毫秒数</param>
    public DelayedTcpProxy(String upstreamHost, Int32 upstreamPort, Int32 delayMs)
    {
        _upstreamHost = upstreamHost;
        _upstreamPort = upstreamPort;
        _delayMs = delayMs;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptTask = AcceptLoopAsync();
    }

    private async Task AcceptLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false);
            }
            catch
            {
                break;
            }

            _ = HandleAsync(client);
        }
    }

    private async Task HandleAsync(TcpClient client)
    {
        var upstream = new TcpClient();
        try
        {
            await upstream.ConnectAsync(_upstreamHost, _upstreamPort, _cts.Token).ConfigureAwait(false);
        }
        catch
        {
            client.Close();
            upstream.Dispose();
            return;
        }

        var id = Interlocked.Increment(ref _nextId);
        _connections[id] = (client, upstream);

        try
        {
            var t1 = PumpAsync(client.GetStream(), upstream.GetStream());
            var t2 = PumpAsync(upstream.GetStream(), client.GetStream());
            await Task.WhenAny(t1, t2).ConfigureAwait(false);
        }
        catch
        {
        }
        finally
        {
            _connections.TryRemove(id, out _);
            client.Close();
            upstream.Close();
        }
    }

    private async Task PumpAsync(NetworkStream from, NetworkStream to)
    {
        var buf = new Byte[16384];
        while (!_cts.IsCancellationRequested)
        {
            Int32 n;
            try
            {
                n = await from.ReadAsync(buf, _cts.Token).ConfigureAwait(false);
            }
            catch
            {
                return;
            }
            if (n <= 0) return;

            // 逐块延迟，模拟慢网络；握手与认证需要多个往返，从而稳定放大打开窗口
            try
            {
                await Task.Delay(_delayMs, _cts.Token).ConfigureAwait(false);
                await to.WriteAsync(buf.AsMemory(0, n), _cts.Token).ConfigureAwait(false);
            }
            catch
            {
                return;
            }
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _listener.Stop();

        foreach (var kv in _connections)
        {
            if (_connections.TryRemove(kv.Key, out var pair))
            {
                pair.Client.Close();
                pair.Upstream.Close();
            }
        }

        try { _acceptTask.ConfigureAwait(false).GetAwaiter().GetResult(); } catch { }
        _cts.Dispose();
    }
}
