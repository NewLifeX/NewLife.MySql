using System.Net;
using System.Net.Sockets;
using NewLife.MySql;
using NewLife.MySql.Common;
using NewLife.MySql.Messages;

namespace UnitTest;

/// <summary>连接池/连接可靠性测试公共工具。基于回环 socket 构造可控连接，无需真实 MySQL 服务器</summary>
/// <remarks>
/// 半开/EOF/取消/存活期等故障在真实 MySQL 上难以确定性复现，这里用 TcpListener 在回环地址上
/// 构造一对 socket，通过关闭服务端、推送残留数据等手段精确模拟各类网络异常。
/// </remarks>
static class ConnectionTestKit
{
    /// <summary>在回环地址上创建一对已连接的 TCP 客户端（client 端与 server 端）</summary>
    /// <returns>客户端与服务端 TCP</returns>
    public static (TcpClient client, TcpClient server) CreatePair()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            var client = new TcpClient();
            client.Connect(IPAddress.Loopback, port);
            var server = listener.AcceptTcpClient();
            return (client, server);
        }
        finally
        {
            listener.Stop();
        }
    }

    /// <summary>用回环 socket 的客户端封装一个 SqlClient。Dispose 时会一并关闭底层 client</summary>
    /// <param name="client">客户端 TCP</param>
    /// <param name="opened">是否模拟为已打开（认证完成）的连接</param>
    /// <returns>封装好的客户端</returns>
    public static SqlClient WrapClient(TcpClient client, Boolean opened = true)
    {
        var setting = new MySqlConnectionStringBuilder("server=127.0.0.1;port=3306;uid=root;pwd=root;database=test");
        var sql = new SqlClient(setting)
        {
            Tcp = client,
            BaseStream = client.GetStream(),
            Timeout = 5,
        };

        if (opened)
        {
            sql.Welcome = new WelcomeMessage { ServerVersion = "8.0.0" };
            sql.Active = true;
            sql.CreatedTime = DateTime.Now;
            sql.LastActive = DateTime.Now;
        }

        return sql;
    }

    /// <summary>轮询等待 socket 健康状态变为期望值，规避 FIN/数据未到达的时序抖动</summary>
    /// <param name="client">被测连接</param>
    /// <param name="expected">期望状态</param>
    /// <param name="timeoutMs">最长等待毫秒</param>
    /// <returns>最终探测到的状态</returns>
    public static SocketHealth WaitHealth(SqlClient client, SocketHealth expected, Int32 timeoutMs = 2000)
    {
        var end = Environment.TickCount + timeoutMs;
        var health = client.IsSocketAlive();
        while (health != expected && Environment.TickCount < end)
        {
            Thread.Sleep(20);
            health = client.IsSocketAlive();
        }

        return health;
    }
}
