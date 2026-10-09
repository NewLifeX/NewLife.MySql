using System.ComponentModel;
using System.Runtime.InteropServices;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>安全传输（Unix Socket / TLS）认证测试</summary>
/// <remarks>
/// caching_sha2_password 完整认证在安全传输（TLS 或 Unix 套接字）下须发送明文密码（由通道加密），
/// 非安全传输才采用 RSA 公钥加密；这里用"新建用户 → 首连（凭据未缓存）"确定性地触发完整认证路径。
/// </remarks>
[Collection(TestCollections.DataModification)]
public class SecureTransportTests
{
    /// <summary>获取可用的 Unix 套接字路径，不存在时返回 null（Windows 或未部署 MySQL 的环境）</summary>
    /// <returns>套接字路径，不可用时返回 null</returns>
    private static String? GetSocketPath()
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && !RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) return null;

        var path = Environment.GetEnvironmentVariable("MYSQL_SOCKET");
        if (!path.IsNullOrEmpty()) return File.Exists(path) ? path : null;

        foreach (var candidate in new[] { "/var/run/mysqld/mysqld.sock", "/tmp/mysql.sock" })
        {
            if (File.Exists(candidate)) return candidate;
        }

        return null;
    }

    [Fact]
    [DisplayName("caching_sha2 完整认证经 Unix Socket 可用（Linux/macOS）")]
    public void FullAuthentication_OverUnixSocket()
    {
        var socket = GetSocketPath();
        if (socket == null) return; // 平台/环境不具备 Unix 套接字，跳过

        var b = new MySqlConnectionStringBuilder(DALTests.GetConnStr()) { Socket = socket };
        FullAuthentication(b.ConnectionString, socket);
    }

    [Fact]
    [DisplayName("caching_sha2 完整认证经 TLS 可用")]
    public void FullAuthentication_OverTls()
    {
        var b = new MySqlConnectionStringBuilder(DALTests.GetConnStr());
        using (var admin = new MySqlConnection(b.ConnectionString))
        {
            admin.Open();
            using var cmd = new MySqlCommand(admin, "SHOW VARIABLES LIKE 'have_ssl'");
            using var reader = cmd.ExecuteReader();
            if (!reader.Read() || !"YES".EqualIgnoreCase(Convert.ToString(reader.GetValue(1)))) return; // 服务器未启用 SSL，跳过
        }

        b.SslMode = "Required";
        FullAuthentication(b.ConnectionString, "TLS");
    }

    [Fact]
    [DisplayName("caching_sha2 完整认证经明文 TCP 可用（RSA 公钥）")]
    public void FullAuthentication_OverPlainTcp()
    {
        FullAuthentication(DALTests.GetConnStr(), "明文 TCP");
    }

    /// <summary>创建临时用户（凭据未缓存），用其建立连接并查询，最后清理</summary>
    /// <param name="connStr">用于被测连接的连接串（含 Socket 或 SslMode）</param>
    /// <param name="tag">失败信息标记（TLS/套接字路径）</param>
    private static void FullAuthentication(String connStr, String tag)
    {
        using var admin = new MySqlConnection(DALTests.GetConnStr());
        admin.Open();

        var user = "nl_auth_" + Rand.Next(1000000);
        const String password = "nl_auth_pw_1";
        try
        {
            using (var cmd = new MySqlCommand(admin, $"CREATE USER '{user}'@'localhost' IDENTIFIED WITH caching_sha2_password BY '{password}'"))
                cmd.ExecuteNonQuery();
            using (var cmd = new MySqlCommand(admin, $"GRANT SELECT ON sys.* TO '{user}'@'localhost'"))
                cmd.ExecuteNonQuery();

            // 新用户凭据未缓存 → 首次连接必然走完整认证
            var b = new MySqlConnectionStringBuilder(connStr) { UserID = user, Password = password };
            using var conn = new MySqlConnection(b.ConnectionString);
            conn.Open();
            using var verify = new MySqlCommand(conn, "SELECT 1");
            var v = Convert.ToInt32(verify.ExecuteScalar());
            Assert.True(v == 1, $"完整认证（{tag}）查询结果异常：{v}");
        }
        finally
        {
            using var drop = new MySqlCommand(admin, $"DROP USER IF EXISTS '{user}'@'localhost'");
            drop.ExecuteNonQuery();
        }
    }
}
