using NewLife.MySql;
using NewLife.MySql.Common;
using Xunit;

namespace UnitTest;

/// <summary>数据库类型检测测试</summary>
public class DatabaseTypeDetectionTests
{
    [Fact(DisplayName = "检测MySQL版本字符串")]
    public void DetectMySQL()
    {
        var connStr = "Server=localhost;Database=test;User Id=root;Password=pass;";
        using var conn = new MySqlConnection(connStr);

        // 模拟 MySQL 5.7 版本字符串
        SetServerVersion(conn, "5.7.33");
        Assert.Equal(DatabaseType.MySQL, conn.DatabaseType);

        // 模拟 MySQL 8.0 版本字符串
        SetServerVersion(conn, "8.0.26");
        Assert.Equal(DatabaseType.MySQL, conn.DatabaseType);

        // 模拟 MySQL 9.0 版本字符串
        SetServerVersion(conn, "9.0.1");
        Assert.Equal(DatabaseType.MySQL, conn.DatabaseType);
    }

    [Fact(DisplayName = "检测OceanBase版本字符串")]
    public void DetectOceanBase()
    {
        var connStr = "Server=localhost;Database=test;User Id=root;Password=pass;";
        using var conn = new MySqlConnection(connStr);

        // 模拟 OceanBase 4.x 版本字符串（基于 MySQL 5.7）
        SetServerVersion(conn, "5.7.25-OceanBase-v4.0.0");
        Assert.Equal(DatabaseType.OceanBase, conn.DatabaseType);

        // 模拟 OceanBase 3.x 版本字符串（基于 MySQL 5.7）
        SetServerVersion(conn, "5.7.25-OceanBase_CE-v3.1.4");
        Assert.Equal(DatabaseType.OceanBase, conn.DatabaseType);

        // 模拟 OceanBase 基于 MySQL 8.0 的版本
        SetServerVersion(conn, "8.0.32-OceanBase-v4.2.0");
        Assert.Equal(DatabaseType.OceanBase, conn.DatabaseType);

        // 测试大小写不敏感
        SetServerVersion(conn, "5.7.25-oceanbase-v4.0.0");
        Assert.Equal(DatabaseType.OceanBase, conn.DatabaseType);
    }

    [Fact(DisplayName = "检测TiDB版本字符串")]
    public void DetectTiDB()
    {
        var connStr = "Server=localhost;Database=test;User Id=root;Password=pass;";
        using var conn = new MySqlConnection(connStr);

        // 模拟 TiDB 5.x 版本字符串
        SetServerVersion(conn, "5.7.25-TiDB-v5.4.0");
        Assert.Equal(DatabaseType.TiDB, conn.DatabaseType);

        // 模拟 TiDB 6.x 版本字符串
        SetServerVersion(conn, "5.7.25-TiDB-v6.5.2");
        Assert.Equal(DatabaseType.TiDB, conn.DatabaseType);

        // 模拟 TiDB 7.x 版本字符串
        SetServerVersion(conn, "8.0.11-TiDB-v7.1.0");
        Assert.Equal(DatabaseType.TiDB, conn.DatabaseType);

        // 测试大小写不敏感
        SetServerVersion(conn, "5.7.25-tidb-v5.4.0");
        Assert.Equal(DatabaseType.TiDB, conn.DatabaseType);
    }

    [Fact(DisplayName = "空版本字符串应返回MySQL")]
    public void EmptyVersionShouldReturnMySQL()
    {
        var connStr = "Server=localhost;Database=test;User Id=root;Password=pass;";
        using var conn = new MySqlConnection(connStr);

        SetServerVersion(conn, "");
        Assert.Equal(DatabaseType.MySQL, conn.DatabaseType);

        SetServerVersion(conn, null!);
        Assert.Equal(DatabaseType.MySQL, conn.DatabaseType);
    }

    /// <summary>使用反射设置连接的服务器版本（用于测试）</summary>
    private static void SetServerVersion(MySqlConnection conn, String version)
    {
        var versionField = typeof(MySqlConnection).GetField("_Version", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        versionField?.SetValue(conn, version);

        var dbTypeField = typeof(MySqlConnection).GetField("_DatabaseType", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        // 调用私有的 DetectDatabaseType 方法
        var detectMethod = typeof(MySqlConnection).GetMethod("DetectDatabaseType", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        var dbType = detectMethod?.Invoke(null, new object[] { version });
        dbTypeField?.SetValue(conn, dbType);
    }
}
