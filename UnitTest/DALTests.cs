using NewLife.MySql;
using XCode.DataAccessLayer;

namespace UnitTest;

[Collection(TestCollections.ReadOnly)]
public class DALTests
{
    private static String _ConnStr = "Server=127.0.0.1;Database=sys;User Id=root;Password=root;";

    static DALTests()
    {
        var f = "Config\\mysql.config".GetFullPath();
        f.EnsureDirectory(true);
        if (File.Exists(f))
            _ConnStr = File.ReadAllText(f).Trim();
        else
            File.WriteAllText(f, _ConnStr);

        // localhost 在部分环境优先解析到 ::1，MySQL 未监听 IPv6 时要等约 1 秒才回退 IPv4，
        // 每个新建连接池都会白付一次超时，统一改为回环 IPv4
        if (_ConnStr.Contains("Server=localhost", StringComparison.OrdinalIgnoreCase))
            _ConnStr = _ConnStr.Replace("Server=localhost", "Server=127.0.0.1", StringComparison.OrdinalIgnoreCase);

        DAL.AddConnStr("mysql", _ConnStr, null, "MySql");
    }

    public static String GetConnStr() => _ConnStr;

    [Fact]
    public void TestInit()
    {
        var db = DbFactory.Create(DatabaseType.MySql);
        Assert.NotNull(db);
        Assert.Equal(DatabaseType.MySql, db.Type);

        var factory = db.Factory;
        Assert.Equal(MySqlClientFactory.Instance, factory);

        var dal = DAL.Create("mysql");
        Assert.NotNull(dal);
        Assert.Equal(DatabaseType.MySql, dal.DbType);
    }

    [Fact]
    public void TestTables()
    {
        var dal = DAL.Create("mysql");

        var tables = dal.Tables;
        Assert.NotNull(tables);
        Assert.NotEmpty(tables);
    }
}
