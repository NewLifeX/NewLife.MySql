using NewLife.MySql;
using XCode.DataAccessLayer;

namespace UnitTest;

public class DALTests
{
    private static String _ConnStr = "Server=localhost;Database=sys;User Id=root;Password=root;";

    static DALTests()
    {
        var f = "Config\\mysql.config".GetFullPath();
        if (File.Exists(f))
            _ConnStr = File.ReadAllText(f);
        else
            File.WriteAllText(f, _ConnStr);

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
