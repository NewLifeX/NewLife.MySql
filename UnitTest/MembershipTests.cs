using NewLife.MySql;
using XCode.DataAccessLayer;
using XCode.Membership;

namespace UnitTest;

public class MembershipTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    static MembershipTests()
    {
        DAL.AddConnStr("membership", _ConnStr.Replace("Database=sys;", "Database=membership;"), null, "MySql");
    }

    [Fact]
    public void TestInit()
    {
        var dal = DAL.Create("membership");
        Assert.NotNull(dal);
        Assert.Equal(DatabaseType.MySql, dal.DbType);
        Assert.Equal(MySqlClientFactory.Instance, dal.Db.Factory);
    }

    [Fact]
    public void CreateTables()
    {
        User.Meta.Session.InitData();
        Role.Meta.Session.InitData();
    }
}
