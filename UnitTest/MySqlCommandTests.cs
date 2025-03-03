using NewLife;
using NewLife.Log;
using NewLife.MySql;

namespace UnitTest;

public class MySqlCommandTests
{
    private String _ConnStr = "Server=localhost;Database=sys;User Id=root;Password=root;";

    [Fact]
    public void TestQuery()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        using var cmd = new MySqlCommand(conn, "select * from sys.user_summary");
        using var dr = cmd.ExecuteReader();

        var dr2 = dr as MySqlDataReader;
        Assert.Equal(cmd, dr2.Command);

        var columns = Enumerable.Range(0, dr.FieldCount).Select(dr.GetName).ToArray();
        XTrace.WriteLine(columns.Join(","));

        Assert.Equal("user", columns[0]);

        var columns2 = dr2.Columns;
        Assert.Equal("user", columns2[0].Name);

        var rows = 0;
        while (dr.Read())
        {
            var values = new Object[dr.FieldCount];
            dr.GetValues(values);
            XTrace.WriteLine(values.Join(","));

            if (rows++ == 0)
                Assert.Equal("root", values[0]);
        }
    }
}
