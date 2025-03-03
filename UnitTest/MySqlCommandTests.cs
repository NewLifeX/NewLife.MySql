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

    [Fact]
    public void ExecuteScalar()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        {
            var sql = "select statements u from sys.user_summary where user='root'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteScalar();

            var n = rs.ToInt();
            Assert.True(n > 0);
        }
    }

    [Fact]
    public void TestExecuteNonQuery()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        {
            var sql = "delete from sys.sys_config where variable='test'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteNonQuery();
            Assert.True(rs >= 0);
        }

        {
            var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('test','123',now(),'Stone')";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteNonQuery();
            Assert.True(rs >= 0);
        }
        {
            var sql = "select value v from sys.sys_config where set_by='Stone'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("123", rs);
        }
        {
            var sql = "update sys.sys_config set value=456 where set_by='Stone'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteNonQuery();
            Assert.True(rs >= 0);
        }
        {
            var sql = "select value v from sys.sys_config where set_by='Stone'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("456", rs);
        }
        {
            var sql = "delete from sys.sys_config where variable='test'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteNonQuery();
            Assert.True(rs >= 0);
        }
    }
}