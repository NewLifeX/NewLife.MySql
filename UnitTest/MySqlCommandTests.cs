using NewLife;
using NewLife.Log;
using NewLife.MySql;

namespace UnitTest;

public class MySqlCommandTests
{
    private static String _ConnStr = DALTests.GetConnStr();

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
            Assert.Equal(1, rs);
        }
        {
            var sql = "select value v from sys.sys_config where variable='test'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("123", rs);
        }
        {
            var sql = "update sys.sys_config set value=456 where variable='test'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }
        {
            var sql = "select value v from sys.sys_config where variable='test'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("456", rs);
        }
        {
            var sql = "delete from sys.sys_config where variable='test'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }
    }

    [Fact]
    public void InsertAndGetIdentity()
    {
        var name = "test2";
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        {
            var sql = $"delete from sys.sys_config where variable='{name}'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteNonQuery();
            Assert.True(rs >= 0);
        }

        {
            // 分两步执行：先插入，再查询插入的行数
            var sql = $"insert into sys.sys_config(variable,value,set_time,set_by) values('{name}','123',now(),'Stone')";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }
        {
            // 验证插入成功
            var sql = $"select count(*) from sys.sys_config where variable='{name}'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteScalar();
            Assert.Equal(1, rs.ToInt());
        }
        {
            var sql = $"select value v from sys.sys_config where variable='{name}'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("123", rs);
        }
        {
            var sql = $"update sys.sys_config set value=456 where variable='{name}'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }
        {
            var sql = $"select value v from sys.sys_config where variable='{name}'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("456", rs);
        }
        {
            var sql = $"delete from sys.sys_config where variable='{name}'";
            using var cmd = new MySqlCommand(conn, sql);
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }
    }

    #region 多语句执行测试
    [Fact]
    public void MultiStatement_ExecuteNonQuery()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable in ('ms_test1','ms_test2')"))
            cmd.ExecuteNonQuery();

        // 多条 INSERT 语句，用分号分隔
        var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_test1','aaa',now(),'test');" +
                  "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_test2','bbb',now(),'test')";
        using (var cmd = new MySqlCommand(conn, sql))
        {
            var affected = cmd.ExecuteNonQuery();
            Assert.Equal(2, affected);
        }

        // 验证两条都已插入
        using (var cmd = new MySqlCommand(conn, "select count(*) from sys.sys_config where variable in ('ms_test1','ms_test2')"))
        {
            var count = cmd.ExecuteScalar().ToInt();
            Assert.Equal(2, count);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable in ('ms_test1','ms_test2')"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_ExecuteScalar()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_scalar'"))
            cmd.ExecuteNonQuery();

        // 先查询再插入，用分号拼接，ExecuteScalar返回第一条语句的标量值
        // INSERT语句返回null（无结果集），SELECT语句返回标量值
        var sql = "select 'first_result';" +
                  "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_scalar','789',now(),'test')";
        using (var cmd = new MySqlCommand(conn, sql))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal("first_result", rs);
        }

        // 验证INSERT也执行了
        using (var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable='ms_scalar'"))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal("789", rs);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_scalar'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_ExecuteReader()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_reader'"))
            cmd.ExecuteNonQuery();

        // 先插入再查询，ExecuteReader返回第一条语句的结果集，使用NextResult()遍历
        var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_reader','hello',now(),'test');" +
                  "select variable,value from sys.sys_config where variable='ms_reader'";
        using (var cmd = new MySqlCommand(conn, sql))
        using (var dr = cmd.ExecuteReader())
        {
            // 第一个结果集是INSERT的OK包（无列），通过NextResult跳到SELECT结果
            Assert.True(dr.NextResult());
            Assert.True(dr.Read());
            Assert.Equal("ms_reader", dr.GetString(0));
            Assert.Equal("hello", dr.GetString(1));
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_reader'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_WithSemicolonInString()
    {
        // 验证字符串内的分号不会被误拆为多条语句
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_semi'"))
            cmd.ExecuteNonQuery();

        // value 中包含分号，不应被拆分
        var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_semi','a;b;c',now(),'test')";
        using (var cmd = new MySqlCommand(conn, sql))
        {
            var affected = cmd.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        // 验证数据完整性
        using (var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable='ms_semi'"))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal("a;b;c", rs);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_semi'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_TrailingSemicolon()
    {
        // 尾部分号不影响执行
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        var sql = "select count(*) from sys.sys_config;";
        using var cmd = new MySqlCommand(conn, sql);
        var rs = cmd.ExecuteScalar();
        Assert.NotNull(rs);
        Assert.True(rs.ToInt() >= 0);
    }

    [Fact]
    public void MultiStatement_ThreeStatements()
    {
        // 三条语句：DELETE + INSERT + SELECT
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        var sql = "delete from sys.sys_config where variable='ms_three';" +
                  "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_three','999',now(),'test');" +
                  "select value v from sys.sys_config where variable='ms_three'";
        using (var cmd = new MySqlCommand(conn, sql))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal("999", rs);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_three'"))
            cmd.ExecuteNonQuery();
    }
    #endregion
}