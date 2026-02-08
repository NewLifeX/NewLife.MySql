using NewLife;
using NewLife.Log;
using NewLife.MySql;
using System.Text;

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

    [Fact]
    public void MultiStatement_MixedDML_InsertUpdateDelete()
    {
        // 测试多种DML语句混合：INSERT + UPDATE + DELETE，验证影响行数累加
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable like 'ms_mixed%'"))
            cmd.ExecuteNonQuery();

        // 插入2条 + 更新1条 + 删除1条 = 影响4行
        var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_mixed1','v1',now(),'test');" +
                  "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_mixed2','v2',now(),'test');" +
                  "update sys.sys_config set value='v1_updated' where variable='ms_mixed1';" +
                  "delete from sys.sys_config where variable='ms_mixed2'";
        
        using (var cmd = new MySqlCommand(conn, sql))
        {
            var affected = cmd.ExecuteNonQuery();
            Assert.Equal(4, affected); // 2 INSERT + 1 UPDATE + 1 DELETE
        }

        // 验证结果
        using (var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable='ms_mixed1'"))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal("v1_updated", rs);
        }

        using (var cmd = new MySqlCommand(conn, "select count(*) from sys.sys_config where variable='ms_mixed2'"))
        {
            var count = cmd.ExecuteScalar().ToInt();
            Assert.Equal(0, count);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_mixed1'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_SelectFirst_ThenInsert()
    {
        // 先SELECT后INSERT，ExecuteScalar应返回第一个SELECT的结果
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_select_first'"))
            cmd.ExecuteNonQuery();

        var sql = "select 'before_insert' as result;" +
                  "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_select_first','data',now(),'test')";
        
        using (var cmd = new MySqlCommand(conn, sql))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal("before_insert", rs);
        }

        // 验证INSERT也执行了
        using (var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable='ms_select_first'"))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal("data", rs);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_select_first'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_InsertFirst_ThenSelect()
    {
        // 先INSERT后SELECT，ExecuteScalar应跳过OK包，返回SELECT的结果
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_insert_first'"))
            cmd.ExecuteNonQuery();

        var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_insert_first','xyz',now(),'test');" +
                  "select value from sys.sys_config where variable='ms_insert_first'";
        
        using (var cmd = new MySqlCommand(conn, sql))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal("xyz", rs);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_insert_first'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_ExecuteReader_MultipleResultSets()
    {
        // 多个SELECT语句，ExecuteReader应该能遍历所有结果集
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable like 'ms_multi_select%'"))
            cmd.ExecuteNonQuery();

        // 准备测试数据
        using (var cmd = new MySqlCommand(conn, "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_multi_select1','aaa',now(),'test'),('ms_multi_select2','bbb',now(),'test')"))
            cmd.ExecuteNonQuery();

        // 三个SELECT语句
        var sql = "select variable,value from sys.sys_config where variable='ms_multi_select1';" +
                  "select variable,value from sys.sys_config where variable='ms_multi_select2';" +
                  "select count(*) as total from sys.sys_config where variable like 'ms_multi_select%'";

        using (var cmd = new MySqlCommand(conn, sql))
        using (var dr = cmd.ExecuteReader())
        {
            // 第一个结果集
            Assert.True(dr.FieldCount > 0);
            Assert.True(dr.Read());
            Assert.Equal("ms_multi_select1", dr.GetString(0));
            Assert.Equal("aaa", dr.GetString(1));
            Assert.False(dr.Read());

            // 第二个结果集
            Assert.True(dr.NextResult());
            Assert.True(dr.FieldCount > 0);
            Assert.True(dr.Read());
            Assert.Equal("ms_multi_select2", dr.GetString(0));
            Assert.Equal("bbb", dr.GetString(1));
            Assert.False(dr.Read());

            // 第三个结果集
            Assert.True(dr.NextResult());
            Assert.True(dr.FieldCount > 0);
            Assert.True(dr.Read());
            Assert.Equal(2, dr.GetInt64(0));
            Assert.False(dr.Read());

            // 没有更多结果集
            Assert.False(dr.NextResult());
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable like 'ms_multi_select%'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_ExecuteReader_MixedOKAndResultSet()
    {
        // 混合OK包和结果集：INSERT + SELECT + UPDATE + SELECT
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_mixed_reader'"))
            cmd.ExecuteNonQuery();

        var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_mixed_reader','initial',now(),'test');" +
                  "select value from sys.sys_config where variable='ms_mixed_reader';" +
                  "update sys.sys_config set value='updated' where variable='ms_mixed_reader';" +
                  "select value from sys.sys_config where variable='ms_mixed_reader'";

        using (var cmd = new MySqlCommand(conn, sql))
        using (var dr = cmd.ExecuteReader())
        {
            // 第一个结果：INSERT的OK包（FieldCount=0）
            Assert.Equal(0, dr.FieldCount);

            // 第二个结果：SELECT结果集
            Assert.True(dr.NextResult());
            Assert.True(dr.FieldCount > 0);
            Assert.True(dr.Read());
            Assert.Equal("initial", dr.GetString(0));
            Assert.False(dr.Read());

            // 第三个结果：UPDATE的OK包（FieldCount=0）
            Assert.True(dr.NextResult());
            Assert.Equal(0, dr.FieldCount);

            // 第四个结果：SELECT结果集
            Assert.True(dr.NextResult());
            Assert.True(dr.FieldCount > 0);
            Assert.True(dr.Read());
            Assert.Equal("updated", dr.GetString(0));
            Assert.False(dr.Read());

            // 没有更多结果集
            Assert.False(dr.NextResult());
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_mixed_reader'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_RecordsAffected_OnlyDML()
    {
        // 只有DML语句时，RecordsAffected应正确累加
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable like 'ms_dml%'"))
            cmd.ExecuteNonQuery();

        // 准备初始数据
        using (var cmd = new MySqlCommand(conn, "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_dml1','v1',now(),'test'),('ms_dml2','v2',now(),'test'),('ms_dml3','v3',now(),'test')"))
            cmd.ExecuteNonQuery();

        // 多条UPDATE和DELETE
        var sql = "update sys.sys_config set value='new_v1' where variable='ms_dml1';" +
                  "update sys.sys_config set value='new_v2' where variable='ms_dml2';" +
                  "delete from sys.sys_config where variable='ms_dml3'";

        using (var cmd = new MySqlCommand(conn, sql))
        {
            var affected = cmd.ExecuteNonQuery();
            Assert.Equal(3, affected); // 2 UPDATE + 1 DELETE
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable like 'ms_dml%'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_RecordsAffected_WithSelect()
    {
        // 包含SELECT语句时，RecordsAffected应只累加DML的影响行数
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_select_dml'"))
            cmd.ExecuteNonQuery();

        var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_select_dml','val',now(),'test');" +
                  "select value from sys.sys_config where variable='ms_select_dml';" +
                  "update sys.sys_config set value='new_val' where variable='ms_select_dml'";

        using (var cmd = new MySqlCommand(conn, sql))
        using (var dr = cmd.ExecuteReader())
        {
            // 消费所有结果集
            while (dr.NextResult()) { }
            
            // RecordsAffected应该是 1 INSERT + 1 UPDATE = 2
            Assert.Equal(2, dr.RecordsAffected);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_select_dml'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_OnlySelects_NoAffectedRows()
    {
        // 只有SELECT语句，RecordsAffected应为0（SELECT不计入影响行数）
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        var sql = "select 1 as a;" +
                  "select 2 as b;" +
                  "select 3 as c";

        using (var cmd = new MySqlCommand(conn, sql))
        {
            var affected = cmd.ExecuteNonQuery();
            // SELECT语句通常不计入RecordsAffected，或者返回-1
            // 这里验证不会累加SELECT的行数
            Assert.True(affected == 0 || affected == -1);
        }
    }

    [Fact]
    public void MultiStatement_EmptyResult_ExecuteScalar()
    {
        // 所有语句都是DML（无结果集），ExecuteScalar应返回null
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_empty'"))
            cmd.ExecuteNonQuery();

        var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_empty','data',now(),'test');" +
                  "delete from sys.sys_config where variable='ms_empty'";

        using (var cmd = new MySqlCommand(conn, sql))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Null(rs);
        }
    }

    [Fact]
    public void MultiStatement_ExecuteReader_PartialRead()
    {
        // 不完全读取结果集，关闭reader应正常（不应导致连接问题）
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable like 'ms_partial%'"))
            cmd.ExecuteNonQuery();

        // 准备数据
        using (var cmd = new MySqlCommand(conn, "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_partial1','v1',now(),'test'),('ms_partial2','v2',now(),'test')"))
            cmd.ExecuteNonQuery();

        var sql = "select variable,value from sys.sys_config where variable like 'ms_partial%';" +
                  "select count(*) from sys.sys_config where variable like 'ms_partial%'";

        using (var cmd = new MySqlCommand(conn, sql))
        using (var dr = cmd.ExecuteReader())
        {
            // 只读取第一个结果集的第一行，然后关闭
            Assert.True(dr.Read());
            var firstVar = dr.GetString(0);
            Assert.True(firstVar == "ms_partial1" || firstVar == "ms_partial2");
            // 不读取第二行，也不调用NextResult
        }

        // 验证连接仍然可用
        using (var cmd = new MySqlCommand(conn, "select count(*) from sys.sys_config where variable like 'ms_partial%'"))
        {
            var count = cmd.ExecuteScalar().ToInt();
            Assert.Equal(2, count);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable like 'ms_partial%'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void MultiStatement_LargeNumberOfStatements()
    {
        // 大量语句（10条INSERT），验证影响行数正确累加
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable like 'ms_large%'"))
            cmd.ExecuteNonQuery();

        var sql = new StringBuilder();
        for (var i = 0; i < 10; i++)
        {
            if (i > 0) sql.Append(';');
            sql.Append($"insert into sys.sys_config(variable,value,set_time,set_by) values('ms_large{i}','v{i}',now(),'test')");
        }

        using (var cmd = new MySqlCommand(conn, sql.ToString()))
        {
            var affected = cmd.ExecuteNonQuery();
            Assert.Equal(10, affected);
        }

        // 验证
        using (var cmd = new MySqlCommand(conn, "select count(*) from sys.sys_config where variable like 'ms_large%'"))
        {
            var count = cmd.ExecuteScalar().ToInt();
            Assert.Equal(10, count);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable like 'ms_large%'"))
            cmd.ExecuteNonQuery();
    }
    #endregion

    #region NextResult 重复调用测试
    [Fact]
    public void NextResult_RepeatedCallAfterSingleInsert()
    {
        // 单条 INSERT 语句，NextResult 第二次调用应返回 false 而不是阻塞
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_repeat_ins'"))
            cmd.ExecuteNonQuery();

        var sql = "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_repeat_ins','v1',now(),'test')";
        using (var cmd = new MySqlCommand(conn, sql))
        using (var dr = cmd.ExecuteReader())
        {
            // ExecuteReader 内部已调用一次 NextResultAsync 获取第一个结果（OK 包）
            Assert.Equal(0, dr.FieldCount);

            // 第二次调用应立即返回 false，不应阻塞
            Assert.False(dr.NextResult());

            // 第三次调用仍应返回 false
            Assert.False(dr.NextResult());
        }

        // 验证连接仍可用
        using (var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable='ms_repeat_ins'"))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal("v1", rs);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_repeat_ins'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void NextResult_RepeatedCallAfterSingleUpdate()
    {
        // 单条 UPDATE 语句，NextResult 第二次调用应返回 false
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 准备数据
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_repeat_upd'"))
            cmd.ExecuteNonQuery();
        using (var cmd = new MySqlCommand(conn, "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_repeat_upd','old',now(),'test')"))
            cmd.ExecuteNonQuery();

        var sql = "update sys.sys_config set value='new' where variable='ms_repeat_upd'";
        using (var cmd = new MySqlCommand(conn, sql))
        using (var dr = cmd.ExecuteReader())
        {
            Assert.Equal(0, dr.FieldCount);
            Assert.False(dr.NextResult());
            Assert.False(dr.NextResult());
        }

        // 验证连接仍可用
        using (var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable='ms_repeat_upd'"))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal("new", rs);
        }

        // 清理
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_repeat_upd'"))
            cmd.ExecuteNonQuery();
    }

    [Fact]
    public void NextResult_RepeatedCallAfterSingleDelete()
    {
        // 单条 DELETE 语句，NextResult 第二次调用应返回 false
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 准备数据
        using (var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable='ms_repeat_del'"))
            cmd.ExecuteNonQuery();
        using (var cmd = new MySqlCommand(conn, "insert into sys.sys_config(variable,value,set_time,set_by) values('ms_repeat_del','v1',now(),'test')"))
            cmd.ExecuteNonQuery();

        var sql = "delete from sys.sys_config where variable='ms_repeat_del'";
        using (var cmd = new MySqlCommand(conn, sql))
        using (var dr = cmd.ExecuteReader())
        {
            Assert.Equal(0, dr.FieldCount);
            Assert.False(dr.NextResult());
            Assert.False(dr.NextResult());
        }

        // 验证连接仍可用
        using (var cmd = new MySqlCommand(conn, "select 1"))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal(1L, rs);
        }
    }

    [Fact]
    public void NextResult_RepeatedCallAfterSingleSelect()
    {
        // 单条 SELECT 语句，读完后 NextResult 应返回 false
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        var sql = "select 42 as answer";
        using (var cmd = new MySqlCommand(conn, sql))
        using (var dr = cmd.ExecuteReader())
        {
            Assert.True(dr.FieldCount > 0);
            Assert.True(dr.Read());
            Assert.Equal(42L, dr.GetInt64(0));
            Assert.False(dr.Read());

            // 行读完后 NextResult 应返回 false
            Assert.False(dr.NextResult());
            Assert.False(dr.NextResult());
        }

        // 验证连接仍可用
        using (var cmd = new MySqlCommand(conn, "select 1"))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal(1L, rs);
        }
    }

    [Fact]
    public void NextResult_RepeatedCallAfterSelectWithoutRead()
    {
        // 单条 SELECT 语句，不调用 Read 就直接调用 NextResult
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        var sql = "select variable,value from sys.sys_config limit 5";
        using (var cmd = new MySqlCommand(conn, sql))
        using (var dr = cmd.ExecuteReader())
        {
            Assert.True(dr.FieldCount > 0);

            // 不调用 Read，直接 NextResult（应自动消费剩余行）
            Assert.False(dr.NextResult());
            Assert.False(dr.NextResult());
        }

        // 验证连接仍可用
        using (var cmd = new MySqlCommand(conn, "select 1"))
        {
            var rs = cmd.ExecuteScalar();
            Assert.Equal(1L, rs);
        }
    }
    #endregion
}