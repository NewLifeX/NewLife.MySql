using NewLife;
using NewLife.MySql;
using NewLife.Reflection;

namespace UnitTest;

public class ParameterizedQueryTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    #region SubstituteParameters 单元测试
    /// <summary>通过反射调用 internal 方法进行测试</summary>
    private static String CallSubstitute(String sql, MySqlParameterCollection parameters)
        => (String)typeof(MySqlCommand).Invoke("SubstituteParameters", sql, parameters)!;

    private static String CallEscape(String value)
        => (String)typeof(MySqlCommand).Invoke("EscapeString", value)!;

    private static String CallSerialize(Object? value)
        => (String)typeof(MySqlCommand).Invoke("SerializeValue", value)!;

    [Fact]
    public void SubstituteNoParams()
    {
        var sql = "select * from sys.user_summary";
        var parameters = new MySqlParameterCollection();

        var result = CallSubstitute(sql, parameters);
        Assert.Equal(sql, result);
    }

    [Fact]
    public void SubstituteStringParam()
    {
        var sql = "select * from sys.sys_config where variable=@name";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("name", "test");

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from sys.sys_config where variable='test'", result);
    }

    [Fact]
    public void SubstituteIntParam()
    {
        var sql = "select * from sys.sys_config where id=@id";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("id", 42);

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from sys.sys_config where id=42", result);
    }

    [Fact]
    public void SubstituteNullParam()
    {
        var sql = "select * from sys.sys_config where value=@val";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("val", null);

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from sys.sys_config where value=NULL", result);
    }

    [Fact]
    public void SubstituteBoolParam()
    {
        var sql = "select * from t where flag=@flag";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("flag", true);

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from t where flag=1", result);
    }

    [Fact]
    public void SubstituteDateTimeParam()
    {
        var dt = new DateTime(2025, 7, 1, 12, 30, 0);
        var sql = "select * from t where created=@dt";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("dt", dt);

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from t where created='2025-07-01 12:30:00'", result);
    }

    [Fact]
    public void SubstituteMultipleParams()
    {
        var sql = "insert into t(name,age) values(@name,@age)";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("name", "Stone");
        parameters.AddWithValue("age", 30);

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("insert into t(name,age) values('Stone',30)", result);
    }

    [Fact]
    public void SubstituteParamWithAtSign()
    {
        var sql = "select * from t where name=@name";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("@name", "Stone");

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from t where name='Stone'", result);
    }

    [Fact]
    public void SubstituteSkipStringLiteral()
    {
        // 字符串字面量中的 @ 不应被替换
        var sql = "select * from t where email='test@example.com' and name=@name";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("name", "Stone");

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from t where email='test@example.com' and name='Stone'", result);
    }
    #endregion

    #region EscapeString 单元测试
    [Fact]
    public void EscapeSingleQuote()
    {
        var result = CallEscape("it's a test");
        Assert.Equal("it\\'s a test", result);
    }

    [Fact]
    public void EscapeBackslash()
    {
        var result = CallEscape("path\\to\\file");
        Assert.Equal("path\\\\to\\\\file", result);
    }

    [Fact]
    public void EscapeNewline()
    {
        var result = CallEscape("line1\nline2");
        Assert.Equal("line1\\nline2", result);
    }
    #endregion

    #region SerializeValue 单元测试
    [Fact]
    public void SerializeNullValue()
    {
        Assert.Equal("NULL", CallSerialize(null));
        Assert.Equal("NULL", CallSerialize(DBNull.Value));
    }

    [Fact]
    public void SerializeGuidValue()
    {
        var guid = Guid.Parse("01234567-89ab-cdef-0123-456789abcdef");
        var result = CallSerialize(guid);
        Assert.Equal("'01234567-89ab-cdef-0123-456789abcdef'", result);
    }

    [Fact]
    public void SerializeByteArrayValue()
    {
        var bytes = new Byte[] { 0xCA, 0xFE };
        var result = CallSerialize(bytes);
        Assert.Equal("X'CAFE'", result);
    }
    #endregion

    #region 参数化执行测试（需要数据库连接）
    [Fact]
    public void ParameterizedInsertUpdateDelete()
    {
        var name = "test_param";
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        {
            using var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable=@name");
            cmd.Parameters.AddWithValue("name", name);
            cmd.ExecuteNonQuery();
        }

        // 参数化插入
        {
            using var cmd = new MySqlCommand(conn, "insert into sys.sys_config(variable,value,set_time,set_by) values(@var,@val,now(),@by)");
            cmd.Parameters.AddWithValue("var", name);
            cmd.Parameters.AddWithValue("val", "hello");
            cmd.Parameters.AddWithValue("by", "UnitTest");
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }

        // 参数化查询
        {
            using var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable=@name");
            cmd.Parameters.AddWithValue("name", name);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("hello", rs);
        }

        // 参数化更新
        {
            using var cmd = new MySqlCommand(conn, "update sys.sys_config set value=@val where variable=@name");
            cmd.Parameters.AddWithValue("val", "world");
            cmd.Parameters.AddWithValue("name", name);
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }

        // 验证更新
        {
            using var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable=@name");
            cmd.Parameters.AddWithValue("name", name);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("world", rs);
        }

        // 参数化删除
        {
            using var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable=@name");
            cmd.Parameters.AddWithValue("name", name);
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }
    }

    [Fact]
    public void ParameterizedQueryPreventsSqlInjection()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 尝试注入，参数化应该安全处理
        using var cmd = new MySqlCommand(conn, "select count(*) from sys.sys_config where variable=@name");
        cmd.Parameters.AddWithValue("name", "'; DROP TABLE sys_config; --");
        var rs = cmd.ExecuteScalar();

        // 不会匹配到任何行，但不会报错
        Assert.Equal(0, rs.ToInt());
    }

    [Fact]
    public void ParameterizedQueryWithNullValue()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        using var cmd = new MySqlCommand(conn, "select count(*) from sys.sys_config where variable=@name");
        cmd.Parameters.AddWithValue("name", DBNull.Value);
        var rs = cmd.ExecuteScalar();

        Assert.Equal(0, rs.ToInt());
    }
    #endregion
}
