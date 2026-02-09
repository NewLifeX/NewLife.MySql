using NewLife;
using NewLife.MySql;

namespace UnitTest;

public class ParameterizedQueryTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    #region SubstituteParameters 单元测试
    /// <summary>直接调用 internal 方法进行测试（已设置 InternalsVisibleTo）</summary>
    private static String CallSubstitute(String sql, MySqlParameterCollection parameters)
        => MySqlCommand.SubstituteParameters(sql, parameters);

    private static String CallEscape(String value)
        => MySqlCommand.EscapeString(value);

    private static String CallSerialize(Object? value)
        => MySqlCommand.SerializeValue(value);

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
            (cmd.Parameters as MySqlParameterCollection).AddWithValue("name", name);
            cmd.ExecuteNonQuery();
        }

        // 参数化插入
        {
            using var cmd = new MySqlCommand(conn, "insert into sys.sys_config(variable,value,set_time,set_by) values(@var,@val,now(),@by)");
            var ps = cmd.Parameters as MySqlParameterCollection;
            ps.AddWithValue("var", name);
            ps.AddWithValue("val", "hello");
            ps.AddWithValue("by", "UnitTest");
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }

        // 参数化查询
        {
            using var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable=@name");
            (cmd.Parameters as MySqlParameterCollection).AddWithValue("name", name);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("hello", rs);
        }

        // 参数化更新
        {
            using var cmd = new MySqlCommand(conn, "update sys.sys_config set value=@val where variable=@name");
            var ps = cmd.Parameters as MySqlParameterCollection;
            ps.AddWithValue("val", "world");
            ps.AddWithValue("name", name);
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }

        // 验证更新
        {
            using var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable=@name");
            (cmd.Parameters as MySqlParameterCollection).AddWithValue("name", name);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("world", rs);
        }

        // 参数化删除
        {
            using var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable=@name");
            (cmd.Parameters as MySqlParameterCollection).AddWithValue("name", name);
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
        (cmd.Parameters as MySqlParameterCollection).AddWithValue("name", "'; DROP TABLE sys_config; --");
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
        (cmd.Parameters as MySqlParameterCollection).AddWithValue("name", DBNull.Value);
        var rs = cmd.ExecuteScalar();

        Assert.Equal(0, rs.ToInt());
    }
    #endregion

    #region SerializeValue 边界类型测试
    [Fact]
    public void SerializeEnumValue()
    {
        var result = CallSerialize(DayOfWeek.Monday);
        Assert.Equal("1", result);
    }

    [Fact]
    public void SerializeFloatValue()
    {
        var result = CallSerialize(3.14f);
        Assert.Equal(3.14f.ToString("R"), result);
    }

    [Fact]
    public void SerializeDoubleValue()
    {
        var result = CallSerialize(3.14159265358979);
        Assert.Equal(3.14159265358979.ToString("R"), result);
    }

    [Fact]
    public void SerializeDecimalValue()
    {
        var result = CallSerialize(123.456m);
        Assert.Equal("123.456", result);
    }

    [Fact]
    public void SerializeDateTimeOffsetValue()
    {
        var dto = new DateTimeOffset(2025, 7, 1, 12, 0, 0, TimeSpan.Zero);
        var result = CallSerialize(dto);
        Assert.Equal("'2025-07-01 12:00:00'", result);
    }

    [Fact]
    public void SerializeBoolTrue()
    {
        Assert.Equal("1", CallSerialize(true));
    }

    [Fact]
    public void SerializeBoolFalse()
    {
        Assert.Equal("0", CallSerialize(false));
    }

    [Fact]
    public void SerializeDateTimeWithMicroseconds()
    {
        var dt = new DateTime(2025, 1, 15, 8, 30, 45, 123).AddTicks(4560);
        var result = CallSerialize(dt);
        // 应包含微秒并去除尾部零
        Assert.StartsWith("'2025-01-15 08:30:45.", result);
        Assert.EndsWith("'", result);
    }
    #endregion

    #region SubstituteParameters 边界场景测试
    [Fact]
    public void SubstituteQuestionMarkParam()
    {
        // MySQL 兼容的 ? 参数标记
        var sql = "select * from t where name=?name";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("name", "test");

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from t where name='test'", result);
    }

    [Fact]
    public void SubstituteSkipDoubleQuotedString()
    {
        // 双引号字符串内的 @ 不应被替换
        var sql = "select * from t where col=\"test@value\" and name=@name";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("name", "hello");

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from t where col=\"test@value\" and name='hello'", result);
    }

    [Fact]
    public void SubstituteUnmatchedParamOutputAsIs()
    {
        // 未匹配到的参数原样输出
        var sql = "select * from t where name=@unknown and id=@id";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("id", 1);

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from t where name=@unknown and id=1", result);
    }

    [Fact]
    public void SubstituteBareAtSignNoParamName()
    {
        // 单独 @ 后面无字母数字，不应崩溃
        var sql = "select @@version";
        var parameters = new MySqlParameterCollection();

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select @@version", result);
    }

    [Fact]
    public void SubstituteEscapedQuoteInString()
    {
        // 字符串中转义的引号不应结束字面量扫描
        var sql = "select * from t where name='it\\'s @notparam' and id=@id";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("id", 42);

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from t where name='it\\'s @notparam' and id=42", result);
    }

    [Fact]
    public void SubstituteDoubleQuoteEscapeInString()
    {
        // 双引号转义 '' 不应结束字面量扫描
        var sql = "select * from t where name='it''s @notparam' and id=@id";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("id", 99);

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from t where name='it''s @notparam' and id=99", result);
    }

    [Fact]
    public void SubstituteDoubleAtSystemVariable()
    {
        // @@系统变量不应被参数替换，即使存在同名参数
        var sql = "select @@version, @name from t";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("name", "test");
        parameters.AddWithValue("version", "should_not_appear");

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select @@version, 'test' from t", result);
    }

    [Fact]
    public void SubstituteDoubleAtWithUnderscore()
    {
        // @@系统变量带下划线
        var sql = "select @@max_connections, @id from t";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("id", 1);

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select @@max_connections, 1 from t", result);
    }

    [Fact]
    public void SubstituteMultipleSameParam()
    {
        // 同一参数在 SQL 中出现多次
        var sql = "select * from t where a=@id or b=@id";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("id", 42);

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from t where a=42 or b=42", result);
    }

    [Fact]
    public void SubstituteParamNameWithDigits()
    {
        // 参数名包含数字
        var sql = "select * from t where id=@p1 and name=@p2";
        var parameters = new MySqlParameterCollection();
        parameters.AddWithValue("p1", 10);
        parameters.AddWithValue("p2", "abc");

        var result = CallSubstitute(sql, parameters);
        Assert.Equal("select * from t where id=10 and name='abc'", result);
    }
    #endregion

    #region EscapeString 扩展测试
    [Fact]
    public void EscapeNoSpecialChars()
    {
        // 无特殊字符时应直接返回原串（快速路径）
        var input = "hello world 123";
        var result = CallEscape(input);
        Assert.Same(input, result);
    }

    [Fact]
    public void EscapeEmptyString()
    {
        var result = CallEscape("");
        Assert.Same("", result);
    }

    [Fact]
    public void EscapeNullChar()
    {
        var result = CallEscape("a\0b");
        Assert.Equal("a\\0b", result);
    }

    [Fact]
    public void EscapeCtrlZ()
    {
        var result = CallEscape("a\u001ab");
        Assert.Equal("a\\Zb", result);
    }

    [Fact]
    public void EscapeDoubleQuote()
    {
        var result = CallEscape("say \"hello\"");
        Assert.Equal("say \\\"hello\\\"", result);
    }

    [Fact]
    public void EscapeCarriageReturn()
    {
        var result = CallEscape("line1\rline2");
        Assert.Equal("line1\\rline2", result);
    }

    [Fact]
    public void EscapeMixedSpecialChars()
    {
        // 多种特殊字符混合
        var result = CallEscape("it's a \"test\"\nwith\\slash");
        Assert.Equal("it\\'s a \\\"test\\\"\\nwith\\\\slash", result);
    }
    #endregion

    #region SerializeValue 整数类型测试
    [Fact]
    public void SerializeInt32()
    {
        Assert.Equal("42", CallSerialize(42));
        Assert.Equal("-1", CallSerialize(-1));
        Assert.Equal("0", CallSerialize(0));
    }

    [Fact]
    public void SerializeInt64()
    {
        Assert.Equal("9999999999", CallSerialize(9999999999L));
        Assert.Equal("-9999999999", CallSerialize(-9999999999L));
    }

    [Fact]
    public void SerializeInt16()
    {
        Assert.Equal("32767", CallSerialize((Int16)32767));
        Assert.Equal("-1", CallSerialize((Int16)(-1)));
    }

    [Fact]
    public void SerializeByte()
    {
        Assert.Equal("255", CallSerialize((Byte)255));
        Assert.Equal("0", CallSerialize((Byte)0));
    }

    [Fact]
    public void SerializeUInt32()
    {
        Assert.Equal("4294967295", CallSerialize(UInt32.MaxValue));
    }

    [Fact]
    public void SerializeUInt64()
    {
        Assert.Equal("18446744073709551615", CallSerialize(UInt64.MaxValue));
    }
    #endregion

    #region ConvertToPositionalParameters 测试
    private static String CallConvert(String sql, MySqlParameterCollection parameters, List<Int32>? paramOrder = null)
        => MySqlCommand.ConvertToPositionalParameters(sql, parameters, paramOrder);

    [Fact]
    public void ConvertNoParams()
    {
        var sql = "select 1";
        var result = CallConvert(sql, new MySqlParameterCollection());
        Assert.Equal(sql, result);
    }

    [Fact]
    public void ConvertBasicParams()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("name", "test");
        ps.AddWithValue("age", 25);

        var result = CallConvert("INSERT INTO t (name, age) VALUES (@name, @age)", ps);
        Assert.Equal("INSERT INTO t (name, age) VALUES (?, ?)", result);
    }

    [Fact]
    public void ConvertSkipStringLiteral()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("name", "test");

        var result = CallConvert("SELECT * FROM t WHERE name=@name AND label='@notparam'", ps);
        Assert.Equal("SELECT * FROM t WHERE name=? AND label='@notparam'", result);
    }

    [Fact]
    public void ConvertSkipDoubleAtSystemVar()
    {
        // @@系统变量不应替换为 ?
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("version", "8.0");
        ps.AddWithValue("id", 1);

        var result = CallConvert("SELECT @@version, @id FROM t", ps);
        Assert.Equal("SELECT @@version, ? FROM t", result);
    }

    [Fact]
    public void ConvertParamOrderMatches()
    {
        // 参数顺序映射应按 SQL 中出现顺序记录
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("a", 1);  // index 0
        ps.AddWithValue("b", 2);  // index 1
        ps.AddWithValue("c", 3);  // index 2

        var order = new List<Int32>();
        var result = CallConvert("INSERT INTO t VALUES (@b, @c, @a)", ps, order);

        Assert.Equal("INSERT INTO t VALUES (?, ?, ?)", result);
        // SQL 中顺序：@b(idx=1), @c(idx=2), @a(idx=0)
        Assert.Equal(3, order.Count);
        Assert.Equal(1, order[0]);
        Assert.Equal(2, order[1]);
        Assert.Equal(0, order[2]);
    }

    [Fact]
    public void ConvertParamOrderInOrder()
    {
        // 参数按顺序添加时，order 应为 [0, 1, 2]
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("a", 1);
        ps.AddWithValue("b", 2);

        var order = new List<Int32>();
        CallConvert("SELECT @a, @b", ps, order);

        Assert.Equal(new[] { 0, 1 }, order.ToArray());
    }

    [Fact]
    public void ConvertParamRepeated()
    {
        // 同一参数在 SQL 中出现多次
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("id", 1);

        var order = new List<Int32>();
        var result = CallConvert("SELECT * FROM t WHERE a=@id OR b=@id", ps, order);

        Assert.Equal("SELECT * FROM t WHERE a=? OR b=?", result);
        Assert.Equal(new[] { 0, 0 }, order.ToArray());
    }

    [Fact]
    public void ConvertUnmatchedParamPreserved()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("id", 1);

        var result = CallConvert("SELECT @unknown, @id FROM t", ps);
        Assert.Equal("SELECT @unknown, ? FROM t", result);
    }
    #endregion

    #region 辅助方法
    /// <summary>获取启用 UseServerPrepare 的连接字符串</summary>
    private static String GetServerPrepareConnStr() => _ConnStr.TrimEnd(';') + ";UseServerPrepare=true;";

    /// <summary>创建测试表</summary>
    private static void CreateTestTable(MySqlConnection conn, String tableName)
    {
        var sql = $@"CREATE TABLE IF NOT EXISTS `{tableName}` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `name` VARCHAR(100) NOT NULL,
            `age` INT NOT NULL DEFAULT 0,
            `score` DECIMAL(10,2) DEFAULT NULL,
            `created` DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        conn.ExecuteNonQuery(sql);
    }

    /// <summary>删除测试表</summary>
    private static void DropTestTable(MySqlConnection conn, String tableName)
    {
        conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{tableName}`");
    }
    #endregion

    #region 服务端预编译模式（UseServerPrepare=true）参数化 CRUD 测试
    [Fact]
    public void ServerPrepare_ParameterizedInsertUpdateDelete()
    {
        var name = "sp_crud_test";
        using var conn = new MySqlConnection(GetServerPrepareConnStr());
        conn.Open();

        // 清理
        {
            using var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable=@name");
            (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", name);
            cmd.ExecuteNonQuery();
        }

        // 参数化插入
        {
            using var cmd = new MySqlCommand(conn, "insert into sys.sys_config(variable,value,set_time,set_by) values(@var,@val,now(),@by)");
            var ps = (cmd.Parameters as MySqlParameterCollection)!;
            ps.AddWithValue("var", name);
            ps.AddWithValue("val", "hello_sp");
            ps.AddWithValue("by", "UnitTest");
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }

        // 参数化查询 ExecuteScalar
        {
            using var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable=@name");
            (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", name);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("hello_sp", rs);
        }

        // 参数化更新
        {
            using var cmd = new MySqlCommand(conn, "update sys.sys_config set value=@val where variable=@name");
            var ps = (cmd.Parameters as MySqlParameterCollection)!;
            ps.AddWithValue("val", "world_sp");
            ps.AddWithValue("name", name);
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }

        // 验证更新
        {
            using var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable=@name");
            (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", name);
            var rs = cmd.ExecuteScalar();
            Assert.Equal("world_sp", rs);
        }

        // 参数化删除
        {
            using var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable=@name");
            (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", name);
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);
        }
    }

    [Fact]
    public void ServerPrepare_SelectWithReader()
    {
        var name = "sp_reader_test";
        using var conn = new MySqlConnection(GetServerPrepareConnStr());
        conn.Open();

        // 清理并准备数据
        {
            using var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable=@name");
            (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", name);
            cmd.ExecuteNonQuery();
        }
        {
            using var cmd = new MySqlCommand(conn, "insert into sys.sys_config(variable,value,set_time,set_by) values(@var,@val,now(),@by)");
            var ps = (cmd.Parameters as MySqlParameterCollection)!;
            ps.AddWithValue("var", name);
            ps.AddWithValue("val", "reader_val");
            ps.AddWithValue("by", "UnitTest");
            cmd.ExecuteNonQuery();
        }

        // 参数化 ExecuteReader 读取多列
        {
            using var cmd = new MySqlCommand(conn, "select variable,value,set_by from sys.sys_config where variable=@name");
            (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", name);
            using var dr = cmd.ExecuteReader();
            Assert.True(dr.Read());
            Assert.Equal(name, dr.GetString(0));
            Assert.Equal("reader_val", dr.GetString(1));
            Assert.Equal("UnitTest", dr.GetString(2));
            Assert.False(dr.Read());
        }

        // 清理
        {
            using var cmd = new MySqlCommand(conn, "delete from sys.sys_config where variable=@name");
            (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", name);
            cmd.ExecuteNonQuery();
        }
    }

    [Fact]
    public void ServerPrepare_NullValue()
    {
        using var conn = new MySqlConnection(GetServerPrepareConnStr());
        conn.Open();

        using var cmd = new MySqlCommand(conn, "select count(*) from sys.sys_config where variable=@name");
        (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", DBNull.Value);
        var rs = cmd.ExecuteScalar();

        Assert.Equal(0, rs.ToInt());
    }

    [Fact]
    public void ServerPrepare_PreventsSqlInjection()
    {
        using var conn = new MySqlConnection(GetServerPrepareConnStr());
        conn.Open();

        using var cmd = new MySqlCommand(conn, "select count(*) from sys.sys_config where variable=@name");
        (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", "'; DROP TABLE sys_config; --");
        var rs = cmd.ExecuteScalar();

        // 不会匹配到任何行，但不会报错
        Assert.Equal(0, rs.ToInt());
    }

    [Fact]
    public void ServerPrepare_MultipleDataTypes()
    {
        var table = "sp_types_" + NewLife.Security.Rand.Next(10000);
        using var conn = new MySqlConnection(GetServerPrepareConnStr());
        conn.Open();
        CreateTestTable(conn, table);

        try
        {
            // 插入含多种数据类型的行
            using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age, score, created) VALUES (@name, @age, @score, @created)");
            var ps = (cmd.Parameters as MySqlParameterCollection)!;
            ps.AddWithValue("name", "TypeTest");
            ps.AddWithValue("age", 42);
            ps.AddWithValue("score", 99.5m);
            ps.AddWithValue("created", new DateTime(2025, 7, 1, 12, 0, 0));
            var rs = cmd.ExecuteNonQuery();
            Assert.Equal(1, rs);

            // 参数化查询验证
            using var qcmd = new MySqlCommand(conn, $"SELECT name, age, score FROM `{table}` WHERE age=@age");
            (qcmd.Parameters as MySqlParameterCollection)!.AddWithValue("age", 42);
            using var dr = qcmd.ExecuteReader();
            Assert.True(dr.Read());
            Assert.Equal("TypeTest", dr.GetString(0));
            Assert.Equal(42, dr.GetInt32(1));
            Assert.Equal(99.5m, dr.GetDecimal(2));
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }

    [Fact]
    public void ServerPrepare_InsertUpdateDelete_WithTestTable()
    {
        var table = "sp_crud_tbl_" + NewLife.Security.Rand.Next(10000);
        using var conn = new MySqlConnection(GetServerPrepareConnStr());
        conn.Open();
        CreateTestTable(conn, table);

        try
        {
            // 插入
            {
                using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age, score) VALUES (@name, @age, @score)");
                var ps = (cmd.Parameters as MySqlParameterCollection)!;
                ps.AddWithValue("name", "Alice");
                ps.AddWithValue("age", 25);
                ps.AddWithValue("score", 88.5m);
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }
            {
                using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age, score) VALUES (@name, @age, @score)");
                var ps = (cmd.Parameters as MySqlParameterCollection)!;
                ps.AddWithValue("name", "Bob");
                ps.AddWithValue("age", 30);
                ps.AddWithValue("score", 92.0m);
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }

            // 查询验证
            {
                using var cmd = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}` WHERE age>=@minAge");
                (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("minAge", 25);
                Assert.Equal(2, cmd.ExecuteScalar().ToInt());
            }

            // 更新
            {
                using var cmd = new MySqlCommand(conn, $"UPDATE `{table}` SET score=@score WHERE name=@name");
                var ps = (cmd.Parameters as MySqlParameterCollection)!;
                ps.AddWithValue("score", 95.0m);
                ps.AddWithValue("name", "Alice");
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }

            // 验证更新
            {
                using var cmd = new MySqlCommand(conn, $"SELECT score FROM `{table}` WHERE name=@name");
                (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", "Alice");
                Assert.Equal(95.0m, Convert.ToDecimal(cmd.ExecuteScalar()));
            }

            // 删除
            {
                using var cmd = new MySqlCommand(conn, $"DELETE FROM `{table}` WHERE name=@name");
                (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", "Bob");
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }

            // 验证删除
            {
                using var cmd = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}`");
                Assert.Equal(1, cmd.ExecuteScalar().ToInt());
            }
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }

    [Fact]
    public void ServerPrepare_ParamOrderMismatch()
    {
        var table = "sp_order_" + NewLife.Security.Rand.Next(10000);
        using var conn = new MySqlConnection(GetServerPrepareConnStr());
        conn.Open();
        CreateTestTable(conn, table);

        try
        {
            // 故意以相反顺序添加参数
            using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age) VALUES (@name, @age)");
            var ps = (cmd.Parameters as MySqlParameterCollection)!;
            ps.AddWithValue("age", 77);
            ps.AddWithValue("name", "Reversed");
            Assert.Equal(1, cmd.ExecuteNonQuery());

            // 验证参数绑定正确
            using var qcmd = new MySqlCommand(conn, $"SELECT name FROM `{table}` WHERE age=77");
            Assert.Equal("Reversed", qcmd.ExecuteScalar()?.ToString());
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }
    #endregion

    #region 显式 Prepare 模式参数化 CRUD 测试
    [Fact]
    public void Prepare_FullCRUD()
    {
        var table = "prep_crud_" + NewLife.Security.Rand.Next(10000);
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();
        CreateTestTable(conn, table);

        try
        {
            // Prepare 插入
            {
                using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age, score) VALUES (@name, @age, @score)");
                var ps = (cmd.Parameters as MySqlParameterCollection)!;
                ps.AddWithValue("name", "Tom");
                ps.AddWithValue("age", 20);
                ps.AddWithValue("score", 80.0m);
                cmd.Prepare();
                Assert.True(cmd.IsPrepared);
                Assert.Equal(1, cmd.ExecuteNonQuery());

                // 修改参数再执行第二条
                cmd.Parameters[0].Value = "Jerry";
                cmd.Parameters[1].Value = 25;
                cmd.Parameters[2].Value = 85.0m;
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }

            // Prepare 查询 ExecuteScalar
            {
                using var cmd = new MySqlCommand(conn, $"SELECT score FROM `{table}` WHERE name=@name");
                (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", "Tom");
                cmd.Prepare();
                Assert.True(cmd.IsPrepared);
                Assert.Equal(80.0m, Convert.ToDecimal(cmd.ExecuteScalar()));

                // 修改参数查询另一条
                cmd.Parameters[0].Value = "Jerry";
                Assert.Equal(85.0m, Convert.ToDecimal(cmd.ExecuteScalar()));
            }

            // Prepare 查询 ExecuteReader
            {
                using var cmd = new MySqlCommand(conn, $"SELECT name, age, score FROM `{table}` WHERE age>=@minAge ORDER BY age");
                (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("minAge", 20);
                cmd.Prepare();
                Assert.True(cmd.IsPrepared);

                using var dr = cmd.ExecuteReader();
                Assert.True(dr.Read());
                Assert.Equal("Tom", dr.GetString(0));
                Assert.Equal(20, dr.GetInt32(1));

                Assert.True(dr.Read());
                Assert.Equal("Jerry", dr.GetString(0));
                Assert.Equal(25, dr.GetInt32(1));

                Assert.False(dr.Read());
            }

            // Prepare 更新
            {
                using var cmd = new MySqlCommand(conn, $"UPDATE `{table}` SET score=@score WHERE name=@name");
                var ps = (cmd.Parameters as MySqlParameterCollection)!;
                ps.AddWithValue("score", 99.0m);
                ps.AddWithValue("name", "Tom");
                cmd.Prepare();
                Assert.True(cmd.IsPrepared);
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }

            // Prepare 查询验证更新
            {
                using var cmd = new MySqlCommand(conn, $"SELECT score FROM `{table}` WHERE name=@name");
                (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", "Tom");
                cmd.Prepare();
                Assert.Equal(99.0m, Convert.ToDecimal(cmd.ExecuteScalar()));
            }

            // Prepare 删除
            {
                using var cmd = new MySqlCommand(conn, $"DELETE FROM `{table}` WHERE name=@name");
                (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", "Jerry");
                cmd.Prepare();
                Assert.True(cmd.IsPrepared);
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }

            // 验证删除后只剩一条
            {
                using var cmd = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}`");
                Assert.Equal(1, cmd.ExecuteScalar().ToInt());
            }
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }

    [Fact]
    public void Prepare_SelectWithMultipleRows()
    {
        var table = "prep_rows_" + NewLife.Security.Rand.Next(10000);
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();
        CreateTestTable(conn, table);

        try
        {
            // 准备数据
            conn.ExecuteNonQuery($"INSERT INTO `{table}` (name, age) VALUES ('A', 10), ('B', 20), ('C', 30), ('D', 40)");

            // Prepare 查询返回多行
            using var cmd = new MySqlCommand(conn, $"SELECT name, age FROM `{table}` WHERE age>@threshold ORDER BY age");
            (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("threshold", 15);
            cmd.Prepare();

            using var dr = cmd.ExecuteReader();
            var names = new List<String>();
            while (dr.Read())
            {
                names.Add(dr.GetString(0));
            }
            Assert.Equal(new[] { "B", "C", "D" }, names.ToArray());
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }

    [Fact]
    public void Prepare_NullParameter()
    {
        var table = "prep_null_" + NewLife.Security.Rand.Next(10000);
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();
        CreateTestTable(conn, table);

        try
        {
            // 插入含 NULL score 的行
            using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age, score) VALUES (@name, @age, @score)");
            var ps = (cmd.Parameters as MySqlParameterCollection)!;
            ps.AddWithValue("name", "NullTest");
            ps.AddWithValue("age", 18);
            ps.AddWithValue("score", DBNull.Value);
            cmd.Prepare();
            Assert.Equal(1, cmd.ExecuteNonQuery());

            // 查询验证 NULL
            using var qcmd = new MySqlCommand(conn, $"SELECT score FROM `{table}` WHERE name='NullTest'");
            var val = qcmd.ExecuteScalar();
            Assert.True(val == null || val == DBNull.Value);
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }
    #endregion

    #region 客户端模式参数化 CRUD（自建表，完整验证）
    [Fact]
    public void ClientSide_ParameterizedCRUD_WithTestTable()
    {
        var table = "cs_crud_" + NewLife.Security.Rand.Next(10000);
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();
        CreateTestTable(conn, table);

        try
        {
            // 插入
            {
                using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age, score) VALUES (@name, @age, @score)");
                var ps = (cmd.Parameters as MySqlParameterCollection)!;
                ps.AddWithValue("name", "Alice");
                ps.AddWithValue("age", 25);
                ps.AddWithValue("score", 88.5m);
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }
            {
                using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age, score) VALUES (@name, @age, @score)");
                var ps = (cmd.Parameters as MySqlParameterCollection)!;
                ps.AddWithValue("name", "Bob");
                ps.AddWithValue("age", 30);
                ps.AddWithValue("score", 92.0m);
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }

            // 查询
            {
                using var cmd = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}` WHERE age>=@minAge");
                (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("minAge", 25);
                Assert.Equal(2, cmd.ExecuteScalar().ToInt());
            }

            // ExecuteReader 多列
            {
                using var cmd = new MySqlCommand(conn, $"SELECT name, age, score FROM `{table}` ORDER BY age");
                using var dr = cmd.ExecuteReader();
                Assert.True(dr.Read());
                Assert.Equal("Alice", dr.GetString(0));
                Assert.Equal(25, dr.GetInt32(1));
                Assert.Equal(88.5m, dr.GetDecimal(2));

                Assert.True(dr.Read());
                Assert.Equal("Bob", dr.GetString(0));
            }

            // 更新
            {
                using var cmd = new MySqlCommand(conn, $"UPDATE `{table}` SET score=@score WHERE name=@name");
                var ps = (cmd.Parameters as MySqlParameterCollection)!;
                ps.AddWithValue("score", 95.0m);
                ps.AddWithValue("name", "Alice");
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }

            // 验证更新
            {
                using var cmd = new MySqlCommand(conn, $"SELECT score FROM `{table}` WHERE name=@name");
                (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", "Alice");
                Assert.Equal(95.0m, Convert.ToDecimal(cmd.ExecuteScalar()));
            }

            // 删除
            {
                using var cmd = new MySqlCommand(conn, $"DELETE FROM `{table}` WHERE name=@name");
                (cmd.Parameters as MySqlParameterCollection)!.AddWithValue("name", "Bob");
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }

            // 验证删除
            {
                using var cmd = new MySqlCommand(conn, $"SELECT COUNT(*) FROM `{table}`");
                Assert.Equal(1, cmd.ExecuteScalar().ToInt());
            }
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }

    [Fact]
    public void ClientSide_MultipleDataTypes()
    {
        var table = "cs_types_" + NewLife.Security.Rand.Next(10000);
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();
        CreateTestTable(conn, table);

        try
        {
            using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age, score, created) VALUES (@name, @age, @score, @created)");
            var ps = (cmd.Parameters as MySqlParameterCollection)!;
            ps.AddWithValue("name", "TypeTest");
            ps.AddWithValue("age", 42);
            ps.AddWithValue("score", 99.5m);
            ps.AddWithValue("created", new DateTime(2025, 7, 1, 12, 0, 0));
            Assert.Equal(1, cmd.ExecuteNonQuery());

            using var qcmd = new MySqlCommand(conn, $"SELECT name, age, score FROM `{table}` WHERE age=@age");
            (qcmd.Parameters as MySqlParameterCollection)!.AddWithValue("age", 42);
            using var dr = qcmd.ExecuteReader();
            Assert.True(dr.Read());
            Assert.Equal("TypeTest", dr.GetString(0));
            Assert.Equal(42, dr.GetInt32(1));
            Assert.Equal(99.5m, dr.GetDecimal(2));
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }

    [Fact]
    public void ClientSide_NullParameter()
    {
        var table = "cs_null_" + NewLife.Security.Rand.Next(10000);
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();
        CreateTestTable(conn, table);

        try
        {
            using var cmd = new MySqlCommand(conn, $"INSERT INTO `{table}` (name, age, score) VALUES (@name, @age, @score)");
            var ps = (cmd.Parameters as MySqlParameterCollection)!;
            ps.AddWithValue("name", "NullTest");
            ps.AddWithValue("age", 18);
            ps.AddWithValue("score", DBNull.Value);
            Assert.Equal(1, cmd.ExecuteNonQuery());

            using var qcmd = new MySqlCommand(conn, $"SELECT score FROM `{table}` WHERE name='NullTest'");
            var val = qcmd.ExecuteScalar();
            Assert.True(val == null || val == DBNull.Value);
        }
        finally
        {
            DropTestTable(conn, table);
        }
    }
    #endregion
}
