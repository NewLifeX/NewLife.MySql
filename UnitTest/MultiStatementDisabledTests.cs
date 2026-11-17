using System.ComponentModel;
using System.Data;
using NewLife;
using NewLife.MySql;
using NewLife.MySql.Messages;

namespace UnitTest;

/// <summary>多语句显式关闭时的行为测试。连接串设置 AllowMultiStatements=false 后驱动不协商 CLIENT_MULTI_STATEMENTS，
/// 服务端拒绝堆叠语句；默认开启的行为见 <see cref="MultiStatementDefaultTests"/></summary>
[Collection(TestCollections.ReadOnly)]
public class MultiStatementDisabledTests
{
    /// <summary>构造显式关闭多语句的连接串（测试库连接串可能显式开启）</summary>
    /// <returns></returns>
    private static String BuildConnStr() => new MySqlConnectionStringBuilder(DALTests.GetConnStr()) { AllowMultiStatements = false }.ConnectionString;

    [Fact]
    [DisplayName("关闭多语句时执行多语句报语法错误并提示开关")]
    public async Task MultiStatement_WithoutFlag_Throws()
    {
        using var conn = new MySqlConnection(BuildConnStr());
        await conn.OpenAsync();

        using var cmd = new MySqlCommand(conn, "SELECT 1; SELECT 2");
        var ex = await Assert.ThrowsAsync<MySqlException>(() => cmd.ExecuteScalarAsync());
        Assert.Contains("AllowMultiStatements", ex.Message);
    }

    [Fact]
    [DisplayName("未开启多语句时不协商MULTI_STATEMENTS标志")]
    public void GetFlags_WithoutFlag_NoMultiStatements()
    {
        var client = new SqlClient(new MySqlConnectionStringBuilder("server=127.0.0.1;AllowMultiStatements=false"));
        var flags = new Authentication(client).GetFlags(ClientFlags.SECURE_CONNECTION);

        Assert.False(flags.HasFlag(ClientFlags.MULTI_STATEMENTS));
        // 多结果集能力仍保留，存储过程 CALL 依赖它
        Assert.True(flags.HasFlag(ClientFlags.MULTI_RESULTS));
    }

    [Fact]
    [DisplayName("存储过程OUT参数在关闭多语句时临时开启能力后仍可调用")]
    public async Task StoredProcedureOutParam_WithoutFlag_StillReachesServer()
    {
        using var conn = new MySqlConnection(BuildConnStr());
        await conn.OpenAsync();

        using var cmd = new MySqlCommand(conn, "sp_no_such_proc") { CommandType = CommandType.StoredProcedure };
        cmd.Parameters.Add(new MySqlParameter("@out", DbType.Int32) { Direction = ParameterDirection.Output });

        // 不再因连接关闭多语句而在客户端被拒绝：驱动用 COM_SET_OPTION 临时开启，由服务端报"过程不存在"
        var ex = await Assert.ThrowsAsync<MySqlException>(() => cmd.ExecuteScalarAsync());
        Assert.DoesNotContain("AllowMultiStatements", ex.Message);

        // 临时开启的多语句能力必须恢复，连接仍可继续使用
        using var verify = new MySqlCommand(conn, "SELECT 1");
        Assert.Equal(1, (await verify.ExecuteScalarAsync()).ToInt());
    }

    [Fact]
    [DisplayName("单条命令的DbBatch在多语句关闭时仍可用")]
    public async Task Batch_SingleCommand_WithoutFlag_Works()
    {
        using var conn = new MySqlConnection(BuildConnStr());
        await conn.OpenAsync();

        using var batch = new MySqlBatch(conn) { Timeout = 30 };
        batch.BatchCommands.Add(new MySqlBatchCommand("SELECT 42"));
        var result = await batch.ExecuteScalarAsync(default);

        Assert.Equal(42, result.ToInt());
    }

    [Fact]
    [DisplayName("多条命令的DbBatch读取器在多语句关闭时明确报不支持")]
    public async Task Batch_MultiCommandReader_WithoutFlag_Throws()
    {
        using var conn = new MySqlConnection(BuildConnStr());
        await conn.OpenAsync();

        using var batch = new MySqlBatch(conn) { Timeout = 30 };
        batch.BatchCommands.Add(new MySqlBatchCommand("SELECT 1"));
        batch.BatchCommands.Add(new MySqlBatchCommand("SELECT 2"));

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => batch.ExecuteReaderAsync());
        Assert.Contains("AllowMultiStatements", ex.Message);
    }
}

/// <summary>多语句默认开启的行为测试。默认值与 MySql.Data 的 AllowBatch 一致，
/// 保证 MySqlBatch 合并执行、存储过程 OUT/INOUT 与 INSERT;SELECT LAST_INSERT_ID() 开箱可用</summary>
[Collection(TestCollections.ReadOnly)]
public class MultiStatementDefaultTests
{
    [Fact]
    [DisplayName("连接串默认开启多语句并协商MULTI_STATEMENTS标志")]
    public void DefaultBuilder_EnablesMultiStatements()
    {
        var builder = new MySqlConnectionStringBuilder("server=127.0.0.1");
        Assert.True(builder.AllowMultiStatements);

        var client = new SqlClient(builder);
        var flags = new Authentication(client).GetFlags(ClientFlags.SECURE_CONNECTION);

        Assert.True(flags.HasFlag(ClientFlags.MULTI_STATEMENTS));
    }

    [Fact]
    [DisplayName("未显式配置时默认连接可直接执行多语句")]
    public async Task DefaultConnection_AllowsMultipleStatements()
    {
        var builder = new MySqlConnectionStringBuilder(DALTests.GetConnStr());
        builder.Remove(nameof(MySqlConnectionStringBuilder.AllowMultiStatements));
        Assert.True(builder.AllowMultiStatements);

        using var conn = new MySqlConnection(builder.ConnectionString);
        await conn.OpenAsync();

        using var cmd = new MySqlCommand(conn, "SELECT 1; SELECT 2");
        Assert.Equal(1, (await cmd.ExecuteScalarAsync()).ToInt());
    }
}
