using System.ComponentModel;
using System.Data;
using NewLife;
using NewLife.MySql;
using NewLife.MySql.Messages;

namespace UnitTest;

/// <summary>多语句显式关闭时的行为测试。连接串设置 AllowMultiStatements=false 后驱动不协商 CLIENT_MULTI_STATEMENTS，
/// 服务端拒绝堆叠语句；默认开启由连接串与握手逻辑覆盖</summary>
[Collection(TestCollections.ReadOnly)]
public class MultiStatementDisabledTests
{
    /// <summary>构造显式关闭多语句的连接串（测试库连接串可能显式开启）</summary>
    /// <returns></returns>
    private static String BuildConnStr() => new MySqlConnectionStringBuilder(DALTests.GetConnStr()) { AllowMultiStatements = false }.ConnectionString;

    [Fact]
    [DisplayName("关闭多语句时执行多语句报语法错误")]
    public async Task MultiStatement_WithoutFlag_Throws()
    {
        using var conn = new MySqlConnection(BuildConnStr());
        await conn.OpenAsync();

        using var cmd = new MySqlCommand(conn, "SELECT 1; SELECT 2");
        await Assert.ThrowsAsync<MySqlException>(() => cmd.ExecuteScalarAsync());
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
    [DisplayName("存储过程OUT参数未开启多语句时给出明确提示")]
    public async Task StoredProcedureOutParam_WithoutFlag_Throws()
    {
        using var conn = new MySqlConnection(BuildConnStr());
        await conn.OpenAsync();

        using var cmd = new MySqlCommand(conn, "sp_no_such_proc") { CommandType = CommandType.StoredProcedure };
        cmd.Parameters.Add(new MySqlParameter("@out", DbType.Int32) { Direction = ParameterDirection.Output });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteScalarAsync());
        Assert.Contains("AllowMultiStatements", ex.Message);
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
