using NewLife.MySql;

namespace UnitTest;

/// <summary>SqlClient.SetDatabaseAsync 方法测试</summary>
public class SetDatabaseAsyncTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    [Fact(DisplayName = "SetDatabaseAsync - 成功切换数据库")]
    public async Task SetDatabaseAsync_Success()
    {
        using var conn = new MySqlConnection(_ConnStr);
        await conn.OpenAsync();

        var client = conn.Client;
        Assert.NotNull(client);

        // 获取原始数据库
        using var cmd1 = conn.CreateCommand();
        cmd1.CommandText = "SELECT DATABASE()";
        var originalDb = cmd1.ExecuteScalar()?.ToString();
        Assert.Equal("sys", originalDb);

        // 使用 SetDatabaseAsync 切换到 information_schema
        await client!.SetDatabaseAsync("information_schema");

        // 验证切换成功
        using var cmd2 = conn.CreateCommand();
        cmd2.CommandText = "SELECT DATABASE()";
        var newDb = cmd2.ExecuteScalar()?.ToString();
        Assert.Equal("information_schema", newDb);

        // 切换回原数据库
        await client.SetDatabaseAsync(originalDb!);

        // 验证切换回原数据库
        using var cmd3 = conn.CreateCommand();
        cmd3.CommandText = "SELECT DATABASE()";
        var db3 = cmd3.ExecuteScalar()?.ToString();
        Assert.Equal(originalDb, db3);
    }

    [Fact(DisplayName = "SetDatabaseAsync - 空数据库名抛出异常")]
    public async Task SetDatabaseAsync_NullDatabase()
    {
        using var conn = new MySqlConnection(_ConnStr);
        await conn.OpenAsync();

        var client = conn.Client!;

        await Assert.ThrowsAsync<ArgumentNullException>(() => client.SetDatabaseAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.SetDatabaseAsync(""));
    }

    [Fact(DisplayName = "SetDatabaseAsync - 无效数据库名抛出异常")]
    public async Task SetDatabaseAsync_InvalidDatabase()
    {
        using var conn = new MySqlConnection(_ConnStr);
        await conn.OpenAsync();

        var client = conn.Client!;

        await Assert.ThrowsAsync<MySqlException>(() => client.SetDatabaseAsync("nonexistent_database_12345"));
    }

    [Fact(DisplayName = "SetDatabaseAsync - 连接保持打开状态")]
    public async Task SetDatabaseAsync_ConnectionStaysOpen()
    {
        using var conn = new MySqlConnection(_ConnStr);
        await conn.OpenAsync();

        var client = conn.Client!;
        var originalClient = client;

        // 切换数据库
        await client.SetDatabaseAsync("information_schema");

        // 验证连接仍然打开且是同一个Client对象
        Assert.Same(originalClient, conn.Client);
        Assert.True(client.Active);

        // 验证可以继续执行命令
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables";
        var count = cmd.ExecuteScalar();
        Assert.NotNull(count);
    }
}
