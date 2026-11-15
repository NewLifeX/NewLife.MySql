using System.ComponentModel;
using NewLife.MySql;

namespace UnitTest;

/// <summary>对象在状态无效时的异常契约。驱动此前在多处使用空引用断言（<c>!</c>），
/// 未打开连接、连接已关闭等场景会抛 NullReferenceException，调用方无法区分编程错误与环境问题</summary>
public class InvalidStateTests
{
    [Fact]
    [DisplayName("未设置连接的命令执行时抛出明确异常")]
    public void CommandWithoutConnection()
    {
        using var cmd = new MySqlCommand { CommandText = "SELECT 1" };

        var ex = Assert.Throws<InvalidOperationException>(() => cmd.ExecuteScalar());
        Assert.IsType<InvalidOperationException>(ex);
    }

    [Fact]
    [DisplayName("连接关闭后读取器读取抛出明确异常")]
    public void ReaderAfterConnectionClosed()
    {
        using var conn = new MySqlConnection(DALTests.GetConnStr());
        conn.Open();

        using var cmd = new MySqlCommand(conn, "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3");
        var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        // 模拟误用：连接先被关闭，读取器仍在读取
        conn.Close();

        var ex = Assert.Throws<InvalidOperationException>(() => reader.Read());
        Assert.IsType<InvalidOperationException>(ex);
    }

    [Fact]
    [DisplayName("CommandBehavior.CloseConnection 在读取器关闭时关闭连接")]
    public void CloseConnectionBehavior()
    {
        var conn = new MySqlConnection(DALTests.GetConnStr());
        conn.Open();

        using (var cmd = new MySqlCommand(conn, "SELECT 1"))
        using (var reader = cmd.ExecuteReader(System.Data.CommandBehavior.CloseConnection))
        {
            Assert.True(reader.Read());
            Assert.Equal(System.Data.ConnectionState.Open, conn.State);
        }

        // 读取器释放后连接随之关闭
        Assert.Equal(System.Data.ConnectionState.Closed, conn.State);
        conn.Dispose();
    }
}
