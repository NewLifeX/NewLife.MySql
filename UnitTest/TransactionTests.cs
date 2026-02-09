using System.Data;
using NewLife;
using NewLife.MySql;

namespace UnitTest;

public class TransactionTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    [Fact]
    public void CommitTransaction()
    {
        var name = "test_tx_commit";
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        conn.ExecuteNonQuery($"delete from sys.sys_config where variable='{name}'");

        // 事务内插入并提交
        using (var tr = conn.BeginTransaction())
        {
            var sql = $"insert into sys.sys_config(variable,value,set_time,set_by) values('{name}','v1',now(),'Test')";
            conn.ExecuteNonQuery(sql);
            tr.Commit();
        }

        // 验证数据已存在
        using var cmd = new MySqlCommand(conn, $"select count(*) from sys.sys_config where variable='{name}'");
        Assert.Equal(1, cmd.ExecuteScalar().ToInt());

        // 清理
        conn.ExecuteNonQuery($"delete from sys.sys_config where variable='{name}'");
    }

    [Fact]
    public void RollbackTransaction()
    {
        var name = "test_tx_rollback";
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        conn.ExecuteNonQuery($"delete from sys.sys_config where variable='{name}'");

        // 事务内插入并回滚
        using (var tr = conn.BeginTransaction())
        {
            var sql = $"insert into sys.sys_config(variable,value,set_time,set_by) values('{name}','v1',now(),'Test')";
            conn.ExecuteNonQuery(sql);
            tr.Rollback();
        }

        // 验证数据不存在
        using var cmd = new MySqlCommand(conn, $"select count(*) from sys.sys_config where variable='{name}'");
        Assert.Equal(0, cmd.ExecuteScalar().ToInt());
    }

    [Fact]
    public void DisposeAutoRollback()
    {
        var name = "test_tx_autorollback";
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        conn.ExecuteNonQuery($"delete from sys.sys_config where variable='{name}'");

        // 事务内插入，不提交也不显式回滚，dispose 应自动回滚
        using (var tr = conn.BeginTransaction())
        {
            var sql = $"insert into sys.sys_config(variable,value,set_time,set_by) values('{name}','v1',now(),'Test')";
            conn.ExecuteNonQuery(sql);
            // 不调用 Commit 或 Rollback，Dispose 应自动回滚
        }

        // 验证数据不存在
        using var cmd = new MySqlCommand(conn, $"select count(*) from sys.sys_config where variable='{name}'");
        Assert.Equal(0, cmd.ExecuteScalar().ToInt());
    }

    [Fact]
    public void CommitThenThrowOnSecondCommit()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        using var tr = conn.BeginTransaction();
        tr.Commit();

        // 第二次提交应抛出异常
        Assert.Throws<InvalidOperationException>(() => tr.Commit());
    }

    [Fact]
    public void RollbackThenThrowOnSecondRollback()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        using var tr = conn.BeginTransaction();
        tr.Rollback();

        // 第二次回滚应抛出异常
        Assert.Throws<InvalidOperationException>(() => tr.Rollback());
    }

    [Fact]
    public void TransactionIsolationLevel()
    {
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        Assert.Equal(IsolationLevel.ReadCommitted, tr.IsolationLevel);
        tr.Rollback();
    }

    [Fact]
    public void TransactionWithParameterizedCommand()
    {
        var name = "test_tx_param";
        using var conn = new MySqlConnection(_ConnStr);
        conn.Open();

        // 清理
        conn.ExecuteNonQuery($"delete from sys.sys_config where variable='{name}'");

        // 事务内参数化插入并提交
        using (var tr = conn.BeginTransaction())
        {
            using var cmd = new MySqlCommand(conn, "insert into sys.sys_config(variable,value,set_time,set_by) values(@var,@val,now(),@by)");
            var ps = cmd.Parameters as MySqlParameterCollection;
            ps.AddWithValue("var", name);
            ps.AddWithValue("val", "tx_value");
            ps.AddWithValue("by", "Test");
            cmd.ExecuteNonQuery();
            tr.Commit();
        }

        // 验证
        {
            using var cmd = new MySqlCommand(conn, "select value from sys.sys_config where variable=@name");
            (cmd.Parameters as MySqlParameterCollection).AddWithValue("name", name);
            Assert.Equal("tx_value", cmd.ExecuteScalar());
        }

        // 清理
        conn.ExecuteNonQuery($"delete from sys.sys_config where variable='{name}'");
    }
}
