using System.Data;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

[Collection(TestCollections.DataModification)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class TransactionTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public TransactionTests()
    {
        _table = "tx_test_" + Rand.Next(10000);
        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();
        var sql = $"CREATE TABLE IF NOT EXISTS `{_table}` (`variable` VARCHAR(128) NOT NULL PRIMARY KEY, `value` VARCHAR(1024) DEFAULT NULL, `set_time` DATETIME DEFAULT CURRENT_TIMESTAMP, `set_by` VARCHAR(128) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        _conn.ExecuteNonQuery(sql);
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    [Fact]
    public void CommitTransaction()
    {
        var name = "test_tx_commit";

        // 清理
        _conn.ExecuteNonQuery($"delete from `{_table}` where variable='{name}'");

        // 事务内插入并提交
        using (var tr = _conn.BeginTransaction())
        {
            var sql = $"insert into `{_table}`(variable,value,set_time,set_by) values('{name}','v1',now(),'Test')";
            _conn.ExecuteNonQuery(sql);
            tr.Commit();
        }

        // 验证数据已存在
        using var cmd = new MySqlCommand(_conn, $"select count(*) from `{_table}` where variable='{name}'");
        Assert.Equal(1, cmd.ExecuteScalar().ToInt());

        // 清理
        _conn.ExecuteNonQuery($"delete from `{_table}` where variable='{name}'");
    }

    [Fact]
    public void RollbackTransaction()
    {
        var name = "test_tx_rollback";

        // 清理
        _conn.ExecuteNonQuery($"delete from `{_table}` where variable='{name}'");

        // 事务内插入并回滚
        using (var tr = _conn.BeginTransaction())
        {
            var sql = $"insert into `{_table}`(variable,value,set_time,set_by) values('{name}','v1',now(),'Test')";
            _conn.ExecuteNonQuery(sql);
            tr.Rollback();
        }

        // 验证数据不存在
        using var cmd = new MySqlCommand(_conn, $"select count(*) from `{_table}` where variable='{name}'");
        Assert.Equal(0, cmd.ExecuteScalar().ToInt());
    }

    [Fact]
    public void DisposeAutoRollback()
    {
        var name = "test_tx_autorollback";

        // 清理
        _conn.ExecuteNonQuery($"delete from `{_table}` where variable='{name}'");

        // 事务内插入，不提交也不显式回滚，dispose 应自动回滚
        using (var tr = _conn.BeginTransaction())
        {
            var sql = $"insert into `{_table}`(variable,value,set_time,set_by) values('{name}','v1',now(),'Test')";
            _conn.ExecuteNonQuery(sql);
            // 不调用 Commit 或 Rollback，Dispose 应自动回滚
        }

        // 验证数据不存在
        using var cmd = new MySqlCommand(_conn, $"select count(*) from `{_table}` where variable='{name}'");
        Assert.Equal(0, cmd.ExecuteScalar().ToInt());
    }

    [Fact]
    public void CommitThenThrowOnSecondCommit()
    {
        using var tr = _conn.BeginTransaction();
        tr.Commit();

        // 第二次提交应抛出异常
        Assert.Throws<InvalidOperationException>(() => tr.Commit());
    }

    [Fact]
    public void RollbackThenThrowOnSecondRollback()
    {
        using var tr = _conn.BeginTransaction();
        tr.Rollback();

        // 第二次回滚应抛出异常
        Assert.Throws<InvalidOperationException>(() => tr.Rollback());
    }

    [Fact]
    public void TransactionIsolationLevel()
    {
        using var tr = _conn.BeginTransaction(IsolationLevel.ReadCommitted);
        Assert.Equal(IsolationLevel.ReadCommitted, tr.IsolationLevel);
        tr.Rollback();
    }

    [Fact]
    public void TransactionWithParameterizedCommand()
    {
        var name = "test_tx_param";

        // 清理
        _conn.ExecuteNonQuery($"delete from `{_table}` where variable='{name}'");

        // 事务内参数化插入并提交
        using (var tr = _conn.BeginTransaction())
        {
            using var cmd = new MySqlCommand(_conn, $"insert into `{_table}`(variable,value,set_time,set_by) values(@var,@val,now(),@by)");
            var ps = cmd.Parameters as MySqlParameterCollection;
            ps.AddWithValue("var", name);
            ps.AddWithValue("val", "tx_value");
            ps.AddWithValue("by", "Test");
            cmd.ExecuteNonQuery();
            tr.Commit();
        }

        // 验证
        {
            using var cmd = new MySqlCommand(_conn, $"select value from `{_table}` where variable=@name");
            (cmd.Parameters as MySqlParameterCollection).AddWithValue("name", name);
            Assert.Equal("tx_value", cmd.ExecuteScalar());
        }

        // 清理
        _conn.ExecuteNonQuery($"delete from `{_table}` where variable='{name}'");
    }
}
