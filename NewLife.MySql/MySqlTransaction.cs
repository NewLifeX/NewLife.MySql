using System.Data;
using System.Data.Common;

namespace NewLife.MySql;

/// <summary>MySql事务</summary>
sealed class MySqlTransaction(MySqlConnection connection, IsolationLevel isolationLevel) : DbTransaction
{
    /// <summary>隔离级别</summary>
    public override IsolationLevel IsolationLevel { get; } = isolationLevel;

    /// <summary>数据库连接</summary>
    protected override DbConnection DbConnection => connection;

    private Boolean _open = true;
    private Boolean _disposed;

    /// <summary>销毁</summary>
    /// <param name="disposing"></param>
    protected override void Dispose(Boolean disposing)
    {
        if (!_disposed)
        {
            base.Dispose(disposing);

            if (disposing && _open && Connection != null && Connection.State == ConnectionState.Open)
            {
                try { Rollback(); } catch { }
            }

            _disposed = true;
        }
    }

    /// <summary>提交事务</summary>
    public override void Commit()
    {
        var conn = connection;
        if (conn == null || conn.State != ConnectionState.Open) throw new ObjectDisposedException(nameof(Connection));
        if (!_open) throw new InvalidOperationException(L.T("Transaction has already been committed or is not pending."));

        using var span = MySqlDiagnostics.StartTransaction(conn, "Commit");
        try
        {
            // 内部 COMMIT 语句不产生 Execute 跨度，避免与 Commit 跨度重复计数
            using var cmd = new MySqlCommand(conn, "COMMIT") { SuppressTracing = true };
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            // COMMIT 报错时服务端已中止或结果未知，事务不能再当作待提交态对外提供
            _open = false;
            span.SetError(ex);
            throw;
        }

        _open = false;
    }

    /// <summary>回滚事务</summary>
    public override void Rollback()
    {
        var conn = connection;
        if (conn == null || conn.State != ConnectionState.Open) throw new ObjectDisposedException(nameof(Connection));
        if (!_open) throw new InvalidOperationException(L.T("Transaction has already been committed or is not pending."));

        using var span = MySqlDiagnostics.StartTransaction(conn, "Rollback");
        try
        {
            // 内部 ROLLBACK 语句不产生 Execute 跨度，避免与 Rollback 跨度重复计数
            using var cmd = new MySqlCommand(conn, "ROLLBACK") { SuppressTracing = true };
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            // ROLLBACK 报错（典型为连接已断）时事务不再可用，避免调用方误以为可重试
            _open = false;
            span.SetError(ex);
            throw;
        }

        _open = false;
    }
}