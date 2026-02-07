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
        if (!_open) throw new InvalidOperationException("Transaction has already been committed or is not pending");

        conn.ExecuteNonQuery("COMMIT");

        _open = false;
    }

    /// <summary>回滚事务</summary>
    public override void Rollback()
    {
        var conn = connection;
        if (conn == null || conn.State != ConnectionState.Open) throw new ObjectDisposedException(nameof(Connection));
        if (!_open) throw new InvalidOperationException("Transaction has already been committed or is not pending");

        conn.ExecuteNonQuery("ROLLBACK");

        _open = false;
    }
}