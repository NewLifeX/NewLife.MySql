using System.Data;
using System.Data.Common;

namespace NewLife.MySql;

sealed class MySqlTransaction(MySqlConnection connection, IsolationLevel isolationLevel) : DbTransaction
{
    public override IsolationLevel IsolationLevel { get; } = isolationLevel;

    protected override DbConnection DbConnection => connection;

    private Boolean _open = true;
    private Boolean _disposed;

    ~MySqlTransaction()
    {
        Dispose(disposing: false);
    }

    protected override void Dispose(Boolean disposing)
    {
        if (!_disposed)
        {
            base.Dispose(disposing);

            if (disposing && _open && Connection != null && Connection.State == ConnectionState.Open)
                Rollback();

            _disposed = true;
        }
    }

    public override void Commit()
    {
        var conn = connection;
        if (conn == null || conn.State != ConnectionState.Open) throw new ObjectDisposedException(nameof(Connection));
        if (!_open) throw new InvalidOperationException("Transaction has already been committed or is not pending");

        conn.ExecuteNonQuery("COMMIT");

        _open = false;
    }

    public override void Rollback()
    {
        var conn = connection;
        if (conn == null || conn.State != ConnectionState.Open) throw new ObjectDisposedException(nameof(Connection));
        if (!_open) throw new InvalidOperationException("Transaction has already been committed or is not pending");

        conn.ExecuteNonQuery("ROLLBACK");

        _open = false;
    }
}