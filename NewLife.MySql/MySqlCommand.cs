using System.Data;
using System.Data.Common;
using NewLife.Data;

namespace NewLife.MySql;

/// <summary>命令</summary>
public class MySqlCommand : DbCommand, IDisposable
{
    #region 属性
    private MySqlConnection _DbConnection;
    /// <summary>连接</summary>
    protected override DbConnection DbConnection { get => _DbConnection; set => _DbConnection = (value as MySqlConnection)!; }

    /// <summary>命令语句</summary>
    public override String CommandText { get; set; }

    /// <summary>命令类型</summary>
    public override CommandType CommandType { get; set; }

    /// <summary>事务</summary>
    protected override DbTransaction DbTransaction { get; set; }

    /// <summary>参数集合</summary>
    protected override DbParameterCollection DbParameterCollection { get; }

    /// <summary>命令语句</summary>
    public override Int32 CommandTimeout { get; set; }

    /// <summary>设计时可见</summary>
    public override Boolean DesignTimeVisible { get; set; }

    /// <summary>更新行方式</summary>
    public override UpdateRowSource UpdatedRowSource { get; set; }
    #endregion

    #region 构造
    /// <summary>实例化</summary>
    public MySqlCommand() { }

    /// <summary>实例化</summary>
    /// <param name="conn"></param>
    /// <param name="commandText"></param>
    public MySqlCommand(DbConnection conn, String commandText)
    {
        Connection = conn;
        CommandText = commandText;
    }
    #endregion

    #region 方法
    /// <summary>创建参数</summary>
    /// <returns></returns>
    protected override DbParameter CreateDbParameter() => MySqlClientFactory.Instance.CreateParameter();

    /// <summary>执行读取器</summary>
    /// <param name="behavior"></param>
    /// <returns></returns>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var sql = CommandText;
        if (sql.IsNullOrEmpty()) throw new ArgumentNullException(nameof(CommandText));

        var conn = _DbConnection;
        var client = conn.Client;

        if (CommandType == CommandType.TableDirect) sql = "Select * From " + sql;

        // 设置行为参数
        if (behavior.HasFlag(CommandBehavior.SchemaOnly))
            new MySqlCommand(conn, "SET SQL_SELECT_LIMIT=0").ExecuteNonQuery();
        else if (behavior.HasFlag(CommandBehavior.SingleRow))
            new MySqlCommand(conn, "SET SQL_SELECT_LIMIT=1").ExecuteNonQuery();

        // 执行读取器
        var reader = new MySqlDataReader
        {
            Command = this
        };
        Execute();
        reader.NextResult();

        return reader;
    }

    /// <summary>执行并返回影响行数</summary>
    /// <returns></returns>
    public override Int32 ExecuteNonQuery()
    {
        using var reader = ExecuteReader();
        reader.Close();
        return reader.RecordsAffected;
    }

    /// <summary>执行并返回第一行</summary>
    /// <returns></returns>
    public override Object ExecuteScalar()
    {
        using var reader = ExecuteReader();
        if (reader.Read()) return reader.GetValue(0);

        return null;
    }

    /// <summary>预编译语句</summary>
    public override void Prepare() => throw new NotImplementedException();

    /// <summary>取消</summary>
    public override void Cancel() { }
    #endregion

    #region 执行
    /// <summary>执行命令，绑定参数后发送请求</summary>
    private void Execute()
    {
        var ms = new MemoryStream();
        ms.Seek(4, SeekOrigin.Current);

        BindParameter(ms);

        ms.Position = 4;
        var pk = new ArrayPacket(ms);

        var client = _DbConnection.Client!;
        client.SendQuery(pk);
    }

    private void BindParameter(Stream ms)
    {
        // 一个字节的查询类型
        ms.WriteByte(0x00);

        ms.Write(CommandText.GetBytes());
    }
    #endregion
}