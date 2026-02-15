using Microsoft.EntityFrameworkCore.Storage;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 数据库创建器。提供数据库创建/删除/存在检查能力</summary>
public class MySqlDatabaseCreator : RelationalDatabaseCreator
{
    private readonly IRelationalConnection _connection;

    /// <summary>实例化</summary>
    /// <param name="dependencies">依赖</param>
    /// <param name="connection">关系型连接</param>
    public MySqlDatabaseCreator(RelationalDatabaseCreatorDependencies dependencies, IRelationalConnection connection)
        : base(dependencies)
    {
        _connection = connection;
    }

    /// <summary>创建数据库</summary>
    public override void Create() => throw new NotSupportedException("请使用 MySQL 命令行或管理工具创建数据库");

    /// <summary>检查数据库是否存在</summary>
    /// <returns></returns>
    public override Boolean Exists()
    {
        try
        {
            _connection.Open();
            _connection.Close();
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>检查是否有表</summary>
    /// <returns></returns>
    public override Boolean HasTables()
    {
        var commandText = "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()";

        using var command = _connection.DbConnection.CreateCommand();
        command.CommandText = commandText;

        _connection.Open();
        try
        {
            var result = command.ExecuteScalar();
            return result != null && Convert.ToInt32(result) > 0;
        }
        finally
        {
            _connection.Close();
        }
    }

    /// <summary>删除数据库</summary>
    public override void Delete() => throw new NotSupportedException("请使用 MySQL 命令行或管理工具删除数据库");
}
