using Microsoft.EntityFrameworkCore.Storage;
using NewLife;
using NewLife.MySql;

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

    /// <summary>创建数据库。连接服务器执行 CREATE DATABASE，支持 EnsureCreated</summary>
    public override void Create()
    {
        var dbName = GetDatabaseName();
        if (dbName.IsNullOrEmpty()) throw new InvalidOperationException("连接字符串未指定数据库名，无法创建");

        // 连接服务器（information_schema）执行 CREATE DATABASE，避免连接不存在的库失败
        using var conn = CreateServerConnection();
        using var command = conn.CreateCommand();
        command.CommandText = $"CREATE DATABASE `{dbName}` CHARACTER SET utf8mb4";
        command.ExecuteNonQuery();
    }

    /// <summary>检查数据库是否存在</summary>
    /// <returns></returns>
    /// <remarks>查询 information_schema.SCHEMATA 判断目标库是否存在，避免连接池复用已建连接导致误判（DROP DATABASE 不中断既有连接）</remarks>
    public override Boolean Exists()
    {
        var dbName = GetDatabaseName();
        if (dbName.IsNullOrEmpty()) return false;

        try
        {
            using var conn = CreateServerConnection();
            using var command = conn.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = @name";
            var p = command.CreateParameter();
            p.ParameterName = "@name";
            p.Value = dbName;
            command.Parameters.Add(p);

            var result = command.ExecuteScalar();
            return result != null && Convert.ToInt32(result) > 0;
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

    /// <summary>删除数据库。连接服务器执行 DROP DATABASE</summary>
    public override void Delete()
    {
        var dbName = GetDatabaseName();
        if (dbName.IsNullOrEmpty()) throw new InvalidOperationException("连接字符串未指定数据库名，无法删除");

        using var conn = CreateServerConnection();
        using var command = conn.CreateCommand();
        command.CommandText = $"DROP DATABASE IF EXISTS `{dbName}`";
        command.ExecuteNonQuery();
    }

    /// <summary>创建到服务器（information_schema）的连接，用于执行 CREATE/DROP DATABASE</summary>
    /// <returns>已打开的连接</returns>
    private MySqlConnection CreateServerConnection()
    {
        var builder = new MySqlConnectionStringBuilder(_connection.ConnectionString ?? "") { Database = "information_schema" };
        var conn = new MySqlConnection(builder.ConnectionString);
        conn.Open();
        return conn;
    }

    /// <summary>从连接字符串提取数据库名</summary>
    /// <returns>数据库名，未指定时返回空串</returns>
    private String GetDatabaseName()
    {
        var builder = new MySqlConnectionStringBuilder(_connection.ConnectionString ?? "");
        return builder.Database ?? "";
    }
}
