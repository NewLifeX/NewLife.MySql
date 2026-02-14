using System.Data.Common;
using Microsoft.EntityFrameworkCore.Storage;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 关系型数据库连接。提供 EF Core 所需的数据库连接抽象</summary>
public class MySqlRelationalConnection : RelationalConnection
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">关系型连接依赖</param>
    public MySqlRelationalConnection(RelationalConnectionDependencies dependencies) : base(dependencies) { }

    /// <summary>创建数据库连接</summary>
    /// <returns></returns>
    protected override DbConnection CreateDbConnection() => new MySqlConnection(ConnectionString ?? "");
}
