using Microsoft.EntityFrameworkCore.Migrations;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 迁移 SQL 生成器。将 EF Core 迁移操作转换为 MySQL DDL 语句</summary>
public class MySqlMigrationsSqlGenerator : MigrationsSqlGenerator
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">迁移 SQL 生成器依赖</param>
    public MySqlMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies) : base(dependencies) { }
}
