using System.Text;
using Microsoft.EntityFrameworkCore.Update;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql UPDATE/INSERT/DELETE SQL 生成器。生成 MySQL 方言的 DML 语句</summary>
public class MySqlUpdateSqlGenerator : UpdateSqlGenerator
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">更新 SQL 生成器依赖</param>
    public MySqlUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies) : base(dependencies) { }
}
