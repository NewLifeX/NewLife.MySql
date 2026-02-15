using System;
using System.Text;
using Microsoft.EntityFrameworkCore.Update;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql UPDATE/INSERT/DELETE SQL 生成器。生成 MySQL 方言的 DML 语句</summary>
public class MySqlUpdateSqlGenerator : UpdateSqlGenerator
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">更新 SQL 生成器依赖</param>
    public MySqlUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies) : base(dependencies) { }

#if NET6_0
    /// <summary>追加自增标识条件</summary>
    /// <param name="commandStringBuilder">SQL 构建器</param>
    /// <param name="columnModification">列修改</param>
    protected override void AppendIdentityWhereCondition(StringBuilder commandStringBuilder, IColumnModification columnModification)
    {
        if (commandStringBuilder == null) throw new ArgumentNullException(nameof(commandStringBuilder));
        if (columnModification == null) throw new ArgumentNullException(nameof(columnModification));

        commandStringBuilder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(columnModification.ColumnName));
        commandStringBuilder.Append(" = ");
        commandStringBuilder.Append("LAST_INSERT_ID()");
    }

    /// <summary>追加影响行数条件</summary>
    /// <param name="commandStringBuilder">SQL 构建器</param>
    /// <param name="expectedRowsAffected">期望影响行数</param>
    protected override void AppendRowsAffectedWhereCondition(StringBuilder commandStringBuilder, Int32 expectedRowsAffected)
    {
        if (commandStringBuilder == null) throw new ArgumentNullException(nameof(commandStringBuilder));

        commandStringBuilder.Append("ROW_COUNT() = ");
        commandStringBuilder.Append(expectedRowsAffected);
    }
#endif
}
