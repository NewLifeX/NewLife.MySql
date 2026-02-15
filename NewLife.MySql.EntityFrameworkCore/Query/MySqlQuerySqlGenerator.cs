using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 查询 SQL 生成器。生成 MySQL 方言的查询 SQL（如 LIMIT/OFFSET）</summary>
public class MySqlQuerySqlGenerator : QuerySqlGenerator
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">查询 SQL 生成器依赖</param>
    public MySqlQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies) : base(dependencies) { }

    /// <summary>生成 MySQL 的 LIMIT/OFFSET 分页子句</summary>
    /// <param name="selectExpression">查询表达式</param>
    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        if (selectExpression == null) throw new ArgumentNullException(nameof(selectExpression));

        if (selectExpression.Limit != null)
        {
            Sql.AppendLine().Append("LIMIT ");
            Visit(selectExpression.Limit);
        }

        if (selectExpression.Offset != null)
        {
            // MySQL 要求 LIMIT 必须在 OFFSET 之前
            // 如果只有 OFFSET 没有 LIMIT，使用一个极大值作为 LIMIT
            if (selectExpression.Limit == null)
            {
                Sql.AppendLine().Append("LIMIT 18446744073709551615");
            }

            Sql.Append(" OFFSET ");
            Visit(selectExpression.Offset);
        }
    }
}
