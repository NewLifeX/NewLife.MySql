using System.Data.Entity.Core.Common.CommandTrees;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
using System.Text;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySQL SQL 生成器。将 EF6 命令树转换为 MySQL SQL 语句</summary>
internal class MySqlSqlGenerator
{
    /// <summary>将命令树转换为SQL语句</summary>
    /// <param name="commandTree">EF6 命令树</param>
    /// <returns></returns>
    public SqlResult GenerateSql(DbCommandTree commandTree)
    {
        if (commandTree == null) throw new ArgumentNullException(nameof(commandTree));

        return commandTree switch
        {
            DbQueryCommandTree query => GenerateQuery(query),
            DbInsertCommandTree insert => GenerateInsert(insert),
            DbUpdateCommandTree update => GenerateUpdate(update),
            DbDeleteCommandTree delete => GenerateDelete(delete),
            _ => throw new NotSupportedException($"不支持的命令树类型：{commandTree.GetType().Name}"),
        };
    }

    private SqlResult GenerateQuery(DbQueryCommandTree query)
    {
        var sb = new StringBuilder();
        var parameters = new Dictionary<String, Object?>();

        GenerateExpression(sb, query.Query, parameters);

        return new SqlResult(sb.ToString(), parameters);
    }

    private SqlResult GenerateInsert(DbInsertCommandTree insert)
    {
        var sb = new StringBuilder();
        var parameters = new Dictionary<String, Object?>();
        var target = GetTargetTable(insert.Target);

        sb.Append("INSERT INTO ");
        sb.Append(QuoteIdentifier(target));
        sb.Append(" (");

        var first = true;
        foreach (var clause in insert.SetClauses.Cast<DbSetClause>())
        {
            if (!first) sb.Append(", ");
            first = false;

            if (clause.Property is DbPropertyExpression prop)
                sb.Append(QuoteIdentifier(prop.Property.Name));
        }

        sb.Append(") VALUES (");

        first = true;
        var paramIndex = 0;
        foreach (var clause in insert.SetClauses.Cast<DbSetClause>())
        {
            if (!first) sb.Append(", ");
            first = false;

            var paramName = $"@p{paramIndex++}";
            sb.Append(paramName);

            if (clause.Value is DbConstantExpression constant)
                parameters[paramName] = constant.Value;
            else
                parameters[paramName] = null;
        }

        sb.Append(')');

        // 如果有返回列（如自增ID），附加 SELECT LAST_INSERT_ID()
        if (insert.Returning != null)
            sb.Append("; SELECT LAST_INSERT_ID()");

        return new SqlResult(sb.ToString(), parameters);
    }

    private SqlResult GenerateUpdate(DbUpdateCommandTree update)
    {
        var sb = new StringBuilder();
        var parameters = new Dictionary<String, Object?>();
        var target = GetTargetTable(update.Target);
        var paramIndex = 0;

        sb.Append("UPDATE ");
        sb.Append(QuoteIdentifier(target));
        sb.Append(" SET ");

        var first = true;
        foreach (var clause in update.SetClauses.Cast<DbSetClause>())
        {
            if (!first) sb.Append(", ");
            first = false;

            if (clause.Property is DbPropertyExpression prop)
                sb.Append(QuoteIdentifier(prop.Property.Name));

            sb.Append(" = ");

            var paramName = $"@p{paramIndex++}";
            sb.Append(paramName);

            if (clause.Value is DbConstantExpression constant)
                parameters[paramName] = constant.Value;
            else
                parameters[paramName] = null;
        }

        if (update.Predicate != null)
        {
            sb.Append(" WHERE ");
            GenerateExpression(sb, update.Predicate, parameters);
        }

        return new SqlResult(sb.ToString(), parameters);
    }

    private SqlResult GenerateDelete(DbDeleteCommandTree delete)
    {
        var sb = new StringBuilder();
        var parameters = new Dictionary<String, Object?>();
        var target = GetTargetTable(delete.Target);

        sb.Append("DELETE FROM ");
        sb.Append(QuoteIdentifier(target));

        if (delete.Predicate != null)
        {
            sb.Append(" WHERE ");
            GenerateExpression(sb, delete.Predicate, parameters);
        }

        return new SqlResult(sb.ToString(), parameters);
    }

    private void GenerateExpression(StringBuilder sb, DbExpression expression, Dictionary<String, Object?> parameters)
    {
        switch (expression)
        {
            case DbScanExpression scan:
                var tableName = scan.Target.Table ?? scan.Target.Name;
                sb.Append(QuoteIdentifier(tableName));
                break;

            case DbPropertyExpression prop:
                if (prop.Instance is DbVariableReferenceExpression varRef)
                {
                    sb.Append(QuoteIdentifier(varRef.VariableName));
                    sb.Append('.');
                }
                sb.Append(QuoteIdentifier(prop.Property.Name));
                break;

            case DbConstantExpression constant:
                var paramName = $"@p{parameters.Count}";
                sb.Append(paramName);
                parameters[paramName] = constant.Value;
                break;

            case DbComparisonExpression comparison:
                GenerateExpression(sb, comparison.Left, parameters);
                sb.Append(comparison.ExpressionKind switch
                {
                    DbExpressionKind.Equals => " = ",
                    DbExpressionKind.NotEquals => " <> ",
                    DbExpressionKind.GreaterThan => " > ",
                    DbExpressionKind.GreaterThanOrEquals => " >= ",
                    DbExpressionKind.LessThan => " < ",
                    DbExpressionKind.LessThanOrEquals => " <= ",
                    _ => " = ",
                });
                GenerateExpression(sb, comparison.Right, parameters);
                break;

            case DbAndExpression and:
                sb.Append('(');
                GenerateExpression(sb, and.Left, parameters);
                sb.Append(" AND ");
                GenerateExpression(sb, and.Right, parameters);
                sb.Append(')');
                break;

            case DbOrExpression or:
                sb.Append('(');
                GenerateExpression(sb, or.Left, parameters);
                sb.Append(" OR ");
                GenerateExpression(sb, or.Right, parameters);
                sb.Append(')');
                break;

            case DbNotExpression not:
                sb.Append("NOT (");
                GenerateExpression(sb, not.Argument, parameters);
                sb.Append(')');
                break;

            case DbIsNullExpression isNull:
                GenerateExpression(sb, isNull.Argument, parameters);
                sb.Append(" IS NULL");
                break;

            case DbProjectExpression project:
                sb.Append("SELECT ");
                GenerateExpression(sb, project.Projection, parameters);
                sb.Append(" FROM ");
                GenerateExpression(sb, project.Input.Expression, parameters);
                sb.Append(" AS ");
                sb.Append(QuoteIdentifier(project.Input.VariableName));
                break;

            case DbFilterExpression filter:
                GenerateExpression(sb, filter.Input.Expression, parameters);
                sb.Append(" AS ");
                sb.Append(QuoteIdentifier(filter.Input.VariableName));
                sb.Append(" WHERE ");
                GenerateExpression(sb, filter.Predicate, parameters);
                break;

            case DbNewInstanceExpression newInstance:
                var firstCol = true;
                foreach (var arg in newInstance.Arguments)
                {
                    if (!firstCol) sb.Append(", ");
                    firstCol = false;
                    GenerateExpression(sb, arg, parameters);
                }
                break;

            case DbVariableReferenceExpression varRefExpr:
                sb.Append(QuoteIdentifier(varRefExpr.VariableName));
                break;

            case DbNullExpression:
                sb.Append("NULL");
                break;

            case DbCastExpression cast:
                sb.Append("CAST(");
                GenerateExpression(sb, cast.Argument, parameters);
                sb.Append(" AS ");
                sb.Append(GetMySqlTypeName(cast.ResultType));
                sb.Append(')');
                break;

            case DbLimitExpression limit:
                GenerateExpression(sb, limit.Argument, parameters);
                sb.Append(" LIMIT ");
                GenerateExpression(sb, limit.Limit, parameters);
                break;

            case DbSortExpression sort:
                GenerateExpression(sb, sort.Input.Expression, parameters);
                sb.Append(" ORDER BY ");
                var firstSort = true;
                foreach (var key in sort.SortOrder)
                {
                    if (!firstSort) sb.Append(", ");
                    firstSort = false;
                    GenerateExpression(sb, key.Expression, parameters);
                    if (!key.Ascending) sb.Append(" DESC");
                }
                break;

            default:
                sb.Append(expression.ToString());
                break;
        }
    }

    private static String GetTargetTable(DbExpressionBinding target)
    {
        if (target.Expression is DbScanExpression scan)
            return scan.Target.Table ?? scan.Target.Name;

        return target.VariableName;
    }

    private static String GetMySqlTypeName(System.Data.Entity.Core.Metadata.Edm.TypeUsage typeUsage)
    {
        if (typeUsage.EdmType is System.Data.Entity.Core.Metadata.Edm.PrimitiveType pt)
        {
            return pt.PrimitiveTypeKind switch
            {
                System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Int32 => "SIGNED",
                System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Int64 => "SIGNED",
                System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Decimal => "DECIMAL",
                System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Double => "DECIMAL",
                System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.String => "CHAR",
                System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.DateTime => "DATETIME",
                _ => "CHAR",
            };
        }
        return "CHAR";
    }

    /// <summary>使用反引号包裹标识符</summary>
    /// <param name="identifier">标识符</param>
    /// <returns></returns>
    internal static String QuoteIdentifier(String identifier) => $"`{identifier}`";
}

/// <summary>SQL 生成结果</summary>
internal class SqlResult
{
    /// <summary>SQL 命令文本</summary>
    public String CommandText { get; }

    /// <summary>参数集合</summary>
    public Dictionary<String, Object?> Parameters { get; }

    /// <summary>实例化</summary>
    /// <param name="commandText">SQL命令文本</param>
    /// <param name="parameters">参数集合</param>
    public SqlResult(String commandText, Dictionary<String, Object?> parameters)
    {
        CommandText = commandText;
        Parameters = parameters;
    }
}
