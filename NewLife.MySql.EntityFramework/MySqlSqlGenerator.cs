using System.Data.Entity.Core.Common.CommandTrees;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
using System.Data.Entity.Core.Metadata.Edm;
using System.Text;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySQL SQL 生成器。将 EF6 命令树转换为 MySQL SQL 语句</summary>
internal class MySqlSqlGenerator
{
    #region 属性
    /// <summary>参数索引计数器，用于生成唯一的参数名</summary>
    private Int32 _paramIndex;
    #endregion

    #region 方法
    /// <summary>将命令树转换为SQL语句</summary>
    /// <param name="commandTree">EF6 命令树</param>
    /// <returns></returns>
    public SqlResult GenerateSql(DbCommandTree commandTree)
    {
        if (commandTree == null) throw new ArgumentNullException(nameof(commandTree));

        _paramIndex = 0;

        return commandTree switch
        {
            DbQueryCommandTree query => GenerateQuery(query),
            DbInsertCommandTree insert => GenerateInsert(insert),
            DbUpdateCommandTree update => GenerateUpdate(update),
            DbDeleteCommandTree delete => GenerateDelete(delete),
            DbFunctionCommandTree function => GenerateFunction(function),
            _ => throw new NotSupportedException($"不支持的命令树类型：{commandTree.GetType().Name}"),
        };
    }

    private SqlResult GenerateQuery(DbQueryCommandTree query)
    {
        var sb = new StringBuilder();
        var parameters = new Dictionary<String, Object?>();

        GenerateExpression(sb, query.Query, parameters);

        // 将查询参数也加入字典
        foreach (var param in query.Parameters)
        {
            if (!parameters.ContainsKey("@" + param.Key))
                parameters["@" + param.Key] = null;
        }

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
        foreach (var clause in insert.SetClauses.Cast<DbSetClause>())
        {
            if (!first) sb.Append(", ");
            first = false;

            var paramName = $"@p{_paramIndex++}";
            sb.Append(paramName);

            if (clause.Value is DbConstantExpression constant)
                parameters[paramName] = constant.Value;
            else if (clause.Value is DbNullExpression)
                parameters[paramName] = DBNull.Value;
            else
                parameters[paramName] = null;
        }

        sb.Append(')');

        // 如果有返回列（如自增ID），生成对应的 SELECT 语句
        if (insert.Returning != null)
            GenerateInsertReturning(sb, insert, target);

        return new SqlResult(sb.ToString(), parameters);
    }

    /// <summary>生成 INSERT 语句的返回列查询</summary>
    private static void GenerateInsertReturning(StringBuilder sb, DbInsertCommandTree insert, String target)
    {
        // 收集返回列名
        var returningColumns = new List<String>();
        CollectReturningColumns(insert.Returning, returningColumns);

        if (returningColumns.Count == 0)
        {
            sb.Append("; SELECT LAST_INSERT_ID()");
            return;
        }

        // 判断是否包含自增列（通过 SetClauses 中未包含的列来推断）
        var setCols = new HashSet<String>(StringComparer.OrdinalIgnoreCase);
        foreach (var clause in insert.SetClauses.Cast<DbSetClause>())
        {
            if (clause.Property is DbPropertyExpression prop)
                setCols.Add(prop.Property.Name);
        }

        // 构造 SELECT 返回列，自增列用 LAST_INSERT_ID()，其他用列名
        sb.Append("; SELECT ");
        var first = true;
        foreach (var col in returningColumns)
        {
            if (!first) sb.Append(", ");
            first = false;

            if (!setCols.Contains(col))
                sb.Append($"LAST_INSERT_ID() AS {QuoteIdentifier(col)}");
            else
                sb.Append(QuoteIdentifier(col));
        }
        sb.Append(" FROM ");
        sb.Append(QuoteIdentifier(target));
        sb.Append(" WHERE ");

        // 使用 LAST_INSERT_ID() 定位刚插入的行
        var hasIdentity = returningColumns.Any(c => !setCols.Contains(c));
        if (hasIdentity)
        {
            var identityCol = returningColumns.First(c => !setCols.Contains(c));
            sb.Append(QuoteIdentifier(identityCol));
            sb.Append(" = LAST_INSERT_ID()");
        }
        else
        {
            sb.Append("ROW_COUNT() > 0 LIMIT 1");
        }
    }

    /// <summary>从 Returning 表达式中收集列名</summary>
    private static void CollectReturningColumns(DbExpression expression, List<String> columns)
    {
        switch (expression)
        {
            case DbNewInstanceExpression newInstance:
                foreach (var arg in newInstance.Arguments)
                {
                    CollectReturningColumns(arg, columns);
                }
                break;
            case DbPropertyExpression prop:
                columns.Add(prop.Property.Name);
                break;
        }
    }

    private SqlResult GenerateUpdate(DbUpdateCommandTree update)
    {
        var sb = new StringBuilder();
        var parameters = new Dictionary<String, Object?>();
        var target = GetTargetTable(update.Target);

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

            var paramName = $"@p{_paramIndex++}";
            sb.Append(paramName);

            if (clause.Value is DbConstantExpression constant)
                parameters[paramName] = constant.Value;
            else if (clause.Value is DbNullExpression)
                parameters[paramName] = DBNull.Value;
            else
                parameters[paramName] = null;
        }

        if (update.Predicate != null)
        {
            sb.Append(" WHERE ");
            GenerateExpression(sb, update.Predicate, parameters);
        }


        // 如果有返回值（如 computed/timestamp 列），查询更新后的列值
        if (update.Returning != null)
        {
            var returningColumns = new List<String>();
            CollectReturningColumns(update.Returning, returningColumns);

            if (returningColumns.Count > 0)
            {
                sb.Append("; SELECT ");
                var firstRet = true;
                foreach (var col in returningColumns)
                {
                    if (!firstRet) sb.Append(", ");
                    firstRet = false;
                    sb.Append(QuoteIdentifier(col));
                }
                sb.Append(" FROM ");
                sb.Append(QuoteIdentifier(target));
                if (update.Predicate != null)
                {
                    sb.Append(" WHERE ");
                    GenerateExpression(sb, update.Predicate, parameters);
                }
            }
            else
            {
                sb.Append("; SELECT ROW_COUNT()");
            }
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

    private SqlResult GenerateFunction(DbFunctionCommandTree function)
    {
        var sb = new StringBuilder();
        var parameters = new Dictionary<String, Object?>();

        sb.Append("SELECT ");
        sb.Append(function.EdmFunction.Name);
        sb.Append('(');

        var first = true;
        foreach (var param in function.Parameters)
        {
            if (!first) sb.Append(", ");
            first = false;

            var paramName = $"@{param.Key}";
            sb.Append(paramName);
            parameters[paramName] = null;
        }

        sb.Append(')');

        return new SqlResult(sb.ToString(), parameters);
    }
    #endregion

    #region 表达式生成
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
                var paramName = $"@p{_paramIndex++}";
                sb.Append(paramName);
                parameters[paramName] = constant.Value;
                break;

            case DbParameterReferenceExpression paramRef:
                sb.Append("@");
                sb.Append(paramRef.ParameterName);
                break;

            case DbComparisonExpression comparison:
                GenerateComparison(sb, comparison, parameters);
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
                GenerateProject(sb, project, parameters);
                break;

            case DbFilterExpression filter:
                GenerateExpression(sb, filter.Input.Expression, parameters);
                sb.Append(" AS ");
                sb.Append(QuoteIdentifier(filter.Input.VariableName));
                sb.Append(" WHERE ");
                GenerateExpression(sb, filter.Predicate, parameters);
                break;

            case DbNewInstanceExpression newInstance:
                GenerateNewInstance(sb, newInstance, parameters);
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
                GenerateSort(sb, sort, parameters);
                break;

            case DbSkipExpression skip:
                GenerateSkip(sb, skip, parameters);
                break;

            case DbFunctionExpression function:
                GenerateFunction(sb, function, parameters);
                break;

            case DbJoinExpression join:
                GenerateJoin(sb, join, parameters);
                break;

            case DbCrossJoinExpression crossJoin:
                GenerateCrossJoin(sb, crossJoin, parameters);
                break;

            case DbGroupByExpression groupBy:
                GenerateGroupBy(sb, groupBy, parameters);
                break;

            case DbDistinctExpression distinct:
                GenerateDistinct(sb, distinct, parameters);
                break;

            case DbLikeExpression like:
                GenerateExpression(sb, like.Argument, parameters);
                sb.Append(" LIKE ");
                GenerateExpression(sb, like.Pattern, parameters);
                if (like.Escape != null && like.Escape is not DbNullExpression)
                {
                    sb.Append(" ESCAPE ");
                    GenerateExpression(sb, like.Escape, parameters);
                }
                break;

            case DbArithmeticExpression arithmetic:
                GenerateArithmetic(sb, arithmetic, parameters);
                break;

            case DbCaseExpression caseExpr:
                GenerateCase(sb, caseExpr, parameters);
                break;

            case DbUnionAllExpression unionAll:
                sb.Append('(');
                GenerateExpression(sb, unionAll.Left, parameters);
                sb.Append(") UNION ALL (");
                GenerateExpression(sb, unionAll.Right, parameters);
                sb.Append(')');
                break;

            case DbIntersectExpression intersect:
                // MySQL 8.0+ 支持 INTERSECT，低版本用子查询模拟
                sb.Append('(');
                GenerateExpression(sb, intersect.Left, parameters);
                sb.Append(") INTERSECT (");
                GenerateExpression(sb, intersect.Right, parameters);
                sb.Append(')');
                break;

            case DbExceptExpression except:
                // MySQL 8.0+ 支持 EXCEPT，低版本用子查询模拟
                sb.Append('(');
                GenerateExpression(sb, except.Left, parameters);
                sb.Append(") EXCEPT (");
                GenerateExpression(sb, except.Right, parameters);
                sb.Append(')');
                break;

            case DbQuantifierExpression quantifier:
                GenerateQuantifier(sb, quantifier, parameters);
                break;

            case DbIsOfExpression isOf:
                // 类型判断，在 MySQL 中简化为 TRUE（单表继承时无需额外判断）
                GenerateExpression(sb, isOf.Argument, parameters);
                sb.Append(" IS NOT NULL");
                break;

            case DbTreatExpression treat:
                // 类型转换，在 MySQL 中直接输出原表达式
                GenerateExpression(sb, treat.Argument, parameters);
                break;

            case DbOfTypeExpression ofType:
                // 类型过滤，直接输出子查询
                GenerateExpression(sb, ofType.Argument, parameters);
                break;

            case DbApplyExpression apply:
                GenerateApply(sb, apply, parameters);
                break;

            case DbElementExpression element:
                sb.Append('(');
                GenerateExpression(sb, element.Argument, parameters);
                sb.Append(" LIMIT 1)");
                break;

            case DbInExpression inExpr:
                GenerateExpression(sb, inExpr.Item, parameters);
                sb.Append(" IN (");
                var firstIn = true;
                foreach (var item in inExpr.List)
                {
                    if (!firstIn) sb.Append(", ");
                    firstIn = false;
                    GenerateExpression(sb, item, parameters);
                }
                sb.Append(')');
                break;

            case DbDerefExpression deref:
                GenerateExpression(sb, deref.Argument, parameters);
                break;

            case DbRefExpression refExpr:
                GenerateExpression(sb, refExpr.Argument, parameters);
                break;

            case DbEntityRefExpression entityRef:
                GenerateExpression(sb, entityRef.Argument, parameters);
                break;

            default:
                sb.Append(expression.ToString());
                break;
        }
    }

    /// <summary>生成比较表达式</summary>
    private void GenerateComparison(StringBuilder sb, DbComparisonExpression comparison, Dictionary<String, Object?> parameters)
    {
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
    }

    /// <summary>生成 SELECT 投影</summary>
    private void GenerateProject(StringBuilder sb, DbProjectExpression project, Dictionary<String, Object?> parameters)
    {
        sb.Append("SELECT ");
        GenerateExpression(sb, project.Projection, parameters);
        sb.Append(" FROM ");
        GenerateExpression(sb, project.Input.Expression, parameters);
        sb.Append(" AS ");
        sb.Append(QuoteIdentifier(project.Input.VariableName));
    }

    /// <summary>生成 NewInstance 表达式（列列表）</summary>
    private void GenerateNewInstance(StringBuilder sb, DbNewInstanceExpression newInstance, Dictionary<String, Object?> parameters)
    {
        var firstCol = true;
        // 如果结果类型有成员定义，输出带别名的列
        if (newInstance.ResultType.EdmType is RowType rowType)
        {
            var members = rowType.Properties.ToArray();
            for (var i = 0; i < newInstance.Arguments.Count; i++)
            {
                if (!firstCol) sb.Append(", ");
                firstCol = false;
                GenerateExpression(sb, newInstance.Arguments[i], parameters);
                if (i < members.Length)
                {
                    sb.Append(" AS ");
                    sb.Append(QuoteIdentifier(members[i].Name));
                }
            }
        }
        else
        {
            foreach (var arg in newInstance.Arguments)
            {
                if (!firstCol) sb.Append(", ");
                firstCol = false;
                GenerateExpression(sb, arg, parameters);
            }
        }
    }

    /// <summary>生成 ORDER BY</summary>
    private void GenerateSort(StringBuilder sb, DbSortExpression sort, Dictionary<String, Object?> parameters)
    {
        GenerateExpression(sb, sort.Input.Expression, parameters);
        sb.Append(" AS ");
        sb.Append(QuoteIdentifier(sort.Input.VariableName));
        sb.Append(" ORDER BY ");
        var firstSort = true;
        foreach (var key in sort.SortOrder)
        {
            if (!firstSort) sb.Append(", ");
            firstSort = false;
            GenerateExpression(sb, key.Expression, parameters);
            if (!key.Ascending) sb.Append(" DESC");
            if (!String.IsNullOrEmpty(key.Collation))
            {
                sb.Append(" COLLATE ");
                sb.Append(key.Collation);
            }
        }
    }

    /// <summary>生成 SKIP（LIMIT offset, count 或 LIMIT count OFFSET offset）</summary>
    private void GenerateSkip(StringBuilder sb, DbSkipExpression skip, Dictionary<String, Object?> parameters)
    {
        GenerateExpression(sb, skip.Input.Expression, parameters);
        sb.Append(" AS ");
        sb.Append(QuoteIdentifier(skip.Input.VariableName));
        sb.Append(" ORDER BY ");
        var firstSort = true;
        foreach (var key in skip.SortOrder)
        {
            if (!firstSort) sb.Append(", ");
            firstSort = false;
            GenerateExpression(sb, key.Expression, parameters);
            if (!key.Ascending) sb.Append(" DESC");
        }
        sb.Append(" LIMIT 18446744073709551610 OFFSET ");
        GenerateExpression(sb, skip.Count, parameters);
    }

    /// <summary>生成函数调用</summary>
    private void GenerateFunction(StringBuilder sb, DbFunctionExpression function, Dictionary<String, Object?> parameters)
    {
        var funcName = function.Function.Name.ToUpperInvariant();

        // 映射 EF 规范函数到 MySQL 函数
        var mysqlFuncName = MapCanonicalFunction(funcName, function);
        if (mysqlFuncName != null)
        {
            GenerateMappedFunction(sb, mysqlFuncName, function, parameters);
            return;
        }

        // 聚合函数
        if (IsAggregateFunction(funcName))
        {
            GenerateAggregateFunction(sb, funcName, function, parameters);
            return;
        }

        // 默认：直接使用函数名
        sb.Append(funcName);
        sb.Append('(');
        var first = true;
        foreach (var arg in function.Arguments)
        {
            if (!first) sb.Append(", ");
            first = false;
            GenerateExpression(sb, arg, parameters);
        }
        sb.Append(')');
    }

    /// <summary>映射 EF6 规范函数到 MySQL 函数名</summary>
    private static String? MapCanonicalFunction(String funcName, DbFunctionExpression function)
    {
        // 只处理 Edm 命名空间的规范函数
        if (function.Function.NamespaceName != "Edm") return null;

        return funcName switch
        {
            // 字符串函数
            "CONCAT" => "CONCAT",
            "CONTAINS" => null, // 特殊处理
            "ENDSWITH" => null,
            "INDEXOF" => "LOCATE",
            "LEFT" => "LEFT",
            "LENGTH" => "CHAR_LENGTH",
            "LTRIM" => "LTRIM",
            "REPLACE" => "REPLACE",
            "REVERSE" => "REVERSE",
            "RIGHT" => "RIGHT",
            "RTRIM" => "RTRIM",
            "STARTSWITH" => null,
            "SUBSTRING" => "SUBSTRING",
            "TOLOWER" => "LOWER",
            "TOUPPER" => "UPPER",
            "TRIM" => "TRIM",

            // 日期时间函数
            "YEAR" => "YEAR",
            "MONTH" => "MONTH",
            "DAY" => "DAY",
            "HOUR" => "HOUR",
            "MINUTE" => "MINUTE",
            "SECOND" => "SECOND",
            "GETDATE" => "NOW",
            "GETCURRENTDATETIME" => "NOW",
            "GETUTCDATE" => "UTC_TIMESTAMP",
            "CURRENTDATETIME" => "NOW",
            "CURRENTUTCDATETIME" => "UTC_TIMESTAMP",
            "ADDYEARS" => null,
            "ADDMONTHS" => null,
            "ADDDAYS" => null,
            "ADDHOURS" => null,
            "ADDMINUTES" => null,
            "ADDSECONDS" => null,
            "ADDMILLISECONDS" => null,
            "DIFFYEARS" => null,
            "DIFFMONTHS" => null,
            "DIFFDAYS" => null,
            "DIFFHOURS" => null,
            "DIFFMINUTES" => null,
            "DIFFSECONDS" => null,
            "CREATEDATETIME" => null,

            // 数学函数
            "ABS" => "ABS",
            "CEILING" => "CEILING",
            "FLOOR" => "FLOOR",
            "ROUND" => "ROUND",
            "TRUNCATE" => "TRUNCATE",
            "POWER" => "POWER",
            "SQRT" => "SQRT",

            // 位运算
            "BITWISEAND" => null,
            "BITWISEOR" => null,
            "BITWISEXOR" => null,
            "BITWISENOT" => null,

            // GUID
            "NEWGUID" => "UUID",

            _ => null,
        };
    }

    /// <summary>生成映射后的函数调用</summary>
    private void GenerateMappedFunction(StringBuilder sb, String mysqlFuncName, DbFunctionExpression function, Dictionary<String, Object?> parameters)
    {
        var funcName = function.Function.Name.ToUpperInvariant();

        // 特殊函数处理
        switch (funcName)
        {
            case "CONTAINS":
                // Contains -> LOCATE(pattern, str) > 0
                sb.Append("(LOCATE(");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(", ");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(") > 0)");
                return;

            case "STARTSWITH":
                // StartsWith -> str LIKE 'pattern%'
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(" LIKE CONCAT(");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(", '%')");
                return;

            case "ENDSWITH":
                // EndsWith -> str LIKE '%pattern'
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(" LIKE CONCAT('%', ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(')');
                return;

            case "INDEXOF":
                // IndexOf -> LOCATE(substr, str) - 1（EF 使用 0-based，MySQL LOCATE 使用 1-based）
                sb.Append("(LOCATE(");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(") - 1)");
                return;

            // 日期加减函数
            case "ADDYEARS":
                sb.Append("DATE_ADD(");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", INTERVAL ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(" YEAR)");
                return;
            case "ADDMONTHS":
                sb.Append("DATE_ADD(");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", INTERVAL ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(" MONTH)");
                return;
            case "ADDDAYS":
                sb.Append("DATE_ADD(");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", INTERVAL ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(" DAY)");
                return;
            case "ADDHOURS":
                sb.Append("DATE_ADD(");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", INTERVAL ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(" HOUR)");
                return;
            case "ADDMINUTES":
                sb.Append("DATE_ADD(");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", INTERVAL ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(" MINUTE)");
                return;
            case "ADDSECONDS":
                sb.Append("DATE_ADD(");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", INTERVAL ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(" SECOND)");
                return;
            case "ADDMILLISECONDS":
                sb.Append("DATE_ADD(");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", INTERVAL ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(" * 1000 MICROSECOND)");
                return;

            // 日期差值函数
            case "DIFFYEARS":
                sb.Append("TIMESTAMPDIFF(YEAR, ");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(')');
                return;
            case "DIFFMONTHS":
                sb.Append("TIMESTAMPDIFF(MONTH, ");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(')');
                return;
            case "DIFFDAYS":
                sb.Append("TIMESTAMPDIFF(DAY, ");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(')');
                return;
            case "DIFFHOURS":
                sb.Append("TIMESTAMPDIFF(HOUR, ");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(')');
                return;
            case "DIFFMINUTES":
                sb.Append("TIMESTAMPDIFF(MINUTE, ");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(')');
                return;
            case "DIFFSECONDS":
                sb.Append("TIMESTAMPDIFF(SECOND, ");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(')');
                return;

            case "CREATEDATETIME":
                // CreateDateTime(year, month, day, hour, minute, second)
                sb.Append("CONCAT(");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(", '-', LPAD(");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(", 2, '0'), '-', LPAD(");
                GenerateExpression(sb, function.Arguments[2], parameters);
                sb.Append(", 2, '0'), ' ', LPAD(");
                GenerateExpression(sb, function.Arguments[3], parameters);
                sb.Append(", 2, '0'), ':', LPAD(");
                GenerateExpression(sb, function.Arguments[4], parameters);
                sb.Append(", 2, '0'), ':', LPAD(");
                GenerateExpression(sb, function.Arguments[5], parameters);
                sb.Append(", 2, '0'))");
                return;

            // 位运算
            case "BITWISEAND":
                sb.Append('(');
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(" & ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(')');
                return;
            case "BITWISEOR":
                sb.Append('(');
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(" | ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(')');
                return;
            case "BITWISEXOR":
                sb.Append('(');
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(" ^ ");
                GenerateExpression(sb, function.Arguments[1], parameters);
                sb.Append(')');
                return;
            case "BITWISENOT":
                sb.Append("(~");
                GenerateExpression(sb, function.Arguments[0], parameters);
                sb.Append(')');
                return;

            // 无参函数
            case "GETDATE" or "GETCURRENTDATETIME" or "CURRENTDATETIME":
                sb.Append("NOW()");
                return;
            case "GETUTCDATE" or "CURRENTUTCDATETIME":
                sb.Append("UTC_TIMESTAMP()");
                return;
            case "NEWGUID":
                sb.Append("UUID()");
                return;
        }

        // 通用映射：直接调用
        sb.Append(mysqlFuncName);
        sb.Append('(');
        var first = true;
        foreach (var arg in function.Arguments)
        {
            if (!first) sb.Append(", ");
            first = false;
            GenerateExpression(sb, arg, parameters);
        }
        sb.Append(')');
    }

    /// <summary>判断是否聚合函数</summary>
    private static Boolean IsAggregateFunction(String funcName)
    {
        return funcName is "COUNT" or "SUM" or "AVG" or "MIN" or "MAX" or "STDEV" or "STDEVP" or "VAR" or "VARP";
    }

    /// <summary>生成聚合函数</summary>
    private void GenerateAggregateFunction(StringBuilder sb, String funcName, DbFunctionExpression function, Dictionary<String, Object?> parameters)
    {
        var mysqlName = funcName switch
        {
            "STDEV" => "STDDEV_SAMP",
            "STDEVP" => "STDDEV_POP",
            "VAR" => "VAR_SAMP",
            "VARP" => "VAR_POP",
            _ => funcName,
        };

        sb.Append(mysqlName);
        sb.Append('(');

        if (function.Arguments.Count == 0)
        {
            sb.Append('*');
        }
        else
        {
            var first = true;
            foreach (var arg in function.Arguments)
            {
                if (!first) sb.Append(", ");
                first = false;
                GenerateExpression(sb, arg, parameters);
            }
        }

        sb.Append(')');
    }

    /// <summary>生成 JOIN</summary>
    private void GenerateJoin(StringBuilder sb, DbJoinExpression join, Dictionary<String, Object?> parameters)
    {
        GenerateExpression(sb, join.Left.Expression, parameters);
        sb.Append(" AS ");
        sb.Append(QuoteIdentifier(join.Left.VariableName));

        sb.Append(join.ExpressionKind switch
        {
            DbExpressionKind.InnerJoin => " INNER JOIN ",
            DbExpressionKind.LeftOuterJoin => " LEFT OUTER JOIN ",
            DbExpressionKind.FullOuterJoin => " FULL OUTER JOIN ",
            _ => " JOIN ",
        });

        GenerateExpression(sb, join.Right.Expression, parameters);
        sb.Append(" AS ");
        sb.Append(QuoteIdentifier(join.Right.VariableName));

        sb.Append(" ON ");
        GenerateExpression(sb, join.JoinCondition, parameters);
    }

    /// <summary>生成 CROSS JOIN</summary>
    private void GenerateCrossJoin(StringBuilder sb, DbCrossJoinExpression crossJoin, Dictionary<String, Object?> parameters)
    {
        var first = true;
        foreach (var input in crossJoin.Inputs)
        {
            if (!first) sb.Append(" CROSS JOIN ");
            first = false;
            GenerateExpression(sb, input.Expression, parameters);
            sb.Append(" AS ");
            sb.Append(QuoteIdentifier(input.VariableName));
        }
    }

    /// <summary>生成 GROUP BY</summary>
    private void GenerateGroupBy(StringBuilder sb, DbGroupByExpression groupBy, Dictionary<String, Object?> parameters)
    {
        sb.Append("SELECT ");

        var first = true;
        foreach (var agg in groupBy.Aggregates)
        {
            if (!first) sb.Append(", ");
            first = false;

            if (agg is DbFunctionAggregate funcAgg)
            {
                sb.Append(funcAgg.Function.Name.ToUpperInvariant());
                sb.Append('(');
                if (funcAgg.Distinct) sb.Append("DISTINCT ");
                var firstArg = true;
                foreach (var arg in funcAgg.Arguments)
                {
                    if (!firstArg) sb.Append(", ");
                    firstArg = false;
                    GenerateExpression(sb, arg, parameters);
                }
                sb.Append(')');
            }
            else if (agg is DbGroupAggregate)
            {
                sb.Append("*");
            }
        }

        sb.Append(" FROM ");
        GenerateExpression(sb, groupBy.Input.Expression, parameters);
        sb.Append(" AS ");
        sb.Append(QuoteIdentifier(groupBy.Input.VariableName));

        if (groupBy.Keys.Count > 0)
        {
            sb.Append(" GROUP BY ");
            first = true;
            foreach (var key in groupBy.Keys)
            {
                if (!first) sb.Append(", ");
                first = false;
                GenerateExpression(sb, key, parameters);
            }
        }
    }

    /// <summary>生成 DISTINCT</summary>
    private void GenerateDistinct(StringBuilder sb, DbDistinctExpression distinct, Dictionary<String, Object?> parameters)
    {
        // 如果子表达式是 Project，将 SELECT 替换为 SELECT DISTINCT
        if (distinct.Argument is DbProjectExpression project)
        {
            sb.Append("SELECT DISTINCT ");
            GenerateExpression(sb, project.Projection, parameters);
            sb.Append(" FROM ");
            GenerateExpression(sb, project.Input.Expression, parameters);
            sb.Append(" AS ");
            sb.Append(QuoteIdentifier(project.Input.VariableName));
        }
        else
        {
            sb.Append("SELECT DISTINCT * FROM (");
            GenerateExpression(sb, distinct.Argument, parameters);
            sb.Append(") AS `__distinct`");
        }
    }

    /// <summary>生成算术运算</summary>
    private void GenerateArithmetic(StringBuilder sb, DbArithmeticExpression arithmetic, Dictionary<String, Object?> parameters)
    {
        if (arithmetic.ExpressionKind == DbExpressionKind.UnaryMinus)
        {
            sb.Append("(-(");
            GenerateExpression(sb, arithmetic.Arguments[0], parameters);
            sb.Append("))");
            return;
        }

        sb.Append('(');
        GenerateExpression(sb, arithmetic.Arguments[0], parameters);
        sb.Append(arithmetic.ExpressionKind switch
        {
            DbExpressionKind.Plus => " + ",
            DbExpressionKind.Minus => " - ",
            DbExpressionKind.Multiply => " * ",
            DbExpressionKind.Divide => " / ",
            DbExpressionKind.Modulo => " % ",
            _ => throw new NotSupportedException($"不支持的算术运算：{arithmetic.ExpressionKind}"),
        });
        GenerateExpression(sb, arithmetic.Arguments[1], parameters);
        sb.Append(')');
    }

    /// <summary>生成 CASE WHEN</summary>
    private void GenerateCase(StringBuilder sb, DbCaseExpression caseExpr, Dictionary<String, Object?> parameters)
    {
        sb.Append("CASE");
        for (var i = 0; i < caseExpr.When.Count; i++)
        {
            sb.Append(" WHEN ");
            GenerateExpression(sb, caseExpr.When[i], parameters);
            sb.Append(" THEN ");
            GenerateExpression(sb, caseExpr.Then[i], parameters);
        }
        if (caseExpr.Else != null && caseExpr.Else is not DbNullExpression)
        {
            sb.Append(" ELSE ");
            GenerateExpression(sb, caseExpr.Else, parameters);
        }
        sb.Append(" END");
    }

    /// <summary>生成量词表达式（ANY/EXISTS）</summary>
    private void GenerateQuantifier(StringBuilder sb, DbQuantifierExpression quantifier, Dictionary<String, Object?> parameters)
    {
        sb.Append("EXISTS (SELECT 1 FROM ");
        GenerateExpression(sb, quantifier.Input.Expression, parameters);
        sb.Append(" AS ");
        sb.Append(QuoteIdentifier(quantifier.Input.VariableName));
        sb.Append(" WHERE ");
        GenerateExpression(sb, quantifier.Predicate, parameters);
        sb.Append(')');
    }

    /// <summary>生成 APPLY（转换为 LATERAL JOIN）</summary>
    private void GenerateApply(StringBuilder sb, DbApplyExpression apply, Dictionary<String, Object?> parameters)
    {
        GenerateExpression(sb, apply.Input.Expression, parameters);
        sb.Append(" AS ");
        sb.Append(QuoteIdentifier(apply.Input.VariableName));

        // MySQL 8.0.14+ 支持 LATERAL 子查询
        sb.Append(apply.ExpressionKind switch
        {
            DbExpressionKind.CrossApply => ", LATERAL (",
            DbExpressionKind.OuterApply => " LEFT JOIN LATERAL (",
            _ => ", LATERAL (",
        });

        GenerateExpression(sb, apply.Apply.Expression, parameters);
        sb.Append(") AS ");
        sb.Append(QuoteIdentifier(apply.Apply.VariableName));

        if (apply.ExpressionKind == DbExpressionKind.OuterApply)
            sb.Append(" ON TRUE");
    }
    #endregion

    #region 辅助
    private static String GetTargetTable(DbExpressionBinding target)
    {
        if (target.Expression is DbScanExpression scan)
            return scan.Target.Table ?? scan.Target.Name;

        return target.VariableName;
    }

    private static String GetMySqlTypeName(TypeUsage typeUsage)
    {
        if (typeUsage.EdmType is PrimitiveType pt)
        {
            return pt.PrimitiveTypeKind switch
            {
                PrimitiveTypeKind.Boolean => "UNSIGNED",
                PrimitiveTypeKind.Byte => "UNSIGNED",
                PrimitiveTypeKind.Int16 => "SIGNED",
                PrimitiveTypeKind.Int32 => "SIGNED",
                PrimitiveTypeKind.Int64 => "SIGNED",
                PrimitiveTypeKind.Single => "DECIMAL",
                PrimitiveTypeKind.Decimal => "DECIMAL",
                PrimitiveTypeKind.Double => "DECIMAL",
                PrimitiveTypeKind.String => "CHAR",
                PrimitiveTypeKind.DateTime => "DATETIME",
                PrimitiveTypeKind.Time => "TIME",
                PrimitiveTypeKind.Binary => "BINARY",
                _ => "CHAR",
            };
        }
        return "CHAR";
    }

    /// <summary>使用反引号包裹标识符</summary>
    /// <param name="identifier">标识符</param>
    /// <returns></returns>
    internal static String QuoteIdentifier(String identifier) => $"`{identifier}`";
    #endregion
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
