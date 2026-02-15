using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 成员翻译器。将 .NET 属性访问翻译为 MySQL 函数（如 String.Length → CHAR_LENGTH）</summary>
public class MySqlMemberTranslator : IMemberTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    /// <summary>实例化</summary>
    /// <param name="sqlExpressionFactory">SQL 表达式工厂</param>
    public MySqlMemberTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    /// <summary>翻译成员访问</summary>
    /// <param name="instance">实例表达式</param>
    /// <param name="member">成员信息</param>
    /// <param name="returnType">返回类型</param>
    /// <param name="logger">诊断日志</param>
    /// <returns></returns>
    public SqlExpression? Translate(SqlExpression? instance, MemberInfo member, Type returnType, IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // String.Length → CHAR_LENGTH(instance)
        if (member.DeclaringType == typeof(String) && member.Name == nameof(String.Length) && instance != null)
            return _sqlExpressionFactory.Function("CHAR_LENGTH", [instance], nullable: true, [false], typeof(Int32));

        // DateTime 属性翻译
        if (member.DeclaringType == typeof(DateTime) && instance != null)
        {
            var funcName = member.Name switch
            {
                nameof(DateTime.Year) => "YEAR",
                nameof(DateTime.Month) => "MONTH",
                nameof(DateTime.Day) => "DAY",
                nameof(DateTime.Hour) => "HOUR",
                nameof(DateTime.Minute) => "MINUTE",
                nameof(DateTime.Second) => "SECOND",
                nameof(DateTime.DayOfYear) => "DAYOFYEAR",
                nameof(DateTime.DayOfWeek) => "DAYOFWEEK",
                nameof(DateTime.Date) => "DATE",
                _ => null
            };

            if (funcName != null)
                return _sqlExpressionFactory.Function(funcName, [instance], nullable: true, [false], returnType);
        }

        // DateTime.Now → NOW()
        if (member.DeclaringType == typeof(DateTime) && instance == null)
        {
            return member.Name switch
            {
                nameof(DateTime.Now) => _sqlExpressionFactory.Function("NOW", [], nullable: false, [], typeof(DateTime)),
                nameof(DateTime.UtcNow) => _sqlExpressionFactory.Function("UTC_TIMESTAMP", [], nullable: false, [], typeof(DateTime)),
                nameof(DateTime.Today) => _sqlExpressionFactory.Function("CURDATE", [], nullable: false, [], typeof(DateTime)),
                _ => null
            };
        }

        return null;
    }
}
