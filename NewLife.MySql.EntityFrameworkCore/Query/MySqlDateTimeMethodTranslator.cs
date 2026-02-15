using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 日期时间方法翻译器。将 DateTime 成员方法翻译为 MySQL 日期函数</summary>
public class MySqlDateTimeMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    private static readonly MethodInfo _dateTimeNow = typeof(DateTime).GetProperty(nameof(DateTime.Now))!.GetMethod!;
    private static readonly MethodInfo _dateTimeUtcNow = typeof(DateTime).GetProperty(nameof(DateTime.UtcNow))!.GetMethod!;
    private static readonly MethodInfo _dateTimeToday = typeof(DateTime).GetProperty(nameof(DateTime.Today))!.GetMethod!;
    private static readonly MethodInfo _addYears = typeof(DateTime).GetMethod(nameof(DateTime.AddYears), [typeof(Int32)])!;
    private static readonly MethodInfo _addMonths = typeof(DateTime).GetMethod(nameof(DateTime.AddMonths), [typeof(Int32)])!;
    private static readonly MethodInfo _addDays = typeof(DateTime).GetMethod(nameof(DateTime.AddDays), [typeof(Double)])!;
    private static readonly MethodInfo _addHours = typeof(DateTime).GetMethod(nameof(DateTime.AddHours), [typeof(Double)])!;
    private static readonly MethodInfo _addMinutes = typeof(DateTime).GetMethod(nameof(DateTime.AddMinutes), [typeof(Double)])!;
    private static readonly MethodInfo _addSeconds = typeof(DateTime).GetMethod(nameof(DateTime.AddSeconds), [typeof(Double)])!;

    /// <summary>实例化</summary>
    /// <param name="sqlExpressionFactory">SQL 表达式工厂</param>
    public MySqlDateTimeMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    /// <summary>翻译方法调用</summary>
    /// <param name="instance">实例表达式</param>
    /// <param name="method">方法信息</param>
    /// <param name="arguments">参数列表</param>
    /// <param name="logger">诊断日志</param>
    /// <returns></returns>
    public SqlExpression? Translate(SqlExpression? instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments, IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance == null) return null;

        if (method == _addYears)
            return DateAdd("YEAR", instance, arguments[0]);

        if (method == _addMonths)
            return DateAdd("MONTH", instance, arguments[0]);

        if (method == _addDays)
            return DateAdd("DAY", instance, arguments[0]);

        if (method == _addHours)
            return DateAdd("HOUR", instance, arguments[0]);

        if (method == _addMinutes)
            return DateAdd("MINUTE", instance, arguments[0]);

        if (method == _addSeconds)
            return DateAdd("SECOND", instance, arguments[0]);

        return null;
    }

    private SqlExpression DateAdd(String part, SqlExpression instance, SqlExpression interval)
    {
        // DATE_ADD(instance, INTERVAL value PART) 在 EF Core 中用 SQL 片段表示
        return _sqlExpressionFactory.Function(
            "DATE_ADD",
            [instance, _sqlExpressionFactory.Fragment($"INTERVAL {interval} {part}")],
            nullable: true,
            [false, false],
            typeof(DateTime));
    }
}
