using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 数学方法翻译器。将 System.Math 方法翻译为 MySQL 函数</summary>
public class MySqlMathTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    private static readonly Dictionary<MethodInfo, String> _methodMap = new()
    {
        [typeof(Math).GetMethod(nameof(Math.Abs), [typeof(Int32)])!] = "ABS",
        [typeof(Math).GetMethod(nameof(Math.Abs), [typeof(Int64)])!] = "ABS",
        [typeof(Math).GetMethod(nameof(Math.Abs), [typeof(Double)])!] = "ABS",
        [typeof(Math).GetMethod(nameof(Math.Abs), [typeof(Single)])!] = "ABS",
        [typeof(Math).GetMethod(nameof(Math.Abs), [typeof(Decimal)])!] = "ABS",
        [typeof(Math).GetMethod(nameof(Math.Ceiling), [typeof(Double)])!] = "CEILING",
        [typeof(Math).GetMethod(nameof(Math.Ceiling), [typeof(Decimal)])!] = "CEILING",
        [typeof(Math).GetMethod(nameof(Math.Floor), [typeof(Double)])!] = "FLOOR",
        [typeof(Math).GetMethod(nameof(Math.Floor), [typeof(Decimal)])!] = "FLOOR",
        [typeof(Math).GetMethod(nameof(Math.Round), [typeof(Double)])!] = "ROUND",
        [typeof(Math).GetMethod(nameof(Math.Round), [typeof(Decimal)])!] = "ROUND",
        [typeof(Math).GetMethod(nameof(Math.Truncate), [typeof(Double)])!] = "TRUNCATE",
        [typeof(Math).GetMethod(nameof(Math.Truncate), [typeof(Decimal)])!] = "TRUNCATE",
        [typeof(Math).GetMethod(nameof(Math.Sqrt), [typeof(Double)])!] = "SQRT",
        [typeof(Math).GetMethod(nameof(Math.Log), [typeof(Double)])!] = "LN",
        [typeof(Math).GetMethod(nameof(Math.Log10), [typeof(Double)])!] = "LOG10",
        [typeof(Math).GetMethod(nameof(Math.Exp), [typeof(Double)])!] = "EXP",
        [typeof(Math).GetMethod(nameof(Math.Pow), [typeof(Double), typeof(Double)])!] = "POW",
        [typeof(Math).GetMethod(nameof(Math.Sin), [typeof(Double)])!] = "SIN",
        [typeof(Math).GetMethod(nameof(Math.Cos), [typeof(Double)])!] = "COS",
        [typeof(Math).GetMethod(nameof(Math.Tan), [typeof(Double)])!] = "TAN",
        [typeof(Math).GetMethod(nameof(Math.Asin), [typeof(Double)])!] = "ASIN",
        [typeof(Math).GetMethod(nameof(Math.Acos), [typeof(Double)])!] = "ACOS",
        [typeof(Math).GetMethod(nameof(Math.Atan), [typeof(Double)])!] = "ATAN",
        [typeof(Math).GetMethod(nameof(Math.Atan2), [typeof(Double), typeof(Double)])!] = "ATAN2",
        [typeof(Math).GetMethod(nameof(Math.Sign), [typeof(Double)])!] = "SIGN",
        [typeof(Math).GetMethod(nameof(Math.Sign), [typeof(Int32)])!] = "SIGN",
        [typeof(Math).GetMethod(nameof(Math.Sign), [typeof(Int64)])!] = "SIGN",
        [typeof(Math).GetMethod(nameof(Math.Sign), [typeof(Decimal)])!] = "SIGN",
    };

    // Round(Double, Int32) 和 Round(Decimal, Int32)
    private static readonly MethodInfo _roundDouble2 = typeof(Math).GetMethod(nameof(Math.Round), [typeof(Double), typeof(Int32)])!;
    private static readonly MethodInfo _roundDecimal2 = typeof(Math).GetMethod(nameof(Math.Round), [typeof(Decimal), typeof(Int32)])!;

    // Max/Min 重载
    private static readonly Dictionary<MethodInfo, String> _maxMinMap = new()
    {
        [typeof(Math).GetMethod(nameof(Math.Max), [typeof(Int32), typeof(Int32)])!] = "GREATEST",
        [typeof(Math).GetMethod(nameof(Math.Max), [typeof(Int64), typeof(Int64)])!] = "GREATEST",
        [typeof(Math).GetMethod(nameof(Math.Max), [typeof(Double), typeof(Double)])!] = "GREATEST",
        [typeof(Math).GetMethod(nameof(Math.Max), [typeof(Decimal), typeof(Decimal)])!] = "GREATEST",
        [typeof(Math).GetMethod(nameof(Math.Min), [typeof(Int32), typeof(Int32)])!] = "LEAST",
        [typeof(Math).GetMethod(nameof(Math.Min), [typeof(Int64), typeof(Int64)])!] = "LEAST",
        [typeof(Math).GetMethod(nameof(Math.Min), [typeof(Double), typeof(Double)])!] = "LEAST",
        [typeof(Math).GetMethod(nameof(Math.Min), [typeof(Decimal), typeof(Decimal)])!] = "LEAST",
    };

    /// <summary>实例化</summary>
    /// <param name="sqlExpressionFactory">SQL 表达式工厂</param>
    public MySqlMathTranslator(ISqlExpressionFactory sqlExpressionFactory)
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
        // 单参数函数映射
        if (_methodMap.TryGetValue(method, out var funcName))
        {
            // Truncate 在 MySQL 中需要两个参数：TRUNCATE(x, 0)
            if (funcName == "TRUNCATE")
                return _sqlExpressionFactory.Function(funcName, [arguments[0], _sqlExpressionFactory.Constant(0)], nullable: true, [false, false], method.ReturnType);

            return _sqlExpressionFactory.Function(funcName, [arguments[0]], nullable: true, [false], method.ReturnType);
        }

        // Round(x, digits)
        if (method == _roundDouble2 || method == _roundDecimal2)
            return _sqlExpressionFactory.Function("ROUND", [arguments[0], arguments[1]], nullable: true, [false, false], method.ReturnType);

        // Max/Min → GREATEST/LEAST
        if (_maxMinMap.TryGetValue(method, out var maxMinFunc))
            return _sqlExpressionFactory.Function(maxMinFunc, [arguments[0], arguments[1]], nullable: true, [false, false], method.ReturnType);

        return null;
    }
}
