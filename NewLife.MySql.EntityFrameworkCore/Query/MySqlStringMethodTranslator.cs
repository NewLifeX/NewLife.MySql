using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 字符串方法翻译器。将 .NET String 方法翻译为 MySQL 函数</summary>
public class MySqlStringMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    private static readonly MethodInfo _contains = typeof(String).GetMethod(nameof(String.Contains), [typeof(String)])!;
    private static readonly MethodInfo _startsWith = typeof(String).GetMethod(nameof(String.StartsWith), [typeof(String)])!;
    private static readonly MethodInfo _endsWith = typeof(String).GetMethod(nameof(String.EndsWith), [typeof(String)])!;
    private static readonly MethodInfo _toUpper = typeof(String).GetMethod(nameof(String.ToUpper), Type.EmptyTypes)!;
    private static readonly MethodInfo _toLower = typeof(String).GetMethod(nameof(String.ToLower), Type.EmptyTypes)!;
    private static readonly MethodInfo _trim = typeof(String).GetMethod(nameof(String.Trim), Type.EmptyTypes)!;
    private static readonly MethodInfo _trimStart = typeof(String).GetMethod(nameof(String.TrimStart), Type.EmptyTypes)!;
    private static readonly MethodInfo _trimEnd = typeof(String).GetMethod(nameof(String.TrimEnd), Type.EmptyTypes)!;
    private static readonly MethodInfo _replace = typeof(String).GetMethod(nameof(String.Replace), [typeof(String), typeof(String)])!;
    private static readonly MethodInfo _substring1 = typeof(String).GetMethod(nameof(String.Substring), [typeof(Int32)])!;
    private static readonly MethodInfo _substring2 = typeof(String).GetMethod(nameof(String.Substring), [typeof(Int32), typeof(Int32)])!;
    private static readonly MethodInfo _indexOf = typeof(String).GetMethod(nameof(String.IndexOf), [typeof(String)])!;
    private static readonly MethodInfo _concat2 = typeof(String).GetMethod(nameof(String.Concat), [typeof(String), typeof(String)])!;
    private static readonly MethodInfo _concat3 = typeof(String).GetMethod(nameof(String.Concat), [typeof(String), typeof(String), typeof(String)])!;
    private static readonly MethodInfo _isNullOrEmpty = typeof(String).GetMethod(nameof(String.IsNullOrEmpty), [typeof(String)])!;
    private static readonly MethodInfo _isNullOrWhiteSpace = typeof(String).GetMethod(nameof(String.IsNullOrWhiteSpace), [typeof(String)])!;

    /// <summary>实例化</summary>
    /// <param name="sqlExpressionFactory">SQL 表达式工厂</param>
    public MySqlStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
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
        if (method == _contains && instance != null)
        {
            // LOCATE(arg, instance) > 0
            var locate = _sqlExpressionFactory.GreaterThan(
                _sqlExpressionFactory.Function("LOCATE", [arguments[0], instance], nullable: true, [false, false], typeof(Int32)),
                _sqlExpressionFactory.Constant(0));
            return locate;
        }

        if (method == _startsWith && instance != null)
        {
            // instance LIKE CONCAT(arg, '%')
            var pattern = _sqlExpressionFactory.Function("CONCAT", [arguments[0], _sqlExpressionFactory.Constant("%")], nullable: true, [false, false], typeof(String));
            return _sqlExpressionFactory.Like(instance, pattern);
        }

        if (method == _endsWith && instance != null)
        {
            // instance LIKE CONCAT('%', arg)
            var pattern = _sqlExpressionFactory.Function("CONCAT", [_sqlExpressionFactory.Constant("%"), arguments[0]], nullable: true, [false, false], typeof(String));
            return _sqlExpressionFactory.Like(instance, pattern);
        }

        if (method == _toUpper && instance != null)
            return _sqlExpressionFactory.Function("UPPER", [instance], nullable: true, [false], typeof(String));

        if (method == _toLower && instance != null)
            return _sqlExpressionFactory.Function("LOWER", [instance], nullable: true, [false], typeof(String));

        if (method == _trim && instance != null)
            return _sqlExpressionFactory.Function("TRIM", [instance], nullable: true, [false], typeof(String));

        if (method == _trimStart && instance != null)
            return _sqlExpressionFactory.Function("LTRIM", [instance], nullable: true, [false], typeof(String));

        if (method == _trimEnd && instance != null)
            return _sqlExpressionFactory.Function("RTRIM", [instance], nullable: true, [false], typeof(String));

        if (method == _replace && instance != null)
            return _sqlExpressionFactory.Function("REPLACE", [instance, arguments[0], arguments[1]], nullable: true, [false, false, false], typeof(String));

        if (method == _substring1 && instance != null)
        {
            // SUBSTRING(instance, arg + 1)，MySQL 字符串位置从 1 开始
            var start = _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1));
            return _sqlExpressionFactory.Function("SUBSTRING", [instance, start], nullable: true, [false, false], typeof(String));
        }

        if (method == _substring2 && instance != null)
        {
            // SUBSTRING(instance, arg0 + 1, arg1)
            var start = _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1));
            return _sqlExpressionFactory.Function("SUBSTRING", [instance, start, arguments[1]], nullable: true, [false, false, false], typeof(String));
        }

        if (method == _indexOf && instance != null)
        {
            // LOCATE(arg, instance) - 1，.NET IndexOf 返回 0-based
            var locate = _sqlExpressionFactory.Function("LOCATE", [arguments[0], instance], nullable: true, [false, false], typeof(Int32));
            return _sqlExpressionFactory.Subtract(locate, _sqlExpressionFactory.Constant(1));
        }

        if (method == _concat2)
            return _sqlExpressionFactory.Function("CONCAT", [arguments[0], arguments[1]], nullable: true, [false, false], typeof(String));

        if (method == _concat3)
            return _sqlExpressionFactory.Function("CONCAT", [arguments[0], arguments[1], arguments[2]], nullable: true, [false, false, false], typeof(String));

        // String.Length 属性在 EF Core 中通过 MemberTranslator 处理，此处不涉及

        return null;
    }
}
