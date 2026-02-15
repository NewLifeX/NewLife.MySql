using Microsoft.EntityFrameworkCore.Query;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 方法调用翻译器提供者。聚合所有 MySQL 方法调用翻译器</summary>
public class MySqlMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">依赖</param>
    public MySqlMethodCallTranslatorProvider(RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sqlExpressionFactory = dependencies.SqlExpressionFactory;

        AddTranslators(
        [
            new MySqlStringMethodTranslator(sqlExpressionFactory),
            new MySqlMathTranslator(sqlExpressionFactory),
            new MySqlDateTimeMethodTranslator(sqlExpressionFactory),
        ]);
    }
}
