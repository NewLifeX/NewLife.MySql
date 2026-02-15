using Microsoft.EntityFrameworkCore.Query;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 成员翻译器提供者。聚合所有 MySQL 成员（属性）翻译器</summary>
public class MySqlMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">依赖</param>
    public MySqlMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sqlExpressionFactory = dependencies.SqlExpressionFactory;

        AddTranslators(
        [
            new MySqlMemberTranslator(sqlExpressionFactory),
        ]);
    }
}
