using Microsoft.EntityFrameworkCore.Query;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 查询 SQL 生成器工厂。创建 MySQL 方言的查询 SQL 生成器</summary>
public class MySqlQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _dependencies;

    /// <summary>实例化</summary>
    /// <param name="dependencies">查询 SQL 生成器依赖</param>
    public MySqlQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    /// <summary>创建查询 SQL 生成器</summary>
    /// <returns></returns>
    public QuerySqlGenerator Create() => new QuerySqlGenerator(_dependencies);
}
