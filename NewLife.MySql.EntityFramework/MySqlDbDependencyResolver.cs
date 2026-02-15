using System.Data.Entity.Infrastructure;
using System.Data.Entity.Infrastructure.DependencyResolution;
using System.Data.Entity.Migrations.Sql;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySql EF6 依赖解析器。在非 DbConfiguration 场景下（如 app.config）自动注册 MySql 提供程序</summary>
public class MySqlDbDependencyResolver : IDbDependencyResolver
{
    /// <summary>解析依赖服务</summary>
    /// <param name="type">服务类型</param>
    /// <param name="key">服务键</param>
    /// <returns></returns>
    public Object? GetService(Type type, Object? key)
    {
        var invariantName = key as String;
        if (!String.Equals(invariantName, MySqlEFConfiguration.ProviderInvariantName, StringComparison.OrdinalIgnoreCase))
            return null;

        if (type == typeof(System.Data.Entity.Core.Common.DbProviderServices))
            return MySqlProviderServices.Instance;

        if (type == typeof(MigrationSqlGenerator))
            return new MySqlMigrationSqlGenerator();

        if (type == typeof(IDbConnectionFactory))
            return new MySqlConnectionFactory();

        if (type == typeof(IDbExecutionStrategy))
            return new MySqlExecutionStrategy();

        return null;
    }

    /// <summary>解析所有匹配的依赖服务</summary>
    /// <param name="type">服务类型</param>
    /// <param name="key">服务键</param>
    /// <returns></returns>
    public IEnumerable<Object> GetServices(Type type, Object? key)
    {
        var service = GetService(type, key);
        if (service != null)
            yield return service;
    }
}
