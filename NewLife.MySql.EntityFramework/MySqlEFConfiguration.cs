using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Data.Entity.Migrations.History;
using System.Data.Entity.Migrations.Sql;
using NewLife.MySql;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySql数据库配置。在应用启动时调用 MySqlEFConfiguration 自动完成 EF6 提供程序注册</summary>
public class MySqlEFConfiguration : DbConfiguration
{
    /// <summary>提供程序不变名称</summary>
    public const String ProviderInvariantName = "NewLife.MySql.MySqlClient";

    /// <summary>实例化MySql EF配置</summary>
    public MySqlEFConfiguration()
    {
        SetProviderFactory(ProviderInvariantName, MySqlClientFactory.Instance);
        SetProviderServices(ProviderInvariantName, MySqlProviderServices.Instance);
        SetDefaultConnectionFactory(new MySqlConnectionFactory());
        SetMigrationSqlGenerator(ProviderInvariantName, () => new MySqlMigrationSqlGenerator());
        SetExecutionStrategy(ProviderInvariantName, () => new MySqlExecutionStrategy());
        SetHistoryContext(ProviderInvariantName, (conn, schema) => new MySqlHistoryContext(conn, schema));
    }
}
