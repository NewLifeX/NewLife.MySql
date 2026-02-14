using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using NewLife.MySql;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySql数据库配置。在应用启动时调用 MySqlEFConfiguration 自动完成 EF6 提供程序注册</summary>
public class MySqlEFConfiguration : DbConfiguration
{
    /// <summary>实例化MySql EF配置</summary>
    public MySqlEFConfiguration()
    {
        SetProviderFactory("NewLife.MySql.MySqlClient", MySqlClientFactory.Instance);
        SetProviderServices("NewLife.MySql.MySqlClient", MySqlProviderServices.Instance);
        SetDefaultConnectionFactory(new MySqlConnectionFactory());
    }
}
