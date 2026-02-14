using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>服务集合扩展方法</summary>
public static class MySqlServiceCollectionExtensions
{
    /// <summary>注册 MySql Entity Framework Core 服务到依赖注入容器</summary>
    /// <param name="services">服务集合</param>
    /// <param name="connectionString">MySQL 连接字符串</param>
    /// <param name="mySqlOptionsAction">MySql 选项配置回调</param>
    /// <returns></returns>
    public static IServiceCollection AddMySql<TContext>(
        this IServiceCollection services,
        String connectionString,
        Action<MySqlDbContextOptionsBuilder>? mySqlOptionsAction = null)
        where TContext : DbContext
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (String.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));

        return services.AddDbContext<TContext>(options => options.UseMySql(connectionString, mySqlOptionsAction));
    }
}
