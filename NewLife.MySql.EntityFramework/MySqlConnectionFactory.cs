using System.Data.Common;
using System.Data.Entity.Infrastructure;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySql连接工厂。用于 EF6 的 IDbConnectionFactory 实现</summary>
public class MySqlConnectionFactory : IDbConnectionFactory
{
    /// <summary>默认连接字符串模板。使用 {0} 作为数据库名占位符</summary>
    public String BaseConnectionString { get; set; } = "Server=localhost;Port=3306;User Id=root;Password=;";

    /// <summary>创建数据库连接</summary>
    /// <param name="nameOrConnectionString">数据库名或完整连接字符串</param>
    /// <returns></returns>
    public DbConnection CreateConnection(String nameOrConnectionString)
    {
        // 如果包含分号，认为是完整连接字符串
        if (nameOrConnectionString.Contains(';'))
            return new MySqlConnection(nameOrConnectionString);

        // 否则作为数据库名，拼接到基础连接字符串
        var builder = new MySqlConnectionStringBuilder(BaseConnectionString)
        {
            Database = nameOrConnectionString
        };
        return new MySqlConnection(builder.ConnectionString);
    }
}
