using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>DbContextOptionsBuilder 扩展。提供 UseMySql 配置入口</summary>
public static class MySqlDbContextOptionsExtensions
{
    /// <summary>配置 DbContext 使用 NewLife.MySql 数据库提供程序</summary>
    /// <param name="optionsBuilder">选项构建器</param>
    /// <param name="connectionString">MySql 连接字符串</param>
    /// <param name="mySqlOptionsAction">MySql 选项配置回调</param>
    /// <returns></returns>
    public static DbContextOptionsBuilder UseMySql(
        this DbContextOptionsBuilder optionsBuilder,
        String connectionString,
        Action<MySqlDbContextOptionsBuilder>? mySqlOptionsAction = null)
    {
        if (optionsBuilder == null) throw new ArgumentNullException(nameof(optionsBuilder));
        if (String.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));

        var extension = (MySqlOptionsExtension)GetOrCreateExtension(optionsBuilder).WithConnectionString(connectionString);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        mySqlOptionsAction?.Invoke(new MySqlDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    /// <summary>配置 DbContext 使用 NewLife.MySql 数据库提供程序（泛型版本）</summary>
    /// <typeparam name="TContext">DbContext 类型</typeparam>
    /// <param name="optionsBuilder">选项构建器</param>
    /// <param name="connectionString">MySql 连接字符串</param>
    /// <param name="mySqlOptionsAction">MySql 选项配置回调</param>
    /// <returns></returns>
    public static DbContextOptionsBuilder<TContext> UseMySql<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        String connectionString,
        Action<MySqlDbContextOptionsBuilder>? mySqlOptionsAction = null)
        where TContext : DbContext
    {
        ((DbContextOptionsBuilder)optionsBuilder).UseMySql(connectionString, mySqlOptionsAction);
        return optionsBuilder;
    }

    /// <summary>配置 DbContext 使用已有的 DbConnection 连接到 MySQL</summary>
    /// <param name="optionsBuilder">选项构建器</param>
    /// <param name="connection">已有的数据库连接</param>
    /// <param name="mySqlOptionsAction">MySql 选项配置回调</param>
    /// <returns></returns>
    public static DbContextOptionsBuilder UseMySql(
        this DbContextOptionsBuilder optionsBuilder,
        DbConnection connection,
        Action<MySqlDbContextOptionsBuilder>? mySqlOptionsAction = null)
    {
        if (optionsBuilder == null) throw new ArgumentNullException(nameof(optionsBuilder));
        if (connection == null) throw new ArgumentNullException(nameof(connection));

        var extension = (MySqlOptionsExtension)GetOrCreateExtension(optionsBuilder).WithConnection(connection);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        mySqlOptionsAction?.Invoke(new MySqlDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    /// <summary>配置 DbContext 使用已有的 DbConnection 连接到 MySQL（泛型版本）</summary>
    /// <typeparam name="TContext">DbContext 类型</typeparam>
    /// <param name="optionsBuilder">选项构建器</param>
    /// <param name="connection">已有的数据库连接</param>
    /// <param name="mySqlOptionsAction">MySql 选项配置回调</param>
    /// <returns></returns>
    public static DbContextOptionsBuilder<TContext> UseMySql<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        DbConnection connection,
        Action<MySqlDbContextOptionsBuilder>? mySqlOptionsAction = null)
        where TContext : DbContext
    {
        ((DbContextOptionsBuilder)optionsBuilder).UseMySql(connection, mySqlOptionsAction);
        return optionsBuilder;
    }

    private static MySqlOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder) =>
        optionsBuilder.Options.FindExtension<MySqlOptionsExtension>() ?? new MySqlOptionsExtension();
}
