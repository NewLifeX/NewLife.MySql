using System.ComponentModel;
using System.Data.Entity.Core.Common;
using System.Data.Entity.Infrastructure;
using System.Data.Entity.Infrastructure.DependencyResolution;
using System.Data.Entity.Migrations.Sql;
using NewLife.MySql;
using NewLife.MySql.EntityFramework;

namespace UnitTest.EntityFramework;

/// <summary>ProviderServices 补充测试</summary>
public class MySqlProviderServicesTests
{
    #region 基本功能
    [Fact]
    [DisplayName("单例实例应不为空")]
    public void Instance_ShouldNotBeNull()
    {
        Assert.NotNull(MySqlProviderServices.Instance);
    }

    [Fact]
    [DisplayName("获取ManifestToken应不为空")]
    public void GetProviderManifestToken_ShouldReturnVersion()
    {
        var conn = new MySqlConnection("Server=localhost;Port=3306;Database=test;");

        // ManifestToken 通常返回服务器版本，连接未打开时返回默认值
        var token = MySqlProviderServices.Instance.GetProviderManifestToken(conn);

        Assert.NotNull(token);
    }

    [Fact]
    [DisplayName("获取Manifest应返回MySqlProviderManifest")]
    public void GetProviderManifest_ShouldReturnInstance()
    {
        var manifest = MySqlProviderServices.Instance.GetProviderManifest("8.0");

        Assert.NotNull(manifest);
        Assert.Equal("NewLife.MySql", manifest.NamespaceName);
    }

    [Fact]
    [DisplayName("不同版本Token都应能获取Manifest")]
    public void GetProviderManifest_DifferentVersions_ShouldWork()
    {
        var m57 = MySqlProviderServices.Instance.GetProviderManifest("5.7");
        var m80 = MySqlProviderServices.Instance.GetProviderManifest("8.0");
        var m84 = MySqlProviderServices.Instance.GetProviderManifest("8.4.0");

        Assert.NotNull(m57);
        Assert.NotNull(m80);
        Assert.NotNull(m84);
    }
    #endregion
}

/// <summary>DependencyResolver 测试</summary>
public class MySqlDbDependencyResolverTests
{
    private readonly MySqlDbDependencyResolver _resolver = new();

    [Fact]
    [DisplayName("应解析DbProviderServices")]
    public void GetService_DbProviderServices_ShouldReturn()
    {
        var result = _resolver.GetService(typeof(DbProviderServices), MySqlEFConfiguration.ProviderInvariantName);

        Assert.NotNull(result);
        Assert.Same(MySqlProviderServices.Instance, result);
    }

    [Fact]
    [DisplayName("应解析MigrationSqlGenerator")]
    public void GetService_MigrationSqlGenerator_ShouldReturn()
    {
        var result = _resolver.GetService(typeof(MigrationSqlGenerator), MySqlEFConfiguration.ProviderInvariantName);

        Assert.NotNull(result);
        Assert.IsType<MySqlMigrationSqlGenerator>(result);
    }

    [Fact]
    [DisplayName("应解析IDbConnectionFactory")]
    public void GetService_IDbConnectionFactory_ShouldReturn()
    {
        var result = _resolver.GetService(typeof(IDbConnectionFactory), MySqlEFConfiguration.ProviderInvariantName);

        Assert.NotNull(result);
        Assert.IsType<MySqlConnectionFactory>(result);
    }

    [Fact]
    [DisplayName("应解析IDbExecutionStrategy")]
    public void GetService_IDbExecutionStrategy_ShouldReturn()
    {
        var result = _resolver.GetService(typeof(IDbExecutionStrategy), MySqlEFConfiguration.ProviderInvariantName);

        Assert.NotNull(result);
        Assert.IsType<MySqlExecutionStrategy>(result);
    }

    [Fact]
    [DisplayName("非MySql提供程序名应返回null")]
    public void GetService_OtherProvider_ShouldReturnNull()
    {
        var result = _resolver.GetService(typeof(DbProviderServices), "System.Data.SqlClient");

        Assert.Null(result);
    }

    [Fact]
    [DisplayName("不匹配的类型应返回null")]
    public void GetService_UnknownType_ShouldReturnNull()
    {
        var result = _resolver.GetService(typeof(String), MySqlEFConfiguration.ProviderInvariantName);

        Assert.Null(result);
    }

    [Fact]
    [DisplayName("GetServices应返回匹配的服务")]
    public void GetServices_ShouldReturnMatchingServices()
    {
        var services = _resolver.GetServices(typeof(DbProviderServices), MySqlEFConfiguration.ProviderInvariantName).ToList();

        Assert.Single(services);
        Assert.Same(MySqlProviderServices.Instance, services[0]);
    }

    [Fact]
    [DisplayName("GetServices非匹配应返回空")]
    public void GetServices_NonMatching_ShouldReturnEmpty()
    {
        var services = _resolver.GetServices(typeof(DbProviderServices), "Other.Provider").ToList();

        Assert.Empty(services);
    }
}

/// <summary>MySqlHistoryContext 测试</summary>
public class MySqlHistoryContextTests
{
    [Fact]
    [DisplayName("HistoryContext应继承HistoryContext基类")]
    public void HistoryContext_ShouldInheritFromHistoryContext()
    {
        Assert.True(typeof(System.Data.Entity.Migrations.History.HistoryContext).IsAssignableFrom(typeof(MySqlHistoryContext)));
    }

    [Fact]
    [DisplayName("HistoryContext应有正确的构造函数签名")]
    public void HistoryContext_ShouldHaveCorrectConstructor()
    {
        // EF6 要求 HistoryContext 子类有 (DbConnection, String) 构造函数
        var ctor = typeof(MySqlHistoryContext).GetConstructor([typeof(System.Data.Common.DbConnection), typeof(String)]);

        Assert.NotNull(ctor);
        Assert.True(ctor.IsPublic);
    }

    [Fact]
    [DisplayName("HistoryContext应重写OnModelCreating")]
    public void HistoryContext_ShouldOverrideOnModelCreating()
    {
        var method = typeof(MySqlHistoryContext).GetMethod(
            "OnModelCreating",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance,
            null,
            [typeof(System.Data.Entity.DbModelBuilder)],
            null);

        Assert.NotNull(method);
        // 确认是重写而非基类方法
        Assert.Equal(typeof(MySqlHistoryContext), method.DeclaringType);
    }
}
