using System.ComponentModel;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;
using NewLife.MySql;
using NewLife.MySql.EntityFrameworkCore;

namespace UnitTest.EntityFrameworkCore;

/// <summary>服务注册和 DbConnection 重载测试</summary>
public class MySqlServiceRegistrationTests : IDisposable
{
    private const String TestConnectionString = "Server=localhost;Port=3306;Database=test;User Id=root;Password=pass;";

    private readonly TestDbContext _context;
    private readonly IServiceProvider _services;

    public MySqlServiceRegistrationTests()
    {
        _context = new TestDbContext(new DbContextOptionsBuilder<TestDbContext>()
            .UseMySql(TestConnectionString)
            .Options);
        _services = ((IInfrastructure<IServiceProvider>)_context).Instance;
    }

    public void Dispose() => _context.Dispose();

    [Fact]
    [DisplayName("UseMySql(DbConnection)应注册Extension")]
    public void UseMySql_WithConnection_ShouldRegisterExtension()
    {
        var connection = new MySqlConnection(TestConnectionString);
        var builder = new DbContextOptionsBuilder<TestDbContext>();

        builder.UseMySql(connection);

        var extension = builder.Options.FindExtension<MySqlOptionsExtension>();
        Assert.NotNull(extension);
    }

    [Fact]
    [DisplayName("UseMySql(DbConnection) null应抛出异常")]
    public void UseMySql_NullConnection_ShouldThrow()
    {
        var builder = new DbContextOptionsBuilder<TestDbContext>();

        Assert.Throws<ArgumentNullException>(() => builder.UseMySql((System.Data.Common.DbConnection)null!));
    }

    [Fact]
    [DisplayName("UseMySql泛型(DbConnection)应返回正确类型")]
    public void UseMySqlGeneric_WithConnection_ShouldReturnCorrectType()
    {
        var connection = new MySqlConnection(TestConnectionString);
        var builder = new DbContextOptionsBuilder<TestDbContext>();

        var result = builder.UseMySql(connection);

        Assert.IsType<DbContextOptionsBuilder<TestDbContext>>(result);
        Assert.NotNull(builder.Options.FindExtension<MySqlOptionsExtension>());
    }

    [Fact]
    [DisplayName("IRelationalTypeMappingSource应注册为MySqlTypeMappingSource")]
    public void Services_ShouldRegisterTypeMappingSource()
    {
        var service = _services.GetRequiredService<IRelationalTypeMappingSource>();

        Assert.IsType<MySqlTypeMappingSource>(service);
    }

    [Fact]
    [DisplayName("ISqlGenerationHelper应注册为MySqlSqlGenerationHelper")]
    public void Services_ShouldRegisterSqlGenerationHelper()
    {
        var service = _services.GetRequiredService<ISqlGenerationHelper>();

        Assert.IsType<MySqlSqlGenerationHelper>(service);
    }

    [Fact]
    [DisplayName("IQuerySqlGeneratorFactory应注册为MySqlQuerySqlGeneratorFactory")]
    public void Services_ShouldRegisterQuerySqlGeneratorFactory()
    {
        var service = _services.GetRequiredService<IQuerySqlGeneratorFactory>();

        Assert.IsType<MySqlQuerySqlGeneratorFactory>(service);
    }

    [Fact]
    [DisplayName("IUpdateSqlGenerator应注册为MySqlUpdateSqlGenerator")]
    public void Services_ShouldRegisterUpdateSqlGenerator()
    {
        var service = _services.GetRequiredService<IUpdateSqlGenerator>();

        Assert.IsType<MySqlUpdateSqlGenerator>(service);
    }

    [Fact]
    [DisplayName("IMigrationsSqlGenerator应注册为MySqlMigrationsSqlGenerator")]
    public void Services_ShouldRegisterMigrationsSqlGenerator()
    {
        var service = _services.GetRequiredService<IMigrationsSqlGenerator>();

        Assert.IsType<MySqlMigrationsSqlGenerator>(service);
    }

    [Fact]
    [DisplayName("IHistoryRepository应注册为MySqlHistoryRepository")]
    public void Services_ShouldRegisterHistoryRepository()
    {
        var service = _services.GetRequiredService<IHistoryRepository>();

        Assert.IsType<MySqlHistoryRepository>(service);
    }

    [Fact]
    [DisplayName("IMethodCallTranslatorProvider应注册为MySqlMethodCallTranslatorProvider")]
    public void Services_ShouldRegisterMethodCallTranslatorProvider()
    {
        var service = _services.GetRequiredService<IMethodCallTranslatorProvider>();

        Assert.IsType<MySqlMethodCallTranslatorProvider>(service);
    }

    [Fact]
    [DisplayName("IMemberTranslatorProvider应注册为MySqlMemberTranslatorProvider")]
    public void Services_ShouldRegisterMemberTranslatorProvider()
    {
        var service = _services.GetRequiredService<IMemberTranslatorProvider>();

        Assert.IsType<MySqlMemberTranslatorProvider>(service);
    }

    [Fact]
    [DisplayName("UseMySql应执行配置回调")]
    public void UseMySql_WithOptionsAction_ShouldInvokeCallback()
    {
        var builder = new DbContextOptionsBuilder<TestDbContext>();
        var callbackInvoked = false;

        builder.UseMySql(TestConnectionString, _ => callbackInvoked = true);

        Assert.True(callbackInvoked);
    }

    [Fact]
    [DisplayName("MySqlOptionsExtension信息应正确填充DebugInfo")]
    public void ExtensionInfo_ShouldPopulateDebugInfo()
    {
        var extension = (MySqlOptionsExtension)new MySqlOptionsExtension().WithConnectionString(TestConnectionString);

        var debugInfo = new Dictionary<String, String>();
        extension.Info.PopulateDebugInfo(debugInfo);

        Assert.True(debugInfo.ContainsKey("NewLife.MySql:ConnectionString"));
        Assert.Equal(TestConnectionString, debugInfo["NewLife.MySql:ConnectionString"]);
    }

    [Fact]
    [DisplayName("MySqlOptionsExtension.ShouldUseSameServiceProvider应正确判断")]
    public void ExtensionInfo_ShouldUseSameServiceProvider_SameType()
    {
        var ext1 = (MySqlOptionsExtension)new MySqlOptionsExtension().WithConnectionString(TestConnectionString);
        var ext2 = (MySqlOptionsExtension)new MySqlOptionsExtension().WithConnectionString(TestConnectionString);

        Assert.True(ext1.Info.ShouldUseSameServiceProvider(ext2.Info));
    }
}
