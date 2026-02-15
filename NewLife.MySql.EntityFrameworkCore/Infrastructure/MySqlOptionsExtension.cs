using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql EF Core 选项扩展。注册 MySQL 提供程序所需的服务</summary>
public class MySqlOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    /// <summary>实例化</summary>
    public MySqlOptionsExtension() { }

    /// <summary>复制构造</summary>
    /// <param name="copyFrom">源扩展</param>
    protected MySqlOptionsExtension(MySqlOptionsExtension copyFrom) : base(copyFrom) { }

    /// <summary>扩展信息</summary>
    public override DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    /// <summary>克隆</summary>
    /// <returns></returns>
    protected override RelationalOptionsExtension Clone() => new MySqlOptionsExtension(this);

    /// <summary>注册MySQL提供程序服务</summary>
    /// <param name="services">服务集合</param>
    public override void ApplyServices(IServiceCollection services)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));

        new EntityFrameworkRelationalServicesBuilder(services)
            .TryAdd<IDatabaseProvider, DatabaseProvider<MySqlOptionsExtension>>()
            .TryAdd<LoggingDefinitions, MySqlLoggingDefinitions>()
            .TryAdd<IRelationalTypeMappingSource, MySqlTypeMappingSource>()
            .TryAdd<ISqlGenerationHelper, MySqlSqlGenerationHelper>()
            .TryAdd<IRelationalConnection, MySqlRelationalConnection>()
            .TryAdd<IRelationalDatabaseCreator, MySqlDatabaseCreator>()
            .TryAdd<IQuerySqlGeneratorFactory, MySqlQuerySqlGeneratorFactory>()
            .TryAdd<IMethodCallTranslatorProvider, MySqlMethodCallTranslatorProvider>()
            .TryAdd<IMemberTranslatorProvider, MySqlMemberTranslatorProvider>()
            .TryAdd<IRelationalAnnotationProvider, MySqlAnnotationProvider>()
            .TryAdd<IProviderConventionSetBuilder, MySqlConventionSetBuilder>()
            .TryAdd<IUpdateSqlGenerator, MySqlUpdateSqlGenerator>()
            .TryAdd<IModificationCommandBatchFactory, MySqlModificationCommandBatchFactory>()
            .TryAdd<IMigrationsSqlGenerator, MySqlMigrationsSqlGenerator>()
            .TryAdd<IHistoryRepository, MySqlHistoryRepository>()
            .TryAddCoreServices();
    }

    /// <summary>扩展信息实现</summary>
    private sealed class ExtensionInfo : RelationalExtensionInfo
    {
        public ExtensionInfo(IDbContextOptionsExtension extension) : base(extension) { }

        public override Boolean ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) => other is ExtensionInfo;

        public override String LogFragment => "using NewLife.MySql ";

        public override void PopulateDebugInfo(IDictionary<String, String> debugInfo)
        {
            debugInfo["NewLife.MySql:ConnectionString"] = (Extension as MySqlOptionsExtension)?.ConnectionString ?? "";
        }
    }
}
