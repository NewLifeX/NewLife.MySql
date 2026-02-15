using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 约定集构建器。为 MySQL 提供模型构建约定</summary>
public class MySqlConventionSetBuilder : RelationalConventionSetBuilder
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">提供程序约定依赖</param>
    /// <param name="relationalDependencies">关系型约定依赖</param>
    public MySqlConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies) { }
}
