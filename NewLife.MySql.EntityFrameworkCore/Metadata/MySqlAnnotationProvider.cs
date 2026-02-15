using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 注解提供者。为迁移操作提供 MySQL 特定的注解信息</summary>
public class MySqlAnnotationProvider : RelationalAnnotationProvider
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">依赖</param>
    public MySqlAnnotationProvider(RelationalAnnotationProviderDependencies dependencies) : base(dependencies) { }
}
