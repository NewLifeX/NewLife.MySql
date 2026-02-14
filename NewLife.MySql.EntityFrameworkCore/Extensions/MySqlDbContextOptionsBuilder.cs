using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql DbContext 选项构建器。用于配置 MySQL 特定的选项</summary>
public class MySqlDbContextOptionsBuilder : RelationalDbContextOptionsBuilder<MySqlDbContextOptionsBuilder, MySqlOptionsExtension>
{
    /// <summary>实例化</summary>
    /// <param name="optionsBuilder">选项构建器</param>
    public MySqlDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder) : base(optionsBuilder) { }
}
