using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.Migrations.History;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySql 迁移历史上下文。自定义 __MigrationHistory 表结构以适配 MySQL 存储限制</summary>
public class MySqlHistoryContext : HistoryContext
{
    /// <summary>实例化</summary>
    /// <param name="existingConnection">现有连接</param>
    /// <param name="defaultSchema">默认 Schema</param>
    public MySqlHistoryContext(DbConnection existingConnection, String defaultSchema)
        : base(existingConnection, defaultSchema) { }

    /// <summary>配置模型。调整列长度以适配 MySQL InnoDB 索引限制（最大 767 字节 / 3072 字节）</summary>
    /// <param name="modelBuilder">模型构建器</param>
    protected override void OnModelCreating(DbModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // MySQL InnoDB 默认索引前缀限制为 767 字节（utf8mb4 下约 191 个字符）
        // 将 MigrationId 和 ContextKey 的长度限制在安全范围内
        modelBuilder.Entity<HistoryRow>().Property(h => h.MigrationId).HasMaxLength(150).IsRequired();
        modelBuilder.Entity<HistoryRow>().Property(h => h.ContextKey).HasMaxLength(300).IsRequired();
    }
}
