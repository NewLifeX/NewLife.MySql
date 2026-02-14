using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 迁移历史记录仓储。使用 MySQL 表存储 EF Core 迁移历史</summary>
public class MySqlHistoryRepository : HistoryRepository
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">迁移历史仓储依赖</param>
    public MySqlHistoryRepository(HistoryRepositoryDependencies dependencies) : base(dependencies) { }

    /// <summary>锁释放行为。MySQL 不使用显式锁</summary>
    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Transaction;

    /// <summary>检查历史表是否存在的 SQL</summary>
    protected override String ExistsSql
    {
        get
        {
            var tableName = Dependencies.SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema);
            return $"SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{TableName}' LIMIT 1;";
        }
    }

    /// <summary>历史表是否存在</summary>
    /// <param name="result">查询结果</param>
    /// <returns></returns>
    protected override Boolean InterpretExistsResult(Object? result) => result != null && result != DBNull.Value;

    /// <summary>获取数据库锁（同步）</summary>
    /// <returns></returns>
    public override IMigrationsDatabaseLock AcquireDatabaseLock() => new MySqlMigrationsDatabaseLock(this);

    /// <summary>获取数据库锁（异步）</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IMigrationsDatabaseLock>(new MySqlMigrationsDatabaseLock(this));


    /// <summary>获取创建历史表的 SQL</summary>
    /// <returns></returns>
    public override String GetCreateIfNotExistsScript()
    {
        var tableName = Dependencies.SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema);
        return $"""
            CREATE TABLE IF NOT EXISTS {tableName} (
                `MigrationId` VARCHAR(150) NOT NULL,
                `ProductVersion` VARCHAR(32) NOT NULL,
                PRIMARY KEY (`MigrationId`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """;
    }

    /// <summary>获取创建历史表的 SQL</summary>
    /// <returns></returns>
    public override String GetBeginIfNotExistsScript(String migrationId) => $"-- Migration: {migrationId}";

    /// <summary>获取结束条件脚本</summary>
    /// <returns></returns>
    public override String GetBeginIfExistsScript(String migrationId) => $"-- Migration exists: {migrationId}";

    /// <summary>获取结束条件脚本</summary>
    /// <returns></returns>
    public override String GetEndIfScript() => String.Empty;

    /// <summary>MySQL 迁移数据库锁（空实现）</summary>
    private sealed class MySqlMigrationsDatabaseLock(MySqlHistoryRepository historyRepository) : IMigrationsDatabaseLock
    {
        public IHistoryRepository HistoryRepository => historyRepository;

        public void Dispose() { }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
