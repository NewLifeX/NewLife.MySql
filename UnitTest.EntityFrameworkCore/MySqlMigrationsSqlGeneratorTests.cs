using System.ComponentModel;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using NewLife.MySql.EntityFrameworkCore;

namespace UnitTest.EntityFrameworkCore;

/// <summary>迁移 SQL 生成器测试</summary>
public class MySqlMigrationsSqlGeneratorTests
{
    private const String TestConnectionString = "Server=localhost;Port=3306;Database=test;User Id=root;Password=pass;";

    [Fact]
    [DisplayName("CreateTable应生成ENGINE和CHARSET")]
    public void CreateTable_ShouldIncludeEngineAndCharset()
    {
        var generator = CreateGenerator();

        var operation = new CreateTableOperation
        {
            Name = "test_table",
            Columns =
            {
                new AddColumnOperation
                {
                    Name = "Id",
                    Table = "test_table",
                    ClrType = typeof(Int32),
                    ColumnType = "INT",
                    IsNullable = false,
                },
                new AddColumnOperation
                {
                    Name = "Name",
                    Table = "test_table",
                    ClrType = typeof(String),
                    ColumnType = "VARCHAR(100)",
                    IsNullable = true,
                }
            },
            PrimaryKey = new AddPrimaryKeyOperation
            {
                Name = "PK_test_table",
                Columns = ["Id"]
            }
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("CREATE TABLE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`test_table`", sql);
        Assert.Contains("`Id`", sql);
        Assert.Contains("`Name`", sql);
        Assert.Contains("PRIMARY KEY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ENGINE=InnoDB", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("utf8mb4", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("CreateTable列应标记NOT NULL")]
    public void CreateTable_NotNullColumn_ShouldIncludeNotNull()
    {
        var generator = CreateGenerator();

        var operation = new CreateTableOperation
        {
            Name = "test_table",
            Columns =
            {
                new AddColumnOperation
                {
                    Name = "Id",
                    Table = "test_table",
                    ClrType = typeof(Int32),
                    ColumnType = "INT",
                    IsNullable = false,
                }
            },
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("NOT NULL", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("AddColumn应生成ALTER TABLE ADD")]
    public void AddColumn_ShouldGenerateAlterTableAdd()
    {
        var generator = CreateGenerator();

        var operation = new AddColumnOperation
        {
            Table = "users",
            Name = "Email",
            ClrType = typeof(String),
            ColumnType = "VARCHAR(200)",
            IsNullable = true,
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("ALTER TABLE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`users`", sql);
        Assert.Contains("ADD", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`Email`", sql);
        Assert.Contains("VARCHAR(200)", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("AlterColumn应生成MODIFY COLUMN")]
    public void AlterColumn_ShouldGenerateModifyColumn()
    {
        var generator = CreateGenerator();

        var operation = new AlterColumnOperation
        {
            Table = "users",
            Name = "Name",
            ClrType = typeof(String),
            ColumnType = "VARCHAR(500)",
            IsNullable = false,
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("ALTER TABLE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("MODIFY COLUMN", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`Name`", sql);
        Assert.Contains("VARCHAR(500)", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("DropColumn应生成ALTER TABLE DROP COLUMN")]
    public void DropColumn_ShouldGenerateDropColumn()
    {
        var generator = CreateGenerator();

        var operation = new DropColumnOperation
        {
            Table = "users",
            Name = "Email",
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("ALTER TABLE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("DROP COLUMN", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`Email`", sql);
    }

    [Fact]
    [DisplayName("RenameTable应生成RENAME TABLE")]
    public void RenameTable_ShouldGenerateRenameTable()
    {
        var generator = CreateGenerator();

        var operation = new RenameTableOperation
        {
            Name = "old_table",
            NewName = "new_table",
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("RENAME TABLE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`old_table`", sql);
        Assert.Contains("`new_table`", sql);
    }

    [Fact]
    [DisplayName("RenameColumn应生成RENAME COLUMN")]
    public void RenameColumn_ShouldGenerateRenameColumn()
    {
        var generator = CreateGenerator();

        var operation = new RenameColumnOperation
        {
            Table = "users",
            Name = "OldName",
            NewName = "NewName",
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("RENAME COLUMN", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`OldName`", sql);
        Assert.Contains("`NewName`", sql);
    }

    [Fact]
    [DisplayName("CreateIndex应生成CREATE INDEX")]
    public void CreateIndex_ShouldGenerateCreateIndex()
    {
        var generator = CreateGenerator();

        var operation = new CreateIndexOperation
        {
            Name = "IX_users_name",
            Table = "users",
            Columns = ["Name"],
            IsUnique = false,
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("CREATE INDEX", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`IX_users_name`", sql);
        Assert.Contains("`users`", sql);
        Assert.Contains("`Name`", sql);
    }

    [Fact]
    [DisplayName("CreateUniqueIndex应生成CREATE UNIQUE INDEX")]
    public void CreateUniqueIndex_ShouldGenerateCreateUniqueIndex()
    {
        var generator = CreateGenerator();

        var operation = new CreateIndexOperation
        {
            Name = "IX_users_email",
            Table = "users",
            Columns = ["Email"],
            IsUnique = true,
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("CREATE UNIQUE INDEX", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("DropIndex应生成DROP INDEX ON")]
    public void DropIndex_ShouldGenerateDropIndexOn()
    {
        var generator = CreateGenerator();

        var operation = new DropIndexOperation
        {
            Name = "IX_users_name",
            Table = "users",
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("DROP INDEX", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`IX_users_name`", sql);
        Assert.Contains("ON", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`users`", sql);
    }

    [Fact]
    [DisplayName("DropTable应生成DROP TABLE IF EXISTS")]
    public void DropTable_ShouldGenerateDropTableIfExists()
    {
        var generator = CreateGenerator();

        var operation = new DropTableOperation { Name = "old_table" };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("DROP TABLE IF EXISTS", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`old_table`", sql);
    }

    [Fact]
    [DisplayName("RenameIndex应生成ALTER TABLE RENAME INDEX")]
    public void RenameIndex_ShouldGenerateRenameIndex()
    {
        var generator = CreateGenerator();

        var operation = new RenameIndexOperation
        {
            Table = "users",
            Name = "IX_old",
            NewName = "IX_new",
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("ALTER TABLE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RENAME INDEX", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`IX_old`", sql);
        Assert.Contains("`IX_new`", sql);
    }

    [Fact]
    [DisplayName("EnsureSchema不应产生SQL")]
    public void EnsureSchema_ShouldNotProduceSql()
    {
        var generator = CreateGenerator();

        var operation = new EnsureSchemaOperation { Name = "dbo" };

        var commands = generator.Generate([operation]);

        // MySQL 不支持 Schema，应为空
        Assert.Empty(commands);
    }

    [Fact]
    [DisplayName("CreateTable带默认值应包含DEFAULT")]
    public void CreateTable_WithDefaultValue_ShouldIncludeDefault()
    {
        var generator = CreateGenerator();

        var operation = new CreateTableOperation
        {
            Name = "test_table",
            Columns =
            {
                new AddColumnOperation
                {
                    Name = "Status",
                    Table = "test_table",
                    ClrType = typeof(Int32),
                    ColumnType = "INT",
                    IsNullable = false,
                    DefaultValue = 1,
                }
            },
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("DEFAULT 1", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("CreateTable带注释应包含COMMENT")]
    public void CreateTable_WithComment_ShouldIncludeComment()
    {
        var generator = CreateGenerator();

        var operation = new CreateTableOperation
        {
            Name = "test_table",
            Columns =
            {
                new AddColumnOperation
                {
                    Name = "Name",
                    Table = "test_table",
                    ClrType = typeof(String),
                    ColumnType = "VARCHAR(100)",
                    IsNullable = true,
                    Comment = "用户名",
                }
            },
        };

        var commands = generator.Generate([operation]);
        var sql = GetSql(commands);

        Assert.Contains("COMMENT '用户名'", sql);
    }

    #region 辅助
    private static IMigrationsSqlGenerator CreateGenerator()
    {
        var options = new DbContextOptionsBuilder()
            .UseMySql(TestConnectionString)
            .Options;

        using var context = new TestDbContext(new DbContextOptionsBuilder<TestDbContext>()
            .UseMySql(TestConnectionString)
            .Options);

        var services = ((IInfrastructure<IServiceProvider>)context).Instance;
        return services.GetRequiredService<IMigrationsSqlGenerator>();
    }

    private static String GetSql(IReadOnlyList<MigrationCommand> commands) =>
        String.Join("\n", commands.Select(c => c.CommandText));
    #endregion
}
