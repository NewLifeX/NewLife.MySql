using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 迁移 SQL 生成器。将 EF Core 迁移操作转换为 MySQL DDL 语句</summary>
public class MySqlMigrationsSqlGenerator : MigrationsSqlGenerator
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">迁移 SQL 生成器依赖</param>
    public MySqlMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies) : base(dependencies) { }

    /// <summary>生成创建表操作的 SQL</summary>
    /// <param name="operation">创建表操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    /// <param name="terminate">是否添加终结符</param>
    protected override void Generate(CreateTableOperation operation, IModel? model, MigrationCommandListBuilder builder, Boolean terminate = true)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.Append("CREATE TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema))
            .AppendLine(" (");

        using (builder.Indent())
        {
            // 生成列定义
            for (var i = 0; i < operation.Columns.Count; i++)
            {
                var column = operation.Columns[i];
                ColumnDefinition(column, model, builder);

                if (i < operation.Columns.Count - 1 || operation.PrimaryKey != null || operation.UniqueConstraints.Count > 0 || operation.ForeignKeys.Count > 0)
                    builder.AppendLine(",");
                else
                    builder.AppendLine();
            }

            // 主键约束
            if (operation.PrimaryKey != null)
            {
                builder.Append("PRIMARY KEY (");
                builder.Append(String.Join(", ", operation.PrimaryKey.Columns.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))));
                builder.Append(")");

                if (operation.UniqueConstraints.Count > 0 || operation.ForeignKeys.Count > 0)
                    builder.AppendLine(",");
                else
                    builder.AppendLine();
            }

            // 唯一约束
            for (var i = 0; i < operation.UniqueConstraints.Count; i++)
            {
                var constraint = operation.UniqueConstraints[i];
                builder.Append("UNIQUE KEY ")
                    .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(constraint.Name))
                    .Append(" (")
                    .Append(String.Join(", ", constraint.Columns.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))))
                    .Append(")");

                if (i < operation.UniqueConstraints.Count - 1 || operation.ForeignKeys.Count > 0)
                    builder.AppendLine(",");
                else
                    builder.AppendLine();
            }

            // 外键约束
            for (var i = 0; i < operation.ForeignKeys.Count; i++)
            {
                var fk = operation.ForeignKeys[i];
                builder.Append("CONSTRAINT ")
                    .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(fk.Name))
                    .Append(" FOREIGN KEY (")
                    .Append(String.Join(", ", fk.Columns.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))))
                    .Append(") REFERENCES ")
                    .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(fk.PrincipalTable, fk.PrincipalSchema))
                    .Append(" (")
                    .Append(String.Join(", ", fk.PrincipalColumns.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))))
                    .Append(")");

                if (fk.OnDelete != ReferentialAction.NoAction)
                {
                    builder.Append(" ON DELETE ").Append(GetReferentialAction(fk.OnDelete));
                }
                if (fk.OnUpdate != ReferentialAction.NoAction)
                {
                    builder.Append(" ON UPDATE ").Append(GetReferentialAction(fk.OnUpdate));
                }

                if (i < operation.ForeignKeys.Count - 1)
                    builder.AppendLine(",");
                else
                    builder.AppendLine();
            }
        }

        builder.Append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>生成列定义</summary>
    /// <param name="operation">列操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    protected override void ColumnDefinition(AddColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" ")
            .Append(operation.ColumnType ?? GetColumnType(operation.ClrType));

        if (!operation.IsNullable)
            builder.Append(" NOT NULL");

        if (operation.DefaultValue != null)
        {
            builder.Append(" DEFAULT ").Append(FormatDefaultValue(operation.DefaultValue));
        }
        else if (!String.IsNullOrEmpty(operation.DefaultValueSql))
        {
            builder.Append(" DEFAULT ").Append(operation.DefaultValueSql);
        }

        // AUTO_INCREMENT 处理
        if (operation[MySqlAnnotationNames.ValueGenerationStrategy] is MySqlValueGenerationStrategy strategy
            && strategy == MySqlValueGenerationStrategy.AutoIncrement)
        {
            builder.Append(" AUTO_INCREMENT");
        }

        if (!String.IsNullOrEmpty(operation.Comment))
        {
            builder.Append(" COMMENT '").Append(operation.Comment.Replace("'", "''")).Append("'");
        }
    }

    /// <summary>生成添加列操作的 SQL</summary>
    /// <param name="operation">添加列操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    /// <param name="terminate">是否添加终结符</param>
    protected override void Generate(AddColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, Boolean terminate = true)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" ADD ");

        ColumnDefinition(operation, model, builder);

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>生成修改列操作的 SQL。MySQL 使用 MODIFY COLUMN 语法</summary>
    /// <param name="operation">修改列操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    protected override void Generate(AlterColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" MODIFY COLUMN ");

        // 构造 AddColumnOperation 复用 ColumnDefinition
        var addColumn = new AddColumnOperation
        {
            Table = operation.Table,
            Schema = operation.Schema,
            Name = operation.Name,
            ClrType = operation.ClrType,
            ColumnType = operation.ColumnType,
            IsNullable = operation.IsNullable,
            DefaultValue = operation.DefaultValue,
            DefaultValueSql = operation.DefaultValueSql,
            Comment = operation.Comment,
        };

        // 复制注解
        foreach (var annotation in operation.GetAnnotations())
        {
            addColumn.AddAnnotation(annotation.Name, annotation.Value);
        }

        ColumnDefinition(addColumn, model, builder);

        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    /// <summary>生成删除列操作的 SQL</summary>
    /// <param name="operation">删除列操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    /// <param name="terminate">是否添加终结符</param>
    protected override void Generate(DropColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, Boolean terminate = true)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" DROP COLUMN ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>生成重命名表操作的 SQL。MySQL 使用 RENAME TABLE 语法</summary>
    /// <param name="operation">重命名表操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    protected override void Generate(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.Append("RENAME TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema))
            .Append(" TO ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName!, operation.NewSchema));

        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    /// <summary>生成重命名列操作的 SQL。MySQL 8.0+ 使用 RENAME COLUMN 语法</summary>
    /// <param name="operation">重命名列操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    protected override void Generate(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" RENAME COLUMN ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" TO ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName));

        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    /// <summary>生成创建索引操作的 SQL</summary>
    /// <param name="operation">创建索引操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    /// <param name="terminate">是否添加终结符</param>
    protected override void Generate(CreateIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, Boolean terminate = true)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.Append("CREATE ");

        if (operation.IsUnique)
            builder.Append("UNIQUE ");

        builder.Append("INDEX ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" ON ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" (")
            .Append(String.Join(", ", operation.Columns.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))))
            .Append(")");

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>生成删除索引操作的 SQL。MySQL 使用 DROP INDEX ... ON 语法</summary>
    /// <param name="operation">删除索引操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    /// <param name="terminate">是否添加终结符</param>
    protected override void Generate(DropIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, Boolean terminate = true)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.Append("DROP INDEX ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" ON ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table!, operation.Schema));

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>生成删除表操作的 SQL</summary>
    /// <param name="operation">删除表操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    /// <param name="terminate">是否添加终结符</param>
    protected override void Generate(DropTableOperation operation, IModel? model, MigrationCommandListBuilder builder, Boolean terminate = true)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.Append("DROP TABLE IF EXISTS ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema));

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>生成确保 Schema 存在的操作。MySQL 不使用 Schema，跳过</summary>
    /// <param name="operation">确保 Schema 操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    protected override void Generate(EnsureSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        // MySQL 不支持 Schema 概念，跳过
    }

    /// <summary>生成重命名索引操作的 SQL。MySQL 8.0+ 支持 ALTER TABLE ... RENAME INDEX</summary>
    /// <param name="operation">重命名索引操作</param>
    /// <param name="model">数据模型</param>
    /// <param name="builder">迁移命令构建器</param>
    protected override void Generate(RenameIndexOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table!, operation.Schema))
            .Append(" RENAME INDEX ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" TO ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName));

        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    #region 辅助
    /// <summary>根据 CLR 类型获取 MySQL 列类型</summary>
    /// <param name="clrType">CLR 类型</param>
    /// <returns></returns>
    private static String GetColumnType(Type clrType)
    {
        var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;

        if (underlyingType == typeof(Int32)) return "INT";
        if (underlyingType == typeof(Int64)) return "BIGINT";
        if (underlyingType == typeof(Int16)) return "SMALLINT";
        if (underlyingType == typeof(Byte)) return "TINYINT UNSIGNED";
        if (underlyingType == typeof(Boolean)) return "TINYINT(1)";
        if (underlyingType == typeof(String)) return "VARCHAR(255)";
        if (underlyingType == typeof(Double)) return "DOUBLE";
        if (underlyingType == typeof(Single)) return "FLOAT";
        if (underlyingType == typeof(Decimal)) return "DECIMAL(65,30)";
        if (underlyingType == typeof(DateTime)) return "DATETIME";
        if (underlyingType == typeof(DateTimeOffset)) return "DATETIME";
        if (underlyingType == typeof(TimeSpan)) return "TIME";
        if (underlyingType == typeof(Guid)) return "CHAR(36)";
        if (underlyingType == typeof(Byte[])) return "BLOB";

        return "VARCHAR(255)";
    }

    /// <summary>格式化默认值字面量</summary>
    /// <param name="value">默认值</param>
    /// <returns></returns>
    private static String FormatDefaultValue(Object value) => value switch
    {
        String s => $"'{s.Replace("'", "''")}'",
        Boolean b => b ? "1" : "0",
        DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
        _ => value.ToString() ?? "NULL"
    };

    /// <summary>获取引用操作文本</summary>
    /// <param name="action">引用操作</param>
    /// <returns></returns>
    private static String GetReferentialAction(ReferentialAction action) => action switch
    {
        ReferentialAction.Cascade => "CASCADE",
        ReferentialAction.Restrict => "RESTRICT",
        ReferentialAction.SetNull => "SET NULL",
        ReferentialAction.SetDefault => "SET DEFAULT",
        _ => "NO ACTION"
    };
    #endregion
}
