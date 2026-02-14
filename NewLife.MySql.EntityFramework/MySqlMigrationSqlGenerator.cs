using System.Data.Entity.Migrations.Model;
using System.Data.Entity.Migrations.Sql;
using System.Text;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySql迁移SQL生成器。将 EF6 迁移操作转换为 MySQL DDL 语句</summary>
public class MySqlMigrationSqlGenerator : MigrationSqlGenerator
{
    /// <summary>生成迁移SQL语句</summary>
    /// <param name="migrationOperations">迁移操作集合</param>
    /// <param name="providerManifestToken">提供程序清单标记</param>
    /// <returns></returns>
    public override IEnumerable<MigrationStatement> Generate(IEnumerable<MigrationOperation> migrationOperations, String providerManifestToken)
    {
        var statements = new List<MigrationStatement>();

        foreach (var operation in migrationOperations)
        {
            var sql = operation switch
            {
                CreateTableOperation createTable => GenerateCreateTable(createTable),
                DropTableOperation dropTable => GenerateDropTable(dropTable),
                AddColumnOperation addColumn => GenerateAddColumn(addColumn),
                DropColumnOperation dropColumn => GenerateDropColumn(dropColumn),
                AlterColumnOperation alterColumn => GenerateAlterColumn(alterColumn),
                RenameTableOperation renameTable => GenerateRenameTable(renameTable),
                AddPrimaryKeyOperation addPk => GenerateAddPrimaryKey(addPk),
                DropPrimaryKeyOperation dropPk => GenerateDropPrimaryKey(dropPk),
                AddForeignKeyOperation addFk => GenerateAddForeignKey(addFk),
                DropForeignKeyOperation dropFk => GenerateDropForeignKey(dropFk),
                CreateIndexOperation createIndex => GenerateCreateIndex(createIndex),
                DropIndexOperation dropIndex => GenerateDropIndex(dropIndex),
                SqlOperation sqlOp => sqlOp.Sql,
                _ => null,
            };

            if (sql != null)
                statements.Add(new MigrationStatement { Sql = sql });
        }

        return statements;
    }

    private static String GenerateCreateTable(CreateTableOperation op)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE {Quote(op.Name)} (");

        var first = true;
        foreach (var col in op.Columns)
        {
            if (!first) sb.AppendLine(",");
            first = false;
            sb.Append($"  {Quote(col.Name)} {GetColumnType(col)}");

            if (!col.IsNullable.GetValueOrDefault(true))
                sb.Append(" NOT NULL");

            if (col.IsIdentity)
                sb.Append(" AUTO_INCREMENT");

            if (col.DefaultValue != null)
                sb.Append($" DEFAULT {FormatDefaultValue(col.DefaultValue)}");
            else if (!String.IsNullOrEmpty(col.DefaultValueSql))
                sb.Append($" DEFAULT {col.DefaultValueSql}");
        }

        // 主键
        if (op.PrimaryKey != null)
        {
            sb.AppendLine(",");
            sb.Append($"  PRIMARY KEY ({String.Join(", ", op.PrimaryKey.Columns.Select(Quote))})");
        }

        sb.AppendLine();
        sb.Append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        return sb.ToString();
    }

    private static String GenerateDropTable(DropTableOperation op) => $"DROP TABLE IF EXISTS {Quote(op.Name)}";

    private static String GenerateAddColumn(AddColumnOperation op)
    {
        var col = op.Column;
        var sb = new StringBuilder();
        sb.Append($"ALTER TABLE {Quote(op.Table)} ADD {Quote(col.Name)} {GetColumnType(col)}");

        if (!col.IsNullable.GetValueOrDefault(true))
            sb.Append(" NOT NULL");

        if (col.DefaultValue != null)
            sb.Append($" DEFAULT {FormatDefaultValue(col.DefaultValue)}");
        else if (!String.IsNullOrEmpty(col.DefaultValueSql))
            sb.Append($" DEFAULT {col.DefaultValueSql}");

        return sb.ToString();
    }

    private static String GenerateDropColumn(DropColumnOperation op) => $"ALTER TABLE {Quote(op.Table)} DROP COLUMN {Quote(op.Name)}";

    private static String GenerateAlterColumn(AlterColumnOperation op)
    {
        var col = op.Column;
        var sb = new StringBuilder();
        sb.Append($"ALTER TABLE {Quote(op.Table)} MODIFY COLUMN {Quote(col.Name)} {GetColumnType(col)}");

        if (!col.IsNullable.GetValueOrDefault(true))
            sb.Append(" NOT NULL");

        if (col.DefaultValue != null)
            sb.Append($" DEFAULT {FormatDefaultValue(col.DefaultValue)}");

        return sb.ToString();
    }

    private static String GenerateRenameTable(RenameTableOperation op) => $"RENAME TABLE {Quote(op.Name)} TO {Quote(op.NewName)}";

    private static String GenerateAddPrimaryKey(AddPrimaryKeyOperation op) =>
        $"ALTER TABLE {Quote(op.Table)} ADD PRIMARY KEY ({String.Join(", ", op.Columns.Select(Quote))})";

    private static String GenerateDropPrimaryKey(DropPrimaryKeyOperation op) =>
        $"ALTER TABLE {Quote(op.Table)} DROP PRIMARY KEY";

    private static String GenerateAddForeignKey(AddForeignKeyOperation op)
    {
        var sb = new StringBuilder();
        sb.Append($"ALTER TABLE {Quote(op.DependentTable)} ADD CONSTRAINT {Quote(op.Name)} ");
        sb.Append($"FOREIGN KEY ({String.Join(", ", op.DependentColumns.Select(Quote))}) ");
        sb.Append($"REFERENCES {Quote(op.PrincipalTable)} ({String.Join(", ", op.PrincipalColumns.Select(Quote))})");

        if (op.CascadeDelete)
            sb.Append(" ON DELETE CASCADE");

        return sb.ToString();
    }

    private static String GenerateDropForeignKey(DropForeignKeyOperation op) =>
        $"ALTER TABLE {Quote(op.DependentTable)} DROP FOREIGN KEY {Quote(op.Name)}";

    private static String GenerateCreateIndex(CreateIndexOperation op)
    {
        var unique = op.IsUnique ? "UNIQUE " : "";
        return $"CREATE {unique}INDEX {Quote(op.Name)} ON {Quote(op.Table)} ({String.Join(", ", op.Columns.Select(Quote))})";
    }

    private static String GenerateDropIndex(DropIndexOperation op) =>
        $"DROP INDEX {Quote(op.Name)} ON {Quote(op.Table)}";

    /// <summary>获取 MySQL 列类型</summary>
    /// <param name="column">列模型</param>
    /// <returns></returns>
    private static String GetColumnType(ColumnModel column)
    {
        // 如果有显式指定的存储类型，直接使用
        if (!String.IsNullOrEmpty(column.StoreType))
            return column.StoreType;

        return column.Type switch
        {
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Boolean => "TINYINT(1)",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Byte => "TINYINT UNSIGNED",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Int16 => "SMALLINT",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Int32 => "INT",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Int64 => "BIGINT",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Single => "FLOAT",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Double => "DOUBLE",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Decimal => $"DECIMAL({column.Precision ?? 18},{column.Scale ?? 2})",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.String when column.MaxLength.HasValue => $"VARCHAR({column.MaxLength.Value})",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.String => "VARCHAR(255)",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Binary when column.MaxLength.HasValue => $"VARBINARY({column.MaxLength.Value})",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Binary => "BLOB",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.DateTime => "DATETIME",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.DateTimeOffset => "DATETIME",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Time => "TIME",
            System.Data.Entity.Core.Metadata.Edm.PrimitiveTypeKind.Guid => "CHAR(36)",
            _ => "VARCHAR(255)",
        };
    }

    /// <summary>格式化默认值</summary>
    /// <param name="value">默认值</param>
    /// <returns></returns>
    private static String FormatDefaultValue(Object value)
    {
        return value switch
        {
            String s => $"'{s.Replace("'", "''")}'",
            Boolean b => b ? "1" : "0",
            DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
            _ => value.ToString() ?? "NULL",
        };
    }

    /// <summary>使用反引号包裹标识符</summary>
    /// <param name="name">标识符</param>
    /// <returns></returns>
    private static String Quote(String name) => $"`{name}`";
}
