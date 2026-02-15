using System.Data.Common;
using System.Data.Entity.Core.Common;
using System.Data.Entity.Core.Common.CommandTrees;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Infrastructure;
using System.Data.Entity.Migrations.Sql;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySql EF6 提供程序服务。实现 DbProviderServices 以支持 EF6 的 DDL/DML 生成和数据库管理</summary>
public class MySqlProviderServices : DbProviderServices
{
    /// <summary>默认实例</summary>
    public static readonly MySqlProviderServices Instance = new();

    #region 方法
    /// <summary>获取数据库提供程序清单</summary>
    /// <param name="manifestToken">清单标记</param>
    /// <returns></returns>
    protected override DbProviderManifest GetDbProviderManifest(String manifestToken) => new MySqlProviderManifest(manifestToken);

    /// <summary>获取数据库提供程序清单标记</summary>
    /// <param name="connection">数据库连接</param>
    /// <returns></returns>
    protected override String GetDbProviderManifestToken(DbConnection connection) => connection.ServerVersion ?? "8.0";

    /// <summary>创建数据库命令定义</summary>
    /// <param name="providerManifest">提供程序清单</param>
    /// <param name="commandTree">命令树</param>
    /// <returns></returns>
    protected override DbCommandDefinition CreateDbCommandDefinition(DbProviderManifest providerManifest, DbCommandTree commandTree)
    {
        if (providerManifest == null) throw new ArgumentNullException(nameof(providerManifest));
        if (commandTree == null) throw new ArgumentNullException(nameof(commandTree));

        var cmd = CreateCommand(providerManifest, commandTree);
        return CreateCommandDefinition(cmd);
    }

    private static DbCommand CreateCommand(DbProviderManifest manifest, DbCommandTree commandTree)
    {
        var cmd = new MySqlCommand();
        var sqlGenerator = new MySqlSqlGenerator();
        var sql = sqlGenerator.GenerateSql(commandTree);

        cmd.CommandText = sql.CommandText;

        foreach (var param in sql.Parameters)
        {
            var p = new MySqlParameter
            {
                ParameterName = param.Key,
                Value = param.Value ?? DBNull.Value
            };
            cmd.Parameters.Add(p);
        }

        return cmd;
    }
    #endregion

    #region 数据库管理
    /// <summary>检查数据库是否存在</summary>
    /// <param name="connection">数据库连接</param>
    /// <param name="commandTimeout">命令超时时间（秒）</param>
    /// <param name="storeItemCollection">存储项集合</param>
    /// <returns></returns>
    protected override Boolean DbDatabaseExists(DbConnection connection, Int32? commandTimeout, StoreItemCollection storeItemCollection)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));

        var builder = new MySqlConnectionStringBuilder(connection.ConnectionString);
        var dbName = builder.Database;
        if (String.IsNullOrEmpty(dbName)) return false;

        // 连接到 information_schema 检查数据库是否存在
        using var conn = CloneConnectionWithoutDatabase(connection);
        EnsureConnectionOpen(conn);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '{EscapeSqlString(dbName)}'";
        if (commandTimeout.HasValue) cmd.CommandTimeout = commandTimeout.Value;

        var result = cmd.ExecuteScalar();
        return Convert.ToInt32(result) > 0;
    }

    /// <summary>创建数据库</summary>
    /// <param name="connection">数据库连接</param>
    /// <param name="commandTimeout">命令超时时间（秒）</param>
    /// <param name="storeItemCollection">存储项集合</param>
    protected override void DbCreateDatabase(DbConnection connection, Int32? commandTimeout, StoreItemCollection storeItemCollection)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));

        var builder = new MySqlConnectionStringBuilder(connection.ConnectionString);
        var dbName = builder.Database;
        if (String.IsNullOrEmpty(dbName))
            throw new InvalidOperationException("连接字符串未指定数据库名");

        // 连接到服务器（不指定数据库）创建数据库
        using var conn = CloneConnectionWithoutDatabase(connection);
        EnsureConnectionOpen(conn);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"CREATE DATABASE IF NOT EXISTS `{dbName}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci";
        if (commandTimeout.HasValue) cmd.CommandTimeout = commandTimeout.Value;

        cmd.ExecuteNonQuery();

        // 如果有 StoreItemCollection，生成表结构
        if (storeItemCollection != null)
        {
            // 切换到新创建的数据库
            using var dbConn = (MySqlConnection)MySqlClientFactory.Instance.CreateConnection();
            dbConn.ConnectionString = connection.ConnectionString;
            EnsureConnectionOpen(dbConn);

            foreach (var container in storeItemCollection.GetItems<EntityContainer>())
            {
                foreach (var entitySet in container.BaseEntitySets.OfType<EntitySet>())
                {
                    var tableSql = GenerateCreateTableSql(entitySet);
                    if (!String.IsNullOrEmpty(tableSql))
                    {
                        using var tableCmd = dbConn.CreateCommand();
                        tableCmd.CommandText = tableSql;
                        if (commandTimeout.HasValue) tableCmd.CommandTimeout = commandTimeout.Value;
                        tableCmd.ExecuteNonQuery();
                    }
                }
            }
        }
    }

    /// <summary>删除数据库</summary>
    /// <param name="connection">数据库连接</param>
    /// <param name="commandTimeout">命令超时时间（秒）</param>
    /// <param name="storeItemCollection">存储项集合</param>
    protected override void DbDeleteDatabase(DbConnection connection, Int32? commandTimeout, StoreItemCollection storeItemCollection)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));

        var builder = new MySqlConnectionStringBuilder(connection.ConnectionString);
        var dbName = builder.Database;
        if (String.IsNullOrEmpty(dbName))
            throw new InvalidOperationException("连接字符串未指定数据库名");

        using var conn = CloneConnectionWithoutDatabase(connection);
        EnsureConnectionOpen(conn);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DROP DATABASE IF EXISTS `{dbName}`";
        if (commandTimeout.HasValue) cmd.CommandTimeout = commandTimeout.Value;

        cmd.ExecuteNonQuery();
    }
    #endregion

    #region 辅助
    /// <summary>克隆连接但不指定数据库，用于管理操作</summary>
    private static MySqlConnection CloneConnectionWithoutDatabase(DbConnection connection)
    {
        var builder = new MySqlConnectionStringBuilder(connection.ConnectionString)
        {
            Database = null
        };
        return new MySqlConnection(builder.ConnectionString);
    }

    /// <summary>确保连接已打开</summary>
    private static void EnsureConnectionOpen(DbConnection connection)
    {
        if (connection.State != System.Data.ConnectionState.Open)
            connection.Open();
    }

    /// <summary>转义 SQL 字符串中的单引号</summary>
    private static String EscapeSqlString(String value) => value.Replace("'", "''").Replace("\\", "\\\\");

    /// <summary>根据 EntitySet 生成建表 SQL</summary>
    private static String? GenerateCreateTableSql(EntitySet entitySet)
    {
        var tableName = entitySet.Table ?? entitySet.Name;
        var entityType = entitySet.ElementType;
        if (entityType.Properties.Count == 0) return null;

        var sb = new System.Text.StringBuilder();
        sb.AppendLine($"CREATE TABLE IF NOT EXISTS `{tableName}` (");

        var first = true;
        var keyColumns = new HashSet<String>(entityType.KeyProperties.Select(p => p.Name), StringComparer.OrdinalIgnoreCase);

        foreach (var prop in entityType.Properties)
        {
            if (!first) sb.AppendLine(",");
            first = false;

            sb.Append($"  `{prop.Name}` {GetMySqlColumnType(prop)}");

            if (!prop.Nullable)
                sb.Append(" NOT NULL");

            // 单列主键且为整数类型，添加 AUTO_INCREMENT
            if (keyColumns.Count == 1 && keyColumns.Contains(prop.Name) && IsIntegerType(prop))
                sb.Append(" AUTO_INCREMENT");
        }

        if (entityType.KeyProperties.Count > 0)
        {
            sb.AppendLine(",");
            sb.Append($"  PRIMARY KEY ({String.Join(", ", entityType.KeyProperties.Select(p => $"`{p.Name}`"))})");
        }

        sb.AppendLine();
        sb.Append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        return sb.ToString();
    }

    /// <summary>将 EDM 属性类型映射到 MySQL 列类型</summary>
    private static String GetMySqlColumnType(EdmProperty property)
    {
        if (property.TypeUsage.EdmType is PrimitiveType pt)
        {
            return pt.PrimitiveTypeKind switch
            {
                PrimitiveTypeKind.Boolean => "TINYINT(1)",
                PrimitiveTypeKind.Byte => "TINYINT UNSIGNED",
                PrimitiveTypeKind.Int16 => "SMALLINT",
                PrimitiveTypeKind.Int32 => "INT",
                PrimitiveTypeKind.Int64 => "BIGINT",
                PrimitiveTypeKind.Single => "FLOAT",
                PrimitiveTypeKind.Double => "DOUBLE",
                PrimitiveTypeKind.Decimal => GetDecimalType(property),
                PrimitiveTypeKind.String => GetStringType(property),
                PrimitiveTypeKind.Binary => GetBinaryType(property),
                PrimitiveTypeKind.DateTime => "DATETIME",
                PrimitiveTypeKind.DateTimeOffset => "DATETIME",
                PrimitiveTypeKind.Time => "TIME",
                PrimitiveTypeKind.Guid => "CHAR(36)",
                _ => "VARCHAR(255)",
            };
        }
        return "VARCHAR(255)";
    }

    private static String GetDecimalType(EdmProperty property)
    {
        var precision = 18;
        var scale = 2;

        if (property.TypeUsage.Facets.TryGetValue("Precision", false, out var precFacet) && precFacet.Value != null)
            precision = Convert.ToInt32(precFacet.Value);
        if (property.TypeUsage.Facets.TryGetValue("Scale", false, out var scaleFacet) && scaleFacet.Value != null)
            scale = Convert.ToInt32(scaleFacet.Value);

        return $"DECIMAL({precision},{scale})";
    }

    private static String GetStringType(EdmProperty property)
    {
        if (property.TypeUsage.Facets.TryGetValue("MaxLength", false, out var maxLenFacet) && maxLenFacet.Value is Int32 maxLen)
        {
            if (maxLen > 16383) return "TEXT";
            return $"VARCHAR({maxLen})";
        }

        // 如果是 MaxLength=Max，使用 LONGTEXT
        if (property.TypeUsage.Facets.TryGetValue("MaxLength", false, out var facet) && facet.IsUnbounded)
            return "LONGTEXT";

        return "VARCHAR(255)";
    }

    private static String GetBinaryType(EdmProperty property)
    {
        if (property.TypeUsage.Facets.TryGetValue("MaxLength", false, out var maxLenFacet) && maxLenFacet.Value is Int32 maxLen)
            return $"VARBINARY({maxLen})";

        return "BLOB";
    }

    private static Boolean IsIntegerType(EdmProperty property) =>
        property.TypeUsage.EdmType is PrimitiveType pt &&
        pt.PrimitiveTypeKind is PrimitiveTypeKind.Int16 or PrimitiveTypeKind.Int32 or PrimitiveTypeKind.Int64;
    #endregion
}
