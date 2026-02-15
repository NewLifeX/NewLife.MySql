using System.Data.Entity.Core.Common;
using System.Data.Entity.Core.Metadata.Edm;
using System.Xml;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySql 提供程序清单。描述 MySQL 数据库的类型系统和功能支持</summary>
internal class MySqlProviderManifest : DbXmlEnabledProviderManifest
{
    /// <summary>清单标记（通常为服务器版本号）</summary>
    public String Token { get; }

    /// <summary>实例化 MySql 提供程序清单</summary>
    /// <param name="manifestToken">清单标记</param>
    public MySqlProviderManifest(String manifestToken) : base(GetManifestXml())
    {
        Token = manifestToken ?? "8.0";
    }

    /// <summary>获取命名空间名称</summary>
    public override String NamespaceName => "NewLife.MySql";

    /// <summary>获取数据库信息</summary>
    /// <param name="informationType">信息类型</param>
    /// <returns></returns>
    protected override XmlReader GetDbInformation(String informationType)
    {
        var xml = informationType switch
        {
            DbProviderManifest.StoreSchemaDefinition or DbProviderManifest.StoreSchemaDefinitionVersion3 => GetStoreSchemaDefinitionXml(),
            DbProviderManifest.StoreSchemaMapping or DbProviderManifest.StoreSchemaMappingVersion3 => GetStoreSchemaMappingXml(),
            _ => $"<?xml version=\"1.0\" encoding=\"utf-8\"?><root>{informationType}</root>",
        };

        return XmlReader.Create(new System.IO.StringReader(xml));
    }

    #region 类型映射
    /// <summary>获取存储类型到模型类型的映射</summary>
    /// <param name="storeType">存储类型</param>
    /// <returns></returns>
    public override TypeUsage GetEdmType(TypeUsage storeType)
    {
        if (storeType == null) throw new ArgumentNullException(nameof(storeType));

        var storeTypeName = storeType.EdmType.Name.ToUpperInvariant();

        return storeTypeName switch
        {
            "INT" or "INTEGER" or "MEDIUMINT" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int32)),
            "BIGINT" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int64)),
            "SMALLINT" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int16)),
            "TINYINT" => GetTinyIntEdmType(storeType),
            "BIT" or "BOOL" or "BOOLEAN" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Boolean)),
            "FLOAT" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Single)),
            "DOUBLE" or "REAL" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Double)),
            "DECIMAL" or "NUMERIC" or "DEC" or "FIXED" => GetDecimalEdmType(storeType),
            "DATETIME" or "TIMESTAMP" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.DateTime)),
            "DATE" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.DateTime)),
            "TIME" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Time)),
            "YEAR" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int16)),
            "CHAR" when IsGuidColumn(storeType) => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Guid)),
            "VARCHAR" or "CHAR" or "TINYTEXT" => GetStringEdmType(storeType, isFixedLength: storeTypeName == "CHAR"),
            "TEXT" or "MEDIUMTEXT" or "LONGTEXT" or "ENUM" or "SET" =>
                TypeUsage.CreateStringTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.String), isUnicode: true, isFixedLength: false),
            "BLOB" or "LONGBLOB" or "MEDIUMBLOB" or "TINYBLOB" =>
                TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Binary)),
            "VARBINARY" or "BINARY" => GetBinaryEdmType(storeType, isFixedLength: storeTypeName == "BINARY"),
            _ => TypeUsage.CreateStringTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.String), isUnicode: true, isFixedLength: false),
        };
    }

    /// <summary>获取模型类型到存储类型的映射</summary>
    /// <param name="edmType">EDM 类型</param>
    /// <returns></returns>
    public override TypeUsage GetStoreType(TypeUsage edmType)
    {
        if (edmType == null) throw new ArgumentNullException(nameof(edmType));

        if (edmType.EdmType is PrimitiveType primitiveType)
        {
            return primitiveType.PrimitiveTypeKind switch
            {
                PrimitiveTypeKind.Boolean => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["bit"]),
                PrimitiveTypeKind.Byte => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["tinyint"]),
                PrimitiveTypeKind.SByte => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["tinyint"]),
                PrimitiveTypeKind.Int16 => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["smallint"]),
                PrimitiveTypeKind.Int32 => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["int"]),
                PrimitiveTypeKind.Int64 => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["bigint"]),
                PrimitiveTypeKind.Single => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["float"]),
                PrimitiveTypeKind.Double => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["double"]),
                PrimitiveTypeKind.Decimal => GetDecimalStoreType(edmType),
                PrimitiveTypeKind.DateTime => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["datetime"]),
                PrimitiveTypeKind.DateTimeOffset => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["datetime"]),
                PrimitiveTypeKind.Time => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["time"]),
                PrimitiveTypeKind.Guid => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["char"]),
                PrimitiveTypeKind.String => GetStringStoreType(edmType),
                PrimitiveTypeKind.Binary => GetBinaryStoreType(edmType),
                _ => throw new NotSupportedException($"不支持的 EDM 类型：{primitiveType.PrimitiveTypeKind}"),
            };
        }

        throw new NotSupportedException($"不支持的 EDM 类型：{edmType.EdmType.Name}");
    }
    #endregion

    #region 类型辅助
    /// <summary>判断是否为 GUID 列（CHAR(36)）</summary>
    private static Boolean IsGuidColumn(TypeUsage storeType)
    {
        if (storeType.Facets.TryGetValue("MaxLength", false, out var maxLenFacet) && maxLenFacet.Value is Int32 maxLen)
            return maxLen == 36;

        return storeType.EdmType.Name.Contains("36");
    }

    /// <summary>TINYINT 映射：TINYINT(1) 映射为 Boolean，其他映射为 Byte</summary>
    private static TypeUsage GetTinyIntEdmType(TypeUsage storeType)
    {
        // 默认映射为 Byte，但 TINYINT(1) 通常表示 Boolean
        if (storeType.Facets.TryGetValue("MaxLength", false, out var facet) && facet.Value is Int32 len && len == 1)
            return TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Boolean));

        return TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Byte));
    }

    /// <summary>获取 Decimal EDM 类型（带精度信息）</summary>
    private static TypeUsage GetDecimalEdmType(TypeUsage storeType)
    {
        Byte precision = 18;
        Byte scale = 0;

        if (storeType.Facets.TryGetValue("Precision", false, out var precFacet) && precFacet.Value is Byte p)
            precision = p;
        if (storeType.Facets.TryGetValue("Scale", false, out var scaleFacet) && scaleFacet.Value is Byte s)
            scale = s;

        return TypeUsage.CreateDecimalTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Decimal), precision, scale);
    }

    /// <summary>获取字符串 EDM 类型（带长度信息）</summary>
    private static TypeUsage GetStringEdmType(TypeUsage storeType, Boolean isFixedLength)
    {
        if (storeType.Facets.TryGetValue("MaxLength", false, out var maxLenFacet) && maxLenFacet.Value is Int32 maxLen)
        {
            return TypeUsage.CreateStringTypeUsage(
                PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.String),
                isUnicode: true,
                isFixedLength: isFixedLength,
                maxLength: maxLen);
        }

        return TypeUsage.CreateStringTypeUsage(
            PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.String),
            isUnicode: true,
            isFixedLength: isFixedLength);
    }

    /// <summary>获取二进制 EDM 类型（带长度信息）</summary>
    private static TypeUsage GetBinaryEdmType(TypeUsage storeType, Boolean isFixedLength)
    {
        if (storeType.Facets.TryGetValue("MaxLength", false, out var maxLenFacet) && maxLenFacet.Value is Int32 maxLen)
        {
            return TypeUsage.CreateBinaryTypeUsage(
                PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Binary),
                isFixedLength: isFixedLength,
                maxLength: maxLen);
        }

        return TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Binary));
    }

    /// <summary>获取 Decimal 存储类型（带精度信息）</summary>
    private TypeUsage GetDecimalStoreType(TypeUsage edmType)
    {
        return TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["decimal"]);
    }

    /// <summary>获取字符串存储类型</summary>
    private TypeUsage GetStringStoreType(TypeUsage edmType)
    {
        var isFixedLength = false;
        if (edmType.Facets.TryGetValue("FixedLength", false, out var fixedFacet) && fixedFacet.Value is Boolean fl)
            isFixedLength = fl;

        if (isFixedLength)
            return TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["char"]);

        // 仅当显式指定了具体 MaxLength 且超过 varchar 上限时才使用 text
        if (edmType.Facets.TryGetValue("MaxLength", false, out var maxLenFacet) &&
            !maxLenFacet.IsUnbounded && maxLenFacet.Value is Int32 maxLen && maxLen > 65535)
            return TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["text"]);

        return TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["varchar"]);
    }

    /// <summary>获取二进制存储类型</summary>
    private TypeUsage GetBinaryStoreType(TypeUsage edmType)
    {
        return TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["blob"]);
    }
    #endregion

    #region 清单XML
    private static XmlReader GetManifestXml()
    {
        // 构建 ProviderManifest XML，包含完整的 MySQL 类型系统
        var xml = @"<?xml version=""1.0"" encoding=""utf-8""?>
<ProviderManifest Namespace=""NewLife.MySql"" xmlns=""http://schemas.microsoft.com/ado/2006/04/edm/providermanifest"">
  <Types>
    <Type Name=""tinyint"" PrimitiveTypeKind=""Byte"" />
    <Type Name=""smallint"" PrimitiveTypeKind=""Int16"" />
    <Type Name=""mediumint"" PrimitiveTypeKind=""Int32"" />
    <Type Name=""int"" PrimitiveTypeKind=""Int32"" />
    <Type Name=""bigint"" PrimitiveTypeKind=""Int64"" />
    <Type Name=""float"" PrimitiveTypeKind=""Single"" />
    <Type Name=""double"" PrimitiveTypeKind=""Double"" />
    <Type Name=""decimal"" PrimitiveTypeKind=""Decimal"">
      <FacetDescriptions>
        <Precision Minimum=""1"" Maximum=""65"" DefaultValue=""18"" />
        <Scale Minimum=""0"" Maximum=""30"" DefaultValue=""0"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""varchar"" PrimitiveTypeKind=""String"">
      <FacetDescriptions>
        <MaxLength Minimum=""1"" Maximum=""65535"" DefaultValue=""255"" />
        <Unicode DefaultValue=""true"" />
        <FixedLength DefaultValue=""false"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""char"" PrimitiveTypeKind=""String"">
      <FacetDescriptions>
        <MaxLength Minimum=""1"" Maximum=""255"" DefaultValue=""1"" />
        <Unicode DefaultValue=""true"" />
        <FixedLength DefaultValue=""true"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""text"" PrimitiveTypeKind=""String"">
      <FacetDescriptions>
        <MaxLength Constant=""true"" DefaultValue=""65535"" />
        <Unicode DefaultValue=""true"" />
        <FixedLength DefaultValue=""false"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""longtext"" PrimitiveTypeKind=""String"">
      <FacetDescriptions>
        <MaxLength Constant=""true"" DefaultValue=""2147483647"" />
        <Unicode DefaultValue=""true"" />
        <FixedLength DefaultValue=""false"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""blob"" PrimitiveTypeKind=""Binary"">
      <FacetDescriptions>
        <MaxLength Constant=""true"" DefaultValue=""65535"" />
        <FixedLength DefaultValue=""false"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""longblob"" PrimitiveTypeKind=""Binary"">
      <FacetDescriptions>
        <MaxLength Constant=""true"" DefaultValue=""2147483647"" />
        <FixedLength DefaultValue=""false"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""binary"" PrimitiveTypeKind=""Binary"">
      <FacetDescriptions>
        <MaxLength Minimum=""1"" Maximum=""255"" DefaultValue=""1"" />
        <FixedLength DefaultValue=""true"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""varbinary"" PrimitiveTypeKind=""Binary"">
      <FacetDescriptions>
        <MaxLength Minimum=""1"" Maximum=""65535"" DefaultValue=""255"" />
        <FixedLength DefaultValue=""false"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""datetime"" PrimitiveTypeKind=""DateTime"">
      <FacetDescriptions>
        <Precision Minimum=""0"" Maximum=""6"" DefaultValue=""0"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""date"" PrimitiveTypeKind=""DateTime"">
      <FacetDescriptions>
        <Precision Constant=""true"" DefaultValue=""0"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""time"" PrimitiveTypeKind=""Time"">
      <FacetDescriptions>
        <Precision Minimum=""0"" Maximum=""6"" DefaultValue=""0"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""timestamp"" PrimitiveTypeKind=""DateTime"">
      <FacetDescriptions>
        <Precision Minimum=""0"" Maximum=""6"" DefaultValue=""0"" />
      </FacetDescriptions>
    </Type>
    <Type Name=""bit"" PrimitiveTypeKind=""Boolean"" />
  </Types>
</ProviderManifest>";

        // EF6 规范函数（Edm 命名空间的 Canonical Functions）由 MySqlSqlGenerator 在 SQL 生成时负责映射
        // 不需要在 ProviderManifest XML 中声明，它们是 EF6 内置的

        return XmlReader.Create(new System.IO.StringReader(xml));
    }

    /// <summary>获取 Store Schema Definition XML</summary>
    private static String GetStoreSchemaDefinitionXml()
    {
        // 完整的 SSDL，描述 MySQL 数据库的元数据查询结构
        return @"<?xml version=""1.0"" encoding=""utf-8""?>
<Schema Namespace=""NewLife.MySql"" Provider=""NewLife.MySql.MySqlClient"" ProviderManifestToken=""8.0""
        Alias=""Self"" xmlns=""http://schemas.microsoft.com/ado/2006/04/edm/ssdl"">
  <EntityContainer Name=""Schema"">
    <EntitySet Name=""STables"" EntityType=""Self.Table"">
      <DefiningQuery>
        SELECT
          TABLE_SCHEMA AS `CatalogName`,
          TABLE_SCHEMA AS `SchemaName`,
          TABLE_NAME AS `Name`,
          TABLE_TYPE AS `TableType`
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
      </DefiningQuery>
    </EntitySet>
    <EntitySet Name=""STableColumns"" EntityType=""Self.TableColumn"">
      <DefiningQuery>
        SELECT
          TABLE_SCHEMA AS `CatalogName`,
          TABLE_SCHEMA AS `SchemaName`,
          TABLE_NAME AS `TableName`,
          COLUMN_NAME AS `Name`,
          ORDINAL_POSITION AS `Ordinal`,
          CASE WHEN IS_NULLABLE='YES' THEN 1 ELSE 0 END AS `IsNullable`,
          DATA_TYPE AS `TypeName`,
          CHARACTER_MAXIMUM_LENGTH AS `MaxLength`,
          NUMERIC_PRECISION AS `Precision`,
          NUMERIC_SCALE AS `Scale`,
          CASE WHEN COLUMN_DEFAULT IS NOT NULL THEN 1 ELSE 0 END AS `HasDefault`,
          COLUMN_DEFAULT AS `Default`,
          CASE WHEN EXTRA LIKE '%auto_increment%' THEN 1 ELSE 0 END AS `IsIdentity`,
          CASE WHEN EXTRA LIKE '%auto_increment%' THEN 1 ELSE 0 END AS `IsStoreGenerated`
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME, ORDINAL_POSITION
      </DefiningQuery>
    </EntitySet>
    <EntitySet Name=""SViews"" EntityType=""Self.View"">
      <DefiningQuery>
        SELECT
          TABLE_SCHEMA AS `CatalogName`,
          TABLE_SCHEMA AS `SchemaName`,
          TABLE_NAME AS `Name`,
          VIEW_DEFINITION AS `ViewDefinition`,
          CASE WHEN IS_UPDATABLE='YES' THEN 1 ELSE 0 END AS `IsUpdatable`
        FROM information_schema.VIEWS
        WHERE TABLE_SCHEMA = DATABASE()
      </DefiningQuery>
    </EntitySet>
    <EntitySet Name=""SViewColumns"" EntityType=""Self.ViewColumn"">
      <DefiningQuery>
        SELECT
          c.TABLE_SCHEMA AS `CatalogName`,
          c.TABLE_SCHEMA AS `SchemaName`,
          c.TABLE_NAME AS `ViewName`,
          c.COLUMN_NAME AS `Name`,
          c.ORDINAL_POSITION AS `Ordinal`,
          CASE WHEN c.IS_NULLABLE='YES' THEN 1 ELSE 0 END AS `IsNullable`,
          c.DATA_TYPE AS `TypeName`,
          c.CHARACTER_MAXIMUM_LENGTH AS `MaxLength`,
          c.NUMERIC_PRECISION AS `Precision`,
          c.NUMERIC_SCALE AS `Scale`,
          CASE WHEN c.COLUMN_DEFAULT IS NOT NULL THEN 1 ELSE 0 END AS `HasDefault`,
          c.COLUMN_DEFAULT AS `Default`,
          0 AS `IsIdentity`,
          0 AS `IsStoreGenerated`
        FROM information_schema.COLUMNS c
        INNER JOIN information_schema.VIEWS v ON c.TABLE_SCHEMA = v.TABLE_SCHEMA AND c.TABLE_NAME = v.TABLE_NAME
        WHERE c.TABLE_SCHEMA = DATABASE()
        ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
      </DefiningQuery>
    </EntitySet>
    <EntitySet Name=""SConstraints"" EntityType=""Self.Constraint"">
      <DefiningQuery>
        SELECT
          CONSTRAINT_SCHEMA AS `CatalogName`,
          CONSTRAINT_SCHEMA AS `SchemaName`,
          TABLE_NAME AS `TableName`,
          CONSTRAINT_NAME AS `Name`,
          CONSTRAINT_TYPE AS `ConstraintType`,
          0 AS `IsDeferrable`,
          0 AS `IsInitiallyDeferred`
        FROM information_schema.TABLE_CONSTRAINTS
        WHERE CONSTRAINT_SCHEMA = DATABASE()
      </DefiningQuery>
    </EntitySet>
    <EntitySet Name=""SConstraintColumns"" EntityType=""Self.ConstraintColumn"">
      <DefiningQuery>
        SELECT
          k.CONSTRAINT_SCHEMA AS `CatalogName`,
          k.CONSTRAINT_SCHEMA AS `SchemaName`,
          k.TABLE_NAME AS `TableName`,
          k.CONSTRAINT_NAME AS `ConstraintName`,
          k.COLUMN_NAME AS `ColumnName`
        FROM information_schema.KEY_COLUMN_USAGE k
        WHERE k.CONSTRAINT_SCHEMA = DATABASE()
      </DefiningQuery>
    </EntitySet>
    <EntitySet Name=""SForeignKeys"" EntityType=""Self.ForeignKey"">
      <DefiningQuery>
        SELECT
          rc.CONSTRAINT_SCHEMA AS `CatalogName`,
          rc.CONSTRAINT_SCHEMA AS `SchemaName`,
          rc.CONSTRAINT_NAME AS `Name`,
          rc.TABLE_NAME AS `TableName`,
          rc.REFERENCED_TABLE_NAME AS `ReferencedTableName`,
          rc.DELETE_RULE AS `DeleteRule`,
          rc.UPDATE_RULE AS `UpdateRule`
        FROM information_schema.REFERENTIAL_CONSTRAINTS rc
        WHERE rc.CONSTRAINT_SCHEMA = DATABASE()
      </DefiningQuery>
    </EntitySet>
    <EntitySet Name=""SForeignKeyColumns"" EntityType=""Self.ForeignKeyColumn"">
      <DefiningQuery>
        SELECT
          k.CONSTRAINT_SCHEMA AS `CatalogName`,
          k.CONSTRAINT_SCHEMA AS `SchemaName`,
          k.CONSTRAINT_NAME AS `ConstraintName`,
          k.TABLE_NAME AS `TableName`,
          k.COLUMN_NAME AS `ColumnName`,
          k.REFERENCED_TABLE_NAME AS `ReferencedTableName`,
          k.REFERENCED_COLUMN_NAME AS `ReferencedColumnName`,
          k.ORDINAL_POSITION AS `Ordinal`
        FROM information_schema.KEY_COLUMN_USAGE k
        WHERE k.CONSTRAINT_SCHEMA = DATABASE()
          AND k.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION
      </DefiningQuery>
    </EntitySet>
  </EntityContainer>
  <EntityType Name=""Table"">
    <Key>
      <PropertyRef Name=""CatalogName"" />
      <PropertyRef Name=""SchemaName"" />
      <PropertyRef Name=""Name"" />
    </Key>
    <Property Name=""CatalogName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""SchemaName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""Name"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""TableType"" Type=""varchar"" MaxLength=""255"" />
  </EntityType>
  <EntityType Name=""TableColumn"">
    <Key>
      <PropertyRef Name=""CatalogName"" />
      <PropertyRef Name=""SchemaName"" />
      <PropertyRef Name=""TableName"" />
      <PropertyRef Name=""Name"" />
    </Key>
    <Property Name=""CatalogName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""SchemaName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""TableName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""Name"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""Ordinal"" Type=""int"" Nullable=""false"" />
    <Property Name=""IsNullable"" Type=""bit"" Nullable=""false"" />
    <Property Name=""TypeName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""MaxLength"" Type=""int"" />
    <Property Name=""Precision"" Type=""int"" />
    <Property Name=""Scale"" Type=""int"" />
    <Property Name=""HasDefault"" Type=""bit"" Nullable=""false"" />
    <Property Name=""Default"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""IsIdentity"" Type=""bit"" Nullable=""false"" />
    <Property Name=""IsStoreGenerated"" Type=""bit"" Nullable=""false"" />
  </EntityType>
  <EntityType Name=""View"">
    <Key>
      <PropertyRef Name=""CatalogName"" />
      <PropertyRef Name=""SchemaName"" />
      <PropertyRef Name=""Name"" />
    </Key>
    <Property Name=""CatalogName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""SchemaName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""Name"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""ViewDefinition"" Type=""longtext"" />
    <Property Name=""IsUpdatable"" Type=""bit"" Nullable=""false"" />
  </EntityType>
  <EntityType Name=""ViewColumn"">
    <Key>
      <PropertyRef Name=""CatalogName"" />
      <PropertyRef Name=""SchemaName"" />
      <PropertyRef Name=""ViewName"" />
      <PropertyRef Name=""Name"" />
    </Key>
    <Property Name=""CatalogName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""SchemaName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""ViewName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""Name"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""Ordinal"" Type=""int"" Nullable=""false"" />
    <Property Name=""IsNullable"" Type=""bit"" Nullable=""false"" />
    <Property Name=""TypeName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""MaxLength"" Type=""int"" />
    <Property Name=""Precision"" Type=""int"" />
    <Property Name=""Scale"" Type=""int"" />
    <Property Name=""HasDefault"" Type=""bit"" Nullable=""false"" />
    <Property Name=""Default"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""IsIdentity"" Type=""bit"" Nullable=""false"" />
    <Property Name=""IsStoreGenerated"" Type=""bit"" Nullable=""false"" />
  </EntityType>
  <EntityType Name=""Constraint"">
    <Key>
      <PropertyRef Name=""CatalogName"" />
      <PropertyRef Name=""SchemaName"" />
      <PropertyRef Name=""TableName"" />
      <PropertyRef Name=""Name"" />
    </Key>
    <Property Name=""CatalogName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""SchemaName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""TableName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""Name"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""ConstraintType"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""IsDeferrable"" Type=""bit"" Nullable=""false"" />
    <Property Name=""IsInitiallyDeferred"" Type=""bit"" Nullable=""false"" />
  </EntityType>
  <EntityType Name=""ConstraintColumn"">
    <Key>
      <PropertyRef Name=""CatalogName"" />
      <PropertyRef Name=""SchemaName"" />
      <PropertyRef Name=""TableName"" />
      <PropertyRef Name=""ConstraintName"" />
      <PropertyRef Name=""ColumnName"" />
    </Key>
    <Property Name=""CatalogName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""SchemaName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""TableName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""ConstraintName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""ColumnName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
  </EntityType>
  <EntityType Name=""ForeignKey"">
    <Key>
      <PropertyRef Name=""CatalogName"" />
      <PropertyRef Name=""SchemaName"" />
      <PropertyRef Name=""Name"" />
    </Key>
    <Property Name=""CatalogName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""SchemaName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""Name"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""TableName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""ReferencedTableName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""DeleteRule"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""UpdateRule"" Type=""varchar"" MaxLength=""255"" />
  </EntityType>
  <EntityType Name=""ForeignKeyColumn"">
    <Key>
      <PropertyRef Name=""CatalogName"" />
      <PropertyRef Name=""SchemaName"" />
      <PropertyRef Name=""ConstraintName"" />
      <PropertyRef Name=""TableName"" />
      <PropertyRef Name=""ColumnName"" />
    </Key>
    <Property Name=""CatalogName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""SchemaName"" Type=""varchar"" MaxLength=""255"" />
    <Property Name=""ConstraintName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""TableName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""ColumnName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""ReferencedTableName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""ReferencedColumnName"" Type=""varchar"" MaxLength=""255"" Nullable=""false"" />
    <Property Name=""Ordinal"" Type=""int"" Nullable=""false"" />
  </EntityType>
</Schema>";
    }

    /// <summary>获取 Store Schema Mapping XML</summary>
    private static String GetStoreSchemaMappingXml()
    {
        return @"<?xml version=""1.0"" encoding=""utf-8""?>
<Mapping Space=""C-S"" xmlns=""urn:schemas-microsoft-com:windows:storage:mapping:CS"">
  <EntityContainerMapping StorageEntityContainer=""Schema"" CdmEntityContainer=""Schema"">
    <EntitySetMapping Name=""STables"">
      <EntityTypeMapping TypeName=""Store.Table"">
        <MappingFragment StoreEntitySet=""STables"">
          <ScalarProperty Name=""CatalogName"" ColumnName=""CatalogName"" />
          <ScalarProperty Name=""SchemaName"" ColumnName=""SchemaName"" />
          <ScalarProperty Name=""Name"" ColumnName=""Name"" />
          <ScalarProperty Name=""TableType"" ColumnName=""TableType"" />
        </MappingFragment>
      </EntityTypeMapping>
    </EntitySetMapping>
    <EntitySetMapping Name=""STableColumns"">
      <EntityTypeMapping TypeName=""Store.TableColumn"">
        <MappingFragment StoreEntitySet=""STableColumns"">
          <ScalarProperty Name=""CatalogName"" ColumnName=""CatalogName"" />
          <ScalarProperty Name=""SchemaName"" ColumnName=""SchemaName"" />
          <ScalarProperty Name=""TableName"" ColumnName=""TableName"" />
          <ScalarProperty Name=""Name"" ColumnName=""Name"" />
          <ScalarProperty Name=""Ordinal"" ColumnName=""Ordinal"" />
          <ScalarProperty Name=""IsNullable"" ColumnName=""IsNullable"" />
          <ScalarProperty Name=""TypeName"" ColumnName=""TypeName"" />
          <ScalarProperty Name=""MaxLength"" ColumnName=""MaxLength"" />
          <ScalarProperty Name=""Precision"" ColumnName=""Precision"" />
          <ScalarProperty Name=""Scale"" ColumnName=""Scale"" />
          <ScalarProperty Name=""HasDefault"" ColumnName=""HasDefault"" />
          <ScalarProperty Name=""Default"" ColumnName=""Default"" />
          <ScalarProperty Name=""IsIdentity"" ColumnName=""IsIdentity"" />
          <ScalarProperty Name=""IsStoreGenerated"" ColumnName=""IsStoreGenerated"" />
        </MappingFragment>
      </EntityTypeMapping>
    </EntitySetMapping>
    <EntitySetMapping Name=""SViews"">
      <EntityTypeMapping TypeName=""Store.View"">
        <MappingFragment StoreEntitySet=""SViews"">
          <ScalarProperty Name=""CatalogName"" ColumnName=""CatalogName"" />
          <ScalarProperty Name=""SchemaName"" ColumnName=""SchemaName"" />
          <ScalarProperty Name=""Name"" ColumnName=""Name"" />
          <ScalarProperty Name=""ViewDefinition"" ColumnName=""ViewDefinition"" />
          <ScalarProperty Name=""IsUpdatable"" ColumnName=""IsUpdatable"" />
        </MappingFragment>
      </EntityTypeMapping>
    </EntitySetMapping>
    <EntitySetMapping Name=""SViewColumns"">
      <EntityTypeMapping TypeName=""Store.ViewColumn"">
        <MappingFragment StoreEntitySet=""SViewColumns"">
          <ScalarProperty Name=""CatalogName"" ColumnName=""CatalogName"" />
          <ScalarProperty Name=""SchemaName"" ColumnName=""SchemaName"" />
          <ScalarProperty Name=""ViewName"" ColumnName=""ViewName"" />
          <ScalarProperty Name=""Name"" ColumnName=""Name"" />
          <ScalarProperty Name=""Ordinal"" ColumnName=""Ordinal"" />
          <ScalarProperty Name=""IsNullable"" ColumnName=""IsNullable"" />
          <ScalarProperty Name=""TypeName"" ColumnName=""TypeName"" />
          <ScalarProperty Name=""MaxLength"" ColumnName=""MaxLength"" />
          <ScalarProperty Name=""Precision"" ColumnName=""Precision"" />
          <ScalarProperty Name=""Scale"" ColumnName=""Scale"" />
          <ScalarProperty Name=""HasDefault"" ColumnName=""HasDefault"" />
          <ScalarProperty Name=""Default"" ColumnName=""Default"" />
          <ScalarProperty Name=""IsIdentity"" ColumnName=""IsIdentity"" />
          <ScalarProperty Name=""IsStoreGenerated"" ColumnName=""IsStoreGenerated"" />
        </MappingFragment>
      </EntityTypeMapping>
    </EntitySetMapping>
    <EntitySetMapping Name=""SConstraints"">
      <EntityTypeMapping TypeName=""Store.Constraint"">
        <MappingFragment StoreEntitySet=""SConstraints"">
          <ScalarProperty Name=""CatalogName"" ColumnName=""CatalogName"" />
          <ScalarProperty Name=""SchemaName"" ColumnName=""SchemaName"" />
          <ScalarProperty Name=""TableName"" ColumnName=""TableName"" />
          <ScalarProperty Name=""Name"" ColumnName=""Name"" />
          <ScalarProperty Name=""ConstraintType"" ColumnName=""ConstraintType"" />
          <ScalarProperty Name=""IsDeferrable"" ColumnName=""IsDeferrable"" />
          <ScalarProperty Name=""IsInitiallyDeferred"" ColumnName=""IsInitiallyDeferred"" />
        </MappingFragment>
      </EntityTypeMapping>
    </EntitySetMapping>
    <EntitySetMapping Name=""SConstraintColumns"">
      <EntityTypeMapping TypeName=""Store.ConstraintColumn"">
        <MappingFragment StoreEntitySet=""SConstraintColumns"">
          <ScalarProperty Name=""CatalogName"" ColumnName=""CatalogName"" />
          <ScalarProperty Name=""SchemaName"" ColumnName=""SchemaName"" />
          <ScalarProperty Name=""TableName"" ColumnName=""TableName"" />
          <ScalarProperty Name=""ConstraintName"" ColumnName=""ConstraintName"" />
          <ScalarProperty Name=""ColumnName"" ColumnName=""ColumnName"" />
        </MappingFragment>
      </EntityTypeMapping>
    </EntitySetMapping>
    <EntitySetMapping Name=""SForeignKeys"">
      <EntityTypeMapping TypeName=""Store.ForeignKey"">
        <MappingFragment StoreEntitySet=""SForeignKeys"">
          <ScalarProperty Name=""CatalogName"" ColumnName=""CatalogName"" />
          <ScalarProperty Name=""SchemaName"" ColumnName=""SchemaName"" />
          <ScalarProperty Name=""Name"" ColumnName=""Name"" />
          <ScalarProperty Name=""TableName"" ColumnName=""TableName"" />
          <ScalarProperty Name=""ReferencedTableName"" ColumnName=""ReferencedTableName"" />
          <ScalarProperty Name=""DeleteRule"" ColumnName=""DeleteRule"" />
          <ScalarProperty Name=""UpdateRule"" ColumnName=""UpdateRule"" />
        </MappingFragment>
      </EntityTypeMapping>
    </EntitySetMapping>
    <EntitySetMapping Name=""SForeignKeyColumns"">
      <EntityTypeMapping TypeName=""Store.ForeignKeyColumn"">
        <MappingFragment StoreEntitySet=""SForeignKeyColumns"">
          <ScalarProperty Name=""CatalogName"" ColumnName=""CatalogName"" />
          <ScalarProperty Name=""SchemaName"" ColumnName=""SchemaName"" />
          <ScalarProperty Name=""ConstraintName"" ColumnName=""ConstraintName"" />
          <ScalarProperty Name=""TableName"" ColumnName=""TableName"" />
          <ScalarProperty Name=""ColumnName"" ColumnName=""ColumnName"" />
          <ScalarProperty Name=""ReferencedTableName"" ColumnName=""ReferencedTableName"" />
          <ScalarProperty Name=""ReferencedColumnName"" ColumnName=""ReferencedColumnName"" />
          <ScalarProperty Name=""Ordinal"" ColumnName=""Ordinal"" />
        </MappingFragment>
      </EntityTypeMapping>
    </EntitySetMapping>
  </EntityContainerMapping>
</Mapping>";
    }
    #endregion
}
