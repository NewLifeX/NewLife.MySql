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
    protected override System.Xml.XmlReader GetDbInformation(String informationType)
    {
        // EF6 使用此方法获取 StoreSchemaDefinition、StoreSchemaMapping 等信息
        // 返回空 XML 以满足接口要求
        var xml = $"<?xml version=\"1.0\" encoding=\"utf-8\"?><root>{informationType}</root>";
        return System.Xml.XmlReader.Create(new System.IO.StringReader(xml));
    }

    /// <summary>获取存储类型到模型类型的映射</summary>
    /// <param name="storeType">存储类型</param>
    /// <returns></returns>
    public override TypeUsage GetEdmType(TypeUsage storeType)
    {
        if (storeType == null) throw new ArgumentNullException(nameof(storeType));

        var storeTypeName = storeType.EdmType.Name.ToUpperInvariant();

        return storeTypeName switch
        {
            "INT" or "INTEGER" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int32)),
            "BIGINT" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int64)),
            "SMALLINT" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int16)),
            "TINYINT" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Byte)),
            "BIT" or "BOOL" or "BOOLEAN" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Boolean)),
            "FLOAT" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Single)),
            "DOUBLE" or "REAL" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Double)),
            "DECIMAL" or "NUMERIC" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Decimal)),
            "DATETIME" or "TIMESTAMP" or "DATE" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.DateTime)),
            "TIME" => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Time)),
            "CHAR" when storeType.EdmType.Name.Contains("36") => TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Guid)),
            "VARCHAR" or "CHAR" or "TEXT" or "LONGTEXT" or "MEDIUMTEXT" or "TINYTEXT" =>
                TypeUsage.CreateStringTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.String), isUnicode: true, isFixedLength: false),
            "BLOB" or "LONGBLOB" or "MEDIUMBLOB" or "TINYBLOB" or "VARBINARY" or "BINARY" =>
                TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Binary)),
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
                PrimitiveTypeKind.Boolean => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["tinyint"]),
                PrimitiveTypeKind.Byte => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["tinyint"]),
                PrimitiveTypeKind.Int16 => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["smallint"]),
                PrimitiveTypeKind.Int32 => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["int"]),
                PrimitiveTypeKind.Int64 => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["bigint"]),
                PrimitiveTypeKind.Single => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["float"]),
                PrimitiveTypeKind.Double => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["double"]),
                PrimitiveTypeKind.Decimal => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["decimal"]),
                PrimitiveTypeKind.DateTime => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["datetime"]),
                PrimitiveTypeKind.Time => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["time"]),
                PrimitiveTypeKind.Guid => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["char"]),
                PrimitiveTypeKind.String => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["varchar"]),
                PrimitiveTypeKind.Binary => TypeUsage.CreateDefaultTypeUsage(StoreTypeNameToStorePrimitiveType["blob"]),
                _ => throw new NotSupportedException($"不支持的 EDM 类型：{primitiveType.PrimitiveTypeKind}"),
            };
        }

        throw new NotSupportedException($"不支持的 EDM 类型：{edmType.EdmType.Name}");
    }

    private static XmlReader GetManifestXml()
    {
        // 构建最小化的 ProviderManifest XML
        var xml = @"<?xml version=""1.0"" encoding=""utf-8""?>
<ProviderManifest Namespace=""NewLife.MySql"" xmlns=""http://schemas.microsoft.com/ado/2006/04/edm/providermanifest"">
  <Types>
    <Type Name=""tinyint"" PrimitiveTypeKind=""Byte"" />
    <Type Name=""smallint"" PrimitiveTypeKind=""Int16"" />
    <Type Name=""int"" PrimitiveTypeKind=""Int32"" />
    <Type Name=""bigint"" PrimitiveTypeKind=""Int64"" />
    <Type Name=""float"" PrimitiveTypeKind=""Single"" />
    <Type Name=""double"" PrimitiveTypeKind=""Double"" />
    <Type Name=""decimal"" PrimitiveTypeKind=""Decimal"">
      <FacetDescription Name=""Precision"" MinValue=""1"" MaxValue=""65"" DefaultValue=""18"" />
      <FacetDescription Name=""Scale"" MinValue=""0"" MaxValue=""30"" DefaultValue=""0"" />
    </Type>
    <Type Name=""varchar"" PrimitiveTypeKind=""String"">
      <FacetDescription Name=""MaxLength"" MinValue=""1"" MaxValue=""65535"" DefaultValue=""255"" />
      <FacetDescription Name=""Unicode"" DefaultValue=""true"" />
      <FacetDescription Name=""FixedLength"" DefaultValue=""false"" />
    </Type>
    <Type Name=""char"" PrimitiveTypeKind=""String"">
      <FacetDescription Name=""MaxLength"" MinValue=""1"" MaxValue=""255"" DefaultValue=""1"" />
      <FacetDescription Name=""Unicode"" DefaultValue=""true"" />
      <FacetDescription Name=""FixedLength"" DefaultValue=""true"" />
    </Type>
    <Type Name=""text"" PrimitiveTypeKind=""String"">
      <FacetDescription Name=""MaxLength"" DefaultValue=""65535"" />
      <FacetDescription Name=""Unicode"" DefaultValue=""true"" />
      <FacetDescription Name=""FixedLength"" DefaultValue=""false"" />
    </Type>
    <Type Name=""blob"" PrimitiveTypeKind=""Binary"">
      <FacetDescription Name=""MaxLength"" DefaultValue=""65535"" />
    </Type>
    <Type Name=""datetime"" PrimitiveTypeKind=""DateTime"" />
    <Type Name=""time"" PrimitiveTypeKind=""Time"" />
    <Type Name=""bit"" PrimitiveTypeKind=""Boolean"" />
  </Types>
  <Functions />
</ProviderManifest>";

        return XmlReader.Create(new System.IO.StringReader(xml));
    }
}
