using System.ComponentModel;
using System.Data.Entity;
using System.Data.Entity.Core.Common;
using System.Data.Entity.Core.Metadata.Edm;
using NewLife.MySql;
using NewLife.MySql.EntityFramework;

namespace UnitTest.EntityFramework;

/// <summary>Provider Manifest 类型映射测试（通过 ProviderServices 公共 API 间接测试）</summary>
public class MySqlProviderManifestTests
{
    /// <summary>通过 ProviderServices 获取 Manifest 实例</summary>
    private static DbProviderManifest GetManifest()
    {
        // 使用 ProviderServices 的公共接口获取 Manifest
        return MySqlProviderServices.Instance.GetProviderManifest("8.0");
    }

    #region 基本属性
    [Fact]
    [DisplayName("Manifest应能通过ProviderServices获取")]
    public void GetManifest_ShouldReturnInstance()
    {
        var manifest = GetManifest();

        Assert.NotNull(manifest);
        Assert.Equal("NewLife.MySql", manifest.NamespaceName);
    }
    #endregion

    #region GetStoreType
    [Fact]
    [DisplayName("Boolean应映射到bit")]
    public void GetStoreType_Boolean_ShouldMapToBit()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Boolean));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("bit", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("Int32应映射到int")]
    public void GetStoreType_Int32_ShouldMapToInt()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int32));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("int", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("Int64应映射到bigint")]
    public void GetStoreType_Int64_ShouldMapToBigint()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int64));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("bigint", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("Int16应映射到smallint")]
    public void GetStoreType_Int16_ShouldMapToSmallint()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int16));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("smallint", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("Byte应映射到tinyint")]
    public void GetStoreType_Byte_ShouldMapToTinyint()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Byte));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("tinyint", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("String应映射到varchar")]
    public void GetStoreType_String_ShouldMapToVarchar()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateStringTypeUsage(
            PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.String),
            isUnicode: true,
            isFixedLength: false);

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("varchar", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("固定长度String应映射到char")]
    public void GetStoreType_FixedString_ShouldMapToChar()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateStringTypeUsage(
            PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.String),
            isUnicode: true,
            isFixedLength: true);

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("char", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("DateTime应映射到datetime")]
    public void GetStoreType_DateTime_ShouldMapToDatetime()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.DateTime));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("datetime", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("Time应映射到time")]
    public void GetStoreType_Time_ShouldMapToTime()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Time));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("time", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("Guid应映射到char")]
    public void GetStoreType_Guid_ShouldMapToChar()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Guid));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("char", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("Binary应映射到blob")]
    public void GetStoreType_Binary_ShouldMapToBlob()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Binary));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("blob", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("Decimal应映射到decimal")]
    public void GetStoreType_Decimal_ShouldMapToDecimal()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Decimal));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("decimal", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("Single应映射到float")]
    public void GetStoreType_Single_ShouldMapToFloat()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Single));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("float", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("Double应映射到double")]
    public void GetStoreType_Double_ShouldMapToDouble()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Double));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("double", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("DateTimeOffset应映射到datetime")]
    public void GetStoreType_DateTimeOffset_ShouldMapToDatetime()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.DateTimeOffset));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("datetime", storeType.EdmType.Name);
    }
    #endregion

    #region 异常
    [Fact]
    [DisplayName("GetStoreType传入null应抛出异常")]
    public void GetStoreType_NullInput_ShouldThrow()
    {
        var manifest = GetManifest();

        Assert.Throws<ArgumentNullException>(() => manifest.GetStoreType(null!));
    }

    [Fact]
    [DisplayName("GetEdmType传入null应抛出异常")]
    public void GetEdmType_NullInput_ShouldThrow()
    {
        var manifest = GetManifest();

        Assert.Throws<ArgumentNullException>(() => manifest.GetEdmType(null!));
    }
    #endregion
}
