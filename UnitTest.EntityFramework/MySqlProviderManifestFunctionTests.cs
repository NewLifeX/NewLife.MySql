using System.ComponentModel;
using System.Data.Entity.Core.Common;
using System.Data.Entity.Core.Metadata.Edm;
using NewLife.MySql.EntityFramework;

namespace UnitTest.EntityFramework;

/// <summary>Provider Manifest 函数声明测试</summary>
public class MySqlProviderManifestFunctionTests
{
    private static DbProviderManifest GetManifest() => MySqlProviderServices.Instance.GetProviderManifest("8.0");

    #region 基本属性
    [Fact]
    [DisplayName("Manifest命名空间应为NewLife.MySql")]
    public void NamespaceName_ShouldBeNewLifeMySql()
    {
        var manifest = GetManifest();

        Assert.Equal("NewLife.MySql", manifest.NamespaceName);
    }

    [Fact]
    [DisplayName("不同Token应都能创建Manifest")]
    public void GetManifest_DifferentToken_ShouldWork()
    {
        var manifest = MySqlProviderServices.Instance.GetProviderManifest("5.7");

        Assert.NotNull(manifest);
    }
    #endregion

    #region GetEdmType 反向映射
    [Fact]
    [DisplayName("INT存储类型应映射到Int32")]
    public void GetEdmType_Int_ShouldMapToInt32()
    {
        var manifest = GetManifest();
        var storeType = TypeUsage.CreateDefaultTypeUsage(manifest.GetStoreType(
            TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int32))).EdmType);

        var edmType = manifest.GetEdmType(storeType);

        Assert.Equal(PrimitiveTypeKind.Int32, ((PrimitiveType)edmType.EdmType).PrimitiveTypeKind);
    }

    [Fact]
    [DisplayName("BIGINT存储类型应映射到Int64")]
    public void GetEdmType_Bigint_ShouldMapToInt64()
    {
        var manifest = GetManifest();
        var storeType = TypeUsage.CreateDefaultTypeUsage(manifest.GetStoreType(
            TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Int64))).EdmType);

        var edmType = manifest.GetEdmType(storeType);

        Assert.Equal(PrimitiveTypeKind.Int64, ((PrimitiveType)edmType.EdmType).PrimitiveTypeKind);
    }

    [Fact]
    [DisplayName("BIT存储类型应映射到Boolean")]
    public void GetEdmType_Bit_ShouldMapToBoolean()
    {
        var manifest = GetManifest();
        var storeType = TypeUsage.CreateDefaultTypeUsage(manifest.GetStoreType(
            TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Boolean))).EdmType);

        var edmType = manifest.GetEdmType(storeType);

        Assert.Equal(PrimitiveTypeKind.Boolean, ((PrimitiveType)edmType.EdmType).PrimitiveTypeKind);
    }

    [Fact]
    [DisplayName("VARCHAR存储类型应映射到String")]
    public void GetEdmType_Varchar_ShouldMapToString()
    {
        var manifest = GetManifest();
        var storeType = TypeUsage.CreateDefaultTypeUsage(manifest.GetStoreType(
            TypeUsage.CreateStringTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.String), isUnicode: true, isFixedLength: false)).EdmType);

        var edmType = manifest.GetEdmType(storeType);

        Assert.Equal(PrimitiveTypeKind.String, ((PrimitiveType)edmType.EdmType).PrimitiveTypeKind);
    }

    [Fact]
    [DisplayName("DATETIME存储类型应映射到DateTime")]
    public void GetEdmType_Datetime_ShouldMapToDateTime()
    {
        var manifest = GetManifest();
        var storeType = TypeUsage.CreateDefaultTypeUsage(manifest.GetStoreType(
            TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.DateTime))).EdmType);

        var edmType = manifest.GetEdmType(storeType);

        Assert.Equal(PrimitiveTypeKind.DateTime, ((PrimitiveType)edmType.EdmType).PrimitiveTypeKind);
    }

    [Fact]
    [DisplayName("TIME存储类型应映射到Time")]
    public void GetEdmType_Time_ShouldMapToTime()
    {
        var manifest = GetManifest();
        var storeType = TypeUsage.CreateDefaultTypeUsage(manifest.GetStoreType(
            TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Time))).EdmType);

        var edmType = manifest.GetEdmType(storeType);

        Assert.Equal(PrimitiveTypeKind.Time, ((PrimitiveType)edmType.EdmType).PrimitiveTypeKind);
    }

    [Fact]
    [DisplayName("BLOB存储类型应映射到Binary")]
    public void GetEdmType_Blob_ShouldMapToBinary()
    {
        var manifest = GetManifest();
        var storeType = TypeUsage.CreateDefaultTypeUsage(manifest.GetStoreType(
            TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Binary))).EdmType);

        var edmType = manifest.GetEdmType(storeType);

        Assert.Equal(PrimitiveTypeKind.Binary, ((PrimitiveType)edmType.EdmType).PrimitiveTypeKind);
    }

    [Fact]
    [DisplayName("FLOAT存储类型应映射到Single")]
    public void GetEdmType_Float_ShouldMapToSingle()
    {
        var manifest = GetManifest();
        var storeType = TypeUsage.CreateDefaultTypeUsage(manifest.GetStoreType(
            TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Single))).EdmType);

        var edmType = manifest.GetEdmType(storeType);

        Assert.Equal(PrimitiveTypeKind.Single, ((PrimitiveType)edmType.EdmType).PrimitiveTypeKind);
    }

    [Fact]
    [DisplayName("DOUBLE存储类型应映射到Double")]
    public void GetEdmType_Double_ShouldMapToDouble()
    {
        var manifest = GetManifest();
        var storeType = TypeUsage.CreateDefaultTypeUsage(manifest.GetStoreType(
            TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Double))).EdmType);

        var edmType = manifest.GetEdmType(storeType);

        Assert.Equal(PrimitiveTypeKind.Double, ((PrimitiveType)edmType.EdmType).PrimitiveTypeKind);
    }

    [Fact]
    [DisplayName("DECIMAL存储类型应映射到Decimal")]
    public void GetEdmType_Decimal_ShouldMapToDecimal()
    {
        var manifest = GetManifest();
        var storeType = TypeUsage.CreateDefaultTypeUsage(manifest.GetStoreType(
            TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.Decimal))).EdmType);

        var edmType = manifest.GetEdmType(storeType);

        Assert.Equal(PrimitiveTypeKind.Decimal, ((PrimitiveType)edmType.EdmType).PrimitiveTypeKind);
    }
    #endregion

    #region GetStoreType 更多类型
    [Fact]
    [DisplayName("SByte应映射到tinyint")]
    public void GetStoreType_SByte_ShouldMapToTinyint()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateDefaultTypeUsage(PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.SByte));

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("tinyint", storeType.EdmType.Name);
    }

    [Fact]
    [DisplayName("超长String应映射到text")]
    public void GetStoreType_LongString_ShouldMapToText()
    {
        var manifest = GetManifest();
        var edmType = TypeUsage.CreateStringTypeUsage(
            PrimitiveType.GetEdmPrimitiveType(PrimitiveTypeKind.String),
            isUnicode: true,
            isFixedLength: false,
            maxLength: 100000);

        var storeType = manifest.GetStoreType(edmType);

        Assert.Equal("text", storeType.EdmType.Name);
    }
    #endregion
}
