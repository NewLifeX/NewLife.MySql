using System.ComponentModel;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using NewLife.MySql.EntityFrameworkCore;

namespace UnitTest.EntityFrameworkCore;

/// <summary>类型映射源测试</summary>
public class MySqlTypeMappingSourceTests
{
    [Theory]
    [DisplayName("CLR类型应映射到正确的MySQL类型")]
    [InlineData(typeof(Int32), "INT")]
    [InlineData(typeof(Int64), "BIGINT")]
    [InlineData(typeof(Int16), "SMALLINT")]
    [InlineData(typeof(Byte), "TINYINT UNSIGNED")]
    [InlineData(typeof(Boolean), "TINYINT(1)")]
    [InlineData(typeof(Double), "DOUBLE")]
    [InlineData(typeof(Single), "FLOAT")]
    [InlineData(typeof(Decimal), "DECIMAL(65,30)")]
    [InlineData(typeof(DateTime), "DATETIME")]
    [InlineData(typeof(Guid), "CHAR(36)")]
    [InlineData(typeof(Byte[]), "BLOB")]
    public void FindMapping_ClrType_ShouldReturnCorrectStoreType(Type clrType, String expectedStoreType)
    {
        var source = CreateTypeMappingSource();

        var mapping = source.FindMapping(clrType);

        Assert.NotNull(mapping);
        Assert.Equal(expectedStoreType, mapping.StoreType);
    }

    [Fact]
    [DisplayName("String类型应映射到VARCHAR")]
    public void FindMapping_String_ShouldReturnVarchar()
    {
        var source = CreateTypeMappingSource();

        var mapping = source.FindMapping(typeof(String));

        Assert.NotNull(mapping);
        Assert.StartsWith("VARCHAR", mapping.StoreType);
    }

    private static MySqlTypeMappingSource CreateTypeMappingSource()
    {
        var valueConverterSelector = new ValueConverterSelector(new ValueConverterSelectorDependencies());
        var jsonDeps = new JsonValueReaderWriterSourceDependencies();
        var deps = new TypeMappingSourceDependencies(
            valueConverterSelector,
            new JsonValueReaderWriterSource(jsonDeps),
            Array.Empty<ITypeMappingSourcePlugin>()
        );
        var relationalDeps = new RelationalTypeMappingSourceDependencies(
            Array.Empty<IRelationalTypeMappingSourcePlugin>()
        );
        return new MySqlTypeMappingSource(deps, relationalDeps);
    }
}
