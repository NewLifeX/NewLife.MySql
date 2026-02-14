using System.ComponentModel;
using NewLife.MySql.EntityFramework;

namespace UnitTest.EntityFramework;

/// <summary>迁移SQL生成器测试</summary>
public class MySqlMigrationSqlGeneratorTests
{
    [Fact]
    [DisplayName("ProviderServices单例应不为空")]
    public void ProviderServices_Instance_ShouldNotBeNull()
    {
        var instance = MySqlProviderServices.Instance;

        Assert.NotNull(instance);
    }

    [Fact]
    [DisplayName("ProviderServices应为同一实例")]
    public void ProviderServices_Instance_ShouldBeSame()
    {
        var a = MySqlProviderServices.Instance;
        var b = MySqlProviderServices.Instance;

        Assert.Same(a, b);
    }
}
