using NewLife.MySql;
using NewLife.MySql.Common;

namespace UnitTest;

/// <summary>数据库类型检测测试</summary>
[Collection(TestCollections.InMemory)]
public class DatabaseTypeDetectionTests
{
    [Theory(DisplayName = "检测MySQL版本字符串")]
    [InlineData("5.7.33")]
    [InlineData("8.0.26")]
    [InlineData("9.0.1")]
    public void DetectMySQL(String version) => Assert.Equal(DatabaseType.MySQL, MySqlConnection.DetectDatabaseType(version));

    [Theory(DisplayName = "检测OceanBase版本字符串")]
    [InlineData("5.7.25-OceanBase-v4.0.0")]
    [InlineData("5.7.25-OceanBase_CE-v3.1.4")]
    [InlineData("8.0.32-OceanBase-v4.2.0")]
    [InlineData("5.7.25-oceanbase-v4.0.0")]
    public void DetectOceanBase(String version) => Assert.Equal(DatabaseType.OceanBase, MySqlConnection.DetectDatabaseType(version));

    [Theory(DisplayName = "检测TiDB版本字符串")]
    [InlineData("5.7.25-TiDB-v5.4.0")]
    [InlineData("5.7.25-TiDB-v6.5.2")]
    [InlineData("8.0.11-TiDB-v7.1.0")]
    [InlineData("5.7.25-tidb-v5.4.0")]
    public void DetectTiDB(String version) => Assert.Equal(DatabaseType.TiDB, MySqlConnection.DetectDatabaseType(version));

    [Theory(DisplayName = "检测Apache Doris版本字符串")]
    [InlineData("5.7.99 Doris version doris-2.0.3-rc06-37d31a5")]
    [InlineData("5.7.99 Doris version doris-3.0.0")]
    [InlineData("5.7.99 Doris version doris-4.1.2")]
    [InlineData("5.7.99 dORIS version doris-2.0.0")]
    public void DetectDoris(String version) => Assert.Equal(DatabaseType.Doris, MySqlConnection.DetectDatabaseType(version));

    [Fact(DisplayName = "检测Amazon Aurora")]
    public void DetectAurora()
    {
        // Aurora 的版本号与 MySQL 相同，产品标识在 version_comment 中
        Assert.Equal(DatabaseType.AmazonAurora, MySqlConnection.DetectDatabaseType("5.7.12-log", "Aurora MySQL Community Edition (GPL)"));
        Assert.Equal(DatabaseType.AmazonAurora, MySqlConnection.DetectDatabaseType("8.0.32", "aurora MySQL"));
        Assert.Equal(DatabaseType.AmazonAurora, MySqlConnection.DetectDatabaseType("5.7.25-Aurora"));
    }

    [Fact(DisplayName = "检测Google Cloud SQL")]
    public void DetectCloudSql()
    {
        // Cloud SQL 的版本号通常带 -google 后缀，产品标识在 version_comment 中
        Assert.Equal(DatabaseType.GoogleCloudSql, MySqlConnection.DetectDatabaseType("8.0.26-google", "Google Cloud SQL"));
        Assert.Equal(DatabaseType.GoogleCloudSql, MySqlConnection.DetectDatabaseType("8.0.32", "google cloud sql"));
        Assert.Equal(DatabaseType.GoogleCloudSql, MySqlConnection.DetectDatabaseType("8.0.26-Cloud SQL"));
    }

    [Fact(DisplayName = "空版本与普通MySQL注释回退MySQL")]
    public void EmptyVersionShouldReturnMySQL()
    {
        Assert.Equal(DatabaseType.MySQL, MySqlConnection.DetectDatabaseType(""));
        Assert.Equal(DatabaseType.MySQL, MySqlConnection.DetectDatabaseType(null));
        Assert.Equal(DatabaseType.MySQL, MySqlConnection.DetectDatabaseType("8.0.26", "MySQL Community Server - GPL"));
    }

    [Fact(DisplayName = "版本号产品标识优先于注释")]
    public void VersionTakesPrecedenceOverComment()
        => Assert.Equal(DatabaseType.TiDB, MySqlConnection.DetectDatabaseType("5.7.25-TiDB-v6.5.2", "Aurora MySQL"));
}