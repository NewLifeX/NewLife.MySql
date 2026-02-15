using System.ComponentModel;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Migrations.Model;
using NewLife.MySql.EntityFramework;

namespace UnitTest.EntityFramework;

/// <summary>SQL 生成器标识符包裹测试（通过迁移输出验证反引号包裹行为）</summary>
public class MySqlSqlGeneratorTests
{
    private readonly MySqlMigrationSqlGenerator _generator = new();
    private const String Token = "8.0";

    [Fact]
    [DisplayName("标识符应使用反引号包裹")]
    public void Identifier_ShouldBeWrappedWithBackticks()
    {
        var op = new DropTableOperation("Users");

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Equal("DROP TABLE IF EXISTS `Users`", sql);
    }

    [Fact]
    [DisplayName("含空格的标识符应正确包裹")]
    public void Identifier_WithSpaces_ShouldBeWrapped()
    {
        var op = new DropTableOperation("Order Details");

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Equal("DROP TABLE IF EXISTS `Order Details`", sql);
    }

    [Fact]
    [DisplayName("列名和表名应分别用反引号包裹")]
    public void ColumnAndTableNames_ShouldBothBeWrapped()
    {
        var op = new DropColumnOperation("MyTable", "MyColumn");

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Equal("ALTER TABLE `MyTable` DROP COLUMN `MyColumn`", sql);
    }

    [Fact]
    [DisplayName("索引名和表名应分别用反引号包裹")]
    public void IndexNames_ShouldBeWrapped()
    {
        var op = new CreateIndexOperation
        {
            Name = "IX_Test",
            Table = "TestTable",
            Columns = { "Column1", "Column2" },
        };

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("`IX_Test`", sql);
        Assert.Contains("`TestTable`", sql);
        Assert.Contains("`Column1`", sql);
        Assert.Contains("`Column2`", sql);
    }
}

