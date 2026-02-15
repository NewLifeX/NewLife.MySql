using System.ComponentModel;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Migrations.Model;
using NewLife.MySql.EntityFramework;

namespace UnitTest.EntityFramework;

/// <summary>迁移SQL生成器补充测试（Schema 处理、列类型扩展、MoveTable 等）</summary>
public class MySqlMigrationSqlGeneratorSchemaTests
{
    private readonly MySqlMigrationSqlGenerator _generator = new();
    private const String Token = "8.0";

    #region Schema 处理
    [Fact]
    [DisplayName("带schema前缀的表名应去除schema")]
    public void Quote_WithSchemaPrefix_ShouldStripSchema()
    {
        var op = new DropTableOperation("dbo.Users");

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Equal("DROP TABLE IF EXISTS `Users`", sql);
    }

    [Fact]
    [DisplayName("不带schema的表名应正常处理")]
    public void Quote_WithoutSchema_ShouldWorkNormally()
    {
        var op = new DropTableOperation("Users");

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Equal("DROP TABLE IF EXISTS `Users`", sql);
    }

    [Fact]
    [DisplayName("带schema前缀的建表操作应去除schema")]
    public void CreateTable_WithSchema_ShouldStripSchema()
    {
        var op = new CreateTableOperation("dbo.Users");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.Int32) { Name = "Id", IsNullable = false });

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("CREATE TABLE `Users`", sql);
        Assert.DoesNotContain("dbo", sql);
    }

    [Fact]
    [DisplayName("带schema的添加列应正确处理")]
    public void AddColumn_WithSchema_ShouldStripSchema()
    {
        var op = new AddColumnOperation("dbo.Users", new ColumnModel(PrimitiveTypeKind.String) { Name = "Email", MaxLength = 200 });

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("ALTER TABLE `Users`", sql);
        Assert.DoesNotContain("dbo", sql);
    }
    #endregion

    #region MoveTable
    [Fact]
    [DisplayName("MoveTable有NewSchema应生成RENAME")]
    public void MoveTable_WithNewSchema_ShouldGenerateRename()
    {
        var op = new MoveTableOperation("Users", "archive");

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Contains("RENAME TABLE", statements[0].Sql);
        Assert.Contains("`archive`.`Users`", statements[0].Sql);
    }

    [Fact]
    [DisplayName("MoveTable无NewSchema应不生成SQL")]
    public void MoveTable_WithoutNewSchema_ShouldReturnEmpty()
    {
        var op = new MoveTableOperation("Users", null);

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Empty(statements);
    }
    #endregion

    #region 列类型边界值
    [Fact]
    [DisplayName("SByte列应映射为TINYINT")]
    public void ColumnType_SByte_ShouldMapToTinyInt()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.SByte) { Name = "Col" });
        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("TINYINT", sql);
        Assert.DoesNotContain("UNSIGNED", sql);
    }

    [Fact]
    [DisplayName("DateTimeOffset列应映射为DATETIME")]
    public void ColumnType_DateTimeOffset_ShouldMapToDatetime()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.DateTimeOffset) { Name = "Col" });
        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("DATETIME", sql);
    }

    [Fact]
    [DisplayName("固定长度Binary列应映射为BINARY")]
    public void ColumnType_FixedBinary_ShouldMapToBinary()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.Binary) { Name = "Col", IsFixedLength = true, MaxLength = 16 });
        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("BINARY(16)", sql);
    }

    [Fact]
    [DisplayName("Byte默认值应格式化为0x")]
    public void DefaultValue_ByteArray_ShouldFormatAsHex()
    {
        var op = new CreateTableOperation("T");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.Binary)
        {
            Name = "Col",
            DefaultValue = new Byte[] { 0xAB, 0xCD, 0xEF },
        });

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("DEFAULT 0xABCDEF", sql);
    }

    [Fact]
    [DisplayName("DateTimeOffset默认值应正确格式化")]
    public void DefaultValue_DateTimeOffset_ShouldFormat()
    {
        var op = new CreateTableOperation("T");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.DateTimeOffset)
        {
            Name = "Col",
            DefaultValue = new DateTimeOffset(2024, 1, 15, 8, 30, 0, TimeSpan.Zero),
        });

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("DEFAULT '2024-01-15 08:30:00'", sql);
    }
    #endregion

    #region AlterTable
    [Fact]
    [DisplayName("AlterTable无注解应不生成SQL")]
    public void AlterTable_NoAnnotations_ShouldReturnEmpty()
    {
        var op = new AlterTableOperation("Users", null);

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Empty(statements);
    }
    #endregion

    #region 重命名列（带schema前缀）
    [Fact]
    [DisplayName("重命名列应处理带schema的表名")]
    public void RenameColumn_WithSchema_ShouldStripSchema()
    {
        var op = new RenameColumnOperation("dbo.Users", "OldName", "NewName");

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("`Users`", sql);
        Assert.DoesNotContain("dbo", sql);
    }
    #endregion

    #region 外键（带schema前缀）
    [Fact]
    [DisplayName("添加外键应处理带schema的表名")]
    public void AddForeignKey_WithSchema_ShouldStripSchema()
    {
        var op = new AddForeignKeyOperation
        {
            Name = "FK_Orders_Users",
            DependentTable = "dbo.Orders",
            DependentColumns = { "UserId" },
            PrincipalTable = "dbo.Users",
            PrincipalColumns = { "Id" },
        };

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("`Orders`", sql);
        Assert.Contains("`Users`", sql);
        Assert.DoesNotContain("dbo", sql);
    }
    #endregion

    #region 索引（带schema前缀）
    [Fact]
    [DisplayName("创建索引应处理带schema的表名")]
    public void CreateIndex_WithSchema_ShouldStripSchema()
    {
        var op = new CreateIndexOperation
        {
            Name = "IX_Users_Email",
            Table = "dbo.Users",
            Columns = { "Email" },
        };

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("`Users`", sql);
        Assert.DoesNotContain("dbo", sql);
    }
    #endregion

    #region 多种列约束组合
    [Fact]
    [DisplayName("建表带NotNull和AutoIncrement和Default应全部包含")]
    public void CreateTable_ComplexColumns_ShouldIncludeAllConstraints()
    {
        var op = new CreateTableOperation("Products");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.Int32)
        {
            Name = "Id",
            IsNullable = false,
            IsIdentity = true,
        });
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.String)
        {
            Name = "Name",
            MaxLength = 200,
            IsNullable = false,
        });
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.Decimal)
        {
            Name = "Price",
            Precision = 10,
            Scale = 2,
            DefaultValue = 0.00m,
        });
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.Boolean)
        {
            Name = "IsActive",
            DefaultValue = true,
        });
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.DateTime)
        {
            Name = "CreatedAt",
            DefaultValueSql = "CURRENT_TIMESTAMP",
        });
        op.PrimaryKey = new AddPrimaryKeyOperation { Columns = { "Id" } };

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("`Id` INT", sql);
        Assert.Contains("NOT NULL", sql);
        Assert.Contains("AUTO_INCREMENT", sql);
        Assert.Contains("`Name` VARCHAR(200)", sql);
        Assert.Contains("DECIMAL(10,2)", sql);
        Assert.Contains("TINYINT(1)", sql);
        Assert.Contains("DEFAULT 1", sql);
        Assert.Contains("DEFAULT CURRENT_TIMESTAMP", sql);
        Assert.Contains("PRIMARY KEY (`Id`)", sql);
        Assert.Contains("ENGINE=InnoDB", sql);
    }
    #endregion

    #region 多列主键和外键组合
    [Fact]
    [DisplayName("复合外键应正确生成")]
    public void AddForeignKey_Composite_ShouldListAllColumns()
    {
        var op = new AddForeignKeyOperation
        {
            Name = "FK_OrderItems_Orders",
            DependentTable = "OrderItems",
            DependentColumns = { "OrderId", "ProductId" },
            PrincipalTable = "Orders",
            PrincipalColumns = { "Id", "ProductId" },
        };

        var sql = _generator.Generate([op], Token).First().Sql;

        Assert.Contains("FOREIGN KEY (`OrderId`, `ProductId`)", sql);
        Assert.Contains("REFERENCES `Orders` (`Id`, `ProductId`)", sql);
    }
    #endregion
}
