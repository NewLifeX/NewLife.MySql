using System.ComponentModel;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Migrations.Model;
using System.Data.Entity.Migrations.Sql;
using NewLife.MySql.EntityFramework;

namespace UnitTest.EntityFramework;

/// <summary>迁移SQL生成器测试</summary>
public class MySqlMigrationSqlGeneratorTests
{
    private readonly MySqlMigrationSqlGenerator _generator = new();
    private const String Token = "8.0";

    #region ProviderServices
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
    #endregion

    #region 建表
    [Fact]
    [DisplayName("生成建表SQL应包含表名和列")]
    public void GenerateCreateTable_ShouldContainTableAndColumns()
    {
        var op = new CreateTableOperation("Users");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.Int32)
        {
            Name = "Id",
            IsNullable = false,
            IsIdentity = true,
        });
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.String)
        {
            Name = "Name",
            MaxLength = 100,
        });
        op.PrimaryKey = new AddPrimaryKeyOperation { Columns = { "Id" } };

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        var sql = statements[0].Sql;
        Assert.Contains("CREATE TABLE `Users`", sql);
        Assert.Contains("`Id` INT", sql);
        Assert.Contains("NOT NULL", sql);
        Assert.Contains("AUTO_INCREMENT", sql);
        Assert.Contains("`Name` VARCHAR(100)", sql);
        Assert.Contains("PRIMARY KEY (`Id`)", sql);
        Assert.Contains("ENGINE=InnoDB", sql);
        Assert.Contains("utf8mb4", sql);
    }

    [Fact]
    [DisplayName("建表应支持默认值")]
    public void GenerateCreateTable_WithDefaultValue_ShouldContainDefault()
    {
        var op = new CreateTableOperation("Config");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.Boolean)
        {
            Name = "IsActive",
            DefaultValue = true,
        });
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.String)
        {
            Name = "Status",
            DefaultValue = "Active",
        });

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        var sql = statements[0].Sql;
        Assert.Contains("DEFAULT 1", sql);
        Assert.Contains("DEFAULT 'Active'", sql);
    }

    [Fact]
    [DisplayName("建表应支持DefaultValueSql")]
    public void GenerateCreateTable_WithDefaultValueSql_ShouldContainExpression()
    {
        var op = new CreateTableOperation("Logs");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.DateTime)
        {
            Name = "CreatedAt",
            DefaultValueSql = "CURRENT_TIMESTAMP",
        });

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Contains("DEFAULT CURRENT_TIMESTAMP", statements[0].Sql);
    }
    #endregion

    #region 删表
    [Fact]
    [DisplayName("删表SQL应包含IF EXISTS")]
    public void GenerateDropTable_ShouldContainIfExists()
    {
        var op = new DropTableOperation("OldTable");

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("DROP TABLE IF EXISTS `OldTable`", statements[0].Sql);
    }
    #endregion

    #region 列操作
    [Fact]
    [DisplayName("添加列应生成正确SQL")]
    public void GenerateAddColumn_ShouldGenerateCorrectSql()
    {
        var op = new AddColumnOperation("Users", new ColumnModel(PrimitiveTypeKind.String)
        {
            Name = "Email",
            MaxLength = 200,
            IsNullable = false,
        });

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        var sql = statements[0].Sql;
        Assert.Contains("ALTER TABLE `Users` ADD `Email` VARCHAR(200)", sql);
        Assert.Contains("NOT NULL", sql);
    }

    [Fact]
    [DisplayName("添加自增列应包含AUTO_INCREMENT")]
    public void GenerateAddColumn_WithIdentity_ShouldContainAutoIncrement()
    {
        var op = new AddColumnOperation("Users", new ColumnModel(PrimitiveTypeKind.Int64)
        {
            Name = "SequenceNo",
            IsIdentity = true,
        });

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Contains("AUTO_INCREMENT", statements[0].Sql);
    }

    [Fact]
    [DisplayName("删除列应生成正确SQL")]
    public void GenerateDropColumn_ShouldGenerateCorrectSql()
    {
        var op = new DropColumnOperation("Users", "OldField");

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("ALTER TABLE `Users` DROP COLUMN `OldField`", statements[0].Sql);
    }

    [Fact]
    [DisplayName("修改列应使用MODIFY COLUMN")]
    public void GenerateAlterColumn_ShouldUseModifyColumn()
    {
        var op = new AlterColumnOperation("Users", new ColumnModel(PrimitiveTypeKind.String)
        {
            Name = "Name",
            MaxLength = 200,
            IsNullable = false,
        }, isDestructiveChange: false);

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        var sql = statements[0].Sql;
        Assert.Contains("ALTER TABLE `Users` MODIFY COLUMN `Name` VARCHAR(200)", sql);
        Assert.Contains("NOT NULL", sql);
    }

    [Fact]
    [DisplayName("重命名列应使用RENAME COLUMN")]
    public void GenerateRenameColumn_ShouldUseRenameColumn()
    {
        var op = new RenameColumnOperation("Users", "OldName", "NewName");

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("ALTER TABLE `Users` RENAME COLUMN `OldName` TO `NewName`", statements[0].Sql);
    }
    #endregion

    #region 重命名表
    [Fact]
    [DisplayName("重命名表应使用RENAME TABLE")]
    public void GenerateRenameTable_ShouldUseRenameTable()
    {
        var op = new RenameTableOperation("OldTable", "NewTable");

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("RENAME TABLE `OldTable` TO `NewTable`", statements[0].Sql);
    }
    #endregion

    #region 主键
    [Fact]
    [DisplayName("添加主键应生成正确SQL")]
    public void GenerateAddPrimaryKey_ShouldGenerateCorrectSql()
    {
        var op = new AddPrimaryKeyOperation { Table = "Users", Columns = { "Id" } };

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("ALTER TABLE `Users` ADD PRIMARY KEY (`Id`)", statements[0].Sql);
    }

    [Fact]
    [DisplayName("添加复合主键")]
    public void GenerateAddPrimaryKey_Composite_ShouldListAllColumns()
    {
        var op = new AddPrimaryKeyOperation { Table = "OrderItems", Columns = { "OrderId", "ProductId" } };

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("ALTER TABLE `OrderItems` ADD PRIMARY KEY (`OrderId`, `ProductId`)", statements[0].Sql);
    }

    [Fact]
    [DisplayName("删除主键应生成正确SQL")]
    public void GenerateDropPrimaryKey_ShouldGenerateCorrectSql()
    {
        var op = new DropPrimaryKeyOperation { Table = "Users" };

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("ALTER TABLE `Users` DROP PRIMARY KEY", statements[0].Sql);
    }
    #endregion

    #region 外键
    [Fact]
    [DisplayName("添加外键应生成正确SQL")]
    public void GenerateAddForeignKey_ShouldGenerateCorrectSql()
    {
        var op = new AddForeignKeyOperation
        {
            Name = "FK_Orders_Users",
            DependentTable = "Orders",
            DependentColumns = { "UserId" },
            PrincipalTable = "Users",
            PrincipalColumns = { "Id" },
        };

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        var sql = statements[0].Sql;
        Assert.Contains("ADD CONSTRAINT `FK_Orders_Users`", sql);
        Assert.Contains("FOREIGN KEY (`UserId`)", sql);
        Assert.Contains("REFERENCES `Users` (`Id`)", sql);
        Assert.DoesNotContain("CASCADE", sql);
    }

    [Fact]
    [DisplayName("添加级联删除外键")]
    public void GenerateAddForeignKey_CascadeDelete_ShouldContainCascade()
    {
        var op = new AddForeignKeyOperation
        {
            Name = "FK_Orders_Users",
            DependentTable = "Orders",
            DependentColumns = { "UserId" },
            PrincipalTable = "Users",
            PrincipalColumns = { "Id" },
            CascadeDelete = true,
        };

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Contains("ON DELETE CASCADE", statements[0].Sql);
    }

    [Fact]
    [DisplayName("删除外键应生成正确SQL")]
    public void GenerateDropForeignKey_ShouldGenerateCorrectSql()
    {
        var op = new DropForeignKeyOperation
        {
            Name = "FK_Orders_Users",
            DependentTable = "Orders",
        };

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("ALTER TABLE `Orders` DROP FOREIGN KEY `FK_Orders_Users`", statements[0].Sql);
    }
    #endregion

    #region 索引
    [Fact]
    [DisplayName("创建索引应生成正确SQL")]
    public void GenerateCreateIndex_ShouldGenerateCorrectSql()
    {
        var op = new CreateIndexOperation
        {
            Name = "IX_Users_Email",
            Table = "Users",
            Columns = { "Email" },
        };

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("CREATE INDEX `IX_Users_Email` ON `Users` (`Email`)", statements[0].Sql);
    }

    [Fact]
    [DisplayName("创建唯一索引")]
    public void GenerateCreateIndex_Unique_ShouldContainUnique()
    {
        var op = new CreateIndexOperation
        {
            Name = "UX_Users_Email",
            Table = "Users",
            Columns = { "Email" },
            IsUnique = true,
        };

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Contains("UNIQUE INDEX", statements[0].Sql);
    }

    [Fact]
    [DisplayName("创建复合索引")]
    public void GenerateCreateIndex_Composite_ShouldListAllColumns()
    {
        var op = new CreateIndexOperation
        {
            Name = "IX_Users_LastFirst",
            Table = "Users",
            Columns = { "LastName", "FirstName" },
        };

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Contains("(`LastName`, `FirstName`)", statements[0].Sql);
    }

    [Fact]
    [DisplayName("删除索引应生成正确SQL")]
    public void GenerateDropIndex_ShouldGenerateCorrectSql()
    {
        var op = new DropIndexOperation
        {
            Name = "IX_Users_Email",
            Table = "Users",
        };

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("DROP INDEX `IX_Users_Email` ON `Users`", statements[0].Sql);
    }

    [Fact]
    [DisplayName("重命名索引应使用ALTER TABLE RENAME INDEX")]
    public void GenerateRenameIndex_ShouldUseAlterTableRenameIndex()
    {
        var op = new RenameIndexOperation("Users", "IX_Old", "IX_New");

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("ALTER TABLE `Users` RENAME INDEX `IX_Old` TO `IX_New`", statements[0].Sql);
    }
    #endregion

    #region SqlOperation
    [Fact]
    [DisplayName("自定义SQL操作应原样输出")]
    public void GenerateSqlOperation_ShouldPassThrough()
    {
        var op = new SqlOperation("INSERT INTO Config (Key, Value) VALUES ('ver', '1.0')");

        var statements = _generator.Generate([op], Token).ToList();

        Assert.Single(statements);
        Assert.Equal("INSERT INTO Config (Key, Value) VALUES ('ver', '1.0')", statements[0].Sql);
    }
    #endregion

    #region 列类型映射（通过建表SQL间接验证）
    [Fact]
    [DisplayName("Boolean列应映射为TINYINT(1)")]
    public void ColumnType_Boolean_ShouldMapToTinyInt1()
    {
        var sql = GenerateAddColumnSql(PrimitiveTypeKind.Boolean, "Flag");
        Assert.Contains("TINYINT(1)", sql);
    }

    [Fact]
    [DisplayName("Byte列应映射为TINYINT UNSIGNED")]
    public void ColumnType_Byte_ShouldMapToTinyIntUnsigned()
    {
        var sql = GenerateAddColumnSql(PrimitiveTypeKind.Byte, "Age");
        Assert.Contains("TINYINT UNSIGNED", sql);
    }

    [Fact]
    [DisplayName("Int16列应映射为SMALLINT")]
    public void ColumnType_Int16_ShouldMapToSmallInt()
    {
        var sql = GenerateAddColumnSql(PrimitiveTypeKind.Int16, "Code");
        Assert.Contains("SMALLINT", sql);
    }

    [Fact]
    [DisplayName("Int32列应映射为INT")]
    public void ColumnType_Int32_ShouldMapToInt()
    {
        var sql = GenerateAddColumnSql(PrimitiveTypeKind.Int32, "Id");
        Assert.Contains("INT", sql);
    }

    [Fact]
    [DisplayName("Int64列应映射为BIGINT")]
    public void ColumnType_Int64_ShouldMapToBigInt()
    {
        var sql = GenerateAddColumnSql(PrimitiveTypeKind.Int64, "BigId");
        Assert.Contains("BIGINT", sql);
    }

    [Fact]
    [DisplayName("Single列应映射为FLOAT")]
    public void ColumnType_Single_ShouldMapToFloat()
    {
        var sql = GenerateAddColumnSql(PrimitiveTypeKind.Single, "Rate");
        Assert.Contains("FLOAT", sql);
    }

    [Fact]
    [DisplayName("Double列应映射为DOUBLE")]
    public void ColumnType_Double_ShouldMapToDouble()
    {
        var sql = GenerateAddColumnSql(PrimitiveTypeKind.Double, "Score");
        Assert.Contains("DOUBLE", sql);
    }

    [Fact]
    [DisplayName("DateTime列应映射为DATETIME")]
    public void ColumnType_DateTime_ShouldMapToDatetime()
    {
        var sql = GenerateAddColumnSql(PrimitiveTypeKind.DateTime, "CreatedAt");
        Assert.Contains("DATETIME", sql);
    }

    [Fact]
    [DisplayName("Time列应映射为TIME")]
    public void ColumnType_Time_ShouldMapToTime()
    {
        var sql = GenerateAddColumnSql(PrimitiveTypeKind.Time, "Duration");
        Assert.Contains("TIME", sql);
    }

    [Fact]
    [DisplayName("Guid列应映射为CHAR(36)")]
    public void ColumnType_Guid_ShouldMapToChar36()
    {
        var sql = GenerateAddColumnSql(PrimitiveTypeKind.Guid, "UniqueId");
        Assert.Contains("CHAR(36)", sql);
    }

    [Fact]
    [DisplayName("有MaxLength的String列应映射为VARCHAR(n)")]
    public void ColumnType_StringWithMaxLength_ShouldMapToVarchar()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.String) { Name = "Col", MaxLength = 100 });
        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("VARCHAR(100)", sql);
    }

    [Fact]
    [DisplayName("无MaxLength的String列应默认为VARCHAR(255)")]
    public void ColumnType_StringNoMaxLength_ShouldDefaultToVarchar255()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.String) { Name = "Col" });
        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("VARCHAR(255)", sql);
    }

    [Fact]
    [DisplayName("超长String列应映射为TEXT")]
    public void ColumnType_LongString_ShouldMapToText()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.String) { Name = "Col", MaxLength = 20000 });
        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("TEXT", sql);
    }

    [Fact]
    [DisplayName("有MaxLength的Binary列应映射为VARBINARY(n)")]
    public void ColumnType_BinaryWithMaxLength_ShouldMapToVarbinary()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.Binary) { Name = "Col", MaxLength = 500 });
        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("VARBINARY(500)", sql);
    }

    [Fact]
    [DisplayName("无MaxLength的Binary列应映射为BLOB")]
    public void ColumnType_BinaryNoMaxLength_ShouldMapToBlob()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.Binary) { Name = "Col" });
        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("BLOB", sql);
    }

    [Fact]
    [DisplayName("Decimal列应包含精度")]
    public void ColumnType_Decimal_ShouldContainPrecision()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.Decimal) { Name = "Col", Precision = 10, Scale = 4 });
        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("DECIMAL(10,4)", sql);
    }

    [Fact]
    [DisplayName("Decimal列默认精度应为18,2")]
    public void ColumnType_Decimal_ShouldDefaultTo18And2()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.Decimal) { Name = "Col" });
        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("DECIMAL(18,2)", sql);
    }

    [Fact]
    [DisplayName("自定义StoreType应优先使用")]
    public void ColumnType_WithStoreType_ShouldUseStoreType()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.String) { Name = "Col", StoreType = "JSON" });
        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("JSON", sql);
    }

    [Fact]
    [DisplayName("固定长度字符串应映射为CHAR")]
    public void ColumnType_FixedString_ShouldMapToChar()
    {
        var op = new AddColumnOperation("T", new ColumnModel(PrimitiveTypeKind.String) { Name = "Col", IsFixedLength = true, MaxLength = 10 });
        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("CHAR(10)", sql);
    }
    #endregion

    #region 默认值格式化（通过建表SQL间接验证）
    [Fact]
    [DisplayName("字符串默认值应转义单引号")]
    public void DefaultValue_String_ShouldEscapeQuotes()
    {
        var op = new CreateTableOperation("T");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.String)
        {
            Name = "Col",
            DefaultValue = "it's a test",
        });

        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("DEFAULT 'it''s a test'", sql);
    }

    [Fact]
    [DisplayName("Boolean默认值应映射为0和1")]
    public void DefaultValue_Boolean_ShouldMapToNumber()
    {
        var op = new CreateTableOperation("T");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.Boolean)
        {
            Name = "Col1",
            DefaultValue = true,
        });
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.Boolean)
        {
            Name = "Col2",
            DefaultValue = false,
        });

        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("DEFAULT 1", sql);
        Assert.Contains("DEFAULT 0", sql);
    }

    [Fact]
    [DisplayName("DateTime默认值应格式化")]
    public void DefaultValue_DateTime_ShouldFormat()
    {
        var op = new CreateTableOperation("T");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.DateTime)
        {
            Name = "Col",
            DefaultValue = new DateTime(2024, 7, 1, 12, 30, 45),
        });

        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("DEFAULT '2024-07-01 12:30:45'", sql);
    }

    [Fact]
    [DisplayName("Guid默认值应包含引号")]
    public void DefaultValue_Guid_ShouldWrapInQuotes()
    {
        var guid = Guid.Parse("12345678-1234-1234-1234-123456789abc");
        var op = new CreateTableOperation("T");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.Guid)
        {
            Name = "Col",
            DefaultValue = guid,
        });

        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("DEFAULT '12345678-1234-1234-1234-123456789abc'", sql);
    }

    [Fact]
    [DisplayName("数值默认值应直接输出")]
    public void DefaultValue_Numeric_ShouldOutputDirectly()
    {
        var op = new CreateTableOperation("T");
        op.Columns.Add(new ColumnModel(PrimitiveTypeKind.Int32)
        {
            Name = "Col",
            DefaultValue = 42,
        });

        var sql = _generator.Generate([op], Token).First().Sql;
        Assert.Contains("DEFAULT 42", sql);
    }
    #endregion

    #region 多操作组合
    [Fact]
    [DisplayName("多个迁移操作应全部生成")]
    public void Generate_MultipleOperations_ShouldGenerateAll()
    {
        var ops = new List<MigrationOperation>
        {
            new CreateTableOperation("Users")
            {
                Columns = { new ColumnModel(PrimitiveTypeKind.Int32) { Name = "Id", IsNullable = false } },
            },
            new CreateIndexOperation { Name = "IX_Users_Id", Table = "Users", Columns = { "Id" } },
            new SqlOperation("SELECT 1"),
        };

        var statements = _generator.Generate(ops, Token).ToList();

        Assert.Equal(3, statements.Count);
    }

    [Fact]
    [DisplayName("空操作集合应返回空结果")]
    public void Generate_EmptyOperations_ShouldReturnEmpty()
    {
        var statements = _generator.Generate([], Token).ToList();

        Assert.Empty(statements);
    }
    #endregion

    #region 辅助
    /// <summary>生成 AddColumn SQL 用于验证列类型映射</summary>
    private String GenerateAddColumnSql(PrimitiveTypeKind type, String name)
    {
        var op = new AddColumnOperation("TestTable", new ColumnModel(type) { Name = name });
        return _generator.Generate([op], Token).First().Sql;
    }
    #endregion
}
