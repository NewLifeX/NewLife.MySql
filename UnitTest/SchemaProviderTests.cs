using System.Data;
using NewLife.MySql;

namespace UnitTest;

/// <summary>SchemaProvider 元数据集合完整测试，通过 MySqlConnection.GetSchema 调用</summary>
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class SchemaProviderTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    #region 辅助
    private static MySqlConnection OpenConnection()
    {
        var conn = new MySqlConnection(_ConnStr);
        conn.Open();
        return conn;
    }
    #endregion

    #region MetaDataCollections
    [Fact(DisplayName = "GetSchema_默认返回MetaDataCollections")]
    public void GetSchema_Default_ReturnsMetaDataCollections()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema();

        Assert.Equal("MetaDataCollections", dt.TableName);
        Assert.True(dt.Rows.Count > 0);
        Assert.Contains("CollectionName", dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
    }

    [Fact(DisplayName = "GetSchema_MetaDataCollections包含所有已知集合")]
    public void GetSchema_MetaDataCollections_ContainsAllKnownCollections()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("MetaDataCollections");

        Assert.Equal("MetaDataCollections", dt.TableName);
        Assert.True(dt.Columns.Contains("CollectionName"));
        Assert.True(dt.Columns.Contains("NumberOfRestrictions"));
        Assert.True(dt.Columns.Contains("NumberOfIdentifierParts"));

        var names = dt.Rows.Cast<DataRow>().Select(r => r["CollectionName"]?.ToString()).ToArray();
        Assert.Contains("MetaDataCollections", names);
        Assert.Contains("DataSourceInformation", names);
        Assert.Contains("Restrictions", names);
        Assert.Contains("DataTypes", names);
        Assert.Contains("ReservedWords", names);
        Assert.Contains("Users", names);
        Assert.Contains("Databases", names);
        Assert.Contains("Tables", names);
        Assert.Contains("Columns", names);
        Assert.Contains("Indexes", names);
        Assert.Contains("IndexColumns", names);
    }
    #endregion

    #region DataSourceInformation
    [Fact(DisplayName = "GetSchema_DataSourceInformation返回服务器信息")]
    public void GetSchema_DataSourceInformation()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("DataSourceInformation");

        Assert.Equal("DataSourceInformation", dt.TableName);
        Assert.Equal(1, dt.Rows.Count);

        var row = dt.Rows[0];
        Assert.Equal("MySQL", row["DataSourceProductName"]?.ToString());
        Assert.NotNull(row["DataSourceProductVersion"]);
        Assert.NotNull(row["ParameterMarkerFormat"]);
        Assert.NotNull(row["ParameterMarkerPattern"]);
    }
    #endregion

    #region Restrictions
    [Fact(DisplayName = "GetSchema_Restrictions返回限制定义")]
    public void GetSchema_Restrictions()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Restrictions");

        Assert.Equal("Restrictions", dt.TableName);
        Assert.True(dt.Rows.Count > 0);
        Assert.True(dt.Columns.Contains("CollectionName"));
        Assert.True(dt.Columns.Contains("RestrictionName"));
        Assert.True(dt.Columns.Contains("RestrictionNumber"));

        var collections = dt.Rows.Cast<DataRow>().Select(r => r["CollectionName"]?.ToString()).Distinct().ToArray();
        Assert.Contains("Tables", collections);
        Assert.Contains("Columns", collections);
        Assert.Contains("Indexes", collections);
        Assert.Contains("IndexColumns", collections);
        Assert.Contains("Databases", collections);
        Assert.Contains("Users", collections);
    }
    #endregion

    #region DataTypes
    [Fact(DisplayName = "GetSchema_DataTypes返回MySQL数据类型")]
    public void GetSchema_DataTypes()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("DataTypes");

        Assert.Equal("DataTypes", dt.TableName);
        Assert.True(dt.Rows.Count > 0);
        Assert.True(dt.Columns.Contains("TypeName"));
        Assert.True(dt.Columns.Contains("ProviderDbType"));
        Assert.True(dt.Columns.Contains("DataType"));

        var typeNames = dt.Rows.Cast<DataRow>().Select(r => r["TypeName"]?.ToString()).ToArray();
        Assert.Contains("INT", typeNames);
        Assert.Contains("VARCHAR", typeNames);
        Assert.Contains("BIGINT", typeNames);
        Assert.Contains("DATETIME", typeNames);
        Assert.Contains("DECIMAL", typeNames);
        Assert.Contains("DOUBLE", typeNames);
        Assert.Contains("TEXT", typeNames);
        Assert.Contains("BLOB", typeNames);
    }

    [Fact(DisplayName = "GetSchema_DataTypes包含无符号类型")]
    public void GetSchema_DataTypes_ContainsUnsignedTypes()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("DataTypes");

        var typeNames = dt.Rows.Cast<DataRow>().Select(r => r["TypeName"]?.ToString()).ToArray();
        Assert.Contains("TINYINT UNSIGNED", typeNames);
        Assert.Contains("SMALLINT UNSIGNED", typeNames);
        Assert.Contains("INT UNSIGNED", typeNames);
        Assert.Contains("BIGINT UNSIGNED", typeNames);
    }

    [Fact(DisplayName = "GetSchema_DataTypes字段属性正确")]
    public void GetSchema_DataTypes_FieldPropertiesCorrect()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("DataTypes");

        // 验证 INT 类型属性
        var intRow = dt.Rows.Cast<DataRow>().First(r => r["TypeName"]?.ToString() == "INT");
        Assert.Equal("System.Int32", intRow["DataType"]?.ToString());
        Assert.Equal(true, intRow["IsAutoincrementable"]);
        Assert.Equal(true, intRow["IsFixedLength"]);

        // 验证 VARCHAR 类型属性
        var varcharRow = dt.Rows.Cast<DataRow>().First(r => r["TypeName"]?.ToString() == "VARCHAR");
        Assert.Equal("System.String", varcharRow["DataType"]?.ToString());
        Assert.Equal("VARCHAR({0})", varcharRow["CreateFormat"]?.ToString());
        Assert.Equal("size", varcharRow["CreateParameters"]?.ToString());

        // 验证 DECIMAL 类型属性
        var decimalRow = dt.Rows.Cast<DataRow>().First(r => r["TypeName"]?.ToString() == "DECIMAL");
        Assert.Equal("System.Decimal", decimalRow["DataType"]?.ToString());
        Assert.Equal("DECIMAL({0},{1})", decimalRow["CreateFormat"]?.ToString());
        Assert.Equal("precision,scale", decimalRow["CreateParameters"]?.ToString());
    }
    #endregion

    #region ReservedWords
    [Fact(DisplayName = "GetSchema_ReservedWords返回保留字")]
    public void GetSchema_ReservedWords()
    {
        using var conn = OpenConnection();

        // ReservedWords.txt 需要作为 EmbeddedResource 嵌入，未嵌入时会抛出异常
        var dt = conn.GetSchema("ReservedWords");

        Assert.Equal("ReservedWords", dt.TableName);
        Assert.True(dt.Rows.Count > 0);

        var words = dt.Rows.Cast<DataRow>().Select(r => r[0]?.ToString()?.ToUpper()).ToArray();
        Assert.Contains("SELECT", words);
        Assert.Contains("FROM", words);
        Assert.Contains("WHERE", words);
    }
    #endregion

    #region Users
    [Fact(DisplayName = "GetSchema_Users返回用户列表")]
    public void GetSchema_Users()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Users");

        Assert.Equal("Users", dt.TableName);
        Assert.True(dt.Rows.Count > 0);
        Assert.True(dt.Columns.Contains("HOST"));
        Assert.True(dt.Columns.Contains("USERNAME"));

        var users = dt.Rows.Cast<DataRow>().Select(r => r["USERNAME"]?.ToString()).ToArray();
        Assert.Contains("root", users);
    }

    [Fact(DisplayName = "GetSchema_Users支持限制条件")]
    public void GetSchema_Users_WithRestriction()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Users", ["root"]);

        Assert.True(dt.Rows.Count > 0);
        foreach (DataRow row in dt.Rows)
        {
            Assert.Equal("root", row["USERNAME"]?.ToString());
        }
    }
    #endregion

    #region Databases
    [Fact(DisplayName = "GetSchema_Databases返回数据库列表")]
    public void GetSchema_Databases()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Databases");

        Assert.Equal("Databases", dt.TableName);
        Assert.True(dt.Columns.Contains("CATALOG_NAME"));
        Assert.True(dt.Columns.Contains("SCHEMA_NAME"));

        Assert.True(dt.Rows.Count > 0);

        var dbs = dt.Rows.Cast<DataRow>().Select(r => r["SCHEMA_NAME"]?.ToString()).ToArray();
        Assert.Contains("information_schema", dbs);
        Assert.Contains("mysql", dbs);
    }

    [Fact(DisplayName = "GetSchema_Databases支持限制条件")]
    public void GetSchema_Databases_WithRestriction()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Databases", ["sys"]);

        Assert.Equal("Databases", dt.TableName);
        Assert.True(dt.Rows.Count > 0);

        var dbs = dt.Rows.Cast<DataRow>().Select(r => r["SCHEMA_NAME"]?.ToString()).ToArray();
        Assert.Contains("sys", dbs);
    }
    #endregion

    #region Tables
    [Fact(DisplayName = "GetSchema_Tables验证列结构")]
    public void GetSchema_Tables()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Tables");

        Assert.Equal("Tables", dt.TableName);
        Assert.True(dt.Columns.Contains("TABLE_CATALOG"));
        Assert.True(dt.Columns.Contains("TABLE_SCHEMA"));
        Assert.True(dt.Columns.Contains("TABLE_NAME"));
        Assert.True(dt.Columns.Contains("TABLE_TYPE"));
        Assert.True(dt.Columns.Contains("ENGINE"));
        Assert.True(dt.Columns.Contains("TABLE_ROWS"));
        Assert.True(dt.Columns.Contains("AUTO_INCREMENT"));
        Assert.True(dt.Columns.Contains("CREATE_TIME"));
        Assert.True(dt.Columns.Contains("TABLE_COLLATION"));
        Assert.True(dt.Columns.Contains("TABLE_COMMENT"));
    }

    [Fact(DisplayName = "GetSchema_Tables返回数据行")]
    public void GetSchema_Tables_WithData()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Tables");
        Assert.True(dt.Rows.Count > 0);

        var row = dt.Rows[0];
        Assert.NotNull(row["TABLE_NAME"]);
        Assert.NotNull(row["TABLE_SCHEMA"]);
    }

    [Fact(DisplayName = "GetSchema_Tables支持数据库限制")]
    public void GetSchema_Tables_WithDatabaseRestriction()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Tables", [null, "sys"]);
        Assert.True(dt.Rows.Count > 0);

        foreach (DataRow row in dt.Rows)
        {
            Assert.Equal("sys", row["TABLE_SCHEMA"]?.ToString());
        }
    }

    [Fact(DisplayName = "GetSchema_Tables支持表名限制")]
    public void GetSchema_Tables_WithTableNameRestriction()
    {
        using var conn = OpenConnection();

        var allTables = conn.GetSchema("Tables");
        Assert.True(allTables.Rows.Count > 0);

        var firstTable = allTables.Rows[0]["TABLE_NAME"]?.ToString();
        var schema = allTables.Rows[0]["TABLE_SCHEMA"]?.ToString();

        var dt = conn.GetSchema("Tables", [null, schema, firstTable]);

        Assert.True(dt.Rows.Count >= 1);
        Assert.Equal(firstTable, dt.Rows[0]["TABLE_NAME"]?.ToString());
    }
    #endregion

    #region Columns
    [Fact(DisplayName = "GetSchema_Columns验证列结构")]
    public void GetSchema_Columns()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Columns");
        Assert.True(dt.Rows.Count > 0);

        Assert.Equal("Columns", dt.TableName);
        Assert.True(dt.Columns.Contains("TABLE_SCHEMA"));
        Assert.True(dt.Columns.Contains("TABLE_NAME"));
        Assert.True(dt.Columns.Contains("COLUMN_NAME"));
        Assert.True(dt.Columns.Contains("ORDINAL_POSITION"));
        Assert.True(dt.Columns.Contains("COLUMN_DEFAULT"));
        Assert.True(dt.Columns.Contains("IS_NULLABLE"));
        Assert.True(dt.Columns.Contains("DATA_TYPE"));
        Assert.True(dt.Columns.Contains("CHARACTER_MAXIMUM_LENGTH"));
        Assert.True(dt.Columns.Contains("NUMERIC_PRECISION"));
        Assert.True(dt.Columns.Contains("NUMERIC_SCALE"));
        Assert.True(dt.Columns.Contains("COLUMN_TYPE"));
        Assert.True(dt.Columns.Contains("COLUMN_KEY"));
        Assert.True(dt.Columns.Contains("EXTRA"));
        Assert.True(dt.Columns.Contains("COLUMN_COMMENT"));
        Assert.True(dt.Columns.Contains("GENERATION_EXPRESSION"));
    }

    [Fact(DisplayName = "GetSchema_Columns支持表限制")]
    public void GetSchema_Columns_WithTableRestriction()
    {
        using var conn = OpenConnection();

        var tables = conn.GetSchema("Tables");
        Assert.True(tables.Rows.Count > 0);

        var schema = tables.Rows[0]["TABLE_SCHEMA"]?.ToString();
        var tableName = tables.Rows[0]["TABLE_NAME"]?.ToString();

        var dt = conn.GetSchema("Columns", [null, schema, tableName]);

        Assert.True(dt.Rows.Count > 0);
        foreach (DataRow row in dt.Rows)
        {
            Assert.Equal(schema, row["TABLE_SCHEMA"]?.ToString());
            Assert.Equal(tableName, row["TABLE_NAME"]?.ToString());
        }
    }

    [Fact(DisplayName = "GetSchema_Columns支持列名限制")]
    public void GetSchema_Columns_WithColumnRestriction()
    {
        using var conn = OpenConnection();

        var allColumns = conn.GetSchema("Columns");
        Assert.True(allColumns.Rows.Count > 0);

        var schema = allColumns.Rows[0]["TABLE_SCHEMA"]?.ToString();
        var tableName = allColumns.Rows[0]["TABLE_NAME"]?.ToString();
        var columnName = allColumns.Rows[0]["COLUMN_NAME"]?.ToString();

        var dt = conn.GetSchema("Columns", [null, schema, tableName, columnName]);

        Assert.True(dt.Rows.Count >= 1);
        Assert.Equal(columnName, dt.Rows[0]["COLUMN_NAME"]?.ToString());
    }
    #endregion

    #region Indexes
    [Fact(DisplayName = "GetSchema_Indexes验证列结构")]
    public void GetSchema_Indexes()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Indexes");

        Assert.Equal("Indexes", dt.TableName);
        Assert.True(dt.Columns.Contains("INDEX_CATALOG"));
        Assert.True(dt.Columns.Contains("INDEX_SCHEMA"));
        Assert.True(dt.Columns.Contains("INDEX_NAME"));
        Assert.True(dt.Columns.Contains("TABLE_NAME"));
        Assert.True(dt.Columns.Contains("UNIQUE"));
        Assert.True(dt.Columns.Contains("PRIMARY"));
        Assert.True(dt.Columns.Contains("TYPE"));
        Assert.True(dt.Columns.Contains("COMMENT"));
    }

    [Fact(DisplayName = "GetSchema_Indexes支持表限制")]
    public void GetSchema_Indexes_WithTableRestriction()
    {
        using var conn = OpenConnection();

        var allIndexes = conn.GetSchema("Indexes");
        Assert.True(allIndexes.Rows.Count > 0);

        var schema = allIndexes.Rows[0]["INDEX_SCHEMA"]?.ToString();
        var tableName = allIndexes.Rows[0]["TABLE_NAME"]?.ToString();

        var dt = conn.GetSchema("Indexes", [null, schema, tableName]);

        Assert.True(dt.Rows.Count >= 1);
        foreach (DataRow row in dt.Rows)
        {
            Assert.Equal(tableName, row["TABLE_NAME"]?.ToString());
        }
    }
    #endregion

    #region IndexColumns
    [Fact(DisplayName = "GetSchema_IndexColumns验证列结构")]
    public void GetSchema_IndexColumns()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("IndexColumns");

        Assert.Equal("IndexColumns", dt.TableName);
        Assert.True(dt.Columns.Contains("INDEX_CATALOG"));
        Assert.True(dt.Columns.Contains("INDEX_SCHEMA"));
        Assert.True(dt.Columns.Contains("INDEX_NAME"));
        Assert.True(dt.Columns.Contains("TABLE_NAME"));
        Assert.True(dt.Columns.Contains("COLUMN_NAME"));
        Assert.True(dt.Columns.Contains("ORDINAL_POSITION"));
        Assert.True(dt.Columns.Contains("SORT_ORDER"));
    }

    [Fact(DisplayName = "GetSchema_IndexColumns支持索引名限制")]
    public void GetSchema_IndexColumns_WithRestriction()
    {
        using var conn = OpenConnection();

        var allIndexes = conn.GetSchema("Indexes");
        Assert.True(allIndexes.Rows.Count > 0);

        var schema = allIndexes.Rows[0]["INDEX_SCHEMA"]?.ToString();
        var tableName = allIndexes.Rows[0]["TABLE_NAME"]?.ToString();
        var indexName = allIndexes.Rows[0]["INDEX_NAME"]?.ToString();

        var dt = conn.GetSchema("IndexColumns", [null, schema, tableName, indexName]);

        Assert.True(dt.Rows.Count >= 1);
        foreach (DataRow row in dt.Rows)
        {
            Assert.Equal(indexName, row["INDEX_NAME"]?.ToString());
            Assert.Equal(tableName, row["TABLE_NAME"]?.ToString());
        }
    }
    #endregion

    #region 大小写不敏感
    [Theory(DisplayName = "GetSchema_集合名大小写不敏感")]
    [InlineData("MetaDataCollections")]
    [InlineData("metadatacollections")]
    [InlineData("METADATACOLLECTIONS")]
    [InlineData("DataTypes")]
    [InlineData("datatypes")]
    [InlineData("Restrictions")]
    [InlineData("restrictions")]
    public void GetSchema_CollectionName_CaseInsensitive(String collectionName)
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema(collectionName);

        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count > 0);
    }
    #endregion

    #region 未知集合
    [Fact(DisplayName = "GetSchema_未知集合名回退到MetaDataCollections")]
    public void GetSchema_UnknownCollection_FallbackToMetaDataCollections()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("NonExistentCollection");

        Assert.Equal("MetaDataCollections", dt.TableName);
    }
    #endregion

    #region Null集合
    [Fact(DisplayName = "GetSchema_Null集合名回退到MetaDataCollections")]
    public void GetSchema_NullCollection_FallbackToMetaDataCollections()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema(null, null);

        Assert.Equal("MetaDataCollections", dt.TableName);
        Assert.True(dt.Rows.Count > 0);
    }
    #endregion
}
