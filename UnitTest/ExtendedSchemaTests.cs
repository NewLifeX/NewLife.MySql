using System.Data;
using NewLife.MySql;

namespace UnitTest;

/// <summary>扩展 Schema 查询测试。验证 Procedures/Views/Triggers/ForeignKeys 元数据集合</summary>
[Collection(TestCollections.SchemaQuery)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class ExtendedSchemaTests
{
    private static String _ConnStr = DALTests.GetConnStr();

    private static MySqlConnection OpenConnection()
    {
        var conn = new MySqlConnection(_ConnStr);
        conn.Open();
        return conn;
    }

    #region MetaDataCollections 扩展验证
    [Fact(DisplayName = "GetSchema_MetaDataCollections包含Procedures")]
    public void MetaDataCollections_ContainsProcedures()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("MetaDataCollections");
        var names = dt.Rows.Cast<DataRow>().Select(r => r["CollectionName"]?.ToString()).ToArray();

        Assert.Contains("Procedures", names);
    }

    [Fact(DisplayName = "GetSchema_MetaDataCollections包含Views")]
    public void MetaDataCollections_ContainsViews()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("MetaDataCollections");
        var names = dt.Rows.Cast<DataRow>().Select(r => r["CollectionName"]?.ToString()).ToArray();

        Assert.Contains("Views", names);
    }

    [Fact(DisplayName = "GetSchema_MetaDataCollections包含Triggers")]
    public void MetaDataCollections_ContainsTriggers()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("MetaDataCollections");
        var names = dt.Rows.Cast<DataRow>().Select(r => r["CollectionName"]?.ToString()).ToArray();

        Assert.Contains("Triggers", names);
    }

    [Fact(DisplayName = "GetSchema_MetaDataCollections包含ForeignKeys")]
    public void MetaDataCollections_ContainsForeignKeys()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("MetaDataCollections");
        var names = dt.Rows.Cast<DataRow>().Select(r => r["CollectionName"]?.ToString()).ToArray();

        Assert.Contains("ForeignKeys", names);
    }
    #endregion

    #region Restrictions 扩展验证
    [Fact(DisplayName = "GetSchema_Restrictions包含Procedures限制")]
    public void Restrictions_ContainsProcedures()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Restrictions");
        var rows = dt.Rows.Cast<DataRow>().Where(r => r["CollectionName"]?.ToString() == "Procedures").ToList();

        Assert.NotEmpty(rows);
        Assert.Equal(4, rows.Count); // Database, Schema, Name, Type
    }

    [Fact(DisplayName = "GetSchema_Restrictions包含Views限制")]
    public void Restrictions_ContainsViews()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Restrictions");
        var rows = dt.Rows.Cast<DataRow>().Where(r => r["CollectionName"]?.ToString() == "Views").ToList();

        Assert.NotEmpty(rows);
        Assert.Equal(3, rows.Count); // Database, Schema, Table
    }

    [Fact(DisplayName = "GetSchema_Restrictions包含Triggers限制")]
    public void Restrictions_ContainsTriggers()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Restrictions");
        var rows = dt.Rows.Cast<DataRow>().Where(r => r["CollectionName"]?.ToString() == "Triggers").ToList();

        Assert.NotEmpty(rows);
        Assert.Equal(4, rows.Count); // Database, Schema, Table, Name
    }

    [Fact(DisplayName = "GetSchema_Restrictions包含ForeignKeys限制")]
    public void Restrictions_ContainsForeignKeys()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Restrictions");
        var rows = dt.Rows.Cast<DataRow>().Where(r => r["CollectionName"]?.ToString() == "ForeignKeys").ToList();

        Assert.NotEmpty(rows);
        Assert.Equal(4, rows.Count); // Database, Schema, Table, Name
    }
    #endregion

    #region Procedures
    [Fact(DisplayName = "GetSchema_Procedures返回正确结构")]
    public void GetSchema_Procedures_ReturnsCorrectStructure()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Procedures");

        Assert.Equal("Procedures", dt.TableName);
        Assert.True(dt.Columns.Contains("SPECIFIC_NAME"));
        Assert.True(dt.Columns.Contains("ROUTINE_NAME"));
        Assert.True(dt.Columns.Contains("ROUTINE_TYPE"));
        Assert.True(dt.Columns.Contains("ROUTINE_DEFINITION"));
        Assert.True(dt.Columns.Contains("CREATED"));
        Assert.True(dt.Columns.Contains("ROUTINE_COMMENT"));
    }

    [Fact(DisplayName = "GetSchema_Procedures不抛异常")]
    public void GetSchema_Procedures_NoException()
    {
        using var conn = OpenConnection();

        // 不抛异常即为通过
        var dt = conn.GetSchema("Procedures");
        Assert.NotNull(dt);
        Assert.Equal("Procedures", dt.TableName);
    }

    [Fact(DisplayName = "GetSchema_Procedures支持数据库限制")]
    public void GetSchema_Procedures_WithDatabaseRestriction()
    {
        using var conn = OpenConnection();

        var restrictions = new String[] { "sys" };
        var dt = conn.GetSchema("Procedures", restrictions);

        Assert.NotNull(dt);
        Assert.Equal("Procedures", dt.TableName);
    }

    [Fact(DisplayName = "GetSchema_Procedures支持类型限制PROCEDURE")]
    public void GetSchema_Procedures_WithTypeRestrictionProcedure()
    {
        using var conn = OpenConnection();

        var restrictions = new String[] { null, null, null, "PROCEDURE" };
        var dt = conn.GetSchema("Procedures", restrictions);

        Assert.NotNull(dt);
        // 所有返回的行都应该是 PROCEDURE 类型
        foreach (DataRow row in dt.Rows)
        {
            Assert.Equal("PROCEDURE", row["ROUTINE_TYPE"]?.ToString());
        }
    }
    #endregion

    #region Views
    [Fact(DisplayName = "GetSchema_Views返回正确结构")]
    public void GetSchema_Views_ReturnsCorrectStructure()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Views");

        Assert.Equal("Views", dt.TableName);
        Assert.True(dt.Columns.Contains("TABLE_CATALOG"));
        Assert.True(dt.Columns.Contains("TABLE_SCHEMA"));
        Assert.True(dt.Columns.Contains("TABLE_NAME"));
        Assert.True(dt.Columns.Contains("VIEW_DEFINITION"));
    }

    [Fact(DisplayName = "GetSchema_Views不抛异常")]
    public void GetSchema_Views_NoException()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Views");
        Assert.NotNull(dt);
        Assert.Equal("Views", dt.TableName);
    }

    [Fact(DisplayName = "GetSchema_Views支持数据库限制")]
    public void GetSchema_Views_WithDatabaseRestriction()
    {
        using var conn = OpenConnection();

        var restrictions = new String[] { "sys" };
        var dt = conn.GetSchema("Views", restrictions);

        Assert.NotNull(dt);
    }
    #endregion

    #region Triggers
    [Fact(DisplayName = "GetSchema_Triggers返回正确结构")]
    public void GetSchema_Triggers_ReturnsCorrectStructure()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Triggers");

        Assert.Equal("Triggers", dt.TableName);
        Assert.True(dt.Columns.Contains("TRIGGER_CATALOG"));
        Assert.True(dt.Columns.Contains("TRIGGER_SCHEMA"));
        Assert.True(dt.Columns.Contains("TRIGGER_NAME"));
        Assert.True(dt.Columns.Contains("EVENT_MANIPULATION"));
        Assert.True(dt.Columns.Contains("EVENT_OBJECT_TABLE"));
        Assert.True(dt.Columns.Contains("ACTION_TIMING"));
        Assert.True(dt.Columns.Contains("ACTION_STATEMENT"));
    }

    [Fact(DisplayName = "GetSchema_Triggers不抛异常")]
    public void GetSchema_Triggers_NoException()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("Triggers");
        Assert.NotNull(dt);
        Assert.Equal("Triggers", dt.TableName);
    }
    #endregion

    #region ForeignKeys
    [Fact(DisplayName = "GetSchema_ForeignKeys返回正确结构")]
    public void GetSchema_ForeignKeys_ReturnsCorrectStructure()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("ForeignKeys");

        Assert.Equal("ForeignKeys", dt.TableName);
        Assert.True(dt.Columns.Contains("CONSTRAINT_CATALOG"));
        Assert.True(dt.Columns.Contains("CONSTRAINT_SCHEMA"));
        Assert.True(dt.Columns.Contains("CONSTRAINT_NAME"));
        Assert.True(dt.Columns.Contains("TABLE_NAME"));
        Assert.True(dt.Columns.Contains("COLUMN_NAME"));
        Assert.True(dt.Columns.Contains("REFERENCED_TABLE_NAME"));
        Assert.True(dt.Columns.Contains("REFERENCED_COLUMN_NAME"));
    }

    [Fact(DisplayName = "GetSchema_ForeignKeys不抛异常")]
    public void GetSchema_ForeignKeys_NoException()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("ForeignKeys");
        Assert.NotNull(dt);
        Assert.Equal("ForeignKeys", dt.TableName);
    }

    [Fact(DisplayName = "GetSchema_ForeignKeys支持数据库限制")]
    public void GetSchema_ForeignKeys_WithDatabaseRestriction()
    {
        using var conn = OpenConnection();

        var restrictions = new String[] { "sys" };
        var dt = conn.GetSchema("ForeignKeys", restrictions);

        Assert.NotNull(dt);
    }

    [Fact(DisplayName = "GetSchema_FOREIGN_KEY_COLUMNS别名也正常")]
    public void GetSchema_ForeignKeyColumns_Alias()
    {
        using var conn = OpenConnection();

        var dt = conn.GetSchema("FOREIGN_KEY_COLUMNS");

        Assert.NotNull(dt);
        Assert.Equal("ForeignKeys", dt.TableName);
    }
    #endregion
}
