using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace NewLife.MySql;

internal class SchemaProvider(MySqlConnection connection)
{
    public static String MetaCollection = "MetaDataCollections";

    public SchemaCollection GetSchema(String? collection, String[]? restrictions)
    {
        collection = collection?.ToUpper(CultureInfo.InvariantCulture);
        switch (collection)
        {
            case "METADATACOLLECTIONS":
                return GetCollections();
            case "DATASOURCEINFORMATION":
                return GetDataSourceInformation();
            case "RESTRICTIONS":
                return GetRestrictions();
            case "DATABASES":
                return GetDatabases(restrictions);
            default:
                restrictions ??= new String[2];
                var db = connection?.Database;
                if (db != null && db.Length > 0 && restrictions.Length > 1 && restrictions[1] == null)
                    restrictions[1] = db;

                return collection switch
                {
                    "TABLES" => GetTables(restrictions),
                    "COLUMNS" => GetColumns(restrictions),
                    "INDEXES" => GetIndexes(restrictions),
                    "INDEXCOLUMNS" => GetIndexColumns(restrictions),
                    _ => GetCollections(),
                };
        }
    }

    protected virtual SchemaCollection GetCollections()
    {
        var data = new Object[][]
        {
            ["MetaDataCollections", 0, 0],
            ["DataSourceInformation", 0, 0],
            ["Restrictions", 0, 0],
            ["Databases", 1, 1],
            ["Tables", 4, 2],
            ["Columns", 4, 4],
            ["IndexColumns", 5, 4],
            ["Indexes", 4, 3],
        };
        var collection = new SchemaCollection("MetaDataCollections");
        collection.AddColumn("CollectionName", typeof(String));
        collection.AddColumn("NumberOfRestrictions", typeof(Int32));
        collection.AddColumn("NumberOfIdentifierParts", typeof(Int32));
        FillTable(collection, data);
        return collection;
    }

    private SchemaCollection GetDataSourceInformation()
    {
        var collection = new SchemaCollection("DataSourceInformation");
        collection.AddColumn("CompositeIdentifierSeparatorPattern", typeof(String));
        collection.AddColumn("DataSourceProductName", typeof(String));
        collection.AddColumn("DataSourceProductVersion", typeof(String));
        collection.AddColumn("DataSourceProductVersionNormalized", typeof(String));
        collection.AddColumn("GroupByBehavior", typeof(GroupByBehavior));
        collection.AddColumn("IdentifierPattern", typeof(String));
        collection.AddColumn("IdentifierCase", typeof(IdentifierCase));
        collection.AddColumn("OrderByColumnsInSelect", typeof(Boolean));
        collection.AddColumn("ParameterMarkerFormat", typeof(String));
        collection.AddColumn("ParameterMarkerPattern", typeof(String));
        collection.AddColumn("ParameterNameMaxLength", typeof(Int32));
        collection.AddColumn("ParameterNamePattern", typeof(String));
        collection.AddColumn("QuotedIdentifierPattern", typeof(String));
        collection.AddColumn("QuotedIdentifierCase", typeof(IdentifierCase));
        collection.AddColumn("StatementSeparatorPattern", typeof(String));
        collection.AddColumn("StringLiteralPattern", typeof(String));
        collection.AddColumn("SupportedJoinOperators", typeof(SupportedJoinOperators));

        var version = Assembly.GetExecutingAssembly().GetName().Version;
        var row = collection.AddRow();
        row["CompositeIdentifierSeparatorPattern"] = "\\.";
        row["DataSourceProductName"] = "MySQL";
        row["DataSourceProductVersion"] = connection.ServerVersion;
        row["DataSourceProductVersionNormalized"] = version + "";
        row["GroupByBehavior"] = GroupByBehavior.Unrelated;
        row["IdentifierPattern"] = "(^\\`\\p{Lo}\\p{Lu}\\p{Ll}_@#][\\p{Lo}\\p{Lu}\\p{Ll}\\p{Nd}@$#_]*$)|(^\\`[^\\`\\0]|\\`\\`+\\`$)|(^\\\" + [^\\\"\\0]|\\\"\\\"+\\\"$)";
        row["IdentifierCase"] = IdentifierCase.Insensitive;
        row["OrderByColumnsInSelect"] = false;
        row["ParameterMarkerFormat"] = "{0}";
        row["ParameterMarkerPattern"] = "(@[A-Za-z0-9_$#]*)";
        row["ParameterNameMaxLength"] = 128;
        row["ParameterNamePattern"] = "^[\\p{Lo}\\p{Lu}\\p{Ll}\\p{Lm}_@#][\\p{Lo}\\p{Lu}\\p{Ll}\\p{Lm}\\p{Nd}\\uff3f_@#\\$]*(?=\\s+|$)";
        row["QuotedIdentifierPattern"] = "(([^\\`]|\\`\\`)*)";
        row["QuotedIdentifierCase"] = IdentifierCase.Sensitive;
        row["StatementSeparatorPattern"] = ";";
        row["StringLiteralPattern"] = "'(([^']|'')*)'";
        row["SupportedJoinOperators"] = 15;

        return collection;
    }

    protected virtual SchemaCollection GetRestrictions()
    {
        var data = new Object[][]
        {
            ["Databases", "Name", "", 0],
            ["Tables", "Database", "", 0],
            ["Tables", "Schema", "", 1],
            ["Tables", "Table", "", 2],
            ["Tables", "TableType", "", 3],
            ["Columns", "Database", "", 0],
            ["Columns", "Schema", "", 1],
            ["Columns", "Table", "", 2],
            ["Columns", "Column", "", 3],
            ["Indexes", "Database", "", 0],
            ["Indexes", "Schema", "", 1],
            ["Indexes", "Table", "", 2],
            ["Indexes", "Name", "", 3],
            ["IndexColumns", "Database", "", 0],
            ["IndexColumns", "Schema", "", 1],
            ["IndexColumns", "Table", "", 2],
            ["IndexColumns", "ConstraintName", "", 3],
            ["IndexColumns", "Column", "", 4]
        };
        var collection = new SchemaCollection("Restrictions");
        collection.AddColumn("CollectionName", typeof(String));
        collection.AddColumn("RestrictionName", typeof(String));
        collection.AddColumn("RestrictionDefault", typeof(String));
        collection.AddColumn("RestrictionNumber", typeof(Int32));
        FillTable(collection, data);
        return collection;
    }

    public virtual SchemaCollection GetDatabases(String[]? restrictions)
    {
        Regex? regex = null;
        var caseSetting = 0;
        var vs = connection.Client?.Variables;
        if (vs != null && vs.TryGetValue("lower_case_table_names", out var value))
            caseSetting = value.ToInt();
        var sql = "SHOW DATABASES";
        if (caseSetting == 0 && restrictions != null && restrictions.Length >= 1)
            sql = sql + " LIKE '" + restrictions[0] + "'";

        var obj = QueryCollection("Databases", sql);
        if (caseSetting != 0 && restrictions != null && restrictions.Length >= 1 && restrictions[0] != null)
            regex = new Regex(restrictions[0], RegexOptions.IgnoreCase);

        var collection = new SchemaCollection("Databases");
        collection.AddColumn("CATALOG_NAME", typeof(String));
        collection.AddColumn("SCHEMA_NAME", typeof(String));
        foreach (var row in obj.Rows)
        {
            if (regex == null || regex.Match(row[0].ToString()).Success)
            {
                collection.AddRow()[1] = row[0];
            }
        }
        return collection;
    }

    public virtual SchemaCollection GetTables(String[] restrictions)
    {
        var collection = new SchemaCollection("Tables");
        collection.AddColumn("TABLE_CATALOG", typeof(String));
        collection.AddColumn("TABLE_SCHEMA", typeof(String));
        collection.AddColumn("TABLE_NAME", typeof(String));
        collection.AddColumn("TABLE_TYPE", typeof(String));
        collection.AddColumn("ENGINE", typeof(String));
        collection.AddColumn("VERSION", typeof(UInt64));
        collection.AddColumn("ROW_FORMAT", typeof(String));
        collection.AddColumn("TABLE_ROWS", typeof(UInt64));
        collection.AddColumn("AVG_ROW_LENGTH", typeof(UInt64));
        collection.AddColumn("DATA_LENGTH", typeof(UInt64));
        collection.AddColumn("MAX_DATA_LENGTH", typeof(UInt64));
        collection.AddColumn("INDEX_LENGTH", typeof(UInt64));
        collection.AddColumn("DATA_FREE", typeof(UInt64));
        collection.AddColumn("AUTO_INCREMENT", typeof(UInt64));
        collection.AddColumn("CREATE_TIME", typeof(DateTime));
        collection.AddColumn("UPDATE_TIME", typeof(DateTime));
        collection.AddColumn("CHECK_TIME", typeof(DateTime));
        collection.AddColumn("TABLE_COLLATION", typeof(String));
        collection.AddColumn("CHECKSUM", typeof(UInt64));
        collection.AddColumn("CREATE_OPTIONS", typeof(String));
        collection.AddColumn("TABLE_COMMENT", typeof(String));

        var dbRestriction = new String[4];
        if (restrictions != null && restrictions.Length >= 2)
        {
            dbRestriction[0] = restrictions[1];
        }
        var dbs = GetDatabases(dbRestriction);
        if (restrictions != null)
        {
            Array.Copy(restrictions, dbRestriction, Math.Min(dbRestriction.Length, restrictions.Length));
        }
        foreach (var row in dbs.Rows)
        {
            dbRestriction[1] = row["SCHEMA_NAME"].ToString();
            FindTables(collection, dbRestriction);
        }
        return collection;
    }

    public virtual SchemaCollection GetColumns(String[] restrictions)
    {
        var collection = new SchemaCollection("Columns");
        collection.AddColumn("TABLE_CATALOG", typeof(String));
        collection.AddColumn("TABLE_SCHEMA", typeof(String));
        collection.AddColumn("TABLE_NAME", typeof(String));
        collection.AddColumn("COLUMN_NAME", typeof(String));
        collection.AddColumn("ORDINAL_POSITION", typeof(UInt64));
        collection.AddColumn("COLUMN_DEFAULT", typeof(String));
        collection.AddColumn("IS_NULLABLE", typeof(String));
        collection.AddColumn("DATA_TYPE", typeof(String));
        collection.AddColumn("CHARACTER_MAXIMUM_LENGTH", typeof(UInt64));
        collection.AddColumn("CHARACTER_OCTET_LENGTH", typeof(UInt64));
        collection.AddColumn("NUMERIC_PRECISION", typeof(UInt64));
        collection.AddColumn("NUMERIC_SCALE", typeof(UInt64));
        collection.AddColumn("CHARACTER_SET_NAME", typeof(String));
        collection.AddColumn("COLLATION_NAME", typeof(String));
        collection.AddColumn("COLUMN_TYPE", typeof(String));
        collection.AddColumn("COLUMN_KEY", typeof(String));
        collection.AddColumn("EXTRA", typeof(String));
        collection.AddColumn("PRIVILEGES", typeof(String));
        collection.AddColumn("COLUMN_COMMENT", typeof(String));
        collection.AddColumn("GENERATION_EXPRESSION", typeof(String));
        String columnName = null;
        if (restrictions != null && restrictions.Length == 4)
        {
            columnName = restrictions[3];
            restrictions[3] = null;
        }
        foreach (var row in GetTables(restrictions).Rows)
        {
            LoadTableColumns(collection, row["TABLE_SCHEMA"].ToString(), row["TABLE_NAME"].ToString(), columnName);
        }
        QuoteDefaultValues(collection);
        return collection;
    }

    private void LoadTableColumns(SchemaCollection schemaCollection, String schema, String tableName, String columnRestriction)
    {
        var sql = $"SHOW FULL COLUMNS FROM `{schema}`.`{tableName}`";
        using var cmd = new MySqlCommand(connection, sql);
        var pos = 1;
        using var reader = cmd.ExecuteReader(CommandBehavior.Default);
        while (reader.Read())
        {
            var @string = reader.GetString(0);
            if (columnRestriction == null || !(@string != columnRestriction))
            {
                var row = schemaCollection.AddRow();
                row["TABLE_CATALOG"] = DBNull.Value;
                row["TABLE_SCHEMA"] = schema;
                row["TABLE_NAME"] = tableName;
                row["COLUMN_NAME"] = @string;
                row["ORDINAL_POSITION"] = pos++;
                row["COLUMN_DEFAULT"] = reader.GetValue(5);
                row["IS_NULLABLE"] = reader.GetString(3);
                row["DATA_TYPE"] = reader.GetString(1);
                row["CHARACTER_MAXIMUM_LENGTH"] = DBNull.Value;
                row["CHARACTER_OCTET_LENGTH"] = DBNull.Value;
                row["NUMERIC_PRECISION"] = DBNull.Value;
                row["NUMERIC_SCALE"] = DBNull.Value;
                row["CHARACTER_SET_NAME"] = reader.GetValue(2);
                row["COLLATION_NAME"] = row["CHARACTER_SET_NAME"];
                row["COLUMN_TYPE"] = reader.GetString(1);
                row["COLUMN_KEY"] = reader.GetString(4);
                row["EXTRA"] = reader.GetString(6);
                row["PRIVILEGES"] = reader.GetString(7);
                row["COLUMN_COMMENT"] = reader.GetString(8);
                row["GENERATION_EXPRESSION"] = reader.GetString(6).Contains("VIRTUAL") ? reader.GetString(9) : String.Empty;
                ParseColumnRow(row);
            }
        }
    }

    protected void QuoteDefaultValues(SchemaCollection schemaCollection)
    {
        if (schemaCollection == null || !schemaCollection.ContainsColumn("COLUMN_DEFAULT")) return;

        foreach (var row in schemaCollection.Rows)
        {
            var arg = row["COLUMN_DEFAULT"];
            if (IsTextType(row["DATA_TYPE"].ToString()))
            {
                row["COLUMN_DEFAULT"] = $"{arg}";
            }
        }
    }

    static Boolean IsTextType(String typename)
    {
        return typename.ToLower(CultureInfo.InvariantCulture) switch
        {
            "char" or "enum" or "text" or "longtext" or "nvarchar" or "tinytext" or "varchar" or "mediumtext" or "nchar" or "set" => true,
            _ => false,
        };
    }

    private static void ParseColumnRow(SchemaRow row)
    {
        var text = row["CHARACTER_SET_NAME"].ToString();
        var num = text.IndexOf('_');
        if (num != -1)
        {
            row["CHARACTER_SET_NAME"] = text.Substring(0, num);
        }
        var text2 = row["DATA_TYPE"].ToString();
        num = text2.IndexOf('(');
        if (num == -1)
        {
            return;
        }
        row["DATA_TYPE"] = text2.Substring(0, num);
        var num2 = text2.IndexOf(')', num);
        var text3 = text2.Substring(num + 1, num2 - (num + 1));
        switch (row["DATA_TYPE"].ToString().ToLower())
        {
            case "char":
            case "varchar":
                row["CHARACTER_MAXIMUM_LENGTH"] = text3;
                break;
            case "real":
            case "decimal":
                {
                    var array = text3.Split([',']);
                    row["NUMERIC_PRECISION"] = array[0];
                    if (array.Length == 2)
                    {
                        row["NUMERIC_SCALE"] = array[1];
                    }
                    break;
                }
        }
    }

    public virtual SchemaCollection GetIndexes(String[] restrictions)
    {
        var collection = new SchemaCollection("Indexes");
        collection.AddColumn("INDEX_CATALOG", typeof(String));
        collection.AddColumn("INDEX_SCHEMA", typeof(String));
        collection.AddColumn("INDEX_NAME", typeof(String));
        collection.AddColumn("TABLE_NAME", typeof(String));
        collection.AddColumn("UNIQUE", typeof(Boolean));
        collection.AddColumn("PRIMARY", typeof(Boolean));
        collection.AddColumn("TYPE", typeof(String));
        collection.AddColumn("COMMENT", typeof(String));

        var ver = new Version(connection.ServerVersion);
        var v801 = new Version(8, 0, 1);
        var array = new String[Math.Max((restrictions != null) ? restrictions.Length : 4, 4)];
        restrictions?.CopyTo(array, 0);
        array[3] = "BASE TABLE";
        foreach (var table in GetTables(array).Rows)
        {
            var sql = String.Format("SHOW INDEX FROM `{0}`.`{1}`", table["TABLE_SCHEMA"], table["TABLE_NAME"]);
            foreach (var row in QueryCollection("indexes", sql).Rows)
            {
                if (row["SEQ_IN_INDEX"].ToInt() == 1 && (restrictions == null || restrictions.Length != 4 || restrictions[3] == null || row["KEY_NAME"].Equals(restrictions[3])))
                {
                    var row2 = collection.AddRow();
                    row2["INDEX_CATALOG"] = null!;
                    row2["INDEX_SCHEMA"] = table["TABLE_SCHEMA"];
                    row2["INDEX_NAME"] = row["KEY_NAME"];
                    row2["TABLE_NAME"] = row["TABLE"];
                    row2["UNIQUE"] = row["NON_UNIQUE"].ToInt() == 0;
                    row2["PRIMARY"] = row["KEY_NAME"].Equals("PRIMARY");
                    row2["TYPE"] = row["INDEX_TYPE"];
                    row2["COMMENT"] = row["COMMENT"];
                }
            }
        }
        return collection;
    }

    public virtual SchemaCollection GetIndexColumns(String[] restrictions)
    {
        var dt = new SchemaCollection("IndexColumns");
        dt.AddColumn("INDEX_CATALOG", typeof(String));
        dt.AddColumn("INDEX_SCHEMA", typeof(String));
        dt.AddColumn("INDEX_NAME", typeof(String));
        dt.AddColumn("TABLE_NAME", typeof(String));
        dt.AddColumn("COLUMN_NAME", typeof(String));
        dt.AddColumn("ORDINAL_POSITION", typeof(Int32));
        dt.AddColumn("SORT_ORDER", typeof(String));
        var array = new String[Math.Max((restrictions == null) ? 4 : restrictions.Length, 4)];
        restrictions?.CopyTo(array, 0);
        array[3] = "BASE TABLE";
        foreach (var table in GetTables(array).Rows)
        {
            var sql = String.Format("SHOW INDEX FROM `{0}`.`{1}`", table["TABLE_SCHEMA"], table["TABLE_NAME"]);
            using var cmd = new MySqlCommand(connection, sql);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var @string = GetString(reader, reader.GetOrdinal("KEY_NAME"));
                var string2 = GetString(reader, reader.GetOrdinal("COLUMN_NAME"));
                if (restrictions == null || ((restrictions.Length < 4 || restrictions[3] == null || !(@string != restrictions[3])) && (restrictions.Length < 5 || restrictions[4] == null || !(string2 != restrictions[4]))))
                {
                    var mySqlSchemaRow = dt.AddRow();
                    mySqlSchemaRow["INDEX_CATALOG"] = null;
                    mySqlSchemaRow["INDEX_SCHEMA"] = table["TABLE_SCHEMA"];
                    mySqlSchemaRow["INDEX_NAME"] = @string;
                    mySqlSchemaRow["TABLE_NAME"] = table["TABLE_NAME"];
                    mySqlSchemaRow["COLUMN_NAME"] = string2;
                    mySqlSchemaRow["ORDINAL_POSITION"] = reader.GetValue(reader.GetOrdinal("SEQ_IN_INDEX"));
                    mySqlSchemaRow["SORT_ORDER"] = reader.GetValue(reader.GetOrdinal("COLLATION"));
                }
            }
        }
        return dt;
    }

    protected static void FillTable(SchemaCollection dt, Object[][] data)
    {
        foreach (var array in data)
        {
            var mySqlSchemaRow = dt.AddRow();
            for (var j = 0; j < array.Length; j++)
            {
                mySqlSchemaRow[j] = array[j];
            }
        }
    }

    private void FindTables(SchemaCollection schema, String[] restrictions)
    {
        var stringBuilder = new StringBuilder();
        var stringBuilder2 = new StringBuilder();
        stringBuilder.AppendFormat(CultureInfo.InvariantCulture, "SHOW TABLE STATUS FROM `{0}`", restrictions[1]);
        if (restrictions != null && restrictions.Length >= 3 && restrictions[2] != null)
        {
            stringBuilder2.AppendFormat(CultureInfo.InvariantCulture, " LIKE '{0}'", restrictions[2]);
        }
        stringBuilder.Append(stringBuilder2.ToString());
        var table_type = (restrictions[1].ToLower() == "information_schema") ? "SYSTEM VIEW" : "BASE TABLE";
        using var cmd = new MySqlCommand(connection, stringBuilder.ToString());
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var mySqlSchemaRow = schema.AddRow();
            mySqlSchemaRow["TABLE_CATALOG"] = null;
            mySqlSchemaRow["TABLE_SCHEMA"] = restrictions[1];
            mySqlSchemaRow["TABLE_NAME"] = reader.GetString(0);
            mySqlSchemaRow["TABLE_TYPE"] = table_type;
            mySqlSchemaRow["ENGINE"] = GetString(reader, 1);
            mySqlSchemaRow["VERSION"] = reader.GetValue(2);
            mySqlSchemaRow["ROW_FORMAT"] = GetString(reader, 3);
            mySqlSchemaRow["TABLE_ROWS"] = reader.GetValue(4);
            mySqlSchemaRow["AVG_ROW_LENGTH"] = reader.GetValue(5);
            mySqlSchemaRow["DATA_LENGTH"] = reader.GetValue(6);
            mySqlSchemaRow["MAX_DATA_LENGTH"] = reader.GetValue(7);
            mySqlSchemaRow["INDEX_LENGTH"] = reader.GetValue(8);
            mySqlSchemaRow["DATA_FREE"] = reader.GetValue(9);
            mySqlSchemaRow["AUTO_INCREMENT"] = reader.GetValue(10);
            mySqlSchemaRow["CREATE_TIME"] = reader.GetValue(11);
            mySqlSchemaRow["UPDATE_TIME"] = reader.GetValue(12);
            mySqlSchemaRow["CHECK_TIME"] = reader.GetValue(13);
            mySqlSchemaRow["TABLE_COLLATION"] = GetString(reader, 14);
            mySqlSchemaRow["CHECKSUM"] = reader.GetValue(15);
            mySqlSchemaRow["CREATE_OPTIONS"] = GetString(reader, 16);
            mySqlSchemaRow["TABLE_COMMENT"] = GetString(reader, 17);
        }
    }

    private static String GetString(DbDataReader reader, Int32 index)
    {
        if (reader.IsDBNull(index)) return null!;

        return reader.GetString(index);
    }

    protected SchemaCollection QueryCollection(String name, String sql)
    {
        var c = new SchemaCollection(name);
        using var cmd = new MySqlCommand(connection, sql);
        using var reader = cmd.ExecuteReader();
        for (var i = 0; i < reader.FieldCount; i++)
        {
            c.AddColumn(reader.GetName(i), reader.GetFieldType(i));
        }
        while (reader.Read())
        {
            var mySqlSchemaRow = c.AddRow();
            for (var j = 0; j < reader.FieldCount; j++)
            {
                mySqlSchemaRow[j] = reader.GetValue(j);
            }
        }

        return c;
    }
}

class SchemaCollection
{
    private readonly List<SchemaColumn> _columns = [];

    private readonly List<SchemaRow> _rows = [];

    private readonly DataTable _table;

    internal Dictionary<String, Int32> Mapping;

    internal Dictionary<Int32, Int32> LogicalMappings;

    public String Name { get; set; }

    public IList<SchemaColumn> Columns => _columns;

    public IList<SchemaRow> Rows => _rows;

    public SchemaCollection()
    {
        Mapping = new Dictionary<String, Int32>(StringComparer.OrdinalIgnoreCase);
        LogicalMappings = [];
    }

    public SchemaCollection(String name) : this() => Name = name;

    internal SchemaColumn AddColumn(String name, Type t)
    {
        var schemaColumn = new SchemaColumn
        {
            Name = name,
            Type = t
        };
        _columns.Add(schemaColumn);
        Mapping.Add(name, _columns.Count - 1);
        LogicalMappings[_columns.Count - 1] = _columns.Count - 1;
        return schemaColumn;
    }

    internal Int32 ColumnIndex(String name)
    {
        var result = -1;
        for (var i = 0; i < _columns.Count; i++)
        {
            if (String.Compare(_columns[i].Name, name, StringComparison.OrdinalIgnoreCase) == 0)
            {
                result = i;
                break;
            }
        }
        return result;
    }

    internal Boolean ContainsColumn(String name) => ColumnIndex(name) >= 0;

    internal SchemaRow AddRow()
    {
        var mySqlSchemaRow = new SchemaRow(this);
        _rows.Add(mySqlSchemaRow);
        return mySqlSchemaRow;
    }

    internal DataTable AsDataTable()
    {
        if (_table != null) return _table;

        var dataTable = new DataTable(Name);
        foreach (var column in Columns)
        {
            dataTable.Columns.Add(column.Name, column.Type);
        }
        foreach (var row in Rows)
        {
            var dataRow = dataTable.NewRow();
            for (var i = 0; i < dataTable.Columns.Count; i++)
            {
                dataRow[i] = row[i] ?? DBNull.Value;
            }
            dataTable.Rows.Add(dataRow);
        }
        return dataTable;
    }
}

class SchemaRow
{
    private Dictionary<Int32, Object> _data;

    internal SchemaCollection Collection { get; }

    internal Object this[String s]
    {
        get
        {
            return GetValueForName(s);
        }
        set
        {
            SetValueForName(s, value);
        }
    }

    internal Object this[Int32 i]
    {
        get
        {
            var key = Collection.LogicalMappings[i];
            if (!_data.ContainsKey(key))
            {
                _data[key] = null;
            }
            return _data[key];
        }
        set
        {
            _data[Collection.LogicalMappings[i]] = value;
        }
    }

    public SchemaRow(SchemaCollection c)
    {
        Collection = c;
        InitMetadata();
    }

    internal void InitMetadata() => _data = [];

    private void SetValueForName(String colName, Object value)
    {
        var i = Collection.Mapping[colName];
        this[i] = value;
    }

    private Object GetValueForName(String colName)
    {
        var num = Collection.Mapping[colName];
        if (!_data.ContainsKey(num))
        {
            _data[num] = null;
        }
        return this[num];
    }
}

class SchemaColumn
{
    public String Name { get; set; }

    public Type Type { get; set; }
}