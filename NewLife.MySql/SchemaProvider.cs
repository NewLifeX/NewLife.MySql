using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace NewLife.MySql;

internal class SchemaProvider(MySqlConnection connection)
{
    public static String MetaCollection = "MetaDataCollections";

    public SchemaCollection GetSchema(String? collection, String?[]? restrictions)
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
            case "DATATYPES":
                return GetDataTypes();
            case "RESERVEDWORDS":
                return GetReservedWords();
            case "USERS":
                return GetUsers(restrictions);
            case "DATABASES":
                return GetDatabases(restrictions);
            case "PROCEDURES":
                return GetProcedures(restrictions);
            case "VIEWS":
                return GetViews(restrictions);
            case "TRIGGERS":
                return GetTriggers(restrictions);
            case "FOREIGNKEYS":
            case "FOREIGN_KEY_COLUMNS":
                return GetForeignKeys(restrictions);
            default:
                restrictions ??= new String[2];
                var db = connection.Database;
                if (!db.IsNullOrEmpty() && restrictions.Length > 1 && restrictions[1] == null)
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
            ["DataTypes", 0, 0],
            ["ReservedWords", 0, 0],
            ["Users", 1, 1],
            ["Databases", 1, 1],
            ["Tables", 4, 2],
            ["Columns", 4, 4],
            ["Indexes", 4, 3],
            ["IndexColumns", 5, 4],
            ["Procedures", 4, 2],
            ["Views", 3, 2],
            ["Triggers", 4, 2],
            ["ForeignKeys", 4, 3],
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
        row["DataSourceProductName"] = connection.DatabaseType + "";
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
            ["Users", "Name", "", 0],
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
            ["IndexColumns", "Column", "", 4],
            ["Procedures", "Database", "", 0],
            ["Procedures", "Schema", "", 1],
            ["Procedures", "Name", "", 2],
            ["Procedures", "Type", "", 3],
            ["Views", "Database", "", 0],
            ["Views", "Schema", "", 1],
            ["Views", "Table", "", 2],
            ["Triggers", "Database", "", 0],
            ["Triggers", "Schema", "", 1],
            ["Triggers", "Table", "", 2],
            ["Triggers", "Name", "", 3],
            ["ForeignKeys", "Database", "", 0],
            ["ForeignKeys", "Schema", "", 1],
            ["ForeignKeys", "Table", "", 2],
            ["ForeignKeys", "Name", "", 3],
        };
        var collection = new SchemaCollection("Restrictions");
        collection.AddColumn("CollectionName", typeof(String));
        collection.AddColumn("RestrictionName", typeof(String));
        collection.AddColumn("RestrictionDefault", typeof(String));
        collection.AddColumn("RestrictionNumber", typeof(Int32));
        FillTable(collection, data);
        return collection;
    }

    /// <summary>添加数据类型行到 SchemaCollection</summary>
    private static void AddDataTypeRow(SchemaCollection collection, String typeName, Int32 providerDbType,
        Int64 columnSize = 0, String? createFormat = null, String? createParameters = null,
        String? dataType = null, Boolean isAutoincrementable = false, Boolean isBestMatch = true,
        Boolean isCaseSensitive = false, Boolean isFixedLength = false, Boolean isFixedPrecisionScale = true,
        Boolean isLong = false, Boolean isNullable = true, Boolean isSearchable = true,
        Boolean isSearchableWithLike = false, Object? isUnsigned = null,
        Int16 maximumScale = 0, Int16 minimumScale = 0, Object? isConcurrencyType = null,
        Boolean isLiteralSupported = false, String? literalPrefix = null,
        Object? literalSuffix = null, Object? nativeDataType = null)
    {
        var row = collection.AddRow();
        row["TypeName"] = typeName;
        row["ProviderDbType"] = providerDbType;
        row["ColumnSize"] = columnSize;
        row["CreateFormat"] = createFormat;
        row["CreateParameters"] = createParameters;
        row["DataType"] = dataType;
        row["IsAutoincrementable"] = isAutoincrementable;
        row["IsBestMatch"] = isBestMatch;
        row["IsCaseSensitive"] = isCaseSensitive;
        row["IsFixedLength"] = isFixedLength;
        row["IsFixedPrecisionScale"] = isFixedPrecisionScale;
        row["IsLong"] = isLong;
        row["IsNullable"] = isNullable;
        row["IsSearchable"] = isSearchable;
        row["IsSearchableWithLike"] = isSearchableWithLike;
        row["IsUnsigned"] = isUnsigned ?? false;
        row["MaximumScale"] = maximumScale;
        row["MinimumScale"] = minimumScale;
        row["IsConcurrencyType"] = isConcurrencyType ?? DBNull.Value;
        row["IsLiteralSupported"] = isLiteralSupported;
        row["LiteralPrefix"] = literalPrefix;
        row["LiteralSuffix"] = literalSuffix ?? DBNull.Value;
        row["NativeDataType"] = nativeDataType ?? DBNull.Value;
    }

    private static SchemaCollection GetDataTypes()
    {
        var collection = new SchemaCollection("DataTypes");
        collection.AddColumn("TypeName", typeof(String));
        collection.AddColumn("ProviderDbType", typeof(Int32));
        collection.AddColumn("ColumnSize", typeof(Int64));
        collection.AddColumn("CreateFormat", typeof(String));
        collection.AddColumn("CreateParameters", typeof(String));
        collection.AddColumn("DataType", typeof(String));
        collection.AddColumn("IsAutoincrementable", typeof(Boolean));
        collection.AddColumn("IsBestMatch", typeof(Boolean));
        collection.AddColumn("IsCaseSensitive", typeof(Boolean));
        collection.AddColumn("IsFixedLength", typeof(Boolean));
        collection.AddColumn("IsFixedPrecisionScale", typeof(Boolean));
        collection.AddColumn("IsLong", typeof(Boolean));
        collection.AddColumn("IsNullable", typeof(Boolean));
        collection.AddColumn("IsSearchable", typeof(Boolean));
        collection.AddColumn("IsSearchableWithLike", typeof(Boolean));
        collection.AddColumn("IsUnsigned", typeof(Boolean));
        collection.AddColumn("MaximumScale", typeof(Int16));
        collection.AddColumn("MinimumScale", typeof(Int16));
        collection.AddColumn("IsConcurrencyType", typeof(Boolean));
        collection.AddColumn("IsLiteralSupported", typeof(Boolean));
        collection.AddColumn("LiteralPrefix", typeof(String));
        collection.AddColumn("LiteralSuffix", typeof(String));
        collection.AddColumn("NativeDataType", typeof(String));

        // BIT
        AddDataTypeRow(collection, "BIT", 16, columnSize: 64, createFormat: "BIT", dataType: "System.UInt64");

        // BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB, BINARY, VARBINARY, VECTOR
        var blobTypes = new[] { "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY", "VECTOR" };
        var blobDbTypes = new[] { 252, 249, 250, 251, 600, 601, 602 };
        var blobSizes = new[] { 65535L, 255L, 16777215L, 4294967295L, 255L, 65535L, 16777215L };
        var blobFormats = new[] { null, null, null, null, "binary({0})", "varbinary({0})", null };
        var blobParams = new[] { null, null, null, null, "length", "length", null };
        for (var i = 0; i < blobTypes.Length; i++)
            AddDataTypeRow(collection, blobTypes[i], blobDbTypes[i], columnSize: blobSizes[i], createFormat: blobFormats[i], createParameters: blobParams[i], dataType: "System.Byte[]", isFixedLength: i >= 4, isLong: blobSizes[i] > 255, literalPrefix: "0x", isConcurrencyType: DBNull.Value, literalSuffix: DBNull.Value);

        // DATE, DATETIME, TIMESTAMP
        AddDataTypeRow(collection, "DATE", 10, createFormat: "DATE", dataType: "System.DateTime", isFixedLength: true);
        AddDataTypeRow(collection, "DATETIME", 12, createFormat: "DATETIME", dataType: "System.DateTime", isFixedLength: true);
        AddDataTypeRow(collection, "TIMESTAMP", 7, createFormat: "TIMESTAMP", dataType: "System.DateTime", isFixedLength: true);

        // TIME
        AddDataTypeRow(collection, "TIME", 13, createFormat: "TIME", dataType: "System.TimeSpan", isFixedLength: true);

        // CHAR, NCHAR, VARCHAR, NVARCHAR, SET, ENUM, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT
        var stringTypes = new[] { "CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "SET", "ENUM", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT" };
        var stringDbTypes = new[] { 254, 254, 15, 15, 248, 247, 249, 252, 250, 251 };
        for (var i = 0; i < stringTypes.Length; i++)
            AddDataTypeRow(collection, stringTypes[i], stringDbTypes[i],
                createFormat: i < 4 ? stringTypes[i] + "({0})" : stringTypes[i],
                createParameters: i < 4 ? "size" : null, dataType: "System.String", isSearchableWithLike: true);

        // DOUBLE
        AddDataTypeRow(collection, "DOUBLE", 5, createFormat: "DOUBLE", dataType: "System.Double", isFixedLength: true);

        // FLOAT
        AddDataTypeRow(collection, "FLOAT", 4, createFormat: "FLOAT", dataType: "System.Single", isFixedLength: true);

        // TINYINT
        AddDataTypeRow(collection, "TINYINT", 1, createFormat: "TINYINT", dataType: "System.Byte", isAutoincrementable: true, isFixedLength: true);

        // SMALLINT
        AddDataTypeRow(collection, "SMALLINT", 2, createFormat: "SMALLINT", dataType: "System.Int16", isAutoincrementable: true, isFixedLength: true);

        // INT, YEAR, MEDIUMINT
        AddDataTypeRow(collection, "INT", 3, createFormat: "INT", dataType: "System.Int32", isAutoincrementable: true, isFixedLength: true);
        AddDataTypeRow(collection, "YEAR", 13, createFormat: "YEAR", dataType: "System.Int32", isAutoincrementable: false, isFixedLength: true);
        AddDataTypeRow(collection, "MEDIUMINT", 9, createFormat: "MEDIUMINT", dataType: "System.Int32", isAutoincrementable: true, isFixedLength: true);

        // BIGINT
        AddDataTypeRow(collection, "BIGINT", 8, createFormat: "BIGINT", dataType: "System.Int64", isAutoincrementable: true, isFixedLength: true);

        // DECIMAL
        AddDataTypeRow(collection, "DECIMAL", 246, createFormat: "DECIMAL({0},{1})", createParameters: "precision,scale", dataType: "System.Decimal", isFixedLength: true);

        // UNSIGNED TINYINT
        AddDataTypeRow(collection, "TINYINT UNSIGNED", 501, createFormat: "TINYINT UNSIGNED", dataType: "System.Byte", isAutoincrementable: true, isFixedLength: true, isUnsigned: true);

        // UNSIGNED SMALLINT
        AddDataTypeRow(collection, "SMALLINT UNSIGNED", 502, createFormat: "SMALLINT UNSIGNED", dataType: "System.UInt16", isAutoincrementable: true, isFixedLength: true, isUnsigned: true);

        // UNSIGNED INT
        AddDataTypeRow(collection, "INT UNSIGNED", 503, createFormat: "INT UNSIGNED", dataType: "System.UInt32", isAutoincrementable: true, isFixedLength: true, isUnsigned: true);

        // UNSIGNED BIGINT
        AddDataTypeRow(collection, "BIGINT UNSIGNED", 504, createFormat: "BIGINT UNSIGNED", dataType: "System.UInt64", isAutoincrementable: true, isFixedLength: true, isUnsigned: true);

        return collection;
    }

    internal static SchemaCollection GetReservedWords()
    {
        var collection = new SchemaCollection("ReservedWords");
        collection.AddColumn(DbMetaDataColumnNames.ReservedWord, typeof(String));

        var text = "NewLife.MySql.ReservedWords.txt";
        using var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(text) ??
            throw new Exception($"Resource {text} not found in {Assembly.GetExecutingAssembly()}.");

        using var streamReader = new StreamReader(stream);
        for (var line = streamReader.ReadLine(); line != null; line = streamReader.ReadLine())
        {
            var array = line.Split([' ']);
            foreach (var value in array)
            {
                if (!value.IsNullOrEmpty())
                {
                    collection.AddRow()[0] = value;
                }
            }
        }
        return collection;
    }

    public SchemaCollection GetUsers(String?[]? restrictions)
    {
        var sql = "SELECT Host, User FROM mysql.user";
        if (restrictions != null && restrictions.Length != 0)
            sql += $" WHERE User LIKE '{restrictions[0]}'";

        var collection = QueryCollection("Users", sql);
        collection.Columns[0].Name = "HOST";
        collection.Columns[1].Name = "USERNAME";

        return collection;
    }

    public virtual SchemaCollection GetDatabases(String?[]? restrictions)
    {
        var vs = connection.Client?.Variables;
        var lowerCase = vs?["lower_case_table_names"]?.ToBoolean() ?? false;

        var sql = "SHOW DATABASES";
        String? name = null;

        // 如果有restrictions，提取数据库名限制
        if (restrictions != null && restrictions.Length >= 1)
        {
            name = restrictions[0];
            if (!name.IsNullOrEmpty()) sql = $"{sql} LIKE '{name}'";
        }

        var obj = QueryCollection("Databases", sql);

        var collection = new SchemaCollection("Databases");
        collection.AddColumn("CATALOG_NAME", typeof(String));
        collection.AddColumn("SCHEMA_NAME", typeof(String));
        foreach (var row in obj.Rows)
        {
            var schemaName = row[0]?.ToString();
            if (schemaName.IsNullOrEmpty()) continue;

            // 如果没有name限制，返回所有数据库
            if (name.IsNullOrEmpty())
            {
                collection.AddRow()[1] = schemaName;
            }
            else
            {
                // 有name限制，根据lowerCase决定是否区分大小写
                if (lowerCase ? schemaName.EqualIgnoreCase(name) : schemaName == name)
                {
                    collection.AddRow()[1] = schemaName;
                }
            }
        }
        return collection;
    }

    public virtual SchemaCollection GetTables(String[]? restrictions)
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
            var schemaName = row["SCHEMA_NAME"]?.ToString();
            if (schemaName.IsNullOrEmpty()) continue;

            dbRestriction[1] = schemaName;
            FindTables(collection, dbRestriction);
        }
        return collection;
    }

    public virtual SchemaCollection GetColumns(String[]? restrictions)
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
        String? columnName = null;
        if (restrictions != null && restrictions.Length == 4)
        {
            columnName = restrictions[3];
            restrictions[3] = null!;
        }
        foreach (var row in GetTables(restrictions).Rows)
        {
            var schemaName = row["TABLE_SCHEMA"]?.ToString();
            var tableName = row["TABLE_NAME"]?.ToString();
            if (schemaName.IsNullOrEmpty() || tableName.IsNullOrEmpty()) continue;

            LoadTableColumns(collection, schemaName, tableName, columnName);
        }
        QuoteDefaultValues(collection);
        return collection;
    }

    private void LoadTableColumns(SchemaCollection schemaCollection, String schema, String tableName, String? columnRestriction)
    {
        try
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
        catch (MySqlException ex) when (ex.Message.Contains("doesn't exist"))
        {
            // 表不存在（可能已被其他测试删除），跳过该表继续处理
        }
    }

    protected void QuoteDefaultValues(SchemaCollection schemaCollection)
    {
        if (schemaCollection == null || !schemaCollection.ContainsColumn("COLUMN_DEFAULT")) return;

        foreach (var row in schemaCollection.Rows)
        {
            var arg = row["COLUMN_DEFAULT"];
            var dataType = row["DATA_TYPE"]?.ToString();
            if (!dataType.IsNullOrEmpty() && IsTextType(dataType) && arg != null)
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
        var text = row["CHARACTER_SET_NAME"]?.ToString();
        if (!text.IsNullOrEmpty())
        {
            var num = text.IndexOf('_');
            if (num != -1)
            {
                row["CHARACTER_SET_NAME"] = text.Substring(0, num);
            }
        }
        var text2 = row["DATA_TYPE"]?.ToString();
        if (text2.IsNullOrEmpty()) return;

        var num2 = text2.IndexOf('(');
        if (num2 == -1)
        {
            return;
        }
        row["DATA_TYPE"] = text2.Substring(0, num2);
        var num3 = text2.IndexOf(')', num2);
        if (num3 == -1) return;

        var text3 = text2.Substring(num2 + 1, num3 - (num2 + 1));
        var dataType = row["DATA_TYPE"]?.ToString() ?? String.Empty;
        switch (dataType.ToLower(CultureInfo.InvariantCulture))
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

    public virtual SchemaCollection GetIndexes(String[]? restrictions)
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

        var array = new String[Math.Max((restrictions != null) ? restrictions.Length : 4, 4)];
        restrictions?.CopyTo(array, 0);
        array[3] = "BASE TABLE";
        foreach (var table in GetTables(array).Rows)
        {
            try
            {
                var sql = String.Format("SHOW INDEX FROM `{0}`.`{1}`", table["TABLE_SCHEMA"], table["TABLE_NAME"]);
                foreach (var row in QueryCollection("indexes", sql).Rows)
                {
                    var keyName = row["KEY_NAME"]?.ToString();
                    if (keyName.IsNullOrEmpty()) continue;

                    if (row["SEQ_IN_INDEX"].ToInt() == 1 && (restrictions == null || restrictions.Length != 4 || restrictions[3] == null || String.Equals(keyName, restrictions[3], StringComparison.Ordinal)))
                    {
                        var tableName = row["TABLE"]?.ToString();
                        if (tableName.IsNullOrEmpty()) continue;

                        var row2 = collection.AddRow();
                        row2["INDEX_CATALOG"] = null;
                        row2["INDEX_SCHEMA"] = table["TABLE_SCHEMA"];
                        row2["INDEX_NAME"] = keyName;
                        row2["TABLE_NAME"] = tableName;
                        row2["UNIQUE"] = row["NON_UNIQUE"].ToInt() == 0;
                        row2["PRIMARY"] = String.Equals(keyName, "PRIMARY", StringComparison.Ordinal);
                        row2["TYPE"] = row["INDEX_TYPE"];
                        row2["COMMENT"] = row["COMMENT"];
                    }
                }
            }
            catch (MySqlException ex) when (ex.Message.Contains("doesn't exist"))
            {
                // 表不存在（可能已被其他测试删除），跳过该表继续处理
            }
        }
        return collection;
    }

    public virtual SchemaCollection GetIndexColumns(String[]? restrictions)
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
            try
            {
                var sql = String.Format("SHOW INDEX FROM `{0}`.`{1}`", table["TABLE_SCHEMA"], table["TABLE_NAME"]);
                using var cmd = new MySqlCommand(connection, sql);
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var keyName = GetString(reader, reader.GetOrdinal("KEY_NAME"));
                    var columnName = GetString(reader, reader.GetOrdinal("COLUMN_NAME"));
                    if (keyName.IsNullOrEmpty() || columnName.IsNullOrEmpty()) continue;

                    if (restrictions == null || ((restrictions.Length < 4 || restrictions[3] == null || String.Equals(keyName, restrictions[3], StringComparison.Ordinal)) && (restrictions.Length < 5 || restrictions[4] == null || String.Equals(columnName, restrictions[4], StringComparison.Ordinal))))
                    {
                        var mySqlSchemaRow = dt.AddRow();
                        mySqlSchemaRow["INDEX_CATALOG"] = null;
                        mySqlSchemaRow["INDEX_SCHEMA"] = table["TABLE_SCHEMA"];
                        mySqlSchemaRow["INDEX_NAME"] = keyName;
                        mySqlSchemaRow["TABLE_NAME"] = table["TABLE_NAME"];
                        mySqlSchemaRow["COLUMN_NAME"] = columnName;
                        mySqlSchemaRow["ORDINAL_POSITION"] = reader.GetValue(reader.GetOrdinal("SEQ_IN_INDEX"));
                        mySqlSchemaRow["SORT_ORDER"] = reader.GetValue(reader.GetOrdinal("COLLATION"));
                    }
                }
            }
            catch (MySqlException ex) when (ex.Message.Contains("doesn't exist"))
            {
                // 表不存在（可能已被其他测试删除），跳过该表继续处理
            }
        }
        return dt;
    }

    /// <summary>获取存储过程和函数列表</summary>
    /// <param name="restrictions">限制条件：[数据库, 架构, 名称, 类型]</param>
    /// <returns></returns>
    public virtual SchemaCollection GetProcedures(String?[]? restrictions)
    {
        var collection = new SchemaCollection("Procedures");
        collection.AddColumn("SPECIFIC_CATALOG", typeof(String));
        collection.AddColumn("SPECIFIC_SCHEMA", typeof(String));
        collection.AddColumn("SPECIFIC_NAME", typeof(String));
        collection.AddColumn("ROUTINE_CATALOG", typeof(String));
        collection.AddColumn("ROUTINE_SCHEMA", typeof(String));
        collection.AddColumn("ROUTINE_NAME", typeof(String));
        collection.AddColumn("ROUTINE_TYPE", typeof(String));
        collection.AddColumn("ROUTINE_DEFINITION", typeof(String));
        collection.AddColumn("CREATED", typeof(DateTime));
        collection.AddColumn("LAST_ALTERED", typeof(DateTime));
        collection.AddColumn("SQL_MODE", typeof(String));
        collection.AddColumn("ROUTINE_COMMENT", typeof(String));
        collection.AddColumn("DEFINER", typeof(String));
        collection.AddColumn("CHARACTER_SET_CLIENT", typeof(String));
        collection.AddColumn("COLLATION_CONNECTION", typeof(String));
        collection.AddColumn("DATABASE_COLLATION", typeof(String));

        String? dbRestriction = null;
        String? nameRestriction = null;
        String? typeRestriction = null;
        if (restrictions != null)
        {
            if (restrictions.Length >= 1) dbRestriction = restrictions[0];
            if (restrictions.Length >= 2 && restrictions[1] != null) dbRestriction ??= restrictions[1];
            if (restrictions.Length >= 3) nameRestriction = restrictions[2];
            if (restrictions.Length >= 4) typeRestriction = restrictions[3];
        }

        // 确定要查询的数据库
        var databases = new List<String>();
        if (!dbRestriction.IsNullOrEmpty())
        {
            databases.Add(dbRestriction!);
        }
        else
        {
            var dbCollection = GetDatabases(null);
            foreach (var row in dbCollection.Rows)
            {
                var dbName = row["SCHEMA_NAME"]?.ToString();
                if (!dbName.IsNullOrEmpty()) databases.Add(dbName);
            }
        }

        foreach (var dbName in databases)
        {
            try
            {
                // 查询存储过程
                if (typeRestriction.IsNullOrEmpty() || typeRestriction.EqualIgnoreCase("PROCEDURE"))
                {
                    var sql = $"SHOW PROCEDURE STATUS WHERE Db = '{dbName}'";
                    if (!nameRestriction.IsNullOrEmpty()) sql += $" AND Name LIKE '{nameRestriction}'";
                    FillProceduresFromQuery(collection, dbName, sql, "PROCEDURE");
                }

                // 查询函数
                if (typeRestriction.IsNullOrEmpty() || typeRestriction.EqualIgnoreCase("FUNCTION"))
                {
                    var sql = $"SHOW FUNCTION STATUS WHERE Db = '{dbName}'";
                    if (!nameRestriction.IsNullOrEmpty()) sql += $" AND Name LIKE '{nameRestriction}'";
                    FillProceduresFromQuery(collection, dbName, sql, "FUNCTION");
                }
            }
            catch (MySqlException)
            {
                // 数据库可能不存在或无权限，跳过
            }
        }

        return collection;
    }

    private void FillProceduresFromQuery(SchemaCollection collection, String dbName, String sql, String routineType)
    {
        // SHOW PROCEDURE/FUNCTION STATUS 返回列: Db,Name,Type,Definer,Modified,Created,Security_type,Comment,character_set_client,collation_connection,Database Collation
        using var cmd = new MySqlCommand(connection, sql);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var row = collection.AddRow();
            row["SPECIFIC_CATALOG"] = DBNull.Value;
            row["SPECIFIC_SCHEMA"] = dbName;
            row["SPECIFIC_NAME"] = reader.GetValue(1);
            row["ROUTINE_CATALOG"] = DBNull.Value;
            row["ROUTINE_SCHEMA"] = dbName;
            row["ROUTINE_NAME"] = reader.GetValue(1);
            row["ROUTINE_TYPE"] = routineType;
            row["ROUTINE_DEFINITION"] = DBNull.Value; // SHOW PROCEDURE STATUS 不含定义体
            row["CREATED"] = reader.GetValue(5);
            row["LAST_ALTERED"] = reader.GetValue(4);
            row["SQL_MODE"] = DBNull.Value;
            row["ROUTINE_COMMENT"] = reader.GetValue(7);
            row["DEFINER"] = reader.GetValue(3);
            row["CHARACTER_SET_CLIENT"] = reader.GetValue(8);
            row["COLLATION_CONNECTION"] = reader.GetValue(9);
            row["DATABASE_COLLATION"] = reader.GetValue(10);
        }
    }

    /// <summary>获取视图列表</summary>
    /// <param name="restrictions">限制条件：[数据库, 架构, 表名]</param>
    /// <returns></returns>
    public virtual SchemaCollection GetViews(String?[]? restrictions)
    {
        var collection = new SchemaCollection("Views");
        collection.AddColumn("TABLE_CATALOG", typeof(String));
        collection.AddColumn("TABLE_SCHEMA", typeof(String));
        collection.AddColumn("TABLE_NAME", typeof(String));
        collection.AddColumn("VIEW_DEFINITION", typeof(String));
        collection.AddColumn("CHECK_OPTION", typeof(String));
        collection.AddColumn("IS_UPDATABLE", typeof(String));
        collection.AddColumn("DEFINER", typeof(String));
        collection.AddColumn("SECURITY_TYPE", typeof(String));
        collection.AddColumn("CHARACTER_SET_CLIENT", typeof(String));
        collection.AddColumn("COLLATION_CONNECTION", typeof(String));

        String? dbRestriction = null;
        String? nameRestriction = null;
        if (restrictions != null)
        {
            if (restrictions.Length >= 1) dbRestriction = restrictions[0];
            if (restrictions.Length >= 2 && restrictions[1] != null) dbRestriction ??= restrictions[1];
            if (restrictions.Length >= 3) nameRestriction = restrictions[2];
        }

        // 确定要查询的数据库
        var databases = new List<String>();
        if (!dbRestriction.IsNullOrEmpty())
        {
            databases.Add(dbRestriction!);
        }
        else
        {
            var dbCollection = GetDatabases(null);
            foreach (var row in dbCollection.Rows)
            {
                var dbName = row["SCHEMA_NAME"]?.ToString();
                if (!dbName.IsNullOrEmpty()) databases.Add(dbName);
            }
        }

        foreach (var dbName in databases)
        {
            try
            {
                var sql = $"SHOW FULL TABLES FROM `{dbName}` WHERE Table_type = 'VIEW'";
                if (!nameRestriction.IsNullOrEmpty()) sql += $" AND Tables_in_{dbName} LIKE '{nameRestriction}'";

                using var cmd = new MySqlCommand(connection, sql);
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var viewName = reader.GetString(0);
                    var row = collection.AddRow();
                    row["TABLE_CATALOG"] = DBNull.Value;
                    row["TABLE_SCHEMA"] = dbName;
                    row["TABLE_NAME"] = viewName;
                    row["VIEW_DEFINITION"] = DBNull.Value;
                    row["CHECK_OPTION"] = DBNull.Value;
                    row["IS_UPDATABLE"] = DBNull.Value;
                    row["DEFINER"] = DBNull.Value;
                    row["SECURITY_TYPE"] = DBNull.Value;
                    row["CHARACTER_SET_CLIENT"] = DBNull.Value;
                    row["COLLATION_CONNECTION"] = DBNull.Value;
                }
            }
            catch (MySqlException)
            {
                // 数据库可能不存在或无权限，跳过
            }
        }

        return collection;
    }

    /// <summary>获取触发器列表</summary>
    /// <param name="restrictions">限制条件：[数据库, 架构, 表名, 触发器名]</param>
    /// <returns></returns>
    public virtual SchemaCollection GetTriggers(String?[]? restrictions)
    {
        var collection = new SchemaCollection("Triggers");
        collection.AddColumn("TRIGGER_CATALOG", typeof(String));
        collection.AddColumn("TRIGGER_SCHEMA", typeof(String));
        collection.AddColumn("TRIGGER_NAME", typeof(String));
        collection.AddColumn("EVENT_MANIPULATION", typeof(String));
        collection.AddColumn("EVENT_OBJECT_CATALOG", typeof(String));
        collection.AddColumn("EVENT_OBJECT_SCHEMA", typeof(String));
        collection.AddColumn("EVENT_OBJECT_TABLE", typeof(String));
        collection.AddColumn("ACTION_ORDER", typeof(Int64));
        collection.AddColumn("ACTION_CONDITION", typeof(String));
        collection.AddColumn("ACTION_STATEMENT", typeof(String));
        collection.AddColumn("ACTION_ORIENTATION", typeof(String));
        collection.AddColumn("ACTION_TIMING", typeof(String));
        collection.AddColumn("ACTION_REFERENCE_OLD_TABLE", typeof(String));
        collection.AddColumn("ACTION_REFERENCE_NEW_TABLE", typeof(String));
        collection.AddColumn("ACTION_REFERENCE_OLD_ROW", typeof(String));
        collection.AddColumn("ACTION_REFERENCE_NEW_ROW", typeof(String));
        collection.AddColumn("CREATED", typeof(DateTime));
        collection.AddColumn("SQL_MODE", typeof(String));
        collection.AddColumn("DEFINER", typeof(String));
        collection.AddColumn("CHARACTER_SET_CLIENT", typeof(String));
        collection.AddColumn("COLLATION_CONNECTION", typeof(String));
        collection.AddColumn("DATABASE_COLLATION", typeof(String));

        String? dbRestriction = null;
        String? tableRestriction = null;
        String? nameRestriction = null;
        if (restrictions != null)
        {
            if (restrictions.Length >= 1) dbRestriction = restrictions[0];
            if (restrictions.Length >= 2 && restrictions[1] != null) dbRestriction ??= restrictions[1];
            if (restrictions.Length >= 3) tableRestriction = restrictions[2];
            if (restrictions.Length >= 4) nameRestriction = restrictions[3];
        }

        // 确定要查询的数据库
        var databases = new List<String>();
        if (!dbRestriction.IsNullOrEmpty())
        {
            databases.Add(dbRestriction!);
        }
        else
        {
            var dbCollection = GetDatabases(null);
            foreach (var row in dbCollection.Rows)
            {
                var dbName = row["SCHEMA_NAME"]?.ToString();
                if (!dbName.IsNullOrEmpty()) databases.Add(dbName);
            }
        }

        foreach (var dbName in databases)
        {
            try
            {
                var sql = $"SHOW TRIGGERS FROM `{dbName}`";
                if (!tableRestriction.IsNullOrEmpty()) sql += $" WHERE `Table` LIKE '{tableRestriction}'";

                using var cmd = new MySqlCommand(connection, sql);
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    // SHOW TRIGGERS 返回列: Trigger,Event,Table,Statement,Timing,Created,sql_mode,Definer,character_set_client,collation_connection,Database Collation
                    var triggerName = reader.GetString(0);
                    if (!nameRestriction.IsNullOrEmpty() && triggerName != nameRestriction) continue;

                    var row = collection.AddRow();
                    row["TRIGGER_CATALOG"] = DBNull.Value;
                    row["TRIGGER_SCHEMA"] = dbName;
                    row["TRIGGER_NAME"] = triggerName;
                    row["EVENT_MANIPULATION"] = reader.GetString(1);
                    row["EVENT_OBJECT_CATALOG"] = DBNull.Value;
                    row["EVENT_OBJECT_SCHEMA"] = dbName;
                    row["EVENT_OBJECT_TABLE"] = reader.GetString(2);
                    row["ACTION_ORDER"] = DBNull.Value;
                    row["ACTION_CONDITION"] = DBNull.Value;
                    row["ACTION_STATEMENT"] = reader.GetString(3);
                    row["ACTION_ORIENTATION"] = DBNull.Value;
                    row["ACTION_TIMING"] = reader.GetString(4);
                    row["ACTION_REFERENCE_OLD_TABLE"] = DBNull.Value;
                    row["ACTION_REFERENCE_NEW_TABLE"] = DBNull.Value;
                    row["ACTION_REFERENCE_OLD_ROW"] = DBNull.Value;
                    row["ACTION_REFERENCE_NEW_ROW"] = DBNull.Value;
                    row["CREATED"] = reader.GetValue(5);
                    row["SQL_MODE"] = reader.GetValue(6);
                    row["DEFINER"] = reader.GetValue(7);
                    row["CHARACTER_SET_CLIENT"] = reader.GetValue(8);
                    row["COLLATION_CONNECTION"] = reader.GetValue(9);
                    row["DATABASE_COLLATION"] = reader.GetValue(10);
                }
            }
            catch (MySqlException)
            {
                // 数据库可能不存在或无权限，跳过
            }
        }

        return collection;
    }

    /// <summary>获取外键列表</summary>
    /// <param name="restrictions">限制条件：[数据库, 架构, 表名, 外键名]</param>
    /// <returns></returns>
    public virtual SchemaCollection GetForeignKeys(String?[]? restrictions)
    {
        var collection = new SchemaCollection("ForeignKeys");
        collection.AddColumn("CONSTRAINT_CATALOG", typeof(String));
        collection.AddColumn("CONSTRAINT_SCHEMA", typeof(String));
        collection.AddColumn("CONSTRAINT_NAME", typeof(String));
        collection.AddColumn("TABLE_CATALOG", typeof(String));
        collection.AddColumn("TABLE_SCHEMA", typeof(String));
        collection.AddColumn("TABLE_NAME", typeof(String));
        collection.AddColumn("COLUMN_NAME", typeof(String));
        collection.AddColumn("ORDINAL_POSITION", typeof(Int64));
        collection.AddColumn("REFERENCED_TABLE_CATALOG", typeof(String));
        collection.AddColumn("REFERENCED_TABLE_SCHEMA", typeof(String));
        collection.AddColumn("REFERENCED_TABLE_NAME", typeof(String));
        collection.AddColumn("REFERENCED_COLUMN_NAME", typeof(String));

        String? dbRestriction = null;
        String? tableRestriction = null;
        String? fkNameRestriction = null;
        if (restrictions != null)
        {
            if (restrictions.Length >= 1) dbRestriction = restrictions[0];
            if (restrictions.Length >= 2 && restrictions[1] != null) dbRestriction ??= restrictions[1];
            if (restrictions.Length >= 3) tableRestriction = restrictions[2];
            if (restrictions.Length >= 4) fkNameRestriction = restrictions[3];
        }

        // 确定要查询的数据库
        var databases = new List<String>();
        if (!dbRestriction.IsNullOrEmpty())
        {
            databases.Add(dbRestriction!);
        }
        else
        {
            var dbCollection = GetDatabases(null);
            foreach (var row in dbCollection.Rows)
            {
                var dbName = row["SCHEMA_NAME"]?.ToString();
                if (!dbName.IsNullOrEmpty()) databases.Add(dbName);
            }
        }

        foreach (var dbName in databases)
        {
            try
            {
                var sql = $"SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{dbName}' AND REFERENCED_TABLE_NAME IS NOT NULL";
                if (!tableRestriction.IsNullOrEmpty()) sql += $" AND TABLE_NAME LIKE '{tableRestriction}'";
                if (!fkNameRestriction.IsNullOrEmpty()) sql += $" AND CONSTRAINT_NAME LIKE '{fkNameRestriction}'";
                sql += " ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION";

                using var cmd = new MySqlCommand(connection, sql);
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var row = collection.AddRow();
                    row["CONSTRAINT_CATALOG"] = DBNull.Value;
                    row["CONSTRAINT_SCHEMA"] = dbName;
                    row["CONSTRAINT_NAME"] = reader["CONSTRAINT_NAME"];
                    row["TABLE_CATALOG"] = DBNull.Value;
                    row["TABLE_SCHEMA"] = dbName;
                    row["TABLE_NAME"] = reader["TABLE_NAME"];
                    row["COLUMN_NAME"] = reader["COLUMN_NAME"];
                    row["ORDINAL_POSITION"] = reader["ORDINAL_POSITION"];
                    row["REFERENCED_TABLE_CATALOG"] = DBNull.Value;
                    row["REFERENCED_TABLE_SCHEMA"] = reader["REFERENCED_TABLE_SCHEMA"];
                    row["REFERENCED_TABLE_NAME"] = reader["REFERENCED_TABLE_NAME"];
                    row["REFERENCED_COLUMN_NAME"] = reader["REFERENCED_COLUMN_NAME"];
                }
            }
            catch (MySqlException)
            {
                // 数据库可能不存在或无权限，跳过
            }
        }

        return collection;
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
        if (restrictions == null || restrictions.Length < 2 || restrictions[1].IsNullOrEmpty()) return;

        var schemaName = restrictions[1];
        var stringBuilder = new StringBuilder();
        var stringBuilder2 = new StringBuilder();
        stringBuilder.AppendFormat(CultureInfo.InvariantCulture, "SHOW TABLE STATUS FROM `{0}`", schemaName);
        if (restrictions.Length >= 3 && restrictions[2] != null)
        {
            stringBuilder2.AppendFormat(CultureInfo.InvariantCulture, " LIKE '{0}'", restrictions[2]);
        }
        stringBuilder.Append(stringBuilder2.ToString());
        var table_type = schemaName.ToLower(CultureInfo.InvariantCulture) == "information_schema" ? "SYSTEM VIEW" : "BASE TABLE";
        using var cmd = new MySqlCommand(connection, stringBuilder.ToString());
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var mySqlSchemaRow = schema.AddRow();
            mySqlSchemaRow["TABLE_CATALOG"] = null;
            mySqlSchemaRow["TABLE_SCHEMA"] = schemaName;
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

    private static String? GetString(DbDataReader reader, Int32 index)
    {
        if (reader.IsDBNull(index)) return null;

        return reader.GetString(index);
    }

    protected SchemaCollection QueryCollection(String name, String sql)
    {
        var collection = new SchemaCollection(name);
        using var cmd = new MySqlCommand(connection, sql);
        using var reader = cmd.ExecuteReader();
        for (var i = 0; i < reader.FieldCount; i++)
        {
            collection.AddColumn(reader.GetName(i), reader.GetFieldType(i));
        }
        while (reader.Read())
        {
            var mySqlSchemaRow = collection.AddRow();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                mySqlSchemaRow[i] = reader.GetValue(i);
            }
        }

        return collection;
    }
}

class SchemaCollection
{
    private readonly List<SchemaColumn> _columns = [];

    private readonly List<SchemaRow> _rows = [];

    private DataTable? _table;

    internal Dictionary<String, Int32> Mapping;

    internal Dictionary<Int32, Int32> LogicalMappings;

    public String Name { get; set; } = String.Empty;

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
        _table = dataTable;
        return dataTable;
    }
}

class SchemaRow(SchemaCollection collection)
{
    private readonly Dictionary<Int32, Object?> _data = [];

    internal SchemaCollection Collection { get; } = collection;

    internal Object? this[String s] { get => GetValueForName(s); set => SetValueForName(s, value); }

    internal Object? this[Int32 i]
    {
        get
        {
            var key = Collection.LogicalMappings[i];
            if (!_data.TryGetValue(key, out var value))
            {
                value = null;
                _data[key] = value;
            }
            return value;
        }
        set
        {
            _data[Collection.LogicalMappings[i]] = value;
        }
    }

    private void SetValueForName(String colName, Object? value)
    {
        var i = Collection.Mapping[colName];
        this[i] = value;
    }

    private Object? GetValueForName(String colName)
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
    public String Name { get; set; } = String.Empty;

    public Type Type { get; set; } = typeof(Object);
}