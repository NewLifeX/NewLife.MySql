using Microsoft.EntityFrameworkCore.Storage;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 类型映射源。提供 .NET 类型与 MySQL 数据库类型之间的映射</summary>
public class MySqlTypeMappingSource : RelationalTypeMappingSource
{
    // 常用类型映射缓存
    private static readonly RelationalTypeMapping _int = new IntTypeMapping("INT", System.Data.DbType.Int32);
    private static readonly RelationalTypeMapping _bigint = new LongTypeMapping("BIGINT", System.Data.DbType.Int64);
    private static readonly RelationalTypeMapping _smallint = new ShortTypeMapping("SMALLINT", System.Data.DbType.Int16);
    private static readonly RelationalTypeMapping _tinyint = new ByteTypeMapping("TINYINT UNSIGNED", System.Data.DbType.Byte);
    private static readonly RelationalTypeMapping _bool = new BoolTypeMapping("TINYINT(1)", System.Data.DbType.Boolean);
    private static readonly RelationalTypeMapping _double = new DoubleTypeMapping("DOUBLE", System.Data.DbType.Double);
    private static readonly RelationalTypeMapping _float = new FloatTypeMapping("FLOAT", System.Data.DbType.Single);
    private static readonly RelationalTypeMapping _decimal = new DecimalTypeMapping("DECIMAL(65,30)", System.Data.DbType.Decimal);
    private static readonly RelationalTypeMapping _datetime = new DateTimeTypeMapping("DATETIME", System.Data.DbType.DateTime);
    private static readonly RelationalTypeMapping _datetimeoffset = new DateTimeOffsetTypeMapping("DATETIME", System.Data.DbType.DateTime);
    private static readonly RelationalTypeMapping _timespan = new TimeSpanTypeMapping("TIME", System.Data.DbType.Time);
    private static readonly RelationalTypeMapping _guid = new GuidTypeMapping("CHAR(36)", System.Data.DbType.Guid);
    private static readonly RelationalTypeMapping _blob = new ByteArrayTypeMapping("BLOB", System.Data.DbType.Binary);

    /// <summary>实例化</summary>
    /// <param name="dependencies">类型映射源依赖</param>
    public MySqlTypeMappingSource(TypeMappingSourceDependencies dependencies, RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies) { }

    /// <summary>查找 CLR 类型对应的类型映射</summary>
    /// <param name="mappingInfo">映射信息</param>
    /// <returns></returns>
    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;

        // 按 CLR 类型查找
        if (clrType != null)
        {
            if (clrType == typeof(Int32)) return _int;
            if (clrType == typeof(Int64)) return _bigint;
            if (clrType == typeof(Int16)) return _smallint;
            if (clrType == typeof(Byte)) return _tinyint;
            if (clrType == typeof(Boolean)) return _bool;
            if (clrType == typeof(Double)) return _double;
            if (clrType == typeof(Single)) return _float;
            if (clrType == typeof(Decimal)) return _decimal;
            if (clrType == typeof(DateTime)) return _datetime;
            if (clrType == typeof(DateTimeOffset)) return _datetimeoffset;
            if (clrType == typeof(TimeSpan)) return _timespan;
            if (clrType == typeof(Guid)) return _guid;
            if (clrType == typeof(Byte[])) return _blob;

            if (clrType == typeof(String))
            {
                var size = mappingInfo.Size;
                if (size.HasValue && size.Value > 65535)
                    return new StringTypeMapping("LONGTEXT", System.Data.DbType.String);

                return new StringTypeMapping($"VARCHAR({size ?? 255})", System.Data.DbType.String, size: size ?? 255);
            }
        }

        // 按存储类型名查找
        var storeTypeName = mappingInfo.StoreTypeName;
        if (storeTypeName != null)
        {
            var upperName = storeTypeName.ToUpperInvariant();
            if (upperName.Contains("INT") && !upperName.Contains("TINY") && !upperName.Contains("SMALL") && !upperName.Contains("BIG"))
                return _int;
            if (upperName.Contains("BIGINT")) return _bigint;
            if (upperName.Contains("SMALLINT")) return _smallint;
            if (upperName.Contains("TINYINT")) return upperName.Contains("(1)") ? _bool : _tinyint;
            if (upperName.Contains("DOUBLE") || upperName.Contains("REAL")) return _double;
            if (upperName.Contains("FLOAT")) return _float;
            if (upperName.Contains("DECIMAL") || upperName.Contains("NUMERIC")) return _decimal;
            if (upperName.Contains("DATETIME") || upperName.Contains("TIMESTAMP")) return _datetime;
            if (upperName.Contains("DATE")) return _datetime;
            if (upperName.Contains("TIME")) return _timespan;
            if (upperName.Contains("CHAR(36)")) return _guid;
            if (upperName.Contains("BLOB") || upperName.Contains("BINARY")) return _blob;
            if (upperName.Contains("BOOL")) return _bool;
            if (upperName.Contains("VARCHAR") || upperName.Contains("TEXT") || upperName.Contains("CHAR"))
                return new StringTypeMapping(storeTypeName, System.Data.DbType.String);
        }

        return base.FindMapping(mappingInfo);
    }
}
