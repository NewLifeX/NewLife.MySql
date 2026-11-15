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
    /// <param name="relationalDependencies">关系型类型映射源依赖</param>
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
            if (clrType == typeof(Decimal))
            {
                // 模型显式配置 Precision/Scale 时按配置生成，避免所有 decimal 都落到 DECIMAL(65,30)
                var precision = mappingInfo.Precision;
                if (precision.HasValue)
                    return new DecimalTypeMapping($"DECIMAL({precision.Value},{mappingInfo.Scale ?? 0})", System.Data.DbType.Decimal);

                return _decimal;
            }
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
            var baseName = GetBaseStoreType(upperName);
            if (baseName is "INT" or "INTEGER" or "MEDIUMINT") return _int;
            if (baseName == "BIGINT") return _bigint;
            if (baseName == "SMALLINT") return _smallint;
            if (baseName == "TINYINT") return upperName.Contains("(1)") ? _bool : _tinyint;
            if (baseName is "DOUBLE" or "REAL") return _double;
            if (baseName == "FLOAT") return _float;
            if (baseName is "DECIMAL" or "NUMERIC")
            {
                var precision = mappingInfo.Precision;
                if (precision.HasValue)
                    return new DecimalTypeMapping($"DECIMAL({precision.Value},{mappingInfo.Scale ?? 0})", System.Data.DbType.Decimal);

                return _decimal;
            }
            if (baseName is "DATETIME" or "TIMESTAMP") return _datetime;
            if (baseName == "DATE") return _datetime;
            if (baseName == "TIME") return _timespan;
            if (upperName.Contains("CHAR(36)")) return _guid;
            if (baseName is "BLOB" or "BINARY") return _blob;
            if (baseName == "BOOL" || baseName == "BOOLEAN") return _bool;
            if (baseName is "VARCHAR" or "TEXT" or "CHAR" or "LONGTEXT" or "MEDIUMTEXT" or "TINYTEXT")
                return new StringTypeMapping(storeTypeName, System.Data.DbType.String);
        }

        return base.FindMapping(mappingInfo);
    }

    /// <summary>提取存储类型的基础名称，去掉长度/精度括号和 UNSIGNED 等修饰</summary>
    /// <param name="storeType">已转大写的存储类型名</param>
    /// <returns>基础类型名，如 POINT、INT、DECIMAL</returns>
    private static String GetBaseStoreType(String storeType)
    {
        var idx = storeType.IndexOf('(');
        if (idx >= 0) storeType = storeType[..idx];

        idx = storeType.IndexOf(' ');
        if (idx >= 0) storeType = storeType[..idx];

        return storeType.Trim();
    }
}
