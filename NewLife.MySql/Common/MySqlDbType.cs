namespace NewLife.MySql.Common;

/// <summary>MySQL数据库字段类型。基于官方MySQL协议定义</summary>
/// <remarks>枚举值0~255为MySQL协议原始类型ID，500+为扩展的无符号/文本/二进制等衍生类型</remarks>
public enum MySqlDbType
{
    /// <summary>小数。DECIMAL，固定精度数值</summary>
    Decimal = 0,

    /// <summary>微小整数。TINYINT，1字节，有符号-128~127，无符号0~255</summary>
    Byte = 1,

    /// <summary>短整数。SMALLINT，2字节</summary>
    Int16 = 2,

    /// <summary>整数。INT，4字节</summary>
    Int32 = 3,

    /// <summary>单精度浮点数。FLOAT，4字节</summary>
    Float = 4,

    /// <summary>双精度浮点数。DOUBLE，8字节</summary>
    Double = 5,

    /// <summary>时间戳。TIMESTAMP</summary>
    Timestamp = 7,

    /// <summary>长整数。BIGINT，8字节</summary>
    Int64 = 8,

    /// <summary>中等整数。MEDIUMINT，3字节</summary>
    Int24 = 9,

    /// <summary>日期。DATE，范围1000-01-01到9999-12-31</summary>
    Date = 10,

    /// <summary>时间。TIME，范围-838:59:59到838:59:59</summary>
    Time = 11,

    /// <summary>日期时间。DATETIME</summary>
    DateTime = 12,

    /// <summary>年份。YEAR，2或4位格式，范围1901~2155</summary>
    Year = 13,

    /// <summary>已过时的日期时间类型，请使用DateTime</summary>
    Newdate = 14,

    /// <summary>变长字符串。VARCHAR，最大65535字符</summary>
    VarString = 15,

    /// <summary>位字段。BIT</summary>
    Bit = 16,

    /// <summary>向量。VECTOR，MySQL 9.0+</summary>
    Vector = 242,

    /// <summary>JSON字符串</summary>
    Json = 245,

    /// <summary>高精度小数。DECIMAL/NUMERIC</summary>
    NewDecimal = 246,

    /// <summary>枚举。ENUM，最多65535个不同值</summary>
    Enum = 247,

    /// <summary>集合。SET，最多64个成员</summary>
    Set = 248,

    /// <summary>微小二进制。TINYBLOB，最大255字节</summary>
    TinyBlob = 249,

    /// <summary>中等二进制。MEDIUMBLOB，最大16M字节</summary>
    MediumBlob = 250,

    /// <summary>长二进制。LONGBLOB，最大4G字节</summary>
    LongBlob = 251,

    /// <summary>二进制块。BLOB，最大65535字节</summary>
    Blob = 252,

    /// <summary>变长字符串。VAR_STRING，最大255字节</summary>
    VarChar = 253,

    /// <summary>固定长度字符串。CHAR/STRING</summary>
    String = 254,

    /// <summary>几何数据。GEOMETRY</summary>
    Geometry = 255,

    /// <summary>无符号微小整数。TINYINT UNSIGNED，1字节，0~255</summary>
    UByte = 501,

    /// <summary>无符号短整数。SMALLINT UNSIGNED，2字节</summary>
    UInt16 = 502,

    /// <summary>无符号整数。INT UNSIGNED，4字节</summary>
    UInt32 = 503,

    /// <summary>无符号长整数。BIGINT UNSIGNED，8字节</summary>
    UInt64 = 508,

    /// <summary>无符号中等整数。MEDIUMINT UNSIGNED，3字节</summary>
    UInt24 = 509,

    /// <summary>微小文本。TINYTEXT，最大255字符</summary>
    TinyText = 749,

    /// <summary>中等文本。MEDIUMTEXT，最大16M字符</summary>
    MediumText = 750,

    /// <summary>长文本。LONGTEXT，最大4G字符</summary>
    LongText = 751,

    /// <summary>文本。TEXT，最大65535字符</summary>
    Text = 752,

    /// <summary>变长二进制。VARBINARY</summary>
    VarBinary = 753,

    /// <summary>固定长度二进制。BINARY</summary>
    Binary = 754,

    /// <summary>全局唯一标识符。GUID/UUID</summary>
    Guid = 854,
};