namespace NewLife.MySql.Common;

/// <summary>数据库类型。支持 MySQL 及其兼容数据库</summary>
public enum DatabaseType
{
    /// <summary>MySQL 标准数据库</summary>
    MySQL = 0,

    /// <summary>OceanBase 分布式数据库（MySQL 协议兼容）</summary>
    OceanBase = 1,

    /// <summary>TiDB 分布式数据库（MySQL 协议兼容）</summary>
    TiDB = 2,
}
