namespace NewLife.MySql.Common;

/// <summary>MySQL字符集。枚举值即为握手包中的字符集编号（character_set_client等会话变量）</summary>
/// <remarks>
/// 握手包 charset 字段决定了连接的 character_set_client / character_set_connection / character_set_results，
/// 等效于 SET NAMES，但在协议层一步完成，无需额外往返。
/// </remarks>
public enum MySqlCharSet : Byte
{
    /// <summary>latin1_swedish_ci。西欧单字节字符集，1字节每字符</summary>
    Latin1 = 8,

    /// <summary>ascii_general_ci。纯ASCII，1字节每字符</summary>
    Ascii = 11,

    /// <summary>gb2312_chinese_ci。简体中文国标，2字节每字符</summary>
    Gb2312 = 24,

    /// <summary>gbk_chinese_ci。简体中文扩展国标，2字节每字符</summary>
    Gbk = 28,

    /// <summary>utf8_general_ci。MySQL的utf8，最多3字节每字符，不支持emoji等4字节Unicode</summary>
    Utf8 = 33,

    /// <summary>utf8mb4_general_ci。完整UTF-8，最多4字节每字符，支持emoji及全部Unicode</summary>
    Utf8Mb4 = 45,

    /// <summary>binary。二进制字节串，不做字符集转换</summary>
    Binary = 63,

    /// <summary>utf8mb4_unicode_ci。utf8mb4的unicode排序规则变体，MySQL 5.5+</summary>
    Utf8Mb4Unicode = 224,
}
