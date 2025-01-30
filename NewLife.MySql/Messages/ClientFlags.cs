namespace NewLife.MySql.Messages;

/// <summary>
/// 客户端参数的摘要说明。
/// </summary>
[Flags]
public enum ClientFlags : UInt32
{
    /// <summary>新的更安全的密码</summary>
    LONG_PASSWORD = 1,
    /// <summary>找到的行数而不是受影响的行数</summary>
    FOUND_ROWS = 2,
    /// <summary>获取所有列标志</summary>
    LONG_FLAG = 4,
    /// <summary>可以在连接时指定数据库</summary>
    CONNECT_WITH_DB = 8,
    /// <summary>不允许使用 db.table.column</summary>
    NO_SCHEMA = 16,
    /// <summary>客户端可以使用压缩协议</summary>
    COMPRESS = 32,
    /// <summary>ODBC 客户端</summary>
    ODBC = 64,
    /// <summary>可以使用 LOAD DATA LOCAL</summary>
    LOCAL_FILES = 128,
    /// <summary>忽略 '(' 前的空格</summary>
    IGNORE_SPACE = 256,
    /// <summary>支持新的 4.1 协议</summary>
    PROTOCOL_41 = 512,
    /// <summary>这是一个交互式客户端</summary>
    INTERACTIVE = 1024,
    /// <summary>握手后切换到 SSL</summary>
    SSL = 2048,
    /// <summary>忽略 sigpipes</summary>
    IGNORE_SIGPIPE = 4096,
    /// <summary>客户端了解事务</summary>
    TRANSACTIONS = 8192,
    /// <summary>旧的 4.1 协议标志</summary>
    RESERVED = 16384,
    /// <summary>新的 4.1 认证</summary>
    SECURE_CONNECTION = 32768,
    /// <summary>允许多语句支持</summary>
    MULTI_STATEMENTS = 65536,
    /// <summary>允许多个结果集</summary>
    MULTI_RESULTS = 131072,
    /// <summary>允许使用 PS 协议的多结果</summary>
    PS_MULTI_RESULTS = 1u << 18,
    /// <summary>客户端支持插件认证</summary>
    PLUGIN_AUTH = (1u << 19),
    /// <summary>允许客户端连接属性</summary>
    CONNECT_ATTRS = (1u << 20),
    /// <summary>支持密码过期 > 5.6.6</summary>
    CAN_HANDLE_EXPIRED_PASSWORD = (1u << 22),
    /// <summary>支持发送会话跟踪变量</summary>
    CLIENT_SESSION_TRACK = (1u << 23),
    /// <summary>支持查询属性</summary>
    CLIENT_QUERY_ATTRIBUTES = (1u << 27),
    /// <summary>验证服务器证书</summary>
    CLIENT_SSL_VERIFY_SERVER_CERT = (1u << 30),
    /// <summary>在连接失败后不重置选项</summary>
    CLIENT_REMEMBER_OPTIONS = (1u << 31),
    /// <summary>支持多因素认证 (MFA)</summary>
    MULTI_FACTOR_AUTHENTICATION = (1u << 28)
}
