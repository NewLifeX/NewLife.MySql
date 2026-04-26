using System.Data.Common;
using NewLife.MySql.Common;

namespace NewLife.MySql;

/// <summary>连接构造器</summary>
public class MySqlConnectionStringBuilder : DbConnectionStringBuilder
{
    #region 属性
    /// <summary>服务器</summary>
    public String? Server { get => this[nameof(Server)] as String; set => this[nameof(Server)] = value; }

    /// <summary>端口</summary>
    public Int32 Port { get => this[nameof(Port)].ToInt(); set => this[nameof(Port)] = value; }

    /// <summary>数据库</summary>
    public String? Database { get => this[nameof(Database)] as String; set => this[nameof(Database)] = value; }

    /// <summary>用户名</summary>
    public String? UserID { get => this[nameof(UserID)] as String; set => this[nameof(UserID)] = value; }

    /// <summary>密码</summary>
    public String? Password { get => this[nameof(Password)] as String; set => this[nameof(Password)] = value; }

    /// <summary>连接超时</summary>
    public Int32 ConnectionTimeout { get => this[nameof(ConnectionTimeout)].ToInt(); set => this[nameof(ConnectionTimeout)] = value; }

    /// <summary>命令超时</summary>
    public Int32 CommandTimeout { get => this[nameof(CommandTimeout)].ToInt(); set => this[nameof(CommandTimeout)] = value; }

    /// <summary>SSL模式。None/Preferred/Required，默认None</summary>
    public String? SslMode { get => this[nameof(SslMode)] as String; set => this[nameof(SslMode)] = value; }

    /// <summary>是否使用服务端预编译执行。启用后参数化查询走 COM_STMT_PREPARE/EXECUTE 二进制协议，默认false</summary>
    public Boolean UseServerPrepare { get => this[nameof(UseServerPrepare)].ToBoolean(); set => this[nameof(UseServerPrepare)] = value; }

    /// <summary>是否启用管道化执行。批量操作时连续发送多个 EXECUTE 包再批量读取响应，默认false</summary>
    public Boolean Pipeline { get => this[nameof(Pipeline)].ToBoolean(); set => this[nameof(Pipeline)] = value; }

    /// <summary>是否输出协议收发包日志。默认false，仅用于排查协议层问题</summary>
    public Boolean TracePackets { get => this[nameof(TracePackets)].ToBoolean(); set => this[nameof(TracePackets)] = value; }

    /// <summary>网络断线时是否自动重连重试。默认true。遇到 IOException/MySqlException(2006/2013) 等临时网络错误时，
    /// 在 ConnectionTimeout 时间内指数退避等待并重建连接重试；超过 ConnectionTimeout 才向上层抛出异常。
    /// 事务内自动跳过重试以避免 DML 重复执行。设为 false 可关闭。</summary>
    public Boolean RetryOnNetworkFailure { get => this[nameof(RetryOnNetworkFailure)].ToBoolean(true); set => this[nameof(RetryOnNetworkFailure)] = value; }

    /// <summary>字符集。默认Utf8Mb4，支持4字节Unicode（含emoji）。握手时写入协议编号，等效于SET NAMES</summary>
    public MySqlCharSet CharSet
    {
        get
        {
            var v = this[nameof(CharSet)];
            if (v is MySqlCharSet cs) return cs;
            // 连接字符串传入的是字符串，尝试解析
            if (v is String s && !s.IsNullOrEmpty())
                return ParseCharSet(s);
            return MySqlCharSet.Utf8Mb4;
        }
        set => this[nameof(CharSet)] = value;
    }
    #endregion

    #region 构造
    private static readonly IDictionary<String, String[]> _options;
    static MySqlConnectionStringBuilder()
    {
        var dic = new Dictionary<String, String[]>
        {
            [nameof(Server)] = ["server", "datasource", "data source"],
            [nameof(Database)] = ["database"],
            [nameof(Port)] = ["port"],
            [nameof(UserID)] = ["uid", "user id", "userid", "user", "username", "user name"],
            [nameof(Password)] = ["pass", "password", "pwd"],
            [nameof(ConnectionTimeout)] = ["connectiontimeout", "connection timeout"],
            [nameof(CommandTimeout)] = ["commandtimeout", "defaultcommandtimeout", "command timeout", "default command timeout"],
            [nameof(SslMode)] = ["sslmode", "ssl mode", "ssl-mode"],
            [nameof(UseServerPrepare)] = ["useserverprepare", "use server prepare", "use_server_prepare"],
            [nameof(Pipeline)] = ["pipeline", "pipelining"],
            [nameof(TracePackets)] = ["tracepackets", "trace packets", "packettrace", "packet trace"],
            [nameof(RetryOnNetworkFailure)] = ["retryonnetworkfailure", "retry on network failure", "retry_on_network_failure"],
            [nameof(CharSet)] = ["charset", "character set", "characterset", "char set"],
        };

        _options = dic;
    }

    /// <summary>实例化</summary>
    public MySqlConnectionStringBuilder()
    {
        Port = 3306;
        ConnectionTimeout = 15;
        CommandTimeout = 30;
        CharSet = MySqlCharSet.Utf8Mb4;
        RetryOnNetworkFailure = true;
    }

    /// <summary>使用连接字符串实例化</summary>
    /// <param name="connStr"></param>
    public MySqlConnectionStringBuilder(String connStr) : this() => ConnectionString = connStr;
    #endregion

    #region 方法
    /// <summary>索引器</summary>
    /// <param name="keyword"></param>
    /// <returns></returns>
    public override Object? this[String keyword]
    {
        get => TryGetValue(keyword, out var value) ? value : null;
        set
        {
            // 替换为标准Key
            var kw = keyword.ToLower();
            foreach (var kv in _options)
            {
                if (kv.Value.Contains(kw))
                {
                    keyword = kv.Key;
                    break;
                }
            }

            base[keyword] = value;
        }
    }

    /// <summary>获取字符集对应的 MySQL 协议编号。用于握手包中的 charset 字段</summary>
    /// <returns>MySQL 字符集编号</returns>
    public Byte GetCharSetNumber() => (Byte)CharSet;

    /// <summary>将字符串解析为 MySqlCharSet 枚举。连接字符串中指定字符集名称时使用</summary>
    /// <param name="name">字符集名称，不区分大小写</param>
    /// <returns>对应的枚举值，未识别时返回 Utf8Mb4</returns>
    public static MySqlCharSet ParseCharSet(String name) => name.ToLower() switch
    {
        "utf8mb4" or "utf8mb4_general_ci" => MySqlCharSet.Utf8Mb4,
        "utf8mb4_unicode_ci" => MySqlCharSet.Utf8Mb4Unicode,
        "utf8" or "utf8_general_ci" => MySqlCharSet.Utf8,
        "binary" => MySqlCharSet.Binary,
        "latin1" or "latin1_swedish_ci" => MySqlCharSet.Latin1,
        "gbk" or "gbk_chinese_ci" => MySqlCharSet.Gbk,
        "gb2312" or "gb2312_chinese_ci" => MySqlCharSet.Gb2312,
        "ascii" or "ascii_general_ci" => MySqlCharSet.Ascii,
        _ => MySqlCharSet.Utf8Mb4,
    };
    #endregion
}