using System.Data.Common;

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
            [nameof(ConnectionTimeout)] = ["connectiontimeout", "connectiontimeout", "connection timeout"],
            [nameof(CommandTimeout)] = ["defaultcommandtimeout", "command timeout", "default command timeout"],
        };

        _options = dic;
    }

    /// <summary>实例化</summary>
    public MySqlConnectionStringBuilder()
    {
        Port = 3306;
        ConnectionTimeout = 15;
        CommandTimeout = 30;
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
        get { return TryGetValue(keyword, out var value) ? value : null; }
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
    #endregion
}