namespace NewLife.MySql.Common;

/// <summary>命令</summary>
public enum DbCmd : Byte
{
    /// <summary>休眠</summary>
    SLEEP = 0,

    /// <summary>退出</summary>
    QUIT = 1,

    /// <summary>初始化数据库</summary>
    INIT_DB = 2,

    /// <summary>查询</summary>
    QUERY = 3,

    /// <summary>字段列表</summary>
    FIELD_LIST = 4,

    /// <summary>创建数据库</summary>
    CREATE_DB = 5,

    /// <summary>删除数据库</summary>
    DROP_DB = 6,

    /// <summary>重新加载</summary>
    RELOAD = 7,

    /// <summary>关闭</summary>
    SHUTDOWN = 8,

    /// <summary>统计</summary>
    STATISTICS = 9,

    /// <summary>进程信息</summary>
    PROCESS_INFO = 10,

    /// <summary>连接</summary>
    CONNECT = 11,

    /// <summary>杀死进程</summary>
    PROCESS_KILL = 12,

    /// <summary>调试</summary>
    DEBUG = 13,

    /// <summary>Ping</summary>
    PING = 14,

    /// <summary>时间</summary>
    TIME = 15,

    /// <summary>延迟插入</summary>
    DELAYED_INSERT = 16,

    /// <summary>更改用户</summary>
    CHANGE_USER = 17,

    /// <summary>二进制日志转储</summary>
    BINLOG_DUMP = 18,

    /// <summary>表转储</summary>
    TABLE_DUMP = 19,

    /// <summary>外部连接</summary>
    CONNECT_OUT = 20,

    /// <summary>注册从属</summary>
    REGISTER_SLAVE = 21,

    /// <summary>准备</summary>
    PREPARE = 22,

    /// <summary>执行</summary>
    EXECUTE = 23,

    /// <summary>长数据</summary>
    LONG_DATA = 24,

    /// <summary>关闭语句</summary>
    CLOSE_STMT = 25,

    /// <summary>重置语句</summary>
    RESET_STMT = 26,

    /// <summary>设置选项</summary>
    SET_OPTION = 27,

    /// <summary>获取</summary>
    FETCH = 28
}
