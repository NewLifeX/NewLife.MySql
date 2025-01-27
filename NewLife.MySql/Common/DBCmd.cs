namespace NewLife.MySql.Common;

/// <summary>命令</summary>
enum DbCmd : Byte
{
    SLEEP = 0,
    QUIT = 1,
    INIT_DB = 2,
    QUERY = 3,
    FIELD_LIST = 4,
    CREATE_DB = 5,
    DROP_DB = 6,
    RELOAD = 7,
    SHUTDOWN = 8,
    STATISTICS = 9,
    PROCESS_INFO = 10,
    CONNECT = 11,
    PROCESS_KILL = 12,
    DEBUG = 13,
    PING = 14,
    TIME = 15,
    DELAYED_INSERT = 16,
    CHANGE_USER = 17,
    BINLOG_DUMP = 18,
    TABLE_DUMP = 19,
    CONNECT_OUT = 20,
    REGISTER_SLAVE = 21,
    PREPARE = 22,
    EXECUTE = 23,
    LONG_DATA = 24,
    CLOSE_STMT = 25,
    RESET_STMT = 26,
    SET_OPTION = 27,
    FETCH = 28
}