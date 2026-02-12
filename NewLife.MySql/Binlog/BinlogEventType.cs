namespace NewLife.MySql.Binlog;

/// <summary>Binlog 事件类型。MySQL 复制协议中定义的所有事件类型</summary>
public enum BinlogEventType : Byte
{
    /// <summary>未知事件</summary>
    UNKNOWN_EVENT = 0,

    /// <summary>起始事件（v3）</summary>
    START_EVENT_V3 = 1,

    /// <summary>查询事件。包含一条 SQL 语句</summary>
    QUERY_EVENT = 2,

    /// <summary>停止事件。服务器关闭时产生</summary>
    STOP_EVENT = 3,

    /// <summary>日志轮转事件。指向下一个 binlog 文件</summary>
    ROTATE_EVENT = 4,

    /// <summary>整型会话变量事件</summary>
    INTVAR_EVENT = 5,

    /// <summary>加载事件</summary>
    LOAD_EVENT = 6,

    /// <summary>从属事件</summary>
    SLAVE_EVENT = 7,

    /// <summary>新建加载事件</summary>
    CREATE_FILE_EVENT = 8,

    /// <summary>追加块事件</summary>
    APPEND_BLOCK_EVENT = 9,

    /// <summary>执行加载事件</summary>
    EXEC_LOAD_EVENT = 10,

    /// <summary>删除文件事件</summary>
    DELETE_FILE_EVENT = 11,

    /// <summary>新建加载事件（新版）</summary>
    NEW_LOAD_EVENT = 12,

    /// <summary>RAND 种子事件</summary>
    RAND_EVENT = 13,

    /// <summary>用户自定义变量事件</summary>
    USER_VAR_EVENT = 14,

    /// <summary>格式描述事件。描述 binlog 文件格式，每个 binlog 文件开头都有</summary>
    FORMAT_DESCRIPTION_EVENT = 15,

    /// <summary>XID 事件。事务提交时产生，包含 XID</summary>
    XID_EVENT = 16,

    /// <summary>开始加载查询事件</summary>
    BEGIN_LOAD_QUERY_EVENT = 17,

    /// <summary>执行加载查询事件</summary>
    EXECUTE_LOAD_QUERY_EVENT = 18,

    /// <summary>表映射事件。描述行事件对应的表结构元数据</summary>
    TABLE_MAP_EVENT = 19,

    /// <summary>行写入事件（v0，已废弃）</summary>
    PRE_GA_WRITE_ROWS_EVENT = 20,

    /// <summary>行更新事件（v0，已废弃）</summary>
    PRE_GA_UPDATE_ROWS_EVENT = 21,

    /// <summary>行删除事件（v0，已废弃）</summary>
    PRE_GA_DELETE_ROWS_EVENT = 22,

    /// <summary>行写入事件（v1）。INSERT 操作产生的行数据</summary>
    WRITE_ROWS_EVENT_V1 = 23,

    /// <summary>行更新事件（v1）。UPDATE 操作产生的行数据，包含前后镜像</summary>
    UPDATE_ROWS_EVENT_V1 = 24,

    /// <summary>行删除事件（v1）。DELETE 操作产生的行数据</summary>
    DELETE_ROWS_EVENT_V1 = 25,

    /// <summary>事件事件</summary>
    INCIDENT_EVENT = 26,

    /// <summary>心跳日志事件。主库定期发送，不写入 binlog 文件</summary>
    HEARTBEAT_LOG_EVENT = 27,

    /// <summary>忽略的日志事件</summary>
    IGNORABLE_LOG_EVENT = 28,

    /// <summary>行查询日志事件</summary>
    ROWS_QUERY_LOG_EVENT = 29,

    /// <summary>行写入事件（v2）。INSERT 操作产生的行数据</summary>
    WRITE_ROWS_EVENT = 30,

    /// <summary>行更新事件（v2）。UPDATE 操作产生的行数据，包含前后镜像</summary>
    UPDATE_ROWS_EVENT = 31,

    /// <summary>行删除事件（v2）。DELETE 操作产生的行数据</summary>
    DELETE_ROWS_EVENT = 32,

    /// <summary>GTID 日志事件。全局事务标识符</summary>
    GTID_LOG_EVENT = 33,

    /// <summary>匿名 GTID 日志事件</summary>
    ANONYMOUS_GTID_LOG_EVENT = 34,

    /// <summary>上一个 GTID 日志事件</summary>
    PREVIOUS_GTIDS_LOG_EVENT = 35,

    /// <summary>事务上下文事件</summary>
    TRANSACTION_CONTEXT_EVENT = 36,

    /// <summary>视图变更事件</summary>
    VIEW_CHANGE_EVENT = 37,

    /// <summary>XA 准备日志事件</summary>
    XA_PREPARE_LOG_EVENT = 38,

    /// <summary>部分更新行事件</summary>
    PARTIAL_UPDATE_ROWS_EVENT = 39,
}
