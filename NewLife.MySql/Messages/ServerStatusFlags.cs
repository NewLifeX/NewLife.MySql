namespace NewLife.MySql.Messages;

/// <summary>MySQL服务端状态标志</summary>
[Flags]
public enum ServerStatusFlags : UInt16
{
    /// <summary>在事务中</summary>
    SERVER_STATUS_IN_TRANS = 0x0001,

    /// <summary>自动提交模式</summary>
    SERVER_STATUS_AUTOCOMMIT = 0x0002,

    /// <summary>还有更多结果集</summary>
    SERVER_MORE_RESULTS_EXISTS = 0x0008,

    /// <summary>无好的索引使用</summary>
    SERVER_QUERY_NO_GOOD_INDEX_USED = 0x0010,

    /// <summary>无索引使用</summary>
    SERVER_QUERY_NO_INDEX_USED = 0x0020,

    /// <summary>使用了游标</summary>
    SERVER_STATUS_CURSOR_EXISTS = 0x0040,

    /// <summary>最后一行已发送</summary>
    SERVER_STATUS_LAST_ROW_SENT = 0x0080,

    /// <summary>数据库已删除</summary>
    SERVER_STATUS_DB_DROPPED = 0x0100,

    /// <summary>无反斜杠转义</summary>
    SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200,

    /// <summary>元数据已变更</summary>
    SERVER_STATUS_METADATA_CHANGED = 0x0400,

    /// <summary>查询很慢</summary>
    SERVER_QUERY_WAS_SLOW = 0x0800,

    /// <summary>输出参数存在</summary>
    SERVER_PS_OUT_PARAMS = 0x1000,

    /// <summary>在只读事务中</summary>
    SERVER_STATUS_IN_TRANS_READONLY = 0x2000,

    /// <summary>会话状态已变更</summary>
    SERVER_SESSION_STATE_CHANGED = 0x4000,
}
