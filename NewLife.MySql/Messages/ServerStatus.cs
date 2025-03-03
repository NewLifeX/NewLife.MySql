namespace NewLife.MySql.Messages;

/// <summary>服务器状态</summary>
[Flags]
public enum ServerStatus : UInt16
{
    /// <summary>事务中</summary>
    InTransaction = 1,

    /// <summary>自动提交模式</summary>
    AutoCommitMode = 2,

    /// <summary>服务器更多结果</summary>
    MoreResults = 4,

    /// <summary>多查询 - 下一个查询存在</summary>
    AnotherQuery = 8, // Multi query - next query exists

    /// <summary>更多结果集</summary>
    BadIndex = 16,

    /// <summary>无索引</summary>
    NoIndex = 32,

    /// <summary>游标存在</summary>
    CursorExists = 64,

    /// <summary>最后一行已发送</summary>
    LastRowSent = 128,

    /// <summary>数据库已被删除</summary>
    DbDropped = 256,

    /// <summary>无反斜杠转义</summary>
    NoBackslashEscapes = 512,

    /// <summary>元数据已更改</summary>
    MetadataChanged = 1024,

    /// <summary>查询速度慢</summary>
    WasSlow = 2048,

    /// <summary>无输出参数</summary>
    OutputParameters = 4096,

    /// <summary>只读事务中</summary>
    InTransactionReadOnly = 8192, // In a read-only transaction

    /// <summary>连接状态信息已更改</summary>
    SessionStateChanged = 16384 // Connection state information has changed
}
