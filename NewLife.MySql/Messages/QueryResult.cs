namespace NewLife.MySql.Messages;

/// <summary>查询结果。解析OK包或结果集头部后的结构化返回</summary>
/// <param name="FieldCount">结果集列数，0表示无结果集（如INSERT/UPDATE/DELETE）</param>
/// <param name="AffectedRows">影响行数</param>
/// <param name="InsertedId">最后插入ID</param>
/// <param name="StatusFlags">服务端状态标志，用于判断是否有更多结果集</param>
/// <param name="Warnings">警告数量</param>
public record QueryResult(Int32 FieldCount, Int32 AffectedRows, Int64 InsertedId, ServerStatusFlags StatusFlags, UInt16 Warnings)
{
    /// <summary>是否有更多结果集</summary>
    public Boolean HasMoreResults => StatusFlags.HasFlag(ServerStatusFlags.SERVER_MORE_RESULTS_EXISTS);
}

/// <summary>行读取结果。读取一行数据或到达EOF后的结构化返回</summary>
/// <param name="HasRow">是否读到了数据行。false表示到达EOF</param>
/// <param name="StatusFlags">EOF时的服务端状态标志</param>
/// <param name="Warnings">警告数量</param>
public record RowResult(Boolean HasRow, ServerStatusFlags StatusFlags, UInt16 Warnings)
{
    /// <summary>是否有更多结果集</summary>
    public Boolean HasMoreResults => StatusFlags.HasFlag(ServerStatusFlags.SERVER_MORE_RESULTS_EXISTS);
}
