namespace NewLife.MySql.Binlog;

/// <summary>Binlog 事件。MySQL 复制协议中的单个事件，包含事件头和数据体</summary>
public class BinlogEvent
{
    #region 属性
    /// <summary>事件时间戳。Unix 秒级时间戳</summary>
    public UInt32 Timestamp { get; set; }

    /// <summary>事件类型</summary>
    public BinlogEventType EventType { get; set; }

    /// <summary>产生事件的服务器 ID</summary>
    public UInt32 ServerId { get; set; }

    /// <summary>事件数据长度（含事件头）</summary>
    public UInt32 EventLength { get; set; }

    /// <summary>下一个事件在 binlog 文件中的偏移位置</summary>
    public UInt32 NextPosition { get; set; }

    /// <summary>事件标志</summary>
    public UInt16 Flags { get; set; }

    /// <summary>事件数据体（不含19字节事件头）</summary>
    public Byte[]? Data { get; set; }

    /// <summary>事件发生时间</summary>
    public DateTime EventTime => new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(Timestamp).ToLocalTime();
    #endregion

    #region 行事件属性
    /// <summary>表 ID。行事件（TABLE_MAP / WRITE_ROWS / UPDATE_ROWS / DELETE_ROWS）专用</summary>
    public UInt64 TableId { get; set; }

    /// <summary>数据库名。TABLE_MAP 事件中解析</summary>
    public String? DatabaseName { get; set; }

    /// <summary>表名。TABLE_MAP 事件中解析</summary>
    public String? TableName { get; set; }

    /// <summary>列数量。TABLE_MAP 事件中解析</summary>
    public Int32 ColumnCount { get; set; }

    /// <summary>列类型数组。TABLE_MAP 事件中解析</summary>
    public Byte[]? ColumnTypes { get; set; }

    /// <summary>行数据。INSERT/DELETE 行事件中解析，每个元素代表一行，每行是列值数组</summary>
    public IList<Object?[]>? Rows { get; set; }

    /// <summary>更新前行数据。UPDATE 行事件专用，与 Rows 一一对应</summary>
    public IList<Object?[]>? BeforeRows { get; set; }
    #endregion

    #region 旋转事件属性
    /// <summary>下一个 binlog 文件名。ROTATE_EVENT 中解析</summary>
    public String? NextBinlogFile { get; set; }

    /// <summary>下一个 binlog 文件起始偏移。ROTATE_EVENT 中解析</summary>
    public UInt64 NextBinlogPosition { get; set; }
    #endregion

    #region 查询事件属性
    /// <summary>SQL 语句。QUERY_EVENT 中解析</summary>
    public String? Query { get; set; }

    /// <summary>查询所在数据库。QUERY_EVENT 中解析</summary>
    public String? QueryDatabase { get; set; }
    #endregion

    /// <summary>已重写</summary>
    public override String ToString()
    {
        var str = $"{EventType} ServerId={ServerId} Len={EventLength} NextPos={NextPosition}";
        if (!TableName.IsNullOrEmpty()) str += $" Table={DatabaseName}.{TableName}";
        if (!Query.IsNullOrEmpty()) str += $" SQL={Query}";
        if (!NextBinlogFile.IsNullOrEmpty()) str += $" NextFile={NextBinlogFile}:{NextBinlogPosition}";
        return str;
    }
}
