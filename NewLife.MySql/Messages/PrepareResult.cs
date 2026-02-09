namespace NewLife.MySql.Messages;

/// <summary>预编译结果。COM_STMT_PREPARE 响应的结构化返回</summary>
/// <param name="StatementId">预编译语句 ID</param>
/// <param name="Columns">参数列信息数组</param>
public record PrepareResult(Int32 StatementId, MySqlColumn[] Columns);
