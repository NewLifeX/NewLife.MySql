#if NET6_0_OR_GREATER
using System.Data;
using System.Data.Common;

namespace NewLife.MySql;

/// <summary>批量命令中的单个命令项</summary>
public class MySqlBatchCommand : DbBatchCommand
{
    /// <summary>命令语句</summary>
    public override String CommandText { get; set; } = "";

    /// <summary>命令类型</summary>
    public override CommandType CommandType { get; set; }

    private readonly MySqlParameterCollection _parameters = new();

    /// <summary>参数集合</summary>
    protected override DbParameterCollection DbParameterCollection => _parameters;

    /// <summary>参数集合</summary>
    public new MySqlParameterCollection Parameters => _parameters;

    /// <summary>影响行数</summary>
    public override Int32 RecordsAffected { get; }

    /// <summary>实例化</summary>
    public MySqlBatchCommand() { }

    /// <summary>使用 SQL 语句实例化</summary>
    /// <param name="commandText">SQL 语句</param>
    public MySqlBatchCommand(String commandText) => CommandText = commandText;
}
#endif
