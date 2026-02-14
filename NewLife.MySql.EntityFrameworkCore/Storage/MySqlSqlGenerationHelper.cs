using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql SQL 生成辅助器。提供 MySQL 特有的 SQL 语法规则（标识符转义、字面量格式化等）</summary>
public class MySqlSqlGenerationHelper : RelationalSqlGenerationHelper
{
    /// <summary>实例化</summary>
    /// <param name="dependencies">SQL 生成辅助器依赖</param>
    public MySqlSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies) : base(dependencies) { }

    /// <summary>语句终结符。MySQL 使用分号</summary>
    public override String StatementTerminator => ";";

    /// <summary>批量语句终结符</summary>
    public override String BatchTerminator => String.Empty;

    /// <summary>生成转义标识符。MySQL 使用反引号</summary>
    /// <param name="identifier">标识符名称</param>
    /// <returns></returns>
    public override String DelimitIdentifier(String identifier) => $"`{EscapeIdentifier(identifier)}`";

    /// <summary>生成转义标识符并写入StringBuilder</summary>
    /// <param name="builder">字符串构建器</param>
    /// <param name="identifier">标识符名称</param>
    public override void DelimitIdentifier(StringBuilder builder, String identifier)
    {
        builder.Append('`');
        EscapeIdentifier(builder, identifier);
        builder.Append('`');
    }

    /// <summary>生成带Schema的转义标识符。MySQL 不使用 Schema，直接返回标识符</summary>
    /// <param name="name">标识符名称</param>
    /// <param name="schema">Schema（忽略）</param>
    /// <returns></returns>
    public override String DelimitIdentifier(String name, String? schema)
        => schema != null ? $"`{EscapeIdentifier(schema)}`.`{EscapeIdentifier(name)}`" : DelimitIdentifier(name);

    /// <summary>生成带Schema的转义标识符并写入StringBuilder</summary>
    /// <param name="builder">字符串构建器</param>
    /// <param name="name">标识符名称</param>
    /// <param name="schema">Schema（忽略）</param>
    public override void DelimitIdentifier(StringBuilder builder, String name, String? schema)
    {
        if (schema != null)
        {
            builder.Append('`');
            EscapeIdentifier(builder, schema);
            builder.Append("`.`");
            EscapeIdentifier(builder, name);
            builder.Append('`');
        }
        else
        {
            DelimitIdentifier(builder, name);
        }
    }

    /// <summary>转义标识符中的特殊字符</summary>
    /// <param name="identifier">标识符</param>
    /// <returns></returns>
    public override String EscapeIdentifier(String identifier) => identifier.Replace("`", "``");

    /// <summary>转义标识符中的特殊字符并写入StringBuilder</summary>
    /// <param name="builder">字符串构建器</param>
    /// <param name="identifier">标识符</param>
    public override void EscapeIdentifier(StringBuilder builder, String identifier) => builder.Append(identifier.Replace("`", "``"));
}
