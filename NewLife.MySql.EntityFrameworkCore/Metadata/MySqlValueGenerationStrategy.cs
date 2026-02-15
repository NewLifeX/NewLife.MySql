namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 值生成策略</summary>
public enum MySqlValueGenerationStrategy
{
    /// <summary>无策略</summary>
    None = 0,

    /// <summary>使用 AUTO_INCREMENT 自增列</summary>
    AutoIncrement = 1,
}
