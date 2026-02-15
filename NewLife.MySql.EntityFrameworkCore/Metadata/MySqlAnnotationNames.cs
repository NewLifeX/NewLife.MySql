namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 注解名称常量</summary>
public static class MySqlAnnotationNames
{
    /// <summary>注解前缀</summary>
    public const String Prefix = "MySql:";

    /// <summary>值生成策略注解</summary>
    public const String ValueGenerationStrategy = Prefix + "ValueGenerationStrategy";

    /// <summary>字符集注解</summary>
    public const String CharSet = Prefix + "CharSet";

    /// <summary>排序规则注解</summary>
    public const String Collation = Prefix + "Collation";

    /// <summary>存储引擎注解</summary>
    public const String Engine = Prefix + "Engine";
}
