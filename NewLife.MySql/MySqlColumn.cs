using NewLife.Buffers;
using NewLife.MySql.Common;

namespace NewLife.MySql;

/// <summary>字段</summary>
public class MySqlColumn
{
    /// <summary>目录</summary>
    public String? Catalog { get; set; }

    /// <summary>数据库</summary>
    public String? Database { get; set; }

    /// <summary>表</summary>
    public String? Table { get; set; }

    /// <summary>实际表</summary>
    public String? RealTable { get; set; }

    /// <summary>名称</summary>
    public String Name { get; set; } = null!;

    /// <summary>原始名称</summary>
    public String? OriginalName { get; set; }

    /// <summary>标记</summary>
    public Byte Flag { get; set; }

    /// <summary>字符集</summary>
    public Int16 Charset { get; set; }

    /// <summary>长度</summary>
    public Int32 Length { get; set; }

    /// <summary>数据类型</summary>
    public MySqlDbType Type { get; set; }

    /// <summary>列标志</summary>
    public Int32 ColumnFlags { get; set; }

    /// <summary>小数位数</summary>
    public Byte Scale { get; set; }

    /// <summary>从 SpanReader 读取列定义数据并填充属性</summary>
    /// <param name="reader">数据读取器</param>
    public void Read(ref SpanReader reader)
    {
        Catalog = reader.ReadString();
        Database = reader.ReadString();
        Table = reader.ReadString();
        RealTable = reader.ReadString();
        Name = reader.ReadString();
        OriginalName = reader.ReadString();
        Flag = reader.ReadByte();
        Charset = reader.ReadInt16();
        Length = reader.ReadInt32();
        Type = (MySqlDbType)reader.ReadByte();
        ColumnFlags = reader.ReadInt16();
        Scale = reader.ReadByte();
    }

    /// <summary>已重载</summary>
    public override String ToString() => $"{Name} {Type}({Length})";
}
