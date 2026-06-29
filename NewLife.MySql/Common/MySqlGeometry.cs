namespace NewLife.MySql.Common;

/// <summary>MySQL 几何数据类型。封装 WKB（Well-Known Binary）格式的几何数据</summary>
/// <remarks>
/// MySQL 使用 WKB 格式存储和传输几何数据（Geometry），遵循 OGC Simple Feature Access 标准。
/// 支持的类型包括 POINT、LINESTRING、POLYGON、MULTIPOINT、MULTILINESTRING、MULTIPOLYGON、GEOMETRYCOLLECTION 等。
/// <para>
/// 使用示例：
/// <code>
/// // 从 WKB 字节创建
/// var geom = new MySqlGeometry(wkbBytes);
///
/// // 直接赋值给参数
/// cmd.Parameters.AddWithValue("@geo", geom);
///
/// // 从 DataReader 读取
/// var geo = (MySqlGeometry)reader.GetValue(0);
/// var bytes = geo.Value; // 获取 WKB 字节
/// </code>
/// </para>
/// </remarks>
public class MySqlGeometry
{
    /// <summary>WKB 字节数据</summary>
    public Byte[] Value { get; }

    /// <summary>从 WKB 字节数组创建几何对象</summary>
    /// <param name="wkb">WKB 格式的字节数据</param>
    public MySqlGeometry(Byte[] wkb) => Value = wkb ?? [];

    /// <summary>从 WKB 字节数组隐式转换为 MySqlGeometry</summary>
    /// <param name="wkb">WKB 字节</param>
    public static implicit operator MySqlGeometry(Byte[] wkb) => new(wkb);

    /// <summary>从 MySqlGeometry 隐式转换为 Byte[]</summary>
    /// <param name="geometry">几何对象</param>
    public static implicit operator Byte[](MySqlGeometry geometry) => geometry.Value;

    /// <summary>返回 WKB 十六进制字符串，用于调试和日志</summary>
    /// <returns>十六进制表示的 WKB 数据</returns>
    public override String ToString() => Value.ToHex();

    /// <summary>比较两个几何对象是否相等</summary>
    /// <param name="obj">要比较的对象</param>
    /// <returns>如果 WKB 字节序列相同则返回 true</returns>
    public override Boolean Equals(Object? obj)
    {
        if (obj is MySqlGeometry other)
            return Value.SequenceEqual(other.Value);
        if (obj is Byte[] bytes)
            return Value.SequenceEqual(bytes);
        return false;
    }

    /// <summary>获取哈希码</summary>
    /// <returns>基于 WKB 数据计算的哈希码</returns>
    public override Int32 GetHashCode()
    {
        var hash = 17;
        foreach (var b in Value)
            hash = hash * 31 + b;
        return hash;
    }
}
