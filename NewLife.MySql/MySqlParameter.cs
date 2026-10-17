using System.Data;
using System.Data.Common;

namespace NewLife.MySql;

/// <summary>参数</summary>
public class MySqlParameter : DbParameter
{
    #region 属性
    /// <summary>参数名</summary>
    public override String? ParameterName { get; set; }

    /// <summary>数值</summary>
    public override Object? Value { get; set; }

    /// <summary>类型</summary>
    public override DbType DbType { get; set; }

    /// <summary>方向</summary>
    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;

    /// <summary>是否允许空</summary>
    public override Boolean IsNullable { get; set; }

    /// <summary>大小</summary>
    public override Int32 Size { get; set; }

    /// <summary>源列</summary>
    public override String? SourceColumn { get; set; }

    /// <summary>映射空</summary>
    public override Boolean SourceColumnNullMapping { get; set; }

    /// <summary>数据行版本。默认 Current，与 ADO.NET 其它提供程序一致；否则 DataAdapter 更新时会因枚举值 0 非法而抛出 DataException</summary>
    public override DataRowVersion SourceVersion { get; set; } = DataRowVersion.Current;
    #endregion

    #region 构造
    /// <summary>实例化参数</summary>
    public MySqlParameter() { }

    /// <summary>实例化参数</summary>
    public MySqlParameter(String name, DbType type)
    {
        ParameterName = name;
        DbType = type;
    }
    #endregion

    #region 方法
    /// <summary>重置类型</summary>
    public override void ResetDbType() => DbType = default;
    #endregion
}