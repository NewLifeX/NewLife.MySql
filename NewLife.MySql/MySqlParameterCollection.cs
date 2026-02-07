using System.Collections;
using System.Data.Common;

namespace NewLife.MySql;

/// <summary>参数集合</summary>
public class MySqlParameterCollection : DbParameterCollection
{
    #region 属性
    private readonly List<MySqlParameter> _items = [];

    /// <summary>同步根</summary>
    public override Object SyncRoot { get; } = new Object();

    /// <summary>参数数量</summary>
    public override Int32 Count => _items.Count;

    /// <summary>是否固定大小</summary>
    public override Boolean IsFixedSize => false;

    /// <summary>是否只读</summary>
    public override Boolean IsReadOnly => false;

    /// <summary>是否同步</summary>
    public override Boolean IsSynchronized => false;
    #endregion

    #region 方法
    /// <summary>添加参数</summary>
    /// <param name="value">参数对象</param>
    /// <returns></returns>
    public override Int32 Add(Object value)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));

        var param = value as MySqlParameter ?? throw new ArgumentException("参数必须是 MySqlParameter 类型", nameof(value));
        _items.Add(param);
        return _items.Count - 1;
    }

    /// <summary>添加参数</summary>
    /// <param name="parameterName">参数名</param>
    /// <param name="value">参数值</param>
    /// <returns></returns>
    public MySqlParameter AddWithValue(String parameterName, Object? value)
    {
        var param = new MySqlParameter { ParameterName = parameterName, Value = value };
        _items.Add(param);
        return param;
    }

    /// <summary>批量添加参数</summary>
    /// <param name="values">参数数组</param>
    public override void AddRange(Array values)
    {
        if (values == null) throw new ArgumentNullException(nameof(values));

        foreach (var item in values)
        {
            Add(item);
        }
    }

    /// <summary>清空参数</summary>
    public override void Clear() => _items.Clear();

    /// <summary>是否包含参数</summary>
    /// <param name="value">参数对象</param>
    /// <returns></returns>
    public override Boolean Contains(Object value) => _items.Contains(value as MySqlParameter);

    /// <summary>是否包含指定名称的参数</summary>
    /// <param name="parameterName">参数名</param>
    /// <returns></returns>
    public override Boolean Contains(String parameterName) => IndexOf(parameterName) >= 0;

    /// <summary>复制到数组</summary>
    /// <param name="array">目标数组</param>
    /// <param name="index">起始索引</param>
    public override void CopyTo(Array array, Int32 index) => ((ICollection)_items).CopyTo(array, index);

    /// <summary>获取枚举器</summary>
    /// <returns></returns>
    public override IEnumerator GetEnumerator() => _items.GetEnumerator();

    /// <summary>获取参数索引</summary>
    /// <param name="parameterName">参数名</param>
    /// <returns></returns>
    public override Int32 IndexOf(String parameterName)
    {
        for (var i = 0; i < _items.Count; i++)
        {
            if (_items[i].ParameterName.EqualIgnoreCase(parameterName)) return i;
        }
        return -1;
    }

    /// <summary>获取参数索引</summary>
    /// <param name="value">参数对象</param>
    /// <returns></returns>
    public override Int32 IndexOf(Object value) => _items.IndexOf(value as MySqlParameter);

    /// <summary>插入参数</summary>
    /// <param name="index">索引</param>
    /// <param name="value">参数对象</param>
    public override void Insert(Int32 index, Object value)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));

        var param = value as MySqlParameter ?? throw new ArgumentException("参数必须是 MySqlParameter 类型", nameof(value));
        _items.Insert(index, param);
    }

    /// <summary>移除参数</summary>
    /// <param name="value">参数对象</param>
    public override void Remove(Object value) => _items.Remove(value as MySqlParameter);

    /// <summary>移除指定索引处的参数</summary>
    /// <param name="index">索引</param>
    public override void RemoveAt(Int32 index) => _items.RemoveAt(index);

    /// <summary>移除指定名称的参数</summary>
    /// <param name="parameterName">参数名</param>
    public override void RemoveAt(String parameterName)
    {
        var idx = IndexOf(parameterName);
        if (idx >= 0) _items.RemoveAt(idx);
    }
    #endregion

    #region 索引
    /// <summary>获取指定索引处的参数</summary>
    /// <param name="index">索引</param>
    /// <returns></returns>
    protected override DbParameter GetParameter(Int32 index) => _items[index];

    /// <summary>获取指定名称的参数</summary>
    /// <param name="parameterName">参数名</param>
    /// <returns></returns>
    protected override DbParameter GetParameter(String parameterName)
    {
        var idx = IndexOf(parameterName);
        if (idx < 0) throw new ArgumentException($"未找到参数 {parameterName}", nameof(parameterName));
        return _items[idx];
    }

    /// <summary>设置指定索引处的参数</summary>
    /// <param name="index">索引</param>
    /// <param name="value">参数对象</param>
    protected override void SetParameter(Int32 index, DbParameter value)
    {
        if (value is not MySqlParameter param) throw new ArgumentException("参数必须是 MySqlParameter 类型", nameof(value));
        _items[index] = param;
    }

    /// <summary>设置指定名称的参数</summary>
    /// <param name="parameterName">参数名</param>
    /// <param name="value">参数对象</param>
    protected override void SetParameter(String parameterName, DbParameter value)
    {
        var idx = IndexOf(parameterName);
        if (idx < 0) throw new ArgumentException($"未找到参数 {parameterName}", nameof(parameterName));
        SetParameter(idx, value);
    }
    #endregion
}
