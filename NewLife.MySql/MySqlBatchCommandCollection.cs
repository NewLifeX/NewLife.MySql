#if NET6_0_OR_GREATER
using System.Collections;
using System.Data.Common;

namespace NewLife.MySql;

/// <summary>批量命令集合</summary>
public class MySqlBatchCommandCollection : DbBatchCommandCollection
{
    private readonly List<MySqlBatchCommand> _items = [];

    /// <summary>命令数量</summary>
    public override Int32 Count => _items.Count;

    /// <summary>是否只读</summary>
    public override Boolean IsReadOnly => false;

    /// <summary>索引器</summary>
    /// <param name="index">索引</param>
    /// <returns></returns>
    public new MySqlBatchCommand this[Int32 index]
    {
        get => _items[index];
        set => _items[index] = value;
    }

    /// <summary>添加命令</summary>
    /// <param name="item">命令对象</param>
    public override void Add(DbBatchCommand item) => _items.Add((MySqlBatchCommand)item);

    /// <summary>清空</summary>
    public override void Clear() => _items.Clear();

    /// <summary>是否包含</summary>
    /// <param name="item">命令对象</param>
    /// <returns></returns>
    public override Boolean Contains(DbBatchCommand item) => _items.Contains((MySqlBatchCommand)item);

    /// <summary>复制到数组</summary>
    /// <param name="array">目标数组</param>
    /// <param name="arrayIndex">起始索引</param>
    public override void CopyTo(DbBatchCommand[] array, Int32 arrayIndex)
    {
        for (var i = 0; i < _items.Count; i++)
            array[arrayIndex + i] = _items[i];
    }

    /// <summary>获取枚举器</summary>
    /// <returns></returns>
    public override IEnumerator<DbBatchCommand> GetEnumerator() => _items.GetEnumerator();

    /// <summary>获取指定索引的命令</summary>
    /// <param name="index">索引</param>
    /// <returns></returns>
    protected override DbBatchCommand GetBatchCommand(Int32 index) => _items[index];

    /// <summary>获取命令索引</summary>
    /// <param name="item">命令对象</param>
    /// <returns></returns>
    public override Int32 IndexOf(DbBatchCommand item) => _items.IndexOf((MySqlBatchCommand)item);

    /// <summary>插入命令</summary>
    /// <param name="index">索引</param>
    /// <param name="item">命令对象</param>
    public override void Insert(Int32 index, DbBatchCommand item) => _items.Insert(index, (MySqlBatchCommand)item);

    /// <summary>移除命令</summary>
    /// <param name="item">命令对象</param>
    /// <returns></returns>
    public override Boolean Remove(DbBatchCommand item) => _items.Remove((MySqlBatchCommand)item);

    /// <summary>移除指定索引处的命令</summary>
    /// <param name="index">索引</param>
    public override void RemoveAt(Int32 index) => _items.RemoveAt(index);

    /// <summary>设置指定索引的命令</summary>
    /// <param name="index">索引</param>
    /// <param name="batchCommand">命令对象</param>
    protected override void SetBatchCommand(Int32 index, DbBatchCommand batchCommand) => _items[index] = (MySqlBatchCommand)batchCommand;
}
#endif
