using System.ComponentModel;
using System.Data;
using System.Data.Common;
using NewLife.MySql;

namespace UnitTest;

/// <summary>MySqlParameterCollection 操作测试</summary>
[Collection(TestCollections.InMemory)]
public class MySqlParameterCollectionTests
{
    [Fact]
    [DisplayName("AddWithValue添加参数后Count正确")]
    public void WhenAddWithValueThenCountIncreases()
    {
        var ps = new MySqlParameterCollection();

        ps.AddWithValue("name", "test");
        ps.AddWithValue("age", 18);

        Assert.Equal(2, ps.Count);
    }

    [Fact]
    [DisplayName("Add添加MySqlParameter对象")]
    public void WhenAddMySqlParameterThenSucceeds()
    {
        var ps = new MySqlParameterCollection();
        var param = new MySqlParameter { ParameterName = "id", Value = 42 };

        var idx = ps.Add(param);

        Assert.Equal(0, idx);
        Assert.Equal(1, ps.Count);
    }

    [Fact]
    [DisplayName("Add传入null抛出ArgumentNullException")]
    public void WhenAddNullThenThrows()
    {
        var ps = new MySqlParameterCollection();

        Assert.Throws<ArgumentNullException>(() => ps.Add(null!));
    }

    [Fact]
    [DisplayName("Add传入非MySqlParameter类型抛出ArgumentException")]
    public void WhenAddWrongTypeThenThrows()
    {
        var ps = new MySqlParameterCollection();

        Assert.Throws<ArgumentException>(() => ps.Add("not a parameter"));
    }

    [Fact]
    [DisplayName("Clear清空所有参数")]
    public void WhenClearThenCountIsZero()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("a", 1);
        ps.AddWithValue("b", 2);

        ps.Clear();

        Assert.Equal(0, ps.Count);
    }

    [Fact]
    [DisplayName("Contains按对象查找参数")]
    public void WhenContainsMySqlParameterThenReturnsTrue()
    {
        var ps = new MySqlParameterCollection();
        var param = new MySqlParameter { ParameterName = "id", Value = 42 };
        ps.Add(param);

        Assert.True(ps.Contains(param));
    }

    [Fact]
    [DisplayName("Contains按名称查找参数")]
    public void WhenContainsByNameThenReturnsTrue()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("name", "test");

        Assert.True(ps.Contains("name"));
        Assert.False(ps.Contains("notexist"));
    }

    [Fact]
    [DisplayName("IndexOf按名称查找参数索引")]
    public void WhenIndexOfByNameThenReturnsCorrectIndex()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("first", 1);
        ps.AddWithValue("second", 2);

        Assert.Equal(0, ps.IndexOf("first"));
        Assert.Equal(1, ps.IndexOf("second"));
        Assert.Equal(-1, ps.IndexOf("notexist"));
    }

    [Fact]
    [DisplayName("IndexOf按对象查找参数索引")]
    public void WhenIndexOfByObjectThenReturnsCorrectIndex()
    {
        var ps = new MySqlParameterCollection();
        var p1 = ps.AddWithValue("first", 1);
        var p2 = ps.AddWithValue("second", 2);

        Assert.Equal(0, ps.IndexOf((Object)p1));
        Assert.Equal(1, ps.IndexOf((Object)p2));
    }

    [Fact]
    [DisplayName("Insert在指定位置插入参数")]
    public void WhenInsertThenParameterAtCorrectPosition()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("first", 1);
        ps.AddWithValue("third", 3);

        var param = new MySqlParameter { ParameterName = "second", Value = 2 };
        ps.Insert(1, param);

        Assert.Equal(3, ps.Count);
        Assert.Equal("second", ((MySqlParameter)ps[1]).ParameterName);
    }

    [Fact]
    [DisplayName("Insert传入null抛出ArgumentNullException")]
    public void WhenInsertNullThenThrows()
    {
        var ps = new MySqlParameterCollection();

        Assert.Throws<ArgumentNullException>(() => ps.Insert(0, null!));
    }

    [Fact]
    [DisplayName("Remove移除指定参数")]
    public void WhenRemoveThenCountDecreases()
    {
        var ps = new MySqlParameterCollection();
        var param = ps.AddWithValue("name", "test");

        ps.Remove(param);

        Assert.Equal(0, ps.Count);
    }

    [Fact]
    [DisplayName("RemoveAt按索引移除参数")]
    public void WhenRemoveAtIndexThenCorrectParamRemoved()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("first", 1);
        ps.AddWithValue("second", 2);

        ps.RemoveAt(0);

        Assert.Equal(1, ps.Count);
        Assert.Equal("second", ((MySqlParameter)ps[0]).ParameterName);
    }

    [Fact]
    [DisplayName("RemoveAt按名称移除参数")]
    public void WhenRemoveAtNameThenCorrectParamRemoved()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("first", 1);
        ps.AddWithValue("second", 2);

        ps.RemoveAt("first");

        Assert.Equal(1, ps.Count);
        Assert.Equal("second", ((MySqlParameter)ps[0]).ParameterName);
    }

    [Fact]
    [DisplayName("RemoveAt不存在的名称不抛异常")]
    public void WhenRemoveAtNonexistentNameThenNoError()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("name", "test");

        ps.RemoveAt("notexist");

        Assert.Equal(1, ps.Count);
    }

    [Fact]
    [DisplayName("按名称获取参数不存在时抛出ArgumentException")]
    public void WhenGetByNonexistentNameThenThrows()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("name", "test");

        Assert.Throws<ArgumentException>(() => { var _ = ps["notexist"]; });
    }

    [Fact]
    [DisplayName("通过索引器按索引获取和设置参数")]
    public void WhenSetParameterByIndexThenValueUpdated()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("name", "old");

        ps[0] = new MySqlParameter { ParameterName = "name", Value = "new" };

        Assert.Equal("new", ((MySqlParameter)ps[0]).Value);
    }

    [Fact]
    [DisplayName("AddRange批量添加参数")]
    public void WhenAddRangeThenAllAdded()
    {
        var ps = new MySqlParameterCollection();
        var arr = new MySqlParameter[]
        {
            new() { ParameterName = "a", Value = 1 },
            new() { ParameterName = "b", Value = 2 },
        };

        ps.AddRange(arr);

        Assert.Equal(2, ps.Count);
    }

    [Fact]
    [DisplayName("GetEnumerator遍历所有参数")]
    public void WhenEnumerateThenAllParamsReturned()
    {
        var ps = new MySqlParameterCollection();
        ps.AddWithValue("a", 1);
        ps.AddWithValue("b", 2);

        var names = new List<String>();
        foreach (MySqlParameter p in ps)
        {
            names.Add(p.ParameterName!);
        }

        Assert.Equal(["a", "b"], names);
    }

    [Fact]
    [DisplayName("IsFixedSize和IsReadOnly返回false")]
    public void WhenCheckPropertiesThenCorrectValues()
    {
        var ps = new MySqlParameterCollection();

        Assert.False(ps.IsFixedSize);
        Assert.False(ps.IsReadOnly);
        Assert.False(ps.IsSynchronized);
        Assert.NotNull(ps.SyncRoot);
    }
}
