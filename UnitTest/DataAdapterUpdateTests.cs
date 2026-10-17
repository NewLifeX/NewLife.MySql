using System.ComponentModel;
using System.Data;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>DataAdapter 更新与异步 API 集成测试。验证 FillAsync/FillSchema/Update 与行事件</summary>
[Collection(TestCollections.DataModification)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class DataAdapterUpdateTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public DataAdapterUpdateTests()
    {
        _table = "adapter_upd_" + Rand.Next(10000);
        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();
        // 本类包含“连接关闭后 Update 自动打开”用例，使用普通表保证跨会话可见
        _conn.ExecuteNonQuery($@"CREATE TABLE IF NOT EXISTS `{_table}` (
            `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `name` VARCHAR(128) NOT NULL,
            `age` INT DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name, age) VALUES ('Alice', 25), ('Bob', 30)");
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    [Fact]
    [DisplayName("FillAsync异步填充DataTable")]
    public async Task WhenFillAsyncThenPopulated()
    {
        using var cmd = new MySqlCommand(_conn, $"SELECT name, age FROM `{_table}` ORDER BY id");
        var adapter = new MySqlDataAdapter(cmd);

        var dt = new DataTable();
        var rowCount = await adapter.FillAsync(dt);

        Assert.Equal(2, rowCount);
        Assert.Equal("Alice", dt.Rows[0]["name"]?.ToString());
    }

    [Fact]
    [DisplayName("FillAsync支持取消令牌")]
    public async Task WhenFillAsyncCanceledThenThrows()
    {
        using var cmd = new MySqlCommand(_conn, $"SELECT name FROM `{_table}`");
        var adapter = new MySqlDataAdapter(cmd);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.FillAsync(new DataTable(), cts.Token));
    }

    [Fact]
    [DisplayName("FillSchema返回列架构信息")]
    public void WhenFillSchemaThenColumnTypesMapped()
    {
        using var cmd = new MySqlCommand(_conn, $"SELECT id, name, age FROM `{_table}`");
        var adapter = new MySqlDataAdapter(cmd);

        var dt = new DataTable();
        var schema = adapter.FillSchema(dt, SchemaType.Source);

        Assert.Equal(3, schema.Columns.Count);
        Assert.Equal(typeof(Int32), dt.Columns["id"]!.DataType);
        Assert.Equal(typeof(String), dt.Columns["name"]!.DataType);
        Assert.Equal(typeof(Int32), dt.Columns["age"]!.DataType);
    }

    [Fact]
    [DisplayName("FillSchemaAsync返回列架构信息")]
    public async Task WhenFillSchemaAsyncThenColumnTypesMapped()
    {
        using var cmd = new MySqlCommand(_conn, $"SELECT id, name, age FROM `{_table}`");
        var adapter = new MySqlDataAdapter(cmd);

        var dt = new DataTable();
        var schema = await adapter.FillSchemaAsync(dt, SchemaType.Source);

        Assert.Equal(3, schema.Columns.Count);
        Assert.Equal(typeof(String), dt.Columns["name"]!.DataType);
    }

    [Fact]
    [DisplayName("UpdateAsync异步插入新增行")]
    public async Task WhenUpdateAsyncAddedRowThenInserted()
    {
        using var select = new MySqlCommand($"SELECT id, name, age FROM `{_table}` ORDER BY id", _conn);
        var adapter = new MySqlDataAdapter(select);
        var insert = new MySqlCommand($"INSERT INTO `{_table}` (name, age) VALUES (@name, @age)", _conn);
        insert.Parameters.AddWithValue("@name", "").SourceColumn = "name";
        insert.Parameters.AddWithValue("@age", 0).SourceColumn = "age";
        adapter.InsertCommand = insert;

        var dt = new DataTable();
        adapter.Fill(dt);
        var row = dt.NewRow();
        row["name"] = "Frank";
        row["age"] = 44;
        dt.Rows.Add(row);

        var count = await adapter.UpdateAsync(dt);

        Assert.Equal(1, count);
        using var verify = new MySqlCommand(_conn, $"SELECT age FROM `{_table}` WHERE name='Frank'");
        Assert.Equal(44, verify.ExecuteScalar().ToInt());
    }

    [Fact]
    [DisplayName("Update插入新增行并触发行事件")]
    public void WhenUpdateAddedRowThenInsertedAndEventsFired()
    {
        using var select = new MySqlCommand($"SELECT id, name, age FROM `{_table}` ORDER BY id", _conn);
        var adapter = new MySqlDataAdapter(select);
        var insert = new MySqlCommand($"INSERT INTO `{_table}` (name, age) VALUES (@name, @age)", _conn);
        insert.Parameters.AddWithValue("@name", "").SourceColumn = "name";
        insert.Parameters.AddWithValue("@age", 0).SourceColumn = "age";
        adapter.InsertCommand = insert;

        var updating = 0;
        var updated = 0;
        adapter.RowUpdating += (s, e) =>
        {
            updating++;
            Assert.Equal(StatementType.Insert, e.StatementType);
            Assert.NotNull(e.Command);
        };
        adapter.RowUpdated += (s, e) =>
        {
            updated++;
            Assert.Equal(1, e.RecordsAffected);
        };

        var dt = new DataTable();
        adapter.Fill(dt);
        var row = dt.NewRow();
        row["name"] = "Dave";
        row["age"] = 40;
        dt.Rows.Add(row);

        var count = adapter.Update(dt);

        Assert.Equal(1, count);
        Assert.Equal(1, updating);
        Assert.Equal(1, updated);
        using var verify = new MySqlCommand(_conn, $"SELECT age FROM `{_table}` WHERE name='Dave'");
        Assert.Equal(40, verify.ExecuteScalar().ToInt());
    }

    [Fact]
    [DisplayName("Update保存修改行与删除行")]
    public void WhenUpdateModifiedAndDeletedRowsThenPersisted()
    {
        using var select = new MySqlCommand($"SELECT id, name, age FROM `{_table}` ORDER BY id", _conn);
        var adapter = new MySqlDataAdapter(select);

        var update = new MySqlCommand($"UPDATE `{_table}` SET age=@age WHERE name=@name", _conn);
        update.Parameters.AddWithValue("@age", 0).SourceColumn = "age";
        update.Parameters.AddWithValue("@name", "").SourceColumn = "name";
        update.Parameters["@name"]!.SourceVersion = DataRowVersion.Original;
        adapter.UpdateCommand = update;

        var delete = new MySqlCommand($"DELETE FROM `{_table}` WHERE name=@name", _conn);
        delete.Parameters.AddWithValue("@name", "").SourceColumn = "name";
        delete.Parameters["@name"]!.SourceVersion = DataRowVersion.Original;
        adapter.DeleteCommand = delete;

        var dt = new DataTable();
        adapter.Fill(dt);

        dt.Rows[0]["age"] = 26;
        dt.Rows[1].Delete();

        var count = adapter.Update(dt);

        Assert.Equal(2, count);
        using var verifyAge = new MySqlCommand(_conn, $"SELECT age FROM `{_table}` WHERE name='Alice'");
        Assert.Equal(26, verifyAge.ExecuteScalar().ToInt());
        using var verifyCount = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}` WHERE name='Bob'");
        Assert.Equal(0, verifyCount.ExecuteScalar().ToInt());
    }

    [Fact]
    [DisplayName("Update自动打开并关闭已关闭的连接")]
    public void WhenUpdateWithClosedConnectionThenAutoOpenAndClose()
    {
        var dt = new DataTable();
        dt.Columns.Add("name", typeof(String));
        dt.Columns.Add("age", typeof(Int32));
        dt.Rows.Add("Eve", 33);

        var insert = new MySqlCommand($"INSERT INTO `{_table}` (name, age) VALUES (@name, @age)", _conn);
        insert.Parameters.AddWithValue("@name", "").SourceColumn = "name";
        insert.Parameters.AddWithValue("@age", 0).SourceColumn = "age";
        var adapter = new MySqlDataAdapter { InsertCommand = insert };

        _conn.Close();
        try
        {
            Assert.Equal(1, adapter.Update(dt));
            Assert.Equal(ConnectionState.Closed, _conn.State);
        }
        finally
        {
            // 恢复连接，Dispose 需要会话内可见的临时表
            _conn.Open();
        }

        using var verify = new MySqlCommand(_conn, $"SELECT COUNT(*) FROM `{_table}` WHERE name='Eve'");
        Assert.Equal(1, verify.ExecuteScalar().ToInt());
    }
}
