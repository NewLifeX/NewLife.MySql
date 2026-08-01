using System.ComponentModel;
using NewLife;
using NewLife.MySql;
using NewLife.Security;

namespace UnitTest;

/// <summary>MySqlDataReader.ReadModelsAsync 实体映射测试。验证列到属性映射、类型转换与 DBNull 处理</summary>
[Collection(TestCollections.DataModification)]
[TestCaseOrderer("NewLife.UnitTest.DefaultOrderer", "NewLife.UnitTest")]
public class ReadModelsAsyncTests : IDisposable
{
    private static String _ConnStr = DALTests.GetConnStr();
    private readonly String _table;
    private readonly MySqlConnection _conn;

    public ReadModelsAsyncTests()
    {
        _table = "model_test_" + Rand.Next(10000);
        _conn = new MySqlConnection(_ConnStr);
        _conn.Open();

        _conn.ExecuteNonQuery($@"CREATE TABLE IF NOT EXISTS `{_table}` (
            `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `name` VARCHAR(64) DEFAULT NULL,
            `age` INT DEFAULT NULL,
            `score` DECIMAL(10,2) DEFAULT NULL,
            `created` DATETIME DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    }

    public void Dispose()
    {
        _conn.ExecuteNonQuery($"DROP TABLE IF EXISTS `{_table}`");
        _conn.Dispose();
    }

    [Fact]
    [DisplayName("ReadModelsAsync映射到普通POCO")]
    public async Task MapToPoco()
    {
        var created = new DateTime(2026, 8, 1, 10, 30, 45);
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name, age, score, created) VALUES ('张三', 30, 99.50, '{created:yyyy-MM-dd HH:mm:ss}')");

        using var cmd = new MySqlCommand(_conn, $"SELECT id, name, age, score, created FROM `{_table}` LIMIT 1");
        using var dr = (MySqlDataReader)await cmd.ExecuteReaderAsync();

        var list = await dr.ReadModelsAsync<TestModel>();

        var model = Assert.Single(list);
        Assert.Equal("张三", model.Name);
        Assert.Equal(30, model.Age);
        Assert.Equal(99.50m, model.Score);
        Assert.Equal(created, model.Created);
    }

    [Fact]
    [DisplayName("ReadModelsAsync处理NULL列保持默认值")]
    public async Task NullColumnKeepsDefault()
    {
        _conn.ExecuteNonQuery($"INSERT INTO `{_table}` (name) VALUES ('空值测试')");

        using var cmd = new MySqlCommand(_conn, $"SELECT id, name, age, score, created FROM `{_table}` LIMIT 1");
        using var dr = (MySqlDataReader)await cmd.ExecuteReaderAsync();

        var list = await dr.ReadModelsAsync<TestModel>();

        var model = Assert.Single(list);
        Assert.Equal("空值测试", model.Name);
        Assert.Equal(0, model.Age);
        Assert.Equal(0m, model.Score);
        Assert.Equal(default, model.Created);
    }

    public class TestModel
    {
        public Int32 Id { get; set; }

        public String? Name { get; set; }

        public Int32 Age { get; set; }

        public Decimal Score { get; set; }

        public DateTime Created { get; set; }
    }
}
