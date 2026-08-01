using System.ComponentModel;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using NewLife.MySql;
using NewLife.MySql.EntityFrameworkCore;
using NewLife.Security;

namespace UnitTest.EntityFrameworkCore;

/// <summary>MySqlDatabaseCreator 测试。验证数据库创建/删除/存在检查（真实 MySQL，root 权限）</summary>
public class MySqlDatabaseCreatorTests : IDisposable
{
    private const String ServerPart = "Server=localhost;Port=3306;User Id=root;Password=root;";
    private readonly String _dbName;
    private readonly TestDbContext _context;
    private readonly IServiceProvider _services;

    public MySqlDatabaseCreatorTests()
    {
        _dbName = "ef_creator_test_" + Rand.Next(10000);
        var connStr = ServerPart + "Database=" + _dbName + ";";
        _context = new TestDbContext(new DbContextOptionsBuilder<TestDbContext>().UseMySql(connStr).Options);
        _services = ((IInfrastructure<IServiceProvider>)_context).Instance;
    }

    public void Dispose()
    {
        _context.Dispose();

        // 清理残留测试库
        try
        {
            using var conn = new MySqlConnection(ServerPart + "Database=information_schema;");
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DROP DATABASE IF EXISTS `{_dbName}`";
            cmd.ExecuteNonQuery();
        }
        catch { }
    }

    [Fact]
    [DisplayName("Create创建数据库且Exists/HasTables正确")]
    public void Create_Exists_HasTables_Delete()
    {
        var creator = _services.GetRequiredService<IRelationalDatabaseCreator>();
        Assert.IsType<MySqlDatabaseCreator>(creator);

        // 确保起始状态干净
        if (creator.Exists())
        {
            creator.Delete();
            Assert.False(creator.Exists());
        }

        creator.Create();
        Assert.True(creator.Exists());
        Assert.False(creator.HasTables());

        creator.Delete();
        Assert.False(creator.Exists());
    }
}
