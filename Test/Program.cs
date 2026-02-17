using System;
using NewLife.Log;
using NewLife.MySql;
using XCode.DataAccessLayer;
using XCode.Membership;

namespace Test;

class Program
{
    static void Main(String[] args)
    {
        XTrace.UseConsole();

        Test1();

        Console.WriteLine("OK!");
        Console.ReadKey();
    }

    static void Test1()
    {
        //var connStr = "data source=127.0.0.1;port=3305;Database=membership;Uid=root;Pwd=root;connectiontimeout=15;command timeout=30";
        var connStr = "data source=127.0.0.1;port=3306;Database=membership;Uid=root;Pwd=root;connectiontimeout=15;command timeout=30";
        var conn = new MySqlConnection(connStr);
        conn.Open();

        // 显示数据库类型信息
        Console.WriteLine($"数据库类型: {conn.DatabaseType}");
        Console.WriteLine($"服务器版本: {conn.ServerVersion}");
        
        // 获取 Schema 信息查看产品名称
        var schema = conn.GetSchema("DataSourceInformation");
        Console.WriteLine($"产品名称: {schema.Rows[0]["DataSourceProductName"]}");
        Console.WriteLine();

        using var cmd = new MySqlCommand(conn, "select * from `user`");
        using var dr = cmd.ExecuteReader();

        Console.WriteLine();
        for (var i = 0; i < dr.FieldCount; i++)
        {
            Console.Write("{0}\t", dr.GetName(i));
        }

        Console.WriteLine();
        while (dr.Read())
        {
            //Console.WriteLine(dr[0]);
            for (var i = 0; i < dr.FieldCount; i++)
            {
                Console.Write("{0}\t", dr.GetValue(i));
            }
            Console.WriteLine();
        }
    }

    static void Test2()
    {
        //var connStr = "data source=127.0.0.1;port=3306;Database=membership8;Uid=root;Pwd=root";
        var connStr = "data source=127.0.0.1;port=3306;Database=mysql;Uid=root;Pwd=root";
        DAL.AddConnStr("membership", connStr, null, "mysql");
        var dal = DAL.Create("membership");

        //var ds = dal.Query("SHOW DATABASES");
        //var ds = dal.Query("SHOW TABLE STATUS FROM `membership`");
        var ds = dal.Query("SHOW FULL COLUMNS FROM `membership`.`menu`");
        //var ds = dal.Query("SHOW INDEX FROM `membership`.`menu`");
        //var ds = dal.Query("SHOW INDEX FROM `membership`.`menu`");
        foreach (var dc in ds.Columns)
        {
            Console.Write("{0},", dc);
        }
        Console.WriteLine();
        foreach (var dr in ds.Rows)
        {
            foreach (var item in dr)
            {
                Console.Write("{0},", item);
            }
            Console.WriteLine();
        }

        var list = Role.FindAll();
    }

    /// <summary>测试 OceanBase/TiDB 数据库连接和类型检测</summary>
    static void TestDistributedDatabases()
    {
        // OceanBase 示例（默认端口 2881）
        var oceanBaseConnStr = "Server=oceanbase-host;Port=2881;Database=test;User Id=root;Password=pass;";
        
        // TiDB 示例（默认端口 4000）
        var tidbConnStr = "Server=tidb-host;Port=4000;Database=test;User Id=root;Password=pass;";

        // 测试 OceanBase
        Console.WriteLine("=== OceanBase 连接测试 ===");
        try
        {
            using var oceanConn = new MySqlConnection(oceanBaseConnStr);
            oceanConn.Open();
            Console.WriteLine($"数据库类型: {oceanConn.DatabaseType}");  // 应输出: OceanBase
            Console.WriteLine($"服务器版本: {oceanConn.ServerVersion}");
            
            var schema = oceanConn.GetSchema("DataSourceInformation");
            Console.WriteLine($"产品名称: {schema.Rows[0]["DataSourceProductName"]}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"连接失败: {ex.Message}");
        }
        Console.WriteLine();

        // 测试 TiDB
        Console.WriteLine("=== TiDB 连接测试 ===");
        try
        {
            using var tidbConn = new MySqlConnection(tidbConnStr);
            tidbConn.Open();
            Console.WriteLine($"数据库类型: {tidbConn.DatabaseType}");  // 应输出: TiDB
            Console.WriteLine($"服务器版本: {tidbConn.ServerVersion}");
            
            var schema = tidbConn.GetSchema("DataSourceInformation");
            Console.WriteLine($"产品名称: {schema.Rows[0]["DataSourceProductName"]}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"连接失败: {ex.Message}");
        }
    }
}