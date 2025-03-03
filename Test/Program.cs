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
}