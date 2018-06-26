using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NewLife.Log;
using NewLife.MySql;

namespace Test
{
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
            var connStr = "data source=127.0.0.1;port=3305;Database=membership;Uid=root;Pwd=root;connectiontimeout=15;command timeout=30";
            var conn = new MySqlConnection(connStr);
            conn.Open();

            using (var cmd = new MySqlCommand(conn, "select * from `user`"))
            using (var dr = cmd.ExecuteReader())
            {
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
        }
    }
}