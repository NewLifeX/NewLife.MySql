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
            var connStr = "data source=127.0.0.1;Database=membership;Uid=root;Pwd=root;connectiontimeout=15;command timeout=30";
            var conn = new MySqlConnection(connStr);
            conn.Open();
            
            
        }
    }
}