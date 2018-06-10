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
            var connStr = "data source=127.0.0.1;Database=mysql;Uid=root;Pwd=Pass@word;connectiontimeout=15;command timeout=30";
            var builder = new MySqlConnectionStringBuilder(connStr);

            foreach (var pi in builder.GetType().GetProperties())
            {
                if (pi.GetIndexParameters().Length > 0) continue;

                Console.WriteLine("{0}:\t{1}", pi.Name, pi.GetValue(builder));
            }

            Console.WriteLine();
            connStr = builder.ConnectionString;
            Console.WriteLine(connStr);
            connStr = builder + "";
            Console.WriteLine(connStr);
        }
    }
}