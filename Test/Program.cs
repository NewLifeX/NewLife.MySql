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
            var buf = new Byte[] { 0x06, 0xAF };
            var id = buf.ToUInt16(0, false);
            Console.WriteLine(id);

            var type = typeof(MySqlClientFactory);
            var ns = type.Assembly.GetManifestResourceNames();
            var ms = type.Assembly.GetManifestResourceStream(ns[2]);
            var str = ms.ToStr();
            Console.WriteLine(str);

            var ss = str.Split("\n").Select(e => e.Trim()).ToArray();
            str = ss.Join(",");
            Console.WriteLine(str);
        }
    }
}