using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NewLife.Data;

namespace NewLife.MySql.Common
{
    static class BinaryHelper
    {
        /// <summary>读取零结尾的C格式字符串</summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static Packet ReadZero(this BinaryReader reader)
        {
            var ms = reader.BaseStream as MemoryStream;
            var p = (Int32)ms.Position;
            var buf = ms.GetBuffer();
            var k = 0;
            for (k = p; k < ms.Length; k++)
            {
                if (buf[k] == 0) break;
            }

            var len = k - p;
            ms.Seek(len + 1, SeekOrigin.Current);

            return new Packet(buf, p, len);
        }

        /// <summary>读取零结尾的C格式字符串</summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static String ReadZeroString(this BinaryReader reader) => reader.ReadZero().ToStr();
    }
}