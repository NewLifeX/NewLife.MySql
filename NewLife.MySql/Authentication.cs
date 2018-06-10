using System;
using System.ComponentModel;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using NewLife.Data;
using NewLife.MySql.Common;

namespace NewLife.MySql
{
    class Authentication
    {
        public Driver Driver { get; set; }

        public void Authenticate(Boolean reset, UInt32 flags, Byte[] seed)
        {
            var dr = Driver;
            var set = dr.Setting;
            var ms = new MemoryStream();
            ms.Seek(4, SeekOrigin.Current);
            var writer = new BinaryWriter(ms);

            // 设置连接标识
            var flags2 = GetFlags((ClientFlags)flags);
            writer.Write((UInt32)flags2);
            writer.Write(0xFF_FFFF);
            writer.Write((Byte)33); // UTF-8
            writer.Write(new Byte[23]);

            // 发送验证
            writer.WriteZeroString(set.UserID);
            var pass = GetPassword(set.Password, seed);
            writer.Write(pass);

            var db = set.Database;
            if (!db.IsNullOrEmpty()) writer.WriteZeroString(db);

            if (reset) writer.Write((UInt16)8);

            writer.WriteZeroString("mysql_native_password");

            var attrs = GetConnectAttrs().GetBytes();
            writer.WriteLength(attrs.Length);
            writer.Write(attrs);

            ms.Position = 4;
            var pk = new Packet(ms);
            dr.SendPacket(pk);

            // 读取响应
            dr.ReadOK();
        }

        public Byte[] GetPassword(String pass, Byte[] seed)
        {
            var bytes = Get411Password(pass, seed);
            if (bytes != null && bytes.Length == 1 && bytes[0] == 0) return null;
            return bytes;
        }

        protected Byte[] Get411Password(String password, Byte[] seedBytes)
        {
            if (password.Length == 0) return new Byte[1];

            var sha = SHA1.Create();

            var firstHash = sha.ComputeHash(password.GetBytes());
            var secondHash = sha.ComputeHash(firstHash);

            var input = new Byte[seedBytes.Length + secondHash.Length];
            Array.Copy(seedBytes, 0, input, 0, seedBytes.Length);
            Array.Copy(secondHash, 0, input, seedBytes.Length, secondHash.Length);
            var thirdHash = sha.ComputeHash(input);

            var buf = new Byte[thirdHash.Length + 1];
            buf[0] = 0x14;
            Array.Copy(thirdHash, 0, buf, 1, thirdHash.Length);

            for (var i = 1; i < buf.Length; i++)
                buf[i] = (Byte)(buf[i] ^ firstHash[i - 1]);

            return buf;
        }

        private ClientFlags GetFlags(ClientFlags caps)
        {
            // 从本地文件加载数据
            var flags = ClientFlags.LOCAL_FILES;

            // UseAffectedRows
            flags |= ClientFlags.FOUND_ROWS;
            flags |= ClientFlags.PROTOCOL_41;
            flags |= ClientFlags.TRANSACTIONS;
            flags |= ClientFlags.MULTI_STATEMENTS;
            flags |= ClientFlags.MULTI_RESULTS;
            if ((caps & ClientFlags.LONG_FLAG) != 0)
                flags |= ClientFlags.LONG_FLAG;

            flags |= ClientFlags.LONG_PASSWORD;

            flags |= ClientFlags.CONNECT_WITH_DB;

            if ((caps & ClientFlags.SECURE_CONNECTION) != 0)
                flags |= ClientFlags.SECURE_CONNECTION;

            if ((caps & ClientFlags.PS_MULTI_RESULTS) != 0)
                flags |= ClientFlags.PS_MULTI_RESULTS;

            if ((caps & ClientFlags.PLUGIN_AUTH) != 0)
                flags |= ClientFlags.PLUGIN_AUTH;

            if ((caps & ClientFlags.CONNECT_ATTRS) != 0)
                flags |= ClientFlags.CONNECT_ATTRS;

            if ((caps & ClientFlags.CAN_HANDLE_EXPIRED_PASSWORD) != 0)
                flags |= ClientFlags.CAN_HANDLE_EXPIRED_PASSWORD;

            return flags;
        }

        internal String GetConnectAttrs()
        {
            var sb = new StringBuilder();
            var att = new ConnectAttributes();
            foreach (var pi in att.GetType().GetProperties())
            {
                var name = pi.Name;
                var dis = pi.GetCustomAttributes(typeof(DisplayNameAttribute), false);
                if (dis.Length > 0)
                    name = (dis[0] as DisplayNameAttribute).DisplayName;

                var value = (String)pi.GetValue(att, null);
                sb.AppendFormat("{0}{1}", (Char)name.Length, name);
                sb.AppendFormat("{0}{1}", (Char)value.Length, value);
            }

            return sb.ToString(); ;
        }
    }
}