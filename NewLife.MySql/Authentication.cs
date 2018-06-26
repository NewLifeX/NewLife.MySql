using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using NewLife.Data;
using NewLife.MySql.Common;

namespace NewLife.MySql
{
    class Authentication
    {
        public SqlClient Client { get; set; }

        private Byte[] _Seed;

        public void Authenticate(Boolean reset, UInt32 flags, Byte[] seed)
        {
            var dr = Client;
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

            var method = Client.AuthMethod;

            // 发送验证
            _Seed = seed;
            writer.WriteZeroString(set.UserID);
            var pass = method.Contains("sha2") ? GetSha256Passord(set.Password) : GetPassword(set.Password, seed);
            writer.Write(pass);

            var db = set.Database;
            if (!db.IsNullOrEmpty()) writer.WriteZeroString(db);

            if (reset) writer.Write((UInt16)8);

            //writer.WriteZeroString("mysql_native_password");
            writer.WriteZeroString(method);

            var attrs = GetConnectAttrs().GetBytes();
            writer.WriteLength(attrs.Length);
            writer.Write(attrs);

            ms.Position = 4;
            var pk = new Packet(ms);
            dr.SendPacket(pk);

            // 读取响应
            var rs = dr.ReadOK();

            // sha256
            if (method.Contains("sha2"))
            {
                ContinueAuthentication(new Byte[] { 1 });
            }
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

        private Byte[] _rsaKey;
        protected Byte[] GetSha256Passord(String password)
        {
            if (_rsaKey == null) return new Byte[] { 1 };

            var src = password.GetBytes();
            var seed = _Seed;

            // 异或加密
            var pass = new Byte[src.Length + 1];
            Array.Copy(src, 0, pass, 0, src.Length);
            pass[src.Length] = 0;

            var xor = new Byte[pass.Length];
            for (var i = 0; i < pass.Length; i++)
            {
                xor[i] = (Byte)(pass[i] ^ seed[i % seed.Length]);
            }

            var rsa = DecodeX509Key(DecodeOpenSslKey(_rsaKey));
            return rsa.Encrypt(xor, true);
        }

        private static RSACryptoServiceProvider DecodeX509Key(Byte[] key)
        {
            if (key == null) return null;

            var array = new Byte[] { 48, 13, 6, 9, 42, 134, 72, 134, 247, 13, 1, 1, 1, 5, 0 };
            using (var input = new MemoryStream(key))
            {
                using (var reader = new BinaryReader(input))
                {
                    switch (reader.ReadUInt16())
                    {
                        case 33072:
                            reader.ReadByte();
                            break;
                        case 33328:
                            reader.ReadInt16();
                            break;
                        default:
                            return null;
                    }
                    var array2 = reader.ReadBytes(15);
                    var flag = true;
                    if (array2.Length == array.Length)
                    {
                        for (var i = 0; i < array.Length; i++)
                        {
                            if (array2[i] != array[i])
                            {
                                flag = false;
                            }
                        }
                    }
                    if (!flag) return null;

                    switch (reader.ReadUInt16())
                    {
                        case 33027:
                            reader.ReadByte();
                            break;
                        case 33283:
                            reader.ReadInt16();
                            break;
                        default:
                            return null;
                    }
                    if (reader.ReadByte() != 0) return null;

                    switch (reader.ReadUInt16())
                    {
                        case 33072:
                            reader.ReadByte();
                            break;
                        case 33328:
                            reader.ReadInt16();
                            break;
                        default:
                            return null;
                    }
                    var num = reader.ReadUInt16();
                    Byte b = 0;
                    Byte b2 = 0;
                    switch (num)
                    {
                        case 33026:
                            b = reader.ReadByte();
                            break;
                        case 33282:
                            b2 = reader.ReadByte();
                            b = reader.ReadByte();
                            break;
                        default:
                            return null;
                    }
                    var num2 = BitConverter.ToInt32(new Byte[] { b, b2, 0, 0 }, 0);
                    var num3 = reader.ReadByte();
                    reader.BaseStream.Seek(-1L, SeekOrigin.Current);
                    if (num3 == 0)
                    {
                        reader.ReadByte();
                        num2--;
                    }
                    var modulus = reader.ReadBytes(num2);
                    if (reader.ReadByte() != 2) return null;

                    var exponent = reader.ReadBytes(reader.ReadByte());

                    var rsa = new RSACryptoServiceProvider();
                    var parameters = new RSAParameters
                    {
                        Modulus = modulus,
                        Exponent = exponent
                    };
                    rsa.ImportParameters(parameters);

                    return rsa;
                }
            }
        }

        private static Byte[] DecodeOpenSslKey(Byte[] rawPublicKey)
        {
            if (rawPublicKey == null) return null;

            rawPublicKey = (from b in rawPublicKey where b != 13 select b).ToArray();
            rawPublicKey = (from b in rawPublicKey where b != 10 select b).ToArray();
            rawPublicKey = TrimByteArray(rawPublicKey);

            var array = new Byte[] { 45, 45, 45, 45, 45, 66, 69, 71, 73, 78, 32, 80, 85, 66, 76, 73, 67, 32, 75, 69, 89, 45, 45, 45, 45, 45 };
            var array2 = new Byte[] { 45, 45, 45, 45, 45, 69, 78, 68, 32, 80, 85, 66, 76, 73, 67, 32, 75, 69, 89, 45, 45, 45, 45, 45 };
            if (StartsWith(rawPublicKey, array) && EndsWith(rawPublicKey, array2))
            {
                var array3 = new Byte[rawPublicKey.Length - array.Length - array2.Length];
                Array.Copy(rawPublicKey, array.Length, array3, 0, array3.Length);

                return Convert.FromBase64String(Encoding.Default.GetString(array3));
            }
            return null;
        }

        private static Byte[] TrimByteArray(Byte[] array)
        {
            var list = new List<Byte>();
            var flag = false;
            var array2 = array;
            foreach (var b in array2)
            {
                if (!flag)
                {
                    if (b == 32)
                    {
                        continue;
                    }
                    flag = true;
                }
                list.Add(b);
            }
            array = list.ToArray();
            list = new List<Byte>();
            for (var num = array.Length - 1; num >= 0; num--)
            {
                if (!flag)
                {
                    if (array[num] == 32)
                    {
                        continue;
                    }
                    flag = true;
                }
                list.Add(array[num]);
            }
            return list.ToArray().Reverse().ToArray();
        }

        private static Boolean StartsWith(Byte[] array, Byte[] containedArray)
        {
            for (var i = 0; i < array.Length && i != containedArray.Length; i++)
            {
                if (array[i] != containedArray[i])
                {
                    return false;
                }
            }
            return true;
        }

        private static Boolean EndsWith(Byte[] array, Byte[] containedArray)
        {
            var num = array.Length - 1;
            var num2 = 0;
            while (num >= 0 && num2 != containedArray.Length)
            {
                if (array[num] != containedArray[containedArray.Length - num2 - 1])
                {
                    return false;
                }
                num--;
                num2++;
            }
            return true;
        }

        private void ContinueAuthentication(Byte[] data)
        {
            _rsaKey = data;
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