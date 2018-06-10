using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using NewLife.Data;
using NewLife.MySql.Common;

namespace NewLife.MySql
{
    class Driver : DisposeBase
    {
        #region 属性
        /// <summary>设置</summary>
        public MySqlConnectionStringBuilder Setting { get; }

        /// <summary>最大包大小</summary>
        public Int64 MaxPacketSize { get; private set; } = 1024;

        public IDictionary<String, String> Variables { get; private set; }
        #endregion

        #region 构造
        public Driver(MySqlConnectionStringBuilder setting)
        {
            Setting = setting;
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            Close();
        }
        #endregion

        #region 打开关闭
        private TcpClient _Client;
        private Stream _Stream;

        public void Open()
        {
            var set = Setting;
            var server = set.Server;
            var port = set.Port;
            if (port == 0) port = 3306;

            var msTimeout = Setting.ConnectionTimeout;
            if (msTimeout <= 0) msTimeout = 15000;

            // 连接网络
            var client = new TcpClient
            {
                ReceiveTimeout = msTimeout
            };
            client.Connect(server, port);

            _Stream = client.GetStream();

            // 读取数据包
            var pk = ReadPacket();
            var ms = pk.GetStream();
            var reader = new BinaryReader(ms);

            // 欢迎包
            var protocol = ms.ReadByte();
            var version = reader.ReadZeroString();
            var threadId = reader.ReadInt32();

            var seedPart1 = reader.ReadZero();

            // 服务器特性
            var capabilities = (UInt32)reader.ReadUInt16();
            var charSet = reader.ReadByte();
            var serverStatus = reader.ReadUInt16();
            capabilities |= ((UInt32)reader.ReadUInt16() << 16);

            ms.Seek(11, SeekOrigin.Current);

            // 加密种子
            var seedPart2 = reader.ReadZero();
            var ms2 = new MemoryStream();
            seedPart1.WriteTo(ms2);
            seedPart2.WriteTo(ms2);
            var seed = ms2.ToArray();

            // 设置连接标识
            var flags = 0;
            var ms3 = new MemoryStream();
            var writer = new BinaryWriter(ms3);
            writer.Write(flags);
            writer.Write(0xFF_FFFF);
            writer.Write((Byte)33); // UTF-8
            writer.Write(new Byte[23]);
        }

        public void Close()
        {
            _Client.TryDispose();
            _Client = null;
            _Stream = null;
        }

        public virtual void Configure(MySqlConnection conn)
        {
            var vs = Variables = LoadVariables(conn);

            if (vs.TryGetValue("max_allowed_packet", out var str)) MaxPacketSize = str.ToLong();
            vs.TryGetValue("character_set_client", out var clientCharSet);
            vs.TryGetValue("character_set_connection", out var connCharSet);
        }
        #endregion

        #region 方法
        /// <summary>加载服务器变量</summary>
        /// <returns></returns>
        private IDictionary<String, String> LoadVariables(MySqlConnection conn)
        {
            var dic = new Dictionary<String, String>();
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SHOW VARIABLES";

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var key = reader.GetString(0);
                        var value = reader.GetString(1);
                        dic[key] = value;
                    }
                }

                return dic;
            }
        }
        #endregion

        #region 网络操作
        /// <summary>读取数据包</summary>
        /// <returns></returns>
        private Packet ReadPacket()
        {
            // 3字节长度 + 1字节序列号
            var buf = _Stream.ReadBytes(4);
            var len = buf[0] + (buf[1] << 8) + (buf[2] << 16);
            var seq = buf[3];

            buf = _Stream.ReadBytes(len);
            var pk = new Packet(buf);

            // 错误包
            if (buf[0] == 0xFF)
            {
                var code = buf.ToUInt16(1);
                var msg = ReadString(pk.Slice(1 + 2));
                // 前面有6字符错误码
                if (!msg.IsNullOrEmpty() && msg[0] == '#') msg = msg.Substring(6);

                throw new MySqlException(code, msg);
            }

            return pk;
        }

        private static String ReadString(Packet pk)
        {
            var buf = pk.Data;
            var k = 0;
            for (k = pk.Offset; k < pk.Count; k++)
            {
                if (buf[k] == 0) break;
            }

            var len = k - pk.Offset;
            return pk.ToStr(null, 0, len);
        }
        #endregion
    }
}