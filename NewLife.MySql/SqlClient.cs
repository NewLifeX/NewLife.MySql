using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using NewLife.Data;
using NewLife.MySql.Common;

namespace NewLife.MySql
{
    /// <summary>客户端</summary>
    public class SqlClient : DisposeBase
    {
        #region 属性
        /// <summary>设置</summary>
        public MySqlConnectionStringBuilder Setting { get; }

        /// <summary>最大包大小</summary>
        public Int64 MaxPacketSize { get; private set; } = 1024;

        /// <summary>服务器特性</summary>
        public UInt32 Capabilities { get; set; }

        /// <summary>服务器变量</summary>
        public IDictionary<String, String> Variables { get; private set; }
        #endregion

        #region 构造
        /// <summary>实例化客户端</summary>
        /// <param name="setting"></param>
        public SqlClient(MySqlConnectionStringBuilder setting)
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

        /// <summary>打开</summary>
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

            // 从欢迎信息读取服务器特性
            var seed = GetWelcome();

            // 验证
            var auth = new Authentication() { Client = this };
            auth.Authenticate(false, Capabilities, seed);
        }

        /// <summary>关闭</summary>
        public void Close()
        {
            _Client.TryDispose();
            _Client = null;
            _Stream = null;
        }

        /// <summary>配置</summary>
        /// <param name="conn"></param>
        public virtual void Configure(MySqlConnection conn)
        {
            var vs = Variables = LoadVariables(conn);

            if (vs.TryGetValue("max_allowed_packet", out var str)) MaxPacketSize = str.ToLong();
            vs.TryGetValue("character_set_client", out var clientCharSet);
            vs.TryGetValue("character_set_connection", out var connCharSet);
        }
        #endregion

        #region 方法
        private Byte[] GetWelcome()
        {
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
            var caps = (UInt32)reader.ReadUInt16();
            var charSet = reader.ReadByte();
            var serverStatus = reader.ReadUInt16();
            caps |= ((UInt32)reader.ReadUInt16() << 16);
            Capabilities = caps;

            ms.Seek(11, SeekOrigin.Current);

            // 加密种子
            var seedPart2 = reader.ReadZero();
            var ms2 = new MemoryStream();
            seedPart1.WriteTo(ms2);
            seedPart2.WriteTo(ms2);
            var seed = ms2.ToArray();

            // 验证方法
            var method = reader.ReadZeroString();
            if (!method.IsNullOrEmpty() && !method.EqualIgnoreCase("mysql_native_password")) throw new NotSupportedException("不支持验证方式 " + method);

            return seed;
        }

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
        public Packet ReadPacket()
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
                var msg = pk.Slice(1 + 2).ReadZeroString();
                // 前面有6字符错误码
                //if (!msg.IsNullOrEmpty() && msg[0] == '#') msg = msg.Substring(6);

                throw new MySqlException(code, msg);
            }

            return pk;
        }

        private Int32 _gid = 1;
        /// <summary>发送数据包</summary>
        /// <param name="pk"></param>
        public void SendPacket(Packet pk)
        {
            //pk.WriteTo(_Stream);

            var len = pk.Total;

            var pk2 = pk;
            if (pk.Offset < 4)
            {
                pk2 = new Packet(new Byte[4]) { Next = pk };
            }
            else
            {
                pk2.Set(pk.Data, pk.Offset - 4, pk.Count + 4);
            }

            pk2[0] = (Byte)(len & 0xFF);
            pk2[1] = (Byte)((len >> 8) & 0xFF);
            pk2[2] = (Byte)((len >> 16) & 0xFF);
            pk2[3] = (Byte)_gid++;

            pk2.WriteTo(_Stream);
        }

        /// <summary>读取OK</summary>
        public void ReadOK()
        {
            var pk = ReadPacket();
            var reader = new BinaryReader(pk.GetStream());

            // 影响行数、最后插入ID
            reader.ReadFieldLength();
            reader.ReadFieldLength();
        }
        #endregion
    }
}