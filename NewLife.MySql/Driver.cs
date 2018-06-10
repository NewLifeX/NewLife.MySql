using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

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

            var client = new TcpClient
            {
                ReceiveTimeout = msTimeout
            };
            client.Connect(server, port);

            _Stream = client.GetStream();
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
    }
}