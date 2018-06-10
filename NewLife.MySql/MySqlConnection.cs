using System;
using System.Data;
using System.Data.Common;

namespace NewLife.MySql
{
    /// <summary>数据库连接</summary>
    public sealed partial class MySqlConnection : DbConnection
    {
        #region 属性
        /// <summary>设置</summary>
        public MySqlConnectionStringBuilder Setting { get; } = new MySqlConnectionStringBuilder();

        /// <summary>连接字符串</summary>
        public override String ConnectionString { get => Setting.ConnectionString; set => Setting.ConnectionString = value; }

        /// <summary>数据库</summary>
        public override String Database => Setting.Server;

        /// <summary>服务器</summary>
        public override String DataSource => Setting.Server;

        private String _Version;
        /// <summary>版本</summary>
        public override String ServerVersion => _Version;

        private ConnectionState _State;
        /// <summary>连接状态</summary>
        public override ConnectionState State => _State;

        /// <summary>基础驱动</summary>
        private Driver Driver { get; set; }
        #endregion

        #region 构造
        /// <summary>实例化</summary>
        public MySqlConnection() { }

        /// <summary>使用连接字符串实例化</summary>
        /// <param name="connStr"></param>
        public MySqlConnection(String connStr) : this()
        {
            ConnectionString = connStr;
        }
        #endregion

        #region 打开关闭
        /// <summary>打开</summary>
        public override void Open()
        {
            if (State == ConnectionState.Open) return;

            SetState(ConnectionState.Connecting);

            // 打开网络连接
            try
            {
                var dr = Driver = new Driver(Setting);
                dr.Open();

                // 配置参数
                dr.Configure(this);

                //LoadVariables();
            }
            catch (Exception)
            {
                SetState(ConnectionState.Closed);
                throw;
            }

            SetState(ConnectionState.Open);
        }

        /// <summary>关闭</summary>
        public override void Close()
        {
            if (State == ConnectionState.Closed) return;

            // 关闭附属对象

            // 关闭网络连接
            Driver.TryDispose();
            Driver = null;

            SetState(ConnectionState.Closed);
        }

        private void SetState(ConnectionState newState)
        {
            if (newState == State) return;

            var oldState = _State;
            _State = newState;

            OnStateChange(new StateChangeEventArgs(oldState, newState));
        }
        #endregion

        #region 接口方法
        /// <summary>开始事务</summary>
        /// <param name="isolationLevel"></param>
        /// <returns></returns>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }

        /// <summary>改变数据库</summary>
        /// <param name="databaseName"></param>
        public override void ChangeDatabase(String databaseName) => throw new NotImplementedException();

        /// <summary>创建命令</summary>
        /// <returns></returns>
        protected override DbCommand CreateDbCommand()
        {
            var cmd = MySqlClientFactory.Instance.CreateCommand();
            cmd.Connection = this;

            return cmd;
        }
        #endregion

        #region 方法
        ///// <summary>加载服务器变量</summary>
        ///// <returns></returns>
        //private IDictionary<String, String> LoadVariables()
        //{
        //    var dic = new Dictionary<String, String>();
        //    using (var cmd = CreateCommand())
        //    {
        //        cmd.CommandText = "SHOW VARIABLES";

        //        using (var reader = cmd.ExecuteReader())
        //        {
        //            while (reader.Read())
        //            {
        //                var key = reader.GetString(0);
        //                var value = reader.GetString(1);
        //                dic[key] = value;
        //            }
        //        }

        //        return dic;
        //    }
        //}
        #endregion
    }
}