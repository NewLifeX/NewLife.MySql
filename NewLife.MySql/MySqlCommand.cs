using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewLife.MySql
{
    /// <summary>命令</summary>
    public class MySqlCommand : DbCommand, IDisposable
    {
        #region 属性
        private MySqlConnection _DbConnection;
        /// <summary>连接</summary>
        protected override DbConnection DbConnection { get => _DbConnection; set => _DbConnection = value as MySqlConnection; }

        /// <summary>命令语句</summary>
        public override String CommandText { get; set; }

        /// <summary>命令类型</summary>
        public override CommandType CommandType { get; set; }

        /// <summary>事务</summary>
        protected override DbTransaction DbTransaction { get; set; }

        /// <summary>参数集合</summary>
        protected override DbParameterCollection DbParameterCollection => throw new NotImplementedException();

        /// <summary>命令语句</summary>
        public override Int32 CommandTimeout { get; set; }

        /// <summary>设计时可见</summary>
        public override Boolean DesignTimeVisible { get; set; }

        /// <summary>更新行方式</summary>
        public override UpdateRowSource UpdatedRowSource { get; set; }
        #endregion

        #region 方法
        /// <summary>创建参数</summary>
        /// <returns></returns>
        protected override DbParameter CreateDbParameter() => MySqlClientFactory.Instance.CreateParameter();

        /// <summary>执行读取器</summary>
        /// <param name="behavior"></param>
        /// <returns></returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            throw new NotImplementedException();
        }

        /// <summary>执行并返回影响行数</summary>
        /// <returns></returns>
        public override Int32 ExecuteNonQuery()
        {
            using (var reader = ExecuteReader())
            {
                reader.Close();
                return reader.RecordsAffected;
            }
        }

        /// <summary>执行并返回第一行</summary>
        /// <returns></returns>
        public override Object ExecuteScalar()
        {
            using (var reader = ExecuteReader())
            {
                if (reader.Read()) return reader.GetValue(0);
            }

            return null;
        }

        /// <summary>预编译语句</summary>
        public override void Prepare()
        {
            throw new NotImplementedException();
        }

        /// <summary>取消</summary>
        public override void Cancel() { }
        #endregion
    }
}