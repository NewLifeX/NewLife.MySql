using System;
using System.Data;
using System.Data.Common;

namespace NewLife.MySql
{
    /// <summary>参数</summary>
    public class MySqlParameter : DbParameter
    {
        #region 属性
        /// <summary>参数名</summary>
        public override String ParameterName { get; set; }

        /// <summary>数值</summary>
        public override Object Value { get; set; }

        /// <summary>类型</summary>
        public override DbType DbType { get; set; }

        /// <summary>方向</summary>
        public override ParameterDirection Direction { get; set; }

        /// <summary>是否允许空</summary>
        public override Boolean IsNullable { get; set; }

        /// <summary>大小</summary>
        public override Int32 Size { get; set; }

        /// <summary>源列</summary>
        public override String SourceColumn { get; set; }

        /// <summary>映射空</summary>
        public override Boolean SourceColumnNullMapping { get; set; }

        /// <summary>数据行版本</summary>
        public override DataRowVersion SourceVersion { get; set; }
        #endregion

        #region 方法
        /// <summary>重置类型</summary>
        public override void ResetDbType()
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}