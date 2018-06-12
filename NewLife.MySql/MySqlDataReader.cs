using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewLife.MySql
{
    /// <summary>数据读取器</summary>
    public class MySqlDataReader : DbDataReader
    {
        #region 属性
        /// <summary>命令</summary>
        public DbCommand Command { get; set; }

        /// <summary>根据索引读取</summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override Object this[Int32 ordinal] => GetValue(ordinal);

        /// <summary>根据名称读取</summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public override Object this[String name] => this[GetOrdinal(name)];

        /// <summary>深度</summary>
        public override Int32 Depth => 0;

        /// <summary>字段数</summary>
        public override Int32 FieldCount => throw new NotImplementedException();

        /// <summary>是否有行</summary>
        public override Boolean HasRows => throw new NotImplementedException();

        /// <summary>是否关闭</summary>
        public override Boolean IsClosed => throw new NotImplementedException();

        /// <summary>影响行数</summary>
        public override Int32 RecordsAffected => throw new NotImplementedException();
        #endregion

        #region 核心方法
        /// <summary>下一结果集</summary>
        /// <returns></returns>
        public override Boolean NextResult()
        {
            throw new NotImplementedException();
        }

        /// <summary>读取一行</summary>
        /// <returns></returns>
        public override Boolean Read()
        {
            throw new NotImplementedException();
        }

        /// <summary>关闭</summary>
        public override void Close()
        {
            throw new NotImplementedException();
        }
        #endregion

        #region 方法
        public override Boolean GetBoolean(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Byte GetByte(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Int64 GetBytes(Int32 ordinal, Int64 dataOffset, Byte[] buffer, Int32 bufferOffset, Int32 length)
        {
            throw new NotImplementedException();
        }

        public override Char GetChar(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Int64 GetChars(Int32 ordinal, Int64 dataOffset, Char[] buffer, Int32 bufferOffset, Int32 length)
        {
            throw new NotImplementedException();
        }

        public override String GetDataTypeName(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Decimal GetDecimal(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Double GetDouble(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Single GetFloat(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Guid GetGuid(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Int16 GetInt16(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Int32 GetInt32(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Int64 GetInt64(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override String GetName(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Int32 GetOrdinal(String name)
        {
            throw new NotImplementedException();
        }

        public override String GetString(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Object GetValue(Int32 ordinal)
        {
            throw new NotImplementedException();
        }

        public override Int32 GetValues(Object[] values)
        {
            throw new NotImplementedException();
        }

        /// <summary>是否空</summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override Boolean IsDBNull(Int32 ordinal) => GetValue(ordinal) == DBNull.Value;
        #endregion

        #region 辅助
        /// <summary>枚举</summary>
        /// <returns></returns>
        public override IEnumerator GetEnumerator()
        {
            var count = FieldCount;
            for (var i = 0; i < count; i++)
            {
                yield return GetValue(i);
            }
        }

        /// <summary>获取架构表</summary>
        /// <returns></returns>
        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}