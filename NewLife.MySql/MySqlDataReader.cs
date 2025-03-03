using System.Collections;
using System.Data;
using System.Data.Common;

namespace NewLife.MySql;

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

    private Int32 _FieldCount;
    /// <summary>字段数</summary>
    public override Int32 FieldCount => _FieldCount;

    /// <summary>是否有行</summary>
    public override Boolean HasRows => _Values != null && _Values.Length > 0;

    private Boolean _IsClosed;
    /// <summary>是否关闭</summary>
    public override Boolean IsClosed => _IsClosed;

    private Int32 _RecordsAffected;
    /// <summary>影响行数</summary>
    public override Int32 RecordsAffected => _RecordsAffected;

    //private String[] _Names;
    //private MySqlDbType[] _Types;
    private MySqlColumn[]? _Columns;
    private Object[]? _Values;
    #endregion

    #region 核心方法
    /// <summary>下一结果集</summary>
    /// <returns></returns>
    public override Boolean NextResult()
    {
        var client = (Command.Connection as MySqlConnection)!.Client!;

        var affectedRow = 0;
        var insertedId = 0L;
        var fieldCount = client.GetResult(ref affectedRow, ref insertedId);

        _FieldCount = fieldCount;
        if (fieldCount <= 0) return false;

        //var names = new String[fieldCount];
        //var types = new MySqlDbType[fieldCount];
        _Columns = client.GetColumns(fieldCount);

        //_Names = names;
        //_Types = types;

        return true;
    }

    /// <summary>读取一行</summary>
    /// <returns></returns>
    public override Boolean Read()
    {
        var client = (Command.Connection as MySqlConnection)!.Client!;

        var values = new Object[_FieldCount];
        if (!client.NextRow(values, _Columns)) return false;

        _Values = values;

        return true;
    }

    /// <summary>关闭</summary>
    public override void Close()
    {
        //throw new NotImplementedException();
    }
    #endregion

    #region 方法
    /// <summary>
    /// 在给定从零开始的列序号时获取该列的名称。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的名称。</returns>
    public override String GetName(Int32 ordinal) => _Columns[ordinal].Name;

    /// <summary>
    /// 在给定列名时获取相应的列序号。
    /// </summary>
    /// <param name="name">列的名称，不区分大小写。</param>
    /// <returns>从零开始的列序号，如果不存在给定列，返回-1。</returns>
    public override Int32 GetOrdinal(String name) => Array.FindIndex(_Columns, p => name.EqualIgnoreCase(p.Name));

    /// <summary>
    /// 获取指定列的数据类型的名称。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>一个字符串，表示数据类型的名称。</returns>
    public override String GetDataTypeName(Int32 ordinal) => _Columns[ordinal].Type.ToString();

    private static ICollection<String> _mytypes = ["VarString", "VarChar", "String", "TinyText", "MediumText", "LongText", "Text"];
    /// <summary>
    /// 获取指定列的数据类型。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的数据类型。</returns>
    public override Type GetFieldType(Int32 ordinal)
    {
        var typeName = _Columns[ordinal].Type.ToString();
        //var _mytypes = new String[] { "VarString", "VarChar", "String", "TinyText", "MediumText", "LongText", "Text" };
        if (_mytypes.Contains(typeName)) return typeof(String);

        //暂时把Enum和Set类型也算做是string
        if (typeName.EqualIgnoreCase("Enum", "Set")) return typeof(String);

        return Type.GetType("System." + typeName);
    }

    /// <summary>
    /// 以 System.Object 实例的形式获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override Object GetValue(Int32 ordinal) => _Values[ordinal];

    /// <summary>
    /// 将当前行的值复制到制定的object数组
    /// </summary>
    /// <param name="values">值复制到的数组</param>
    /// <returns>实际复制的对象个数</returns>
    public override Int32 GetValues(Object[] values)
    {
        var count = values.Length < _FieldCount ? values.Length : _FieldCount;
        for (var i = 0; i < count; i++)
        {
            values[i] = _Values[i];
        }
        return count;
    }

    /// <summary>是否空</summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>true 如果指定的列等效于 System.DBNull; 否则为 false。</returns>
    public override Boolean IsDBNull(Int32 ordinal) => GetValue(ordinal) == DBNull.Value;

    /// <summary>
    /// 以布尔值的形式获取指定列的值
    /// </summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Boolean GetBoolean(Int32 ordinal) => (Boolean)_Values[ordinal];

    /// <summary>
    /// 以字节的形式获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override Byte GetByte(Int32 ordinal) => (Byte)_Values[ordinal];

    /// <summary>
    /// 从指定的列中，从 dataOffset 处开始读取字节流到 buffer 缓冲区，从 bufferOffset 所指示的位置开始写入 。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <param name="dataOffset">读取字节数组的起始位置</param>
    /// <param name="buffer">缓冲区</param>
    /// <param name="bufferOffset">缓冲区写入起始位置</param>
    /// <param name="length">最大写入长度</param>
    /// <returns></returns>
    public override Int64 GetBytes(Int32 ordinal, Int64 dataOffset, Byte[] buffer, Int32 bufferOffset, Int32 length)
    {
        var buf = _Values[ordinal] as Byte[];
        if (buf == null || buf.Length == 0) return 0L;

        //return buffer.Write(bufferOffset, buf, dataOffset, length);

        var count = length;
        if (count <= 0) count = buf.Length - (Int32)dataOffset;
        if (count > buffer.Length - bufferOffset) count = buffer.Length - bufferOffset;

        if (count > 0) Buffer.BlockCopy(buf, (Int32)dataOffset, buffer, bufferOffset, count);

        return count;

    }

    /// <summary>
    /// 以单个字符的形式获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override Char GetChar(Int32 ordinal) => Convert.ToChar(_Values[ordinal]);

    /// <summary>
    /// 
    /// </summary>
    /// <param name="ordinal"></param>
    /// <param name="dataOffset"></param>
    /// <param name="buffer"></param>
    /// <param name="bufferOffset"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public override Int64 GetChars(Int32 ordinal, Int64 dataOffset, Char[] buffer, Int32 bufferOffset, Int32 length)
    {
        var str = GetString(ordinal);
        if (String.IsNullOrEmpty(str)) return 0L;

        //转换为char数组
        var sourceBuffer = str.ToCharArray();

        //计算写入长度
        var maxReadCount = sourceBuffer.LongLength - dataOffset;  //最大能读
        var maxWriteCount = buffer.Length - bufferOffset;         //最大能写

        var count = maxReadCount < maxWriteCount ? maxReadCount : maxWriteCount;
        count = count < length ? count : length; //取3者最小值
        count = count < 0 ? 0 : count;  //不能小于0

        //写入
        for (var i = 0; i < count; i++)
        {
            buffer[bufferOffset + i] = sourceBuffer[dataOffset + i];
        }

        return count;
    }

    /// <summary>
    /// 以 System.DateTime 对象的形式获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override DateTime GetDateTime(Int32 ordinal) => (DateTime)_Values[ordinal];

    /// <summary>
    /// 以 System.Decimal 对象的形式获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override Decimal GetDecimal(Int32 ordinal) => (Decimal)_Values[ordinal];

    /// <summary>
    /// 以双精度浮点数字的形式获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override Double GetDouble(Int32 ordinal) => (Double)_Values[ordinal];

    /// <summary>
    /// 以单精度浮点数字的形式获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override Single GetFloat(Int32 ordinal) => (Single)_Values[ordinal];

    /// <summary>
    /// 以全局唯一标识符 (GUID) 的形式获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override Guid GetGuid(Int32 ordinal) => (Guid)_Values[ordinal];

    /// <summary>
    /// 16 位有符号整数形式获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override Int16 GetInt16(Int32 ordinal) => (Int16)_Values[ordinal];

    /// <summary>
    /// 作为 32 位有符号整数获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override Int32 GetInt32(Int32 ordinal) => (Int32)_Values[ordinal];

    /// <summary>
    /// 以 64 位有符号整数的形式获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override Int64 GetInt64(Int32 ordinal) => (Int64)_Values[ordinal];

    /// <summary>
    /// 以 System.String 实例的形式获取指定列的值。
    /// </summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override String GetString(Int32 ordinal) => (String)_Values[ordinal];
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