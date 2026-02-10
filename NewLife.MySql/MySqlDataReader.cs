using System.Collections;
using System.Data;
using System.Data.Common;
using System.Text;
using NewLife.MySql.Common;

namespace NewLife.MySql;

/// <summary>数据读取器</summary>
public class MySqlDataReader : DbDataReader
{
    #region 属性
    /// <summary>命令</summary>
    public DbCommand Command { get; set; } = null!;

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

    private Boolean _hasMoreResults;
    /// <summary>是否有更多结果集</summary>
    public Boolean HasMoreResults => _hasMoreResults;

    private MySqlColumn[]? _Columns;
    /// <summary>列集合</summary>
    public MySqlColumn[]? Columns => _Columns;

    private Object[]? _Values;
    /// <summary>当前行数值集合</summary>
    public Object[]? Values => _Values;

    private Boolean _allRowsConsumed = true;

    private Boolean _hasReadResult;

    /// <summary>是否使用二进制协议读取行数据（COM_STMT_EXECUTE 预编译语句结果集）</summary>
    internal Boolean IsBinaryProtocol { get; set; }
    #endregion

    #region 核心方法
    /// <summary>下一结果集</summary>
    /// <returns></returns>
    public override Boolean NextResult() => NextResultAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>读取一行</summary>
    /// <returns></returns>
    public override Boolean Read() => ReadAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>关闭。无需消费剩余结果集，连接从池中取出时 SqlClient.Reset 会清理网络流残余数据</summary>
    public override void Close()
    {
        if (_IsClosed) return;

        _IsClosed = true;
    }
    #endregion

    #region 方法
    /// <summary>获取指定列的名称</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的名称</returns>
    public override String GetName(Int32 ordinal) => _Columns![ordinal].Name;

    /// <summary>获取指定列名对应的列序号</summary>
    /// <param name="name">列名，不区分大小写</param>
    /// <returns>从零开始的列序号，不存在返回-1</returns>
    public override Int32 GetOrdinal(String name) => _Columns == null ? -1 : Array.FindIndex(_Columns, p => name.EqualIgnoreCase(p.Name));

    /// <summary>获取指定列的数据类型名称</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>数据类型名称</returns>
    public override String GetDataTypeName(Int32 ordinal) => _Columns![ordinal].Type.ToString();

    /// <summary>获取指定列的数据类型</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的数据类型</returns>
    public override Type GetFieldType(Int32 ordinal)
    {
        var col = _Columns![ordinal];

        return col.Type switch
        {
            // 有符号整数
            MySqlDbType.Byte => typeof(SByte),
            MySqlDbType.Int16 => typeof(Int16),
            MySqlDbType.Int24 or MySqlDbType.Int32 => typeof(Int32),
            MySqlDbType.Int64 => typeof(Int64),

            // 无符号整数
            MySqlDbType.UByte => typeof(Byte),
            MySqlDbType.UInt16 => typeof(UInt16),
            MySqlDbType.UInt24 or MySqlDbType.UInt32 => typeof(UInt32),
            MySqlDbType.UInt64 => typeof(UInt64),

            // 浮点数
            MySqlDbType.Float => typeof(Single),
            MySqlDbType.Double => typeof(Double),
            MySqlDbType.Decimal or MySqlDbType.NewDecimal => typeof(Decimal),

            // 日期时间
            MySqlDbType.DateTime or MySqlDbType.Timestamp or MySqlDbType.Date or MySqlDbType.Newdate => typeof(DateTime),
            MySqlDbType.Time => typeof(TimeSpan),
            MySqlDbType.Year => typeof(Int32),

            // 字符串
            MySqlDbType.VarString or MySqlDbType.String or MySqlDbType.VarChar => typeof(String),
            MySqlDbType.TinyText or MySqlDbType.MediumText or MySqlDbType.LongText or MySqlDbType.Text => typeof(String),
            MySqlDbType.Enum or MySqlDbType.Set or MySqlDbType.Json => typeof(String),

            // 二进制
            MySqlDbType.Blob or MySqlDbType.TinyBlob or MySqlDbType.MediumBlob or MySqlDbType.LongBlob => typeof(Byte[]),
            MySqlDbType.Binary or MySqlDbType.VarBinary => typeof(Byte[]),
            MySqlDbType.Geometry or MySqlDbType.Vector => typeof(Byte[]),

            // 其他
            MySqlDbType.Bit => typeof(UInt64),
            MySqlDbType.Guid => typeof(Guid),
            _ => typeof(String),
        };
    }

    /// <summary>获取指定列的值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Object GetValue(Int32 ordinal) => _Values![ordinal];

    /// <summary>将当前行的值复制到指定数组</summary>
    /// <param name="values">目标数组</param>
    /// <returns>实际复制的对象个数</returns>
    public override Int32 GetValues(Object[] values)
    {
        var count = values.Length < _FieldCount ? values.Length : _FieldCount;
        for (var i = 0; i < count; i++)
        {
            values[i] = _Values![i];
        }
        return count;
    }

    /// <summary>是否空</summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>true 如果指定的列等效于 System.DBNull; 否则为 false。</returns>
    public override Boolean IsDBNull(Int32 ordinal) => GetValue(ordinal) == DBNull.Value;

    /// <summary>获取指定列的布尔值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Boolean GetBoolean(Int32 ordinal) => (Boolean)_Values![ordinal];

    /// <summary>获取指定列的字节值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Byte GetByte(Int32 ordinal) => (Byte)_Values![ordinal];

    /// <summary>从指定列读取字节流到缓冲区</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <param name="dataOffset">数据读取起始偏移</param>
    /// <param name="buffer">目标缓冲区</param>
    /// <param name="bufferOffset">缓冲区写入起始位置</param>
    /// <param name="length">最大读取长度</param>
    /// <returns>实际读取的字节数</returns>
    public override Int64 GetBytes(Int32 ordinal, Int64 dataOffset, Byte[] buffer, Int32 bufferOffset, Int32 length)
    {
        var buf = _Values![ordinal] as Byte[];
        if (buf == null || buf.Length == 0) return 0L;

        //return buffer.Write(bufferOffset, buf, dataOffset, length);

        var count = length;
        if (count <= 0) count = buf.Length - (Int32)dataOffset;
        if (count > buffer.Length - bufferOffset) count = buffer.Length - bufferOffset;

        if (count > 0) Buffer.BlockCopy(buf, (Int32)dataOffset, buffer, bufferOffset, count);

        return count;

    }

    /// <summary>获取指定列的字符值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Char GetChar(Int32 ordinal) => Convert.ToChar(_Values![ordinal]);

    /// <summary>从指定列读取字符流到缓冲区</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <param name="dataOffset">数据读取起始偏移</param>
    /// <param name="buffer">目标字符缓冲区</param>
    /// <param name="bufferOffset">缓冲区写入起始位置</param>
    /// <param name="length">最大读取长度</param>
    /// <returns>实际读取的字符数</returns>
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

    /// <summary>获取指定列的日期时间值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override DateTime GetDateTime(Int32 ordinal) => (DateTime)_Values![ordinal];

    /// <summary>获取指定列的Decimal值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Decimal GetDecimal(Int32 ordinal) => (Decimal)_Values![ordinal];

    /// <summary>获取指定列的双精度浮点值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Double GetDouble(Int32 ordinal) => (Double)_Values![ordinal];

    /// <summary>获取指定列的单精度浮点值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Single GetFloat(Int32 ordinal) => (Single)_Values![ordinal];

    /// <summary>获取指定列的GUID值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Guid GetGuid(Int32 ordinal) => (Guid)_Values![ordinal];

    /// <summary>获取指定列的16位有符号整数值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Int16 GetInt16(Int32 ordinal) => (Int16)_Values![ordinal];

    /// <summary>获取指定列的32位有符号整数值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Int32 GetInt32(Int32 ordinal) => (Int32)_Values![ordinal];

    /// <summary>获取指定列的64位有符号整数值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Int64 GetInt64(Int32 ordinal) => (Int64)_Values![ordinal];

    /// <summary>以 System.String 实例的形式获取指定列的值</summary>
    /// <param name="ordinal">从零开始的列序号。</param>
    /// <returns>指定列的值。</returns>
    public override String GetString(Int32 ordinal)
    {
        var val = _Values![ordinal];
        if (val is String s) return s;
        if (val is Byte[] buf) return Encoding.UTF8.GetString(buf);
        return val?.ToString() ?? String.Empty;
    }
    #endregion

    #region 异步方法
    /// <summary>异步读取一行</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public override async Task<Boolean> ReadAsync(CancellationToken cancellationToken)
    {
        if (_FieldCount <= 0) return false;

        var client = (Command.Connection as MySqlConnection)!.Client!;

        var values = new Object[_FieldCount];
        var result = IsBinaryProtocol
            ? await client.NextBinaryRowAsync(values, _Columns!, cancellationToken).ConfigureAwait(false)
            : await client.NextRowAsync(values, _Columns!, cancellationToken).ConfigureAwait(false);

        if (!result.HasRow)
        {
            // EOF 到达，记录是否有更多结果集
            _hasMoreResults = result.HasMoreResults;
            _allRowsConsumed = true;
            return false;
        }

        _Values = values;
        _allRowsConsumed = false;
        return true;
    }

    /// <summary>异步下一结果集。支持服务端多语句返回的多结果集</summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public override async Task<Boolean> NextResultAsync(CancellationToken cancellationToken)
    {
        var client = (Command.Connection as MySqlConnection)!.Client!;

        // 如果当前结果集的行未消费完，先消费掉
        if (_FieldCount > 0 && !_allRowsConsumed)
        {
            var values = new Object[_FieldCount];
            while (true)
            {
                var row = IsBinaryProtocol
                    ? await client.NextBinaryRowAsync(values, _Columns!, cancellationToken).ConfigureAwait(false)
                    : await client.NextRowAsync(values, _Columns!, cancellationToken).ConfigureAwait(false);
                if (!row.HasRow)
                {
                    _hasMoreResults = row.HasMoreResults;
                    _allRowsConsumed = true;
                    break;
                }
            }
        }

        // 已经读过结果且没有更多结果集，直接返回 false。
        // 用 _hasReadResult 区分初始状态（从未读过）和已读过 OK 包（FieldCount=0）的状态
        if (_hasReadResult && !_hasMoreResults)
        {
            _FieldCount = 0;
            _Columns = null;
            _Values = null;
            return false;
        }

        // 读取下一个结果（第一次或后续）
        var response = await client.ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        var qr = client.GetResult(response);

        _hasReadResult = true;
        _RecordsAffected += qr.AffectedRows;
        _hasMoreResults = qr.HasMoreResults;

        _FieldCount = qr.FieldCount;
        _Values = null;
        _allRowsConsumed = true;

        if (qr.FieldCount <= 0)
        {
            // OK 包（INSERT/UPDATE/DELETE）
            _Columns = null;
            // 根据 ADO.NET 标准，NextResult 每次只移动一个结果。
            // 即使当前结果是 OK 包（FieldCount=0），我们也成功读取了一个结果，应返回 true。
            // 下次调用时，通过 _hasMoreResults 判断是否继续。
            return true;
        }

        _Columns = await client.GetColumnsAsync(qr.FieldCount, cancellationToken).ConfigureAwait(false);
        _allRowsConsumed = false;

        return true;
    }

    /// <summary>异步关闭。无需消费剩余结果集，连接从池中取出时 SqlClient.Reset 会清理网络流残余数据</summary>
    /// <returns></returns>
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    public override Task CloseAsync()
    {
        if (_IsClosed) return Task.CompletedTask;

        _IsClosed = true;

        return Task.CompletedTask;
    }
#endif
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

    /// <summary>获取架构表。返回列的元数据信息</summary>
    /// <returns></returns>
    public override DataTable GetSchemaTable()
    {
        var columns = _Columns;
        if (columns == null || columns.Length == 0) return new DataTable("SchemaTable");

        var dt = new DataTable("SchemaTable");
        dt.Columns.Add("ColumnName", typeof(String));
        dt.Columns.Add("ColumnOrdinal", typeof(Int32));
        dt.Columns.Add("ColumnSize", typeof(Int32));
        dt.Columns.Add("NumericPrecision", typeof(Int32));
        dt.Columns.Add("NumericScale", typeof(Int32));
        dt.Columns.Add("DataType", typeof(Type));
        dt.Columns.Add("IsLong", typeof(Boolean));
        dt.Columns.Add("AllowDBNull", typeof(Boolean));
        dt.Columns.Add("IsReadOnly", typeof(Boolean));
        dt.Columns.Add("IsUnique", typeof(Boolean));
        dt.Columns.Add("IsKey", typeof(Boolean));
        dt.Columns.Add("BaseTableName", typeof(String));
        dt.Columns.Add("BaseColumnName", typeof(String));
        dt.Columns.Add("BaseCatalogName", typeof(String));

        for (var i = 0; i < columns.Length; i++)
        {
            var col = columns[i];
            var row = dt.NewRow();
            row["ColumnName"] = col.Name;
            row["ColumnOrdinal"] = i;
            row["ColumnSize"] = col.Length;
            row["NumericPrecision"] = col.Length;
            row["NumericScale"] = (Int32)col.Scale;
            row["DataType"] = GetFieldType(i);
            row["IsLong"] = col.Length > 255;
            row["AllowDBNull"] = true;
            row["IsReadOnly"] = false;
            row["IsUnique"] = false;
            row["IsKey"] = false;
            row["BaseTableName"] = col.RealTable;
            row["BaseColumnName"] = col.OriginalName;
            row["BaseCatalogName"] = col.Database;
            dt.Rows.Add(row);
        }

        return dt;
    }
    #endregion
}