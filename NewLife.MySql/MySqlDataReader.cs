using System.Collections;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Text;
using NewLife.Data;
using NewLife.MySql.Common;
using NewLife.Reflection;
using NewLife.Serialization;

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

    /// <summary>取走当前行值数组。调用后下次 Read 将分配新数组，避免逐列复制开销</summary>
    /// <remarks>供 DbTable 等上层组件直接持有行数据使用，无需额外分配和复制</remarks>
    /// <returns>当前行数组，调用方拥有所有权</returns>
    public Object[]? TakeValues()
    {
        var values = _Values;
        _Values = null;
        return values;
    }

    private Boolean _allRowsConsumed = true;

    private Boolean _hasReadResult;

    /// <summary>是否使用二进制协议读取行数据（COM_STMT_EXECUTE 预编译语句结果集）</summary>
    internal Boolean IsBinaryProtocol { get; set; }

    /// <summary>当前读取器持有的连接独占租约</summary>
    internal MySqlConnection.ConnectionOperationLease? OperationLease { get; set; }

    /// <summary>执行命令前底层客户端超时值</summary>
    internal Int32 OriginalTimeout { get; set; }

    /// <summary>关闭读取器时是否恢复底层客户端超时值</summary>
    internal Boolean RestoreTimeoutOnClose { get; set; }

    /// <summary>命令执行阶段超时。用于等待首包和结果集切换</summary>
    internal Int32 CommandPhaseTimeout { get; set; }

    /// <summary>结果读取阶段超时。用于逐行读取和跳过剩余行</summary>
    internal Int32 ReadPhaseTimeout { get; set; }
    #endregion

    #region 核心方法
    /// <summary>下一结果集</summary>
    /// <returns></returns>
    public override Boolean NextResult() => NextResultAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>读取一行</summary>
    /// <returns></returns>
    public override Boolean Read() => ReadAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

    /// <summary>关闭。无需消费剩余结果集，连接从池中取出时 SqlClient.Reset 会清理网络流残余数据</summary>
    public override void Close() => CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
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
    public override Boolean GetBoolean(Int32 ordinal) => Convert.ToBoolean(_Values![ordinal]);

    /// <summary>获取指定列的字节值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Byte GetByte(Int32 ordinal) => Convert.ToByte(_Values![ordinal]);

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

        //计算写入长度
        var maxReadCount = str.Length - dataOffset;               //最大能读
        var maxWriteCount = buffer.Length - bufferOffset;         //最大能写

        var count = maxReadCount < maxWriteCount ? maxReadCount : maxWriteCount;
        count = count < length ? count : length; //取3者最小值
        count = count < 0 ? 0 : count;  //不能小于0

        //写入
        if (count > 0)
            str.CopyTo((Int32)dataOffset, buffer, bufferOffset, (Int32)count);

        return count;
    }

    /// <summary>获取指定列的日期时间值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override DateTime GetDateTime(Int32 ordinal) => Convert.ToDateTime(_Values![ordinal]);

    /// <summary>获取指定列的Decimal值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Decimal GetDecimal(Int32 ordinal) => Convert.ToDecimal(_Values![ordinal]);

    /// <summary>获取指定列的双精度浮点值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Double GetDouble(Int32 ordinal) => Convert.ToDouble(_Values![ordinal]);

    /// <summary>获取指定列的单精度浮点值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Single GetFloat(Int32 ordinal) => Convert.ToSingle(_Values![ordinal]);

    /// <summary>获取指定列的GUID值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Guid GetGuid(Int32 ordinal)
    {
        var val = _Values![ordinal];
        if (val is Guid g) return g;
        if (val is String s) return Guid.Parse(s);
        if (val is Byte[] buf) return new Guid(buf);
        return (Guid)val;
    }

    /// <summary>获取指定列的16位有符号整数值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Int16 GetInt16(Int32 ordinal) => Convert.ToInt16(_Values![ordinal]);

    /// <summary>获取指定列的32位有符号整数值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Int32 GetInt32(Int32 ordinal) => Convert.ToInt32(_Values![ordinal]);

    /// <summary>获取指定列的64位有符号整数值</summary>
    /// <param name="ordinal">从零开始的列序号</param>
    /// <returns>指定列的值</returns>
    public override Int64 GetInt64(Int32 ordinal) => Convert.ToInt64(_Values![ordinal]);

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
        SetClientTimeout(client, ReadPhaseTimeout);

        // 复用上一行的数组，避免每行分配一个新 Object[]，减少 GC 压力
        var values = _Values;
        if (values == null || values.Length != _FieldCount)
            values = new Object[_FieldCount];

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

        // 如果当前结果集的行未消费完，先跳过剩余行（不解析内容）
        if (_FieldCount > 0 && !_allRowsConsumed)
        {
            SetClientTimeout(client, ReadPhaseTimeout);
            while (true)
            {
                var row = await client.SkipRowAsync(cancellationToken).ConfigureAwait(false);
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
        var previousTimeout = client.Timeout;
        SetClientTimeout(client, CommandPhaseTimeout);

        try
        {
            using var response = await client.ReadPacketAsync(cancellationToken).ConfigureAwait(false);
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
        finally
        {
            SetClientTimeout(client, ReadPhaseTimeout > 0 ? ReadPhaseTimeout : previousTimeout);
        }
    }

    /// <summary>异步关闭。无需消费剩余结果集，连接从池中取出时 SqlClient.Reset 会清理网络流残余数据</summary>
    /// <returns></returns>
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    public override Task CloseAsync()
        => CloseCoreAsync();
#else
    private Task CloseAsync()
        => CloseCoreAsync();
#endif

    private async Task CloseCoreAsync()
    {
        if (_IsClosed) return;

        _IsClosed = true;

        try
        {
            while (await NextResultAsync(CancellationToken.None).ConfigureAwait(false)) { }
        }
        catch
        {
            // 关闭阶段尽力清理协议流，失败时仍需释放连接占用，避免后续请求永久阻塞。
        }
        finally
        {
            if (RestoreTimeoutOnClose && Command.Connection is MySqlConnection conn && conn.Client != null)
                conn.Client.Timeout = OriginalTimeout;

            RestoreTimeoutOnClose = false;
            OperationLease?.Dispose();
            OperationLease = null;
        }
    }

    private static void SetClientTimeout(SqlClient client, Int32 timeout)
    {
        if (timeout > 0 && client.Timeout != timeout)
            client.Timeout = timeout;
    }

    /// <summary>异步读取当前结果集到 DbTable，跳过外部 DbTable.ReadData 的逐列复制开销</summary>
    /// <remarks>
    /// 直接在驱动层填充 DbTable，利用 TakeValues 实现行数组零拷贝转移，
    /// 避免外部 ReadData 每行通过 dr[i] 逐列读取并二次装箱。
    /// DBNull 值按列类型填充为对应的默认值（0/false/DateTime.MinValue/null）。
    /// </remarks>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>填充完毕的 DbTable</returns>
    public async Task<DbTable> ReadTableAsync(CancellationToken cancellationToken = default)
    {
        var dt = new DbTable();
        var columns = _Columns;
        var count = _FieldCount;

        // 填充列名和类型
        var cs = new String[count];
        var ts = new Type[count];
        for (var i = 0; i < count; i++)
        {
            cs[i] = columns![i].Name;
            ts[i] = GetFieldType(i);
        }
        dt.Columns = cs;
        dt.Types = ts;

        // 构建默认值缓存，避免每行每列重复计算
        var defaults = new Object?[count];
        for (var i = 0; i < count; i++)
        {
            defaults[i] = GetDefaultValue(Type.GetTypeCode(ts[i]));
        }

        // 逐行读取，TakeValues 零拷贝转移行数组所有权
        var rows = new List<Object?[]>();
        while (await ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var values = TakeValues()!;
            // 将 DBNull 替换为类型默认值
            for (var i = 0; i < values.Length; i++)
            {
                if (values[i] == DBNull.Value)
                    values[i] = defaults[i];
            }
            rows.Add(values);
        }
        dt.Rows = rows;
        dt.Total = rows.Count;

        return dt;
    }

    /// <summary>异步读取当前结果集并直接映射为实体对象列表，跳过 DbTable 中间层</summary>
    /// <remarks>
    /// 在驱动层直接将每行数据映射到实体属性，避免先构建 DbTable 再 ReadModels 的二次遍历。
    /// 支持 IModel 索引器快速赋值，回退到反射 SetValue。属性名匹配不区分大小写。
    /// </remarks>
    /// <typeparam name="T">实体类型，需有无参构造函数</typeparam>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>实体对象列表</returns>
    public async Task<IList<T>> ReadModelsAsync<T>(CancellationToken cancellationToken = default)
    {
        var columns = _Columns;
        var count = _FieldCount;

        // 构建列名到属性的映射
        var pis = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var dic = pis.ToDictionary(e => SerialHelper.GetName(e), e => e, StringComparer.OrdinalIgnoreCase);

        // 预计算列名到属性的映射数组，避免每行重复字典查找
        var colNames = new String[count];
        var colProps = new PropertyInfo?[count];
        for (var i = 0; i < count; i++)
        {
            var name = columns![i].Name;
            colNames[i] = name;
            dic.TryGetValue(name, out colProps[i]);
        }

        var list = new List<T>();
        while (await ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var model = typeof(T).CreateInstance();
            if (model == null) continue;

            var values = _Values!;
            for (var i = 0; i < values.Length; i++)
            {
                var pi = colProps[i];
                if (pi == null || !pi.CanWrite) continue;

                var val = values[i];
                if (val == DBNull.Value) continue;

                val = val.ChangeType(pi.PropertyType);
                if (model is IModel ext)
                    ext[pi.Name] = val;
                else
                    model.SetValue(pi, val);
            }

            list.Add((T)model);
        }

        return list;
    }
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

    private static IDictionary<TypeCode, Object?>? _defaults;
    private static Object? GetDefaultValue(TypeCode tc)
    {
        if (_defaults == null)
        {
            var dic = new Dictionary<TypeCode, Object?>
            {
                [TypeCode.Boolean] = false,
                [TypeCode.Char] = (Char)0,
                [TypeCode.SByte] = (SByte)0,
                [TypeCode.Byte] = (Byte)0,
                [TypeCode.Int16] = (Int16)0,
                [TypeCode.UInt16] = (UInt16)0,
                [TypeCode.Int32] = 0,
                [TypeCode.UInt32] = (UInt32)0,
                [TypeCode.Int64] = (Int64)0,
                [TypeCode.UInt64] = (UInt64)0,
                [TypeCode.Single] = (Single)0,
                [TypeCode.Double] = (Double)0,
                [TypeCode.Decimal] = (Decimal)0,
                [TypeCode.DateTime] = DateTime.MinValue,
            };
            _defaults = dic;
        }

        return _defaults.TryGetValue(tc, out var obj) ? obj : null;
    }
    #endregion
}