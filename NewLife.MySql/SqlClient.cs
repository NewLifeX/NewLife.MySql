using System.Net.Sockets;
using NewLife.Buffers;
using NewLife.Collections;
using NewLife.Data;
using NewLife.MySql.Common;
using NewLife.MySql.Messages;

namespace NewLife.MySql;

/// <summary>客户端</summary>
public class SqlClient : DisposeBase
{
    #region 属性
    /// <summary>设置</summary>
    public MySqlConnectionStringBuilder Setting { get; }

    /// <summary>最大包大小</summary>
    public Int64 MaxPacketSize { get; private set; }

    /// <summary>服务器特性</summary>
    public ClientFlags Capability { get; set; }

    /// <summary>服务器变量</summary>
    public IDictionary<String, String>? Variables { get; set; }

    private Stream? _stream;
    /// <summary>基础数据流</summary>
    public Stream? BaseStream { get => _stream; set => _stream = value; }

    /// <summary>欢迎信息</summary>
    public WelcomeMessage? Welcome { get; set; }

    private TcpClient? _client;
    private Byte _seq = 1;
    #endregion

    #region 构造
    /// <summary>实例化客户端</summary>
    /// <param name="setting"></param>
    public SqlClient(MySqlConnectionStringBuilder setting) => Setting = setting;

    /// <summary>销毁</summary>
    /// <param name="disposing"></param>
    protected override void Dispose(Boolean disposing)
    {
        base.Dispose(disposing);

        Close();
    }
    #endregion

    #region 打开关闭
    /// <summary>打开</summary>
    public void Open()
    {
        var set = Setting;
        var server = set.Server;
        var port = set.Port;
        if (port == 0) port = 3306;

        var msTimeout = Setting.ConnectionTimeout * 1000;
        if (msTimeout <= 0) msTimeout = 15000;

        // 连接网络
        var client = new TcpClient
        {
            ReceiveTimeout = msTimeout
        };
        client.Connect(server, port);

        // 获取网络流，将来可以在这里加上TLS支持
        _client = client;
        _stream = client.GetStream();

        // 从欢迎信息读取服务器特性
        var welcome = GetWelcome();
        Welcome = welcome;
        Capability = welcome.Capability;

        // 验证方法
        var method = welcome.AuthMethod;
        if (!method.IsNullOrEmpty() && !method.EqualIgnoreCase("mysql_native_password", "caching_sha2_password"))
            throw new NotSupportedException("不支持验证方式 " + method);

        // 验证
        var auth = new Authentication(this);
        auth.Authenticate(welcome, false);
    }

    /// <summary>关闭</summary>
    public void Close()
    {
        if (_stream != null)
        {
            _seq = 0;
            SendPacket([(Byte)DbCmd.QUIT]);
        }

        _client.TryDispose();
        _client = null;
        _stream = null;
    }

    /// <summary>配置</summary>
    public virtual void Configure()
    {
        var vs = Variables ??= LoadVariables();

        if (vs.TryGetValue("max_allowed_packet", out var str)) MaxPacketSize = str.ToLong();
        vs.TryGetValue("character_set_client", out var clientCharSet);
        vs.TryGetValue("character_set_connection", out var connCharSet);
    }
    #endregion

    #region 方法
    /// <summary>握手欢迎消息。从中得知服务器验证方式等一系列参数信息</summary>
    /// <returns></returns>
    /// <exception cref="NotSupportedException"></exception>
    private WelcomeMessage GetWelcome()
    {
        // 读取数据包
        var rs = ReadPacket();

        var msg = new WelcomeMessage();
        msg.Read(rs.Data.GetSpan());

        return msg;
    }

    /// <summary>加载服务器变量</summary>
    /// <returns></returns>
    private IDictionary<String, String> LoadVariables()
    {
        var dic = new Dictionary<String, String>();
        var conn = new MySqlConnection { Client = this };
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SHOW VARIABLES";

        using var reader = cmd.ExecuteReader();

        while (reader.Read())
        {
            var key = reader.GetString(0);
            var value = reader.GetString(1);
            dic[key] = value;
        }

        return dic;
    }
    #endregion

    #region 网络操作
    /// <summary>读取数据包</summary>
    /// <returns></returns>
    public Response ReadPacket()
    {
        var ms = _stream ?? throw new InvalidOperationException("未打开连接");

        // 3字节长度 + 1字节序列号
        var buf = Pool.Shared.Rent(4);
        var count = ms.ReadExactly(buf, 0, 4);
        if (count < 4) throw new InvalidDataException($"读取数据包头部失败，可用{count}字节");

        var rs = new Response(ms)
        {
            Length = buf[0] + (buf[1] << 8) + (buf[2] << 16),
            Sequence = buf[3],
            //Kind = buf[4],
        };
        _seq = (Byte)(rs.Sequence + 1);
        Pool.Shared.Return(buf);

        // 读取数据。长度必须刚好，因为可能有多帧数据包
        var len = rs.Length;
        var pk = new OwnerPacket(len);
        ms.ReadExactly(pk.Buffer, pk.Offset, len);
        count = len;

        //// 多次读取，直到满足需求
        //count = 0;
        //while (count < len)
        //{
        //    var n = _stream.Read(pk.Buffer, pk.Offset + count, len - count);
        //    if (n <= 0) break;

        //    count += n;
        //}

        pk.Resize(count);
        rs.Set(pk);

        // 错误包
        if (rs.IsError)
        {
            var reader = new SpanReader(pk.Slice(1, -1));
            var code = reader.ReadUInt16();
            var msg = reader.ReadZeroString();

            // 前面有6字符错误码
            if (!msg.IsNullOrEmpty() && msg[0] == '#')
                throw new MySqlException(code, msg[..6], msg[6..]);
            else
                throw new MySqlException(code, msg);
        }

        return rs;
    }

    /// <summary>发送数据包。建议数据包头部预留4字节空间以填充帧头</summary>
    /// <param name="pk"></param>
    public void SendPacket(IPacket pk)
    {
        var ms = _stream ?? throw new InvalidOperationException("未打开连接");

        var len = pk.Total;

        var pk2 = pk.ExpandHeader(4);

        pk2[0] = (Byte)(len & 0xFF);
        pk2[1] = (Byte)((len >> 8) & 0xFF);
        pk2[2] = (Byte)((len >> 16) & 0xFF);
        pk2[3] = _seq++;

        pk2.CopyTo(ms);
        ms.Flush();
    }

    /// <summary>发送数据包</summary>
    /// <param name="buf"></param>
    public void SendPacket(Byte[] buf) => SendPacket((ArrayPacket)buf);

    /// <summary>重置。干掉历史残留数据</summary>
    public void Reset()
    {
        var ns = _stream;
        if (ns == null) return;

        // 干掉历史残留数据
        if (ns is NetworkStream { DataAvailable: true } nss)
        {
            var buf = Pool.Shared.Rent(1024);

            Int32 count;
            do
            {
                count = ns.Read(buf, 0, buf.Length);
            } while (count > 0 && nss.DataAvailable);

            Pool.Shared.Return(buf);
        }
    }
    #endregion

    #region 查询命令
    /// <summary>发送查询请求</summary>
    /// <param name="pk"></param>
    public void SendQuery(IPacket pk)
    {
        pk[0] = (Byte)DbCmd.QUERY;

        // 每一次查询请求，序列号都要重置为0
        _seq = 0;

        SendPacket(pk);
    }

    /// <summary>获取结果</summary>
    /// <param name="affectedRow"></param>
    /// <param name="insertedId"></param>
    /// <returns></returns>
    public Int32 GetResult(ref Int32 affectedRow, ref Int64 insertedId)
    {
        var rs = ReadPacket();
        var reader = rs.CreateReader(0);

        if (rs.IsOK)
        {
            reader.Advance(1);
            affectedRow = reader.ReadUInt16();
            var status = (ServerStatus)reader.ReadUInt16();
            var warning = reader.ReadUInt16();
            return 0;
        }
        else
        {
            // 读取列信息
            return reader.ReadLength();
        }
    }

    /// <summary>读取列信息</summary>
    /// <param name="count"></param>
    public MySqlColumn[] GetColumns(Int32 count)
    {
        var list = new List<MySqlColumn>(count);
        for (var i = 0; i < count; i++)
        {
            var rs = ReadPacket();
            if (rs.IsEOF) break;

            //var reader = rs.CreateReader(0);
            var reader = new SpanReader(rs.Data);

            var dc = new MySqlColumn
            {
                Catalog = reader.ReadString(),
                Database = reader.ReadString(),
                Table = reader.ReadString(),
                RealTable = reader.ReadString(),
                Name = reader.ReadString(),
                OriginalName = reader.ReadString(),
                Flag = reader.ReadByte(),
                Charset = reader.ReadInt16(),
                Length = reader.ReadInt32(),
                Type = (MySqlDbType)reader.ReadByte(),
                ColumnFlags = reader.ReadInt16(),
                Scale = reader.ReadByte()
            };

            //var ms = rs.Stream;
            //if (ms.Position + 2 < ms.Length) reader.ReadInt16();
            if (reader.FreeCapacity >= 2) reader.ReadInt16();

            list.Add(dc);
        }

        {
            var pk = ReadPacket();
            if (pk.IsEOF) { }
        }

        return list.ToArray();
    }

    /// <summary>读取下一行</summary>
    /// <param name="values"></param>
    /// <param name="columns"></param>
    /// <returns></returns>
    public Boolean NextRow(Object[] values, MySqlColumn[] columns)
    {
        var rs = ReadPacket();
        if (rs.IsEOF) return false;

        // 第一回读取长度
        var reader = rs.CreateReader(0);
        for (var i = 0; i < values.Length; i++)
        {
            var len = reader.ReadLength();
            if (len == -1)
            {
                values[i] = DBNull.Value;
                continue;
            }

            var buf = reader.ReadBytes(len);
            values[i] = columns[i].Type switch
            {
                MySqlDbType.Decimal or MySqlDbType.NewDecimal => Decimal.Parse(buf.ToStr()),
                MySqlDbType.Byte or MySqlDbType.Int16 or MySqlDbType.Int24 or MySqlDbType.Int32 or MySqlDbType.Int64 or MySqlDbType.UInt16 or MySqlDbType.UInt24 or MySqlDbType.UInt32 or MySqlDbType.UInt64 => Int64.Parse(buf.ToStr()),
                MySqlDbType.Float or MySqlDbType.Double => Double.Parse(buf.ToStr()),
                MySqlDbType.DateTime or MySqlDbType.Timestamp or MySqlDbType.Date or MySqlDbType.Time => buf.ToStr().ToDateTime(),
                MySqlDbType.VarChar or MySqlDbType.String or MySqlDbType.TinyText or MySqlDbType.MediumText or MySqlDbType.LongText or MySqlDbType.Text or MySqlDbType.VarString or MySqlDbType.Enum => buf.ToStr(),
                MySqlDbType.Blob or MySqlDbType.TinyBlob or MySqlDbType.MediumBlob or MySqlDbType.LongBlob => buf.ToStr(),
                MySqlDbType.Bit => buf[0],
                MySqlDbType.Json => buf.ToStr(),
                MySqlDbType.Guid => buf.ToStr(),
                _ => buf.ToArray(),
            };

            len = -1;
        }

        return true;
    }
    #endregion
}