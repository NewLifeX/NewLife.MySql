using System.Net.Sockets;
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
    public Int64 MaxPacketSize { get; private set; } = 1024;

    /// <summary>服务器特性</summary>
    public ClientFlags Capability { get; set; }

    /// <summary>服务器变量</summary>
    public IDictionary<String, String> Variables { get; private set; } = new Dictionary<String, String>();

    private Stream? _stream;
    /// <summary>基础数据流</summary>
    public Stream? BaseStream { get => _stream; set => _stream = value; }

    /// <summary>欢迎信息</summary>
    public WelcomeMessage? Welcome { get; set; }

    private TcpClient? _Client;
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

        _stream = client.GetStream();

        // 从欢迎信息读取服务器特性
        var welcome = GetWelcome();
        Welcome = welcome;
        Capability = welcome.Capability;

        // 验证
        var auth = new Authentication(this);
        auth.Authenticate(welcome, false);
    }

    /// <summary>关闭</summary>
    public void Close()
    {
        _Client.TryDispose();
        _Client = null;
        _stream = null;
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
    private WelcomeMessage GetWelcome()
    {
        // 读取数据包
        var pk = ReadPacket();

        var msg = new WelcomeMessage();
        msg.Read(pk.AsSpan());

        // 验证方法
        var method = msg.AuthMethod;
        if (!method.IsNullOrEmpty() && !method.EqualIgnoreCase("mysql_native_password", "caching_sha2_password"))
            throw new NotSupportedException("不支持验证方式 " + method);

        return msg;
    }

    /// <summary>加载服务器变量</summary>
    /// <returns></returns>
    private IDictionary<String, String> LoadVariables(MySqlConnection conn)
    {
        var dic = new Dictionary<String, String>();
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
    public Packet ReadPacket()
    {
        var ms = _stream ?? throw new InvalidOperationException("未打开连接");

        // 3字节长度 + 1字节序列号
        var buf = ms.ReadBytes(4);
        var len = buf[0] + (buf[1] << 8) + (buf[2] << 16);
        _seq = buf[3];

        buf = ms.ReadBytes(len);
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
        else if (pk[0] == 0xFE)
        {
            var reader = new BinaryReader(pk.GetStream());

            var warnings = reader.ReadUInt16();
            var status = reader.ReadUInt16();

            pk = null;
        }

        return pk;
    }

    /// <summary>发送数据包</summary>
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

    /// <summary>读取OK</summary>
    public Packet ReadOK()
    {
        var pk = ReadPacket();
        var reader = new BinaryReader(pk.GetStream());

        // 影响行数、最后插入ID
        reader.ReadFieldLength();
        reader.ReadFieldLength();

        return pk;
    }

    /// <summary>读取EOF</summary>
    public void ReadEOF()
    {
        var pk = ReadPacket();
        //if (pk[0] == 254)
        //{
        //    var reader = new BinaryReader(pk.GetStream());

        //    var warnings = reader.ReadUInt16();
        //    var status = reader.ReadUInt16();
        //}
    }

    /// <summary>发送查询请求</summary>
    /// <param name="pk"></param>
    public void SendQuery(Packet pk)
    {
        pk[0] = (Byte)DbCmd.QUERY;

        _seq = 0;
        SendPacket(pk);
    }

    /// <summary>获取结果</summary>
    /// <param name="affectedRow"></param>
    /// <param name="insertedId"></param>
    /// <returns></returns>
    public Int32 GetResult(ref Int32 affectedRow, ref Int64 insertedId)
    {
        var pk = ReadPacket();
        var reader = new BinaryReader(pk.GetStream());

        // 读取列信息
        var fieldCount = (Int32)reader.ReadFieldLength();

        return fieldCount;
    }

    /// <summary>读取列信息</summary>
    /// <param name="names"></param>
    /// <param name="types"></param>
    public void GetColumns(String[] names, MySqlDbType[] types)
    {
        for (var i = 0; i < names.Length; i++)
        {
            var pk = ReadPacket();
            var ms = pk.GetStream();
            var reader = new BinaryReader(ms);

            var catelog = reader.ReadString();
            var database = reader.ReadString();
            var table = reader.ReadString();
            var realtable = reader.ReadString();

            names[i] = reader.ReadString();

            var oriName = reader.ReadString();
            var b = reader.ReadByte();
            var charSet = reader.ReadInt16();
            var length = reader.ReadInt32();

            types[i] = (MySqlDbType)reader.ReadByte();

            var colFlags = reader.ReadInt16();
            var scale = reader.ReadByte();

            if (ms.Position + 2 < ms.Length) reader.ReadInt16();
        }

        ReadEOF();
    }

    /// <summary>读取下一行</summary>
    /// <param name="values"></param>
    /// <param name="types"></param>
    /// <returns></returns>
    public Boolean NextRow(Object[] values, MySqlDbType[] types)
    {
        var pk = ReadPacket();
        if (pk == null) return false;

        var ms = pk.GetStream();
        var reader = new BinaryReader(ms);
        for (var i = 0; i < values.Length; i++)
        {
            var len = (Int32)reader.ReadFieldLength();
            if (len == -1)
            {
                values[i] = DBNull.Value;
                continue;
            }

            var p = ms.Position;
            var buf = reader.ReadBytes(len);
            //values[i] = buf;

            values[i] = types[i] switch
            {
                MySqlDbType.Decimal or MySqlDbType.NewDecimal => Decimal.Parse(buf.ToStr()),
                MySqlDbType.Byte or MySqlDbType.Int16 or MySqlDbType.Int32 or MySqlDbType.Int64 or MySqlDbType.UInt16 or MySqlDbType.UInt32 or MySqlDbType.UInt64 => Int64.Parse(buf.ToStr()),
                MySqlDbType.Float or MySqlDbType.Double => Double.Parse(buf.ToStr()),
                MySqlDbType.DateTime => buf.ToStr().ToDateTime(),
                MySqlDbType.VarChar or MySqlDbType.String or MySqlDbType.TinyText or MySqlDbType.MediumText or MySqlDbType.LongText or MySqlDbType.Text or MySqlDbType.VarString or MySqlDbType.Enum => buf.ToStr(),
                MySqlDbType.Blob => buf.ToStr(),
                _ => buf,
            };

            //ms.Position = p + len;
        }

        //ReadEOF();

        return true;
    }
    #endregion
}