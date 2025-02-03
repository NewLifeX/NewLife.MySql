using System.ComponentModel;
using System.Security.Cryptography;
using System.Text;
using NewLife.Buffers;
using NewLife.Data;
using NewLife.MySql.Common;
using NewLife.MySql.Messages;

namespace NewLife.MySql;

class Authentication(SqlClient client)
{
    private SqlClient _client = client;

    public void Authenticate(WelcomeMessage welcome, Boolean reset)
    {
        var client = _client;
        var set = client.Setting;

        // 从共享池申请内存，跳过4字节头部，便于SendPacket内部填充帧头
        using var pk = new OwnerPacket(1024);
        var writer = new SpanWriter(pk);
        writer.Advance(4);

        // 设置连接标识
        var flags2 = GetFlags(welcome.Capability);
        writer.Write((UInt32)flags2);
        writer.Write(0xFF_FFFF); // MaxPacket
        writer.Write((Byte)33); // UTF-8
        writer.Write(new Byte[23]);

        var method = welcome.AuthMethod!;

        // 写入用户名密码
        var seed = welcome.Seed!;
        writer.WriteZeroString(set.UserID!);
        var pass = method.Contains("sha2") ? GetSha256Password(set.Password!, seed) : Get411Password(set.Password!, seed);
        writer.WriteByte(pass.Length);
        writer.Write(pass);

        // 写入数据库
        var db = set.Database;
        if (!db.IsNullOrEmpty()) writer.WriteZeroString(db);

        if (reset) writer.Write((UInt16)8);

        //writer.WriteZeroString("mysql_native_password");
        writer.WriteZeroString(method);

        // 连接属性
        var attrs = GetConnectAttrs().GetBytes();
        writer.WriteLength(attrs.Length);
        writer.Write(attrs);

        var pk2 = pk.Slice(4, writer.Position - 4);

        // 发送验证
        client.SendPacket(pk2);

        // 读取响应
        var rs = client.ReadPacket();

        //// sha256
        //if (method.Contains("sha2"))
        //{
        //    ContinueAuthentication([1]);
        //}

        // 如果返回0xFE，表示需要继续验证。例如 caching_sha2_password 验证升级为 mysql_native_password 验证
        if (rs[0] == 0xFE) rs = ContinueAuthentication(rs.Slice(1), set.Password!);
    }

    private IPacket ContinueAuthentication(IPacket rs, String password)
    {
        var reader = new SpanReader(rs);

        var authMethod = reader.ReadZeroString();
        var authData = reader.ReadZero();
        if (authMethod == "mysql_native_password")
        {
            var pass = Get411Password(password, authData[..^1].ToArray());
            _client.SendPacket(pass);

            return client.ReadPacket();
        }
        else
            throw new NotSupportedException(authMethod);
    }

    protected Byte[] Get411Password(String password, Byte[] seed)
    {
        if (password.Length == 0) return new Byte[1];

        //return password.GetBytes().SHA1(seed);
        var sha = SHA1.Create();

        var firstHash = sha.ComputeHash(password.GetBytes());
        var secondHash = sha.ComputeHash(firstHash);

        var input = new Byte[seed.Length + secondHash.Length];
        Array.Copy(seed, 0, input, 0, seed.Length);
        Array.Copy(secondHash, 0, input, seed.Length, secondHash.Length);
        var thirdHash = sha.ComputeHash(input);

        var buf = thirdHash;
        for (var i = 0; i < buf.Length; i++)
            buf[i] = (Byte)(buf[i] ^ firstHash[i]);

        return buf;
    }

    protected Byte[] GetSha256Password(String password, Byte[] seed)
    {
        if (password.IsNullOrEmpty()) return [1];

        return password.GetBytes().SHA256(seed);
    }

    private ClientFlags GetFlags(ClientFlags caps)
    {
        ClientFlags flags = 0;

        //// 从本地文件加载数据
        //flags |= ClientFlags.LOCAL_FILES;

        // UseAffectedRows
        flags |= ClientFlags.FOUND_ROWS;
        flags |= ClientFlags.PROTOCOL_41;
        flags |= ClientFlags.TRANSACTIONS;
        flags |= ClientFlags.MULTI_STATEMENTS;
        flags |= ClientFlags.MULTI_RESULTS;
        if ((caps & ClientFlags.LONG_FLAG) != 0)
            flags |= ClientFlags.LONG_FLAG;

        flags |= ClientFlags.LONG_PASSWORD;

        flags |= ClientFlags.CONNECT_WITH_DB;

        if ((caps & ClientFlags.SECURE_CONNECTION) != 0)
            flags |= ClientFlags.SECURE_CONNECTION;

        if ((caps & ClientFlags.PS_MULTI_RESULTS) != 0)
            flags |= ClientFlags.PS_MULTI_RESULTS;

        if ((caps & ClientFlags.PLUGIN_AUTH) != 0)
            flags |= ClientFlags.PLUGIN_AUTH;

        if ((caps & ClientFlags.CONNECT_ATTRS) != 0)
            flags |= ClientFlags.CONNECT_ATTRS;

        if ((caps & ClientFlags.CAN_HANDLE_EXPIRED_PASSWORD) != 0)
            flags |= ClientFlags.CAN_HANDLE_EXPIRED_PASSWORD;

        // 支持发送会话跟踪变量
        flags |= ClientFlags.CLIENT_SESSION_TRACK;

        // 查询属性
        if ((caps & ClientFlags.CLIENT_QUERY_ATTRIBUTES) != 0)
            flags |= ClientFlags.CLIENT_QUERY_ATTRIBUTES;

        // MFA
        if ((caps & ClientFlags.MULTI_FACTOR_AUTHENTICATION) != 0)
            flags |= ClientFlags.MULTI_FACTOR_AUTHENTICATION;

        return flags;
    }

    internal String GetConnectAttrs()
    {
        var sb = new StringBuilder();
        var att = new ConnectAttributes();
        foreach (var pi in att.GetType().GetProperties())
        {
            var name = pi.Name;
            var dis = pi.GetCustomAttributes(typeof(DisplayNameAttribute), false);
            if (dis.Length > 0)
                name = (dis[0] as DisplayNameAttribute)!.DisplayName;

            var value = (String)pi.GetValue(att, null);
            sb.AppendFormat("{0}{1}", (Char)name.Length, name);
            sb.AppendFormat("{0}{1}", (Char)Encoding.UTF8.GetByteCount(value), value);
        }

        return sb.ToString();
    }
}