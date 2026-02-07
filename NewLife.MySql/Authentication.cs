using System.ComponentModel;
using System.Security.Cryptography;
using System.Text;
using NewLife.Buffers;
using NewLife.Data;
using NewLife.MySql.Common;
using NewLife.MySql.Messages;
using NewLife.Security;

namespace NewLife.MySql;

class Authentication(SqlClient client)
{
    /// <summary>异步验证</summary>
    /// <param name="welcome">欢迎消息</param>
    /// <param name="reset">是否重置</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns></returns>
    public async Task AuthenticateAsync(WelcomeMessage welcome, Boolean reset, CancellationToken cancellationToken)
    {
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
        await client.SendPacketAsync(pk2, cancellationToken).ConfigureAwait(false);

        // 读取响应
        var rs = await client.ReadPacketAsync(cancellationToken).ConfigureAwait(false);

        // 如果返回0xFE，表示需要继续验证。例如 caching_sha2_password 验证降级为 mysql_native_password 验证
        if (rs.IsEOF)
            await ToNativePasswordAsync(rs, set.Password!, cancellationToken).ConfigureAwait(false);
        else if (rs.Data[0] == 0x01 && rs.Data[1] == 0x04)
            await PerformFullAuthenticationAsync(set.Password!, seed, cancellationToken).ConfigureAwait(false);
    }

    private async Task ToNativePasswordAsync(Response rs, String password, CancellationToken cancellationToken)
    {
        var reader = new SpanReader(rs.Data.Slice(1));
        var authMethod = reader.ReadZeroString();
        var authData = reader.ReadZero();
        if (authMethod == "mysql_native_password")
        {
            var pass = Get411Password(password, authData.ToArray());
            await client.SendPacketAsync((ArrayPacket)pass, cancellationToken).ConfigureAwait(false);

            var rs2 = await client.ReadPacketAsync(cancellationToken).ConfigureAwait(false);
            if (!rs2.IsOK)
                throw new InvalidOperationException("验证失败");
        }
        else
            throw new NotSupportedException(authMethod);
    }

    private async Task PerformFullAuthenticationAsync(String password, Byte[] seedBytes, CancellationToken cancellationToken)
    {
        // request_public_key
        var buf = new Byte[] { 0x02 };
        await client.SendPacketAsync((ArrayPacket)buf, cancellationToken).ConfigureAwait(false);

        // 读取响应
        var rs = await client.ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        var reader = new SpanReader(rs.Data);
        if (reader.ReadByte() != 0x01) return;

        var key = reader.ReadZeroString();

        // 混淆密码
        var obfuscated = GetXor(Encoding.Default.GetBytes(password), seedBytes);

        // 使用公钥加密密码
        var encryptedPassword = RSAHelper.Encrypt(obfuscated, key);

        // 发送加密后的密码
        await client.SendPacketAsync((ArrayPacket)encryptedPassword, cancellationToken).ConfigureAwait(false);

        // 读取响应
        var rs2 = await client.ReadPacketAsync(cancellationToken).ConfigureAwait(false);
        if (!rs2.IsOK)
            throw new InvalidOperationException("验证失败");
    }

    #region 辅助
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

    protected Byte[] GetXor(Byte[] src, Byte[] pattern)
    {
        var src2 = new Byte[src.Length + 1];
        Array.Copy(src, 0, src2, 0, src.Length);
        src2[src.Length] = 0;
        var result = new Byte[src2.Length];
        for (var i = 0; i < src2.Length; i++)
        {
            result[i] = (Byte)(src2[i] ^ (pattern[i % pattern.Length]));
        }
        return result;
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

    private String? _atts;
    internal String GetConnectAttrs()
    {
        if (_atts != null) return _atts;

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

        return _atts = sb.ToString();
    }
    #endregion
}