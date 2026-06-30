using System.Text;

namespace NewLife.MySql.Common;

/// <summary>WebAuthn / FIDO2 认证处理器。处理 authentication_webauthn_client 插件流程</summary>
/// <remarks>
/// MySQL 8.2+ 支持 WebAuthn 多因素认证。客户端收到 auth switch 到 authentication_webauthn_client 后，
/// 解析服务器发送的 CBOR 编码的 PublicKeyCredentialRequestOptions，调用平台认证器签名，返回 CBOR 编码的断言响应。
/// </remarks>
public static class WebAuthnHandler
{
    /// <summary>从 CBOR 编码的数据中提取 challenge 和 rpId</summary>
    /// <param name="cborData">服务器发送的 CBOR 数据</param>
    /// <returns>解析后的 WebAuthn 请求参数</returns>
    public static WebAuthnRequest ParseRequest(Byte[] cborData)
    {
        var request = new WebAuthnRequest();
        var reader = new CborReader(cborData);
        if (!reader.ReadMap(out var mapSize)) return request;

        for (var i = 0; i < mapSize; i++)
        {
            if (!reader.ReadInt32(out var key)) break;

            switch (key)
            {
                case 1: // rpId
                    if (reader.ReadString(out var rpId))
                        request.RpId = rpId;
                    break;
                case 2: // challenge
                    if (reader.ReadBytes(out var challenge))
                        request.Challenge = challenge;
                    break;
                case 3: // timeout
                    if (reader.ReadInt32(out var timeout))
                        request.Timeout = timeout;
                    break;
                case 4: // allowCredentials
                    reader.SkipValue();
                    break;
                case 5: // userVerification
                    if (reader.ReadString(out var uv))
                        request.UserVerification = uv;
                    break;
                default:
                    reader.SkipValue();
                    break;
            }
        }

        return request;
    }

    /// <summary>构造 WebAuthn 断言响应的 CBOR 编码</summary>
    /// <param name="response">认证器响应数据</param>
    /// <returns>CBOR 编码的 AuthenticatorAssertionResponse</returns>
    public static Byte[] BuildAssertionResponse(WebAuthnAssertion response)
    {
        var writer = new CborWriter();

        // { "authenticatorData": h'...', "clientDataJSON": "...", "signature": h'...' }
        writer.WriteMap(3);

        writer.WriteString("authenticatorData");
        writer.WriteBytes(response.AuthenticatorData ?? []);

        writer.WriteString("clientDataJSON");
        writer.WriteString(response.ClientDataJson ?? "{}");

        writer.WriteString("signature");
        writer.WriteBytes(response.Signature ?? []);

        return writer.ToArray();
    }

    /// <summary>构造 clientDataJSON 的 UTF-8 字节数组</summary>
    /// <param name="challengeBase64Url">Base64URL 编码的 challenge</param>
    /// <param name="rpId">依赖方 ID（服务器主机名）</param>
    /// <returns>JSON 格式的 clientData 字节数组</returns>
    public static Byte[] BuildClientDataJson(String challengeBase64Url, String rpId)
    {
        var json = $$"""{"type":"webauthn.get","challenge":"{{challengeBase64Url}}","origin":"https://{{rpId}}","crossOrigin":false}""";
        return Encoding.UTF8.GetBytes(json);
    }

    /// <summary>对数据进行 Base64URL 编码（无填充）</summary>
    public static String Base64UrlEncode(Byte[] data)
    {
        return Convert.ToBase64String(data)
            .Replace('+', '-')
            .Replace('/', '_')
            .TrimEnd('=');
    }

    /// <summary>对 Base64URL 编码的数据进行解码</summary>
    public static Byte[] Base64UrlDecode(String base64Url)
    {
        var s = base64Url.Replace('-', '+').Replace('_', '/');
        switch (s.Length % 4)
        {
            case 2: s += "=="; break;
            case 3: s += "="; break;
        }
        return Convert.FromBase64String(s);
    }
}

/// <summary>WebAuthn 请求参数</summary>
public class WebAuthnRequest
{
    /// <summary>依赖方 ID</summary>
    public String? RpId { get; set; }

    /// <summary>挑战字节</summary>
    public Byte[]? Challenge { get; set; }

    /// <summary>超时时间（毫秒）</summary>
    public Int32 Timeout { get; set; }

    /// <summary>用户验证要求</summary>
    public String? UserVerification { get; set; }
}

/// <summary>WebAuthn 断言响应数据</summary>
public class WebAuthnAssertion
{
    /// <summary>认证器数据（由平台认证器生成）</summary>
    public Byte[]? AuthenticatorData { get; set; }

    /// <summary>客户端数据 JSON（UTF-8 编码）</summary>
    public String? ClientDataJson { get; set; }

    /// <summary>签名（由平台认证器生成）</summary>
    public Byte[]? Signature { get; set; }
}

/// <summary>最小化的 CBOR 读取器，仅支持 WebAuthn 所需的数据类型</summary>
class CborReader
{
    private readonly Byte[] _data;
    private Int32 _pos;

    public CborReader(Byte[] data)
    {
        _data = data;
        _pos = 0;
    }

    /// <summary>读取 Map 头，返回元素个数</summary>
    public Boolean ReadMap(out Int32 count)
    {
        if (_pos >= _data.Length) { count = 0; return false; }
        var major = (_data[_pos] >> 5) & 0x07;
        if (major != 5) { count = 0; return false; }
        count = (Int32)ReadArgument();
        return true;
    }

    /// <summary>读取整数值（键）</summary>
    public Boolean ReadInt32(out Int32 value)
    {
        if (_pos >= _data.Length) { value = 0; return false; }
        var major = (_data[_pos] >> 5) & 0x07;
        if (major != 0 && major != 1) { value = 0; return false; }
        value = (Int32)ReadArgument();
        // 负整数
        if (major == 1) value = -1 - value;
        return true;
    }

    /// <summary>读取字符串值</summary>
    public Boolean ReadString(out String value)
    {
        if (_pos >= _data.Length) { value = ""; return false; }
        var major = (_data[_pos] >> 5) & 0x07;
        if (major != 3) { value = ""; return false; }
        var len = (Int32)ReadArgument();
        if (_pos + len > _data.Length) { value = ""; return false; }
        value = Encoding.UTF8.GetString(_data, _pos, len);
        _pos += len;
        return true;
    }

    /// <summary>读取字节数组值</summary>
    public Boolean ReadBytes(out Byte[] value)
    {
        if (_pos >= _data.Length) { value = []; return false; }
        var major = (_data[_pos] >> 5) & 0x07;
        if (major != 2) { value = []; return false; }
        var len = (Int32)ReadArgument();
        if (_pos + len > _data.Length) { value = []; return false; }
        value = new Byte[len];
        Array.Copy(_data, _pos, value, 0, len);
        _pos += len;
        return true;
    }

    /// <summary>跳过当前值</summary>
    public void SkipValue()
    {
        if (_pos >= _data.Length) return;
        var major = (_data[_pos] >> 5) & 0x07;
        var arg = ReadArgument();
        switch (major)
        {
            case 2: // 字节串
            case 3: // 文本串
                _pos += (Int32)arg;
                break;
            case 4: // 数组
            case 5: // Map
                // 已包含子元素个数，跳过子元素需递归
                for (var i = 0; i < (Int32)arg; i++)
                {
                    SkipValue();
                    if (major == 5) SkipValue(); // Map 每项有 key + value
                }
                break;
            // 其他简单类型无需额外处理
        }
    }

    private UInt64 ReadArgument()
    {
        if (_pos >= _data.Length) return 0;
        var additional = _data[_pos] & 0x1F;
        _pos++;

        return additional switch
        {
            <= 23 => (UInt64)additional,
            24 => ReadByte(),
            25 => ReadUInt16(),
            26 => ReadUInt32(),
            27 => ReadUInt64(),
            _ => 0u,
        };
    }

    private UInt64 ReadByte() => _pos < _data.Length ? _data[_pos++] : 0u;
    private UInt64 ReadUInt16()
    {
        if (_pos + 2 > _data.Length) { _pos = _data.Length; return 0; }
        var v = (UInt64)(_data[_pos] << 8 | _data[_pos + 1]);
        _pos += 2;
        return v;
    }
    private UInt64 ReadUInt32()
    {
        if (_pos + 4 > _data.Length) { _pos = _data.Length; return 0; }
        var v = (UInt64)(_data[_pos] << 24 | _data[_pos + 1] << 16 | _data[_pos + 2] << 8 | _data[_pos + 3]);
        _pos += 4;
        return v;
    }
    private UInt64 ReadUInt64()
    {
        if (_pos + 8 > _data.Length) { _pos = _data.Length; return 0; }
        var v = (UInt64)_data[_pos] << 56 | (UInt64)_data[_pos + 1] << 48 | (UInt64)_data[_pos + 2] << 40 | (UInt64)_data[_pos + 3] << 32
              | (UInt64)_data[_pos + 4] << 24 | (UInt64)_data[_pos + 5] << 16 | (UInt64)_data[_pos + 6] << 8 | _data[_pos + 7];
        _pos += 8;
        return v;
    }
}

/// <summary>最小化的 CBOR 写入器，仅支持 WebAuthn 所需的数据类型</summary>
class CborWriter
{
    private readonly MemoryStream _stream = new();

    public void WriteMap(Int32 count)
    {
        WriteMajorType(5, (UInt64)count);
    }

    public void WriteString(String value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteMajorType(3, (UInt64)bytes.Length);
        _stream.Write(bytes, 0, bytes.Length);
    }

    public void WriteBytes(Byte[] value)
    {
        WriteMajorType(2, (UInt64)value.Length);
        _stream.Write(value, 0, value.Length);
    }

    private void WriteMajorType(Int32 major, UInt64 argument)
    {
        if (argument <= 23)
            _stream.WriteByte((Byte)((major << 5) | (Int32)argument));
        else if (argument <= 0xFF)
            _stream.Write([(Byte)((major << 5) | 24), (Byte)argument], 0, 2);
        else if (argument <= 0xFFFF)
        {
            _stream.WriteByte((Byte)((major << 5) | 25));
            _stream.WriteByte((Byte)(argument >> 8));
            _stream.WriteByte((Byte)argument);
        }
        else if (argument <= 0xFFFFFFFF)
        {
            _stream.WriteByte((Byte)((major << 5) | 26));
            _stream.WriteByte((Byte)(argument >> 24));
            _stream.WriteByte((Byte)(argument >> 16));
            _stream.WriteByte((Byte)(argument >> 8));
            _stream.WriteByte((Byte)argument);
        }
        else
        {
            _stream.WriteByte((Byte)((major << 5) | 27));
            _stream.WriteByte((Byte)(argument >> 56));
            _stream.WriteByte((Byte)(argument >> 48));
            _stream.WriteByte((Byte)(argument >> 40));
            _stream.WriteByte((Byte)(argument >> 32));
            _stream.WriteByte((Byte)(argument >> 24));
            _stream.WriteByte((Byte)(argument >> 16));
            _stream.WriteByte((Byte)(argument >> 8));
            _stream.WriteByte((Byte)argument);
        }
    }

    public Byte[] ToArray() => _stream.ToArray();
}
