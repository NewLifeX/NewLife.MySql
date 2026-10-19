using System.ComponentModel;
using System.Text;
using System.Text.Json;
using NewLife.MySql.Common;

namespace UnitTest;

/// <summary>WebAuthn 认证处理器测试。覆盖 CBOR 请求解析、断言响应构造、clientDataJSON 与 Base64URL 编解码</summary>
/// <remarks>对应待确认项 SYS-3：平台认证器需真实硬件，此处覆盖驱动侧全部可测逻辑（CBOR 协议编解码）</remarks>
[Collection(TestCollections.InMemory)]
public class WebAuthnHandlerTests
{
    #region 请求解析
    [Fact]
    [DisplayName("ParseRequest解析标准CBOR请求的全部字段")]
    public void ParseRequest_StandardRequest_ExtractsFields()
    {
        var cbor = BuildStandardRequest();
        var request = WebAuthnHandler.ParseRequest(cbor);

        Assert.Equal("example.com", request.RpId);
        Assert.Equal([1, 2, 3, 4, 5, 6], request.Challenge);
        Assert.Equal(60000, request.Timeout);
        Assert.Equal("required", request.UserVerification);
    }

    [Fact]
    [DisplayName("ParseRequest对空数据与非CBOR数据安全返回")]
    public void ParseRequest_InvalidData_ReturnsWithoutThrow()
    {
        // 空数据
        var r1 = WebAuthnHandler.ParseRequest([]);
        Assert.Null(r1.RpId);
        Assert.Null(r1.Challenge);

        // 非 Map（0x00 = 整数 0）
        var r2 = WebAuthnHandler.ParseRequest([0x00, 0x01, 0x02]);
        Assert.Null(r2.RpId);

        // 截断：map(3) 后仅有部分内容
        var r3 = WebAuthnHandler.ParseRequest([0xA3, 0x01, 0x6B, 0x65]);
        Assert.Null(r3.RpId);
    }

    [Fact]
    [DisplayName("ParseRequest跳过未知键与allowCredentials")]
    public void ParseRequest_SkipsUnknownAndNestedValues()
    {
        // map(3)：
        //   6 → 数组(2)：["abc", h'AA']（未知键 + 嵌套，验证 SkipValue）
        //   1 → "rpid"
        //   2 → h'AABB'
        var b = new List<Byte> { 0xA3 };
        b.Add(0x06);
        b.Add(0x82); // array(2)
        b.Add(0x63); // text(3)
        b.AddRange("abc"u8.ToArray());
        b.Add(0x41); // bytes(1)
        b.Add(0xAA);
        b.Add(0x01);
        b.Add(0x64); // text(4)
        b.AddRange("rpid"u8.ToArray());
        b.Add(0x02);
        b.Add(0x42); // bytes(2)
        b.AddRange([0xAA, 0xBB]);

        var request = WebAuthnHandler.ParseRequest([.. b]);

        Assert.Equal("rpid", request.RpId);
        Assert.Equal([0xAA, 0xBB], request.Challenge);
    }
    #endregion

    #region 断言响应构造
    [Fact]
    [DisplayName("BuildAssertionResponse输出合法CBOR三键结构")]
    public void BuildAssertionResponse_EncodesThreeKeys()
    {
        var data = new Byte[] { 0x01, 0x02, 0x03, 0x04 };
        var signature = new Byte[] { 0x10, 0x20, 0x30, 0x40 };

        var cbor = WebAuthnHandler.BuildAssertionResponse(new WebAuthnAssertion
        {
            AuthenticatorData = data,
            ClientDataJson = """{"type":"webauthn.get"}""",
            Signature = signature,
        });

        var (count, values) = DecodeMap(cbor);
        Assert.Equal(3, count);
        Assert.Equal(data, values["authenticatorData"]);
        Assert.Equal("""{"type":"webauthn.get"}""", values["clientDataJSON"]);
        Assert.Equal(signature, values["signature"]);
    }

    [Fact]
    [DisplayName("BuildAssertionResponse对空值字段使用默认值")]
    public void BuildAssertionResponse_NullFields_UseDefaults()
    {
        var cbor = WebAuthnHandler.BuildAssertionResponse(new WebAuthnAssertion());

        var (_, values) = DecodeMap(cbor);
        Assert.Empty((Byte[])values["authenticatorData"]);
        Assert.Empty((Byte[])values["signature"]);
        Assert.Equal("{}", values["clientDataJSON"]);
    }
    #endregion

    #region clientDataJSON 与 Base64URL
    [Fact]
    [DisplayName("BuildClientDataJson生成含type/challenge/origin的合法JSON")]
    public void BuildClientDataJson_ValidJson()
    {
        var json = Encoding.UTF8.GetString(WebAuthnHandler.BuildClientDataJson("AQID", "example.com"));

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal("webauthn.get", root.GetProperty("type").GetString());
        Assert.Equal("AQID", root.GetProperty("challenge").GetString());
        Assert.Equal("https://example.com", root.GetProperty("origin").GetString());
        Assert.False(root.GetProperty("crossOrigin").GetBoolean());
    }

    [Fact]
    [DisplayName("Base64URL编解码往返一致且不含转义字符")]
    public void Base64Url_RoundTrip()
    {
        // 含 + / = 的 Base64 字符，验证替换为 URL 安全字符且无填充
        var raw = new Byte[] { 0xFA, 0xFF, 0xBE, 0xEF, 0x01 };

        var encoded = WebAuthnHandler.Base64UrlEncode(raw);
        Assert.DoesNotContain('+', encoded);
        Assert.DoesNotContain('/', encoded);
        Assert.DoesNotContain('=', encoded);

        Assert.Equal(raw, WebAuthnHandler.Base64UrlDecode(encoded));
    }

    [Fact]
    [DisplayName("Base64URL解码兼容标准与无填充输入")]
    public void Base64Url_DecodeCompatible()
    {
        // 无填充（长度非4倍数，需补全）
        Assert.Equal([1, 2, 3], WebAuthnHandler.Base64UrlDecode("AQID"));
        // URL 安全字符替换
        Assert.Equal([0xFB, 0xFF], WebAuthnHandler.Base64UrlDecode("-_8"));
    }
    #endregion

    #region 辅助
    /// <summary>构造标准 CBOR 请求：map(5) = {1:rpId, 2:challenge, 3:timeout, 4:allowCredentials, 5:userVerification}</summary>
    /// <returns>CBOR 字节数组</returns>
    private static Byte[] BuildStandardRequest()
    {
        var b = new List<Byte> { 0xA5 };

        b.Add(0x01);
        b.Add(0x6B); // text(11)
        b.AddRange("example.com"u8.ToArray());

        b.Add(0x02);
        b.Add(0x46); // bytes(6)
        b.AddRange([1, 2, 3, 4, 5, 6]);

        b.Add(0x03);
        b.Add(0x19); // uint16
        b.AddRange([0xEA, 0x60]); // 60000

        // allowCredentials: 数组(1) → map(2) → {"type":"public-key","id":h'0909'}
        b.Add(0x04);
        b.Add(0x81);
        b.Add(0xA2);
        b.Add(0x64);
        b.AddRange("type"u8.ToArray());
        b.Add(0x6A);
        b.AddRange("public-key"u8.ToArray());
        b.Add(0x62);
        b.AddRange("id"u8.ToArray());
        b.Add(0x42);
        b.AddRange([0x09, 0x09]);

        b.Add(0x05);
        b.Add(0x68); // text(8)
        b.AddRange("required"u8.ToArray());

        return [.. b];
    }

    /// <summary>解码 CborWriter 产物（map ≤ 23 项、键值长度 ≤ 23 的简化场景）</summary>
    /// <param name="data">CBOR 字节数组</param>
    /// <returns>条目数与键值字典</returns>
    private static (Int32 Count, Dictionary<String, Object> Values) DecodeMap(Byte[] data)
    {
        var pos = 0;
        Assert.Equal(5, (data[pos] >> 5) & 0x07); // map
        var count = data[pos] & 0x1F;
        pos++;

        var values = new Dictionary<String, Object>();
        for (var i = 0; i < count; i++)
        {
            Assert.Equal(3, (data[pos] >> 5) & 0x07); // text 键
            var klen = data[pos] & 0x1F;
            pos++;
            var key = Encoding.UTF8.GetString(data, pos, klen);
            pos += klen;

            var major = (data[pos] >> 5) & 0x07;
            var len = data[pos] & 0x1F;
            pos++;
            if (major == 2)
            {
                values[key] = data[pos..(pos + len)];
            }
            else
            {
                Assert.Equal(3, major);
                values[key] = Encoding.UTF8.GetString(data, pos, len);
            }
            pos += len;
        }

        Assert.Equal(data.Length, pos);
        return (count, values);
    }
    #endregion
}
