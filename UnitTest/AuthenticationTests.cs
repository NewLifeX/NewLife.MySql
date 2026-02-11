using System.ComponentModel;
using System.Security.Cryptography;
using System.Text;
using NewLife;
using NewLife.MySql;
using NewLife.MySql.Messages;

namespace UnitTest;

/// <summary>Authentication 辅助方法单元测试</summary>
[Collection(TestCollections.InMemory)]
public class AuthenticationTests
{
    /// <summary>创建 Authentication 实例用于测试</summary>
    private static Authentication CreateAuth() => new(new SqlClient());

    [Fact]
    [DisplayName("Get411Password空密码返回单字节数组")]
    public void WhenEmptyPasswordThenReturnsSingleByteArray()
    {
        var auth = CreateAuth();
        var seed = new Byte[20];

        var result = auth.Get411Password("", seed);

        Assert.NotNull(result);
        Assert.Single(result);
    }

    [Fact]
    [DisplayName("Get411Password正常密码返回20字节哈希")]
    public void WhenNormalPasswordThenReturns20ByteHash()
    {
        var auth = CreateAuth();
        var seed = new Byte[20];
        for (var i = 0; i < seed.Length; i++) seed[i] = (Byte)(i + 1);

        var result = auth.Get411Password("root", seed);

        Assert.NotNull(result);
        Assert.Equal(20, result.Length);
    }

    [Fact]
    [DisplayName("Get411Password相同输入产生相同结果")]
    public void WhenSameInputThenSameOutput()
    {
        var auth = CreateAuth();
        var seed = new Byte[20];
        for (var i = 0; i < seed.Length; i++) seed[i] = (Byte)(i + 10);

        var result1 = auth.Get411Password("testpass", seed);
        var result2 = auth.Get411Password("testpass", seed);

        Assert.Equal(result1, result2);
    }

    [Fact]
    [DisplayName("Get411Password不同密码产生不同结果")]
    public void WhenDifferentPasswordThenDifferentOutput()
    {
        var auth = CreateAuth();
        var seed = new Byte[20];
        for (var i = 0; i < seed.Length; i++) seed[i] = (Byte)i;

        var result1 = auth.Get411Password("password1", seed);
        var result2 = auth.Get411Password("password2", seed);

        Assert.NotEqual(result1, result2);
    }

    [Fact]
    [DisplayName("Get411Password不同seed产生不同结果")]
    public void WhenDifferentSeedThenDifferentOutput()
    {
        var auth = CreateAuth();
        var seed1 = new Byte[20];
        var seed2 = new Byte[20];
        for (var i = 0; i < 20; i++)
        {
            seed1[i] = (Byte)i;
            seed2[i] = (Byte)(i + 100);
        }

        var result1 = auth.Get411Password("root", seed1);
        var result2 = auth.Get411Password("root", seed2);

        Assert.NotEqual(result1, result2);
    }

    [Fact]
    [DisplayName("GetSha256Password空密码返回单元素数组")]
    public void WhenEmptySha256PasswordThenReturnsSingleElementArray()
    {
        var auth = CreateAuth();
        var seed = new Byte[20];

        var result = auth.GetSha256Password("", seed);

        Assert.NotNull(result);
        Assert.Single(result);
        Assert.Equal(1, result[0]);
    }

    [Fact]
    [DisplayName("GetSha256Password正常密码返回32字节哈希")]
    public void WhenNormalSha256PasswordThenReturns32ByteHash()
    {
        var auth = CreateAuth();
        var seed = new Byte[20];
        for (var i = 0; i < seed.Length; i++) seed[i] = (Byte)(i + 1);

        var result = auth.GetSha256Password("root", seed);

        Assert.NotNull(result);
        Assert.Equal(32, result.Length);
    }

    [Fact]
    [DisplayName("GetXor基本异或正确性")]
    public void WhenXorThenCorrectResult()
    {
        var auth = CreateAuth();
        var src = new Byte[] { 0x41, 0x42, 0x43 }; // "ABC"
        var pattern = new Byte[] { 0xFF };

        var result = auth.GetXor(src, pattern);

        // src2 = [0x41, 0x42, 0x43, 0x00]，与 0xFF 异或
        Assert.Equal(4, result.Length);
        Assert.Equal((Byte)(0x41 ^ 0xFF), result[0]);
        Assert.Equal((Byte)(0x42 ^ 0xFF), result[1]);
        Assert.Equal((Byte)(0x43 ^ 0xFF), result[2]);
        Assert.Equal((Byte)(0x00 ^ 0xFF), result[3]); // 追加的0字节
    }

    [Fact]
    [DisplayName("GetXor结果比源长1字节")]
    public void WhenXorThenResultIsOneByteLonger()
    {
        var auth = CreateAuth();
        var src = new Byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 };
        var pattern = new Byte[] { 0xAA, 0xBB };

        var result = auth.GetXor(src, pattern);

        // 结果 = src.Length + 1（追加 0 字节后异或）
        Assert.Equal(6, result.Length);
    }

    [Fact]
    [DisplayName("GetXor循环使用pattern")]
    public void WhenXorPatternShorterThenCycles()
    {
        var auth = CreateAuth();
        var src = new Byte[] { 0x10, 0x20, 0x30, 0x40 };
        var pattern = new Byte[] { 0xAA, 0xBB };

        var result = auth.GetXor(src, pattern);

        // pattern 循环: AA BB AA BB AA
        Assert.Equal((Byte)(0x10 ^ 0xAA), result[0]);
        Assert.Equal((Byte)(0x20 ^ 0xBB), result[1]);
        Assert.Equal((Byte)(0x30 ^ 0xAA), result[2]);
        Assert.Equal((Byte)(0x40 ^ 0xBB), result[3]);
        Assert.Equal((Byte)(0x00 ^ 0xAA), result[4]); // 追加0
    }

    [Fact]
    [DisplayName("GetConnectAttrs返回非空字符串")]
    public void WhenGetConnectAttrsThenReturnsNonEmpty()
    {
        var auth = CreateAuth();

        var result = auth.GetConnectAttrs();

        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    [Fact]
    [DisplayName("GetConnectAttrs多次调用返回相同结果")]
    public void WhenGetConnectAttrsCalledTwiceThenSameResult()
    {
        var auth = CreateAuth();

        var result1 = auth.GetConnectAttrs();
        var result2 = auth.GetConnectAttrs();

        Assert.Same(result1, result2);
    }

    [Fact]
    [DisplayName("GetFlags总是包含基础标志")]
    public void WhenGetFlagsThenIncludesBaseFlags()
    {
        var auth = CreateAuth();
        var caps = ClientFlags.LONG_FLAG | ClientFlags.SECURE_CONNECTION | ClientFlags.PLUGIN_AUTH;

        var result = auth.GetFlags(caps);

        Assert.True(result.HasFlag(ClientFlags.FOUND_ROWS));
        Assert.True(result.HasFlag(ClientFlags.PROTOCOL_41));
        Assert.True(result.HasFlag(ClientFlags.TRANSACTIONS));
        Assert.True(result.HasFlag(ClientFlags.MULTI_STATEMENTS));
        Assert.True(result.HasFlag(ClientFlags.MULTI_RESULTS));
        Assert.True(result.HasFlag(ClientFlags.LONG_PASSWORD));
        Assert.True(result.HasFlag(ClientFlags.CONNECT_WITH_DB));
    }

    [Fact]
    [DisplayName("GetFlags条件标志仅在服务器支持时启用")]
    public void WhenCapsMissingThenConditionalFlagsNotSet()
    {
        var auth = CreateAuth();
        ClientFlags caps = 0;

        var result = auth.GetFlags(caps);

        Assert.False(result.HasFlag(ClientFlags.LONG_FLAG));
        Assert.False(result.HasFlag(ClientFlags.SECURE_CONNECTION));
        Assert.False(result.HasFlag(ClientFlags.PLUGIN_AUTH));
        Assert.False(result.HasFlag(ClientFlags.PS_MULTI_RESULTS));
        Assert.False(result.HasFlag(ClientFlags.CONNECT_ATTRS));
    }

    [Fact]
    [DisplayName("GetFlags条件标志在服务器支持时启用")]
    public void WhenCapsProvidedThenConditionalFlagsSet()
    {
        var auth = CreateAuth();
        var caps = ClientFlags.LONG_FLAG | ClientFlags.SECURE_CONNECTION | ClientFlags.PS_MULTI_RESULTS
            | ClientFlags.PLUGIN_AUTH | ClientFlags.CONNECT_ATTRS | ClientFlags.CAN_HANDLE_EXPIRED_PASSWORD
            | ClientFlags.CLIENT_QUERY_ATTRIBUTES | ClientFlags.MULTI_FACTOR_AUTHENTICATION;

        var result = auth.GetFlags(caps);

        Assert.True(result.HasFlag(ClientFlags.LONG_FLAG));
        Assert.True(result.HasFlag(ClientFlags.SECURE_CONNECTION));
        Assert.True(result.HasFlag(ClientFlags.PS_MULTI_RESULTS));
        Assert.True(result.HasFlag(ClientFlags.PLUGIN_AUTH));
        Assert.True(result.HasFlag(ClientFlags.CONNECT_ATTRS));
        Assert.True(result.HasFlag(ClientFlags.CAN_HANDLE_EXPIRED_PASSWORD));
        Assert.True(result.HasFlag(ClientFlags.CLIENT_QUERY_ATTRIBUTES));
        Assert.True(result.HasFlag(ClientFlags.MULTI_FACTOR_AUTHENTICATION));
    }
}
