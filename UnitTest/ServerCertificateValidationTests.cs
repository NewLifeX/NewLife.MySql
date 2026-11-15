using System.ComponentModel;
using System.Net.Security;
using NewLife.MySql;

namespace UnitTest;

/// <summary>服务端证书校验策略测试。VerifyFull/VerifyCA/VerifyIdentity 必须拒绝自签证书与主机名不匹配，
/// Required/Preferred 沿用不校验的历史行为</summary>
[Collection(TestCollections.InMemory)]
public class ServerCertificateValidationTests
{
    [Fact]
    [DisplayName("要求校验时放行合法证书")]
    public void VerifyMode_AcceptsValidCertificate()
    {
        Assert.True(SqlClient.AcceptServerCertificate(true, SslPolicyErrors.None));
    }

    [Fact]
    [DisplayName("要求校验时拒绝证书链错误")]
    public void VerifyMode_RejectsChainError()
    {
        Assert.False(SqlClient.AcceptServerCertificate(true, SslPolicyErrors.RemoteCertificateChainErrors));
    }

    [Fact]
    [DisplayName("要求校验时拒绝主机名不匹配")]
    public void VerifyMode_RejectsNameMismatch()
    {
        Assert.False(SqlClient.AcceptServerCertificate(true, SslPolicyErrors.RemoteCertificateNameMismatch));
    }

    [Fact]
    [DisplayName("不要求校验时保持历史行为放行任意证书")]
    public void NonVerifyMode_AcceptsAnyCertificate()
    {
        var errors = SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch;
        Assert.True(SqlClient.AcceptServerCertificate(false, errors));
    }

    [Theory]
    [DisplayName("受支持的SslMode取值")]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("None")]
    [InlineData("Disabled")]
    [InlineData("Preferred")]
    [InlineData("Required")]
    [InlineData("VerifyCA")]
    [InlineData("VerifyFull")]
    [InlineData("VerifyIdentity")]
    public void SupportedSslModes(String mode) => Assert.True(SqlClient.IsSupportedSslMode(mode));

    [Theory]
    [DisplayName("拼错的SslMode被拒绝，避免静默降级明文")]
    [InlineData("Require")]
    [InlineData("VerfiyFull")]
    [InlineData("ssl")]
    public void UnsupportedSslMode_Throws(String mode)
    {
        Assert.False(SqlClient.IsSupportedSslMode(mode));

        using var conn = new MySqlConnection($"Server=127.0.0.1;Port=3306;User Id=root;Password=root;SslMode={mode}");
        Assert.Throws<ArgumentException>(() => conn.Open());
    }

    [Theory]
    [DisplayName("非UTF8字符集建连前被拒绝")]
    [InlineData("gbk")]
    [InlineData("gb2312")]
    [InlineData("latin1")]
    public void NonUtf8CharSet_Throws(String charSet)
    {
        using var conn = new MySqlConnection($"Server=127.0.0.1;Port=3306;User Id=root;Password=root;CharSet={charSet}");
        Assert.Throws<NotSupportedException>(() => conn.Open());
    }
}
