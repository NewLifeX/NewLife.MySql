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
}
