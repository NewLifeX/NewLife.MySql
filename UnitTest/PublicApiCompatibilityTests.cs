using System.Reflection;
using NewLife.Data;
using NewLife.MySql;
using NewLife.MySql.Common;

namespace UnitTest;

/// <summary>公共 API 二进制兼容性测试。旧版已编译调用方依赖这些精确签名，新增快速路径只能以内部重载或新方法扩展</summary>
public class PublicApiCompatibilityTests
{
    [Fact(DisplayName = "SqlClient 保留旧版公共方法签名")]
    public void SqlClientKeepsLegacySignatures()
    {
        AssertLegacy(typeof(SqlClient), "ReadPacketAsync", typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "SendPacketAsync", typeof(IPacket), typeof(CancellationToken), typeof(Boolean));
        AssertLegacy(typeof(SqlClient), "SendCommandAsync", typeof(DbCmd), typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "SendCommandAsync", typeof(DbCmd), typeof(Int32), typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "SendCommandAsync", typeof(DbCmd), typeof(String), typeof(Boolean), typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "SendQueryAsync", typeof(IPacket), typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "GetColumnsAsync", typeof(Int32), typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "NextRowAsync", typeof(Object[]), typeof(MySqlColumn[]), typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "NextBinaryRowAsync", typeof(Object[]), typeof(MySqlColumn[]), typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "SkipRowAsync", typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "PrepareStatementAsync", typeof(String), typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "ExecuteStatementAsync", typeof(Int32), typeof(MySqlParameterCollection), typeof(MySqlColumn[]), typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "ExecuteStatementAsync", typeof(Int32), typeof(Object[]), typeof(MySqlColumn[]), typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "CloseStatementAsync", typeof(Int32), typeof(CancellationToken));
        AssertLegacy(typeof(SqlClient), "ResetStatementAsync", typeof(Int32), typeof(CancellationToken));
    }

    [Fact(DisplayName = "MySqlCommand 保留旧版公共方法签名")]
    public void MySqlCommandKeepsLegacySignatures()
    {
        AssertLegacy(typeof(MySqlCommand), "UnprepareAsync", typeof(CancellationToken));
    }

    private static void AssertLegacy(Type type, String name, params Type[] parameterTypes)
    {
        var method = type.GetMethod(name, BindingFlags.Public | BindingFlags.Instance, null, parameterTypes, null);
        Assert.True(method != null, $"{type.Name}.{name}({String.Join(", ", parameterTypes.Select(e => e.Name))}) 缺失，破坏二进制兼容");
    }
}
